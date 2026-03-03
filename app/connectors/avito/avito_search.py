import logging
import datetime
from sqlalchemy import select, and_, update
from app.db.session import AsyncSessionLocal
from app.db.models import (
    Account, 
    JobContext, 
    Candidate, 
    AvitoSearchQuota, 
    AvitoSearchStat
)
from app.connectors.avito.client import avito
from app.core.rabbitmq import mq
from app.utils.redis_lock import get_redis_client

logger = logging.getLogger("AvitoSearch")

class AvitoSearchService:
    # Маппинг твоих вакансий на ID специализаций Авито
    SPECIALIZATION_MAP = {
        "повар": "10173",             # Туризм, рестораны
        "горничная": "10173,16844",    # Туризм + Домашний персонал
        "кухонный работник": "10173",  # Туризм, рестораны
        "технический работник": "10184,10175", # ЖКХ + Без опыта
    }

    async def discover_and_propose(self):
        async with AsyncSessionLocal() as db:
            # 1. Получаем все активные аккаунты Авито
            stmt = select(Account).filter_by(platform="avito", is_active=True)
            accounts = (await db.execute(stmt)).scalars().all()

            for acc in accounts:
                # 2. ПЕРВИЧНАЯ ПРОВЕРКА КВОТЫ (чтобы не запрашивать список вакансий зря)
                quota = await db.scalar(select(AvitoSearchQuota).filter_by(account_id=acc.id))
                if not quota or quota.remaining_limits <= 0:
                    logger.warning(f"⚠️ У аккаунта {acc.name} (ID: {acc.id}) закончились лимиты.")
                    continue

                # 3. Получаем только активные вакансии из БД
                vac_stmt = select(JobContext).filter_by(account_id=acc.id, is_active=True)
                vacancies = (await db.execute(vac_stmt)).scalars().all()

                for vac in vacancies:
                    # 4. ПРОВЕРКА: Активна ли вакансия на самом Авито?
                    is_still_active = await self._check_avito_vacancy_status(acc, vac, db)
                    
                    if is_still_active:
                        # Запускаем поиск кандидатов
                        await self._search_for_vacancy(acc, vac, db)
                    else:
                        logger.info(f"🚫 Вакансия {vac.external_id} ({vac.title}) закрыта на Авито. Пропускаем.")
            
            # Финальный коммит всех изменений статусов вакансий
            await db.commit()

    async def _check_avito_vacancy_status(self, account, vacancy, db) -> bool:
        """Запрашивает статус вакансии через Job API и обновляет БД"""
        try:
            vac_data = await avito.get_job_details(vacancy.external_id, account, db)
            status_from_api = vac_data.raw_json.get("is_active", False)
            
            if not status_from_api:
                vacancy.is_active = False
                logger.warning(f"📉 Вакансия {vacancy.title} деактивирована на стороне Авито.")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки статуса вакансии {vacancy.external_id}: {e}")
            return True

    async def _get_location_id(self, account, db, city_name: str) -> int:
        """Получает числовой ID локации Авито по названию"""
        if not city_name: return None
        try:
            resp = await avito._request("GET", "/core/v1/locations", account, db, params={"q": city_name})
            return resp[0].get("id") if resp else None
        except:
            return None

    async def _update_daily_stats(self, db, account_id, vacancy_id):
        """Ведет учет потраченных лимитов по дням и вакансиям"""
        today = datetime.date.today()
        stat_stmt = select(AvitoSearchStat).filter(
            and_(
                AvitoSearchStat.account_id == account_id,
                AvitoSearchStat.vacancy_id == vacancy_id,
                AvitoSearchStat.date == today
            )
        )
        stat = await db.scalar(stat_stmt)

        if stat:
            stat.spent_count += 1
        else:
            new_stat = AvitoSearchStat(
                account_id=account_id,
                vacancy_id=vacancy_id,
                date=today,
                spent_count=1
            )
            db.add(new_stat)

    async def _search_for_vacancy(self, account, vacancy, db):
        try:
            redis = get_redis_client()
            cursor_key = f"avito_search_cursor:{account.id}:{vacancy.id}"
            last_cursor = await redis.get(cursor_key)

            location_id = await self._get_location_id(account, db, vacancy.city)
            spec_id = self.SPECIALIZATION_MAP.get(vacancy.title.lower(), "")
            
            search_params = {
                "query": vacancy.title,
                "age_min": 30, 
                "age_max": 55, 
                "per_page": 20,
                "fields": "location,address_details,nationality,age"
            }
            if last_cursor: search_params["cursor"] = last_cursor
            if spec_id: search_params["specialization"] = spec_id
            if location_id: search_params["location"] = location_id

            # Выполняем поиск резюме
            results = await avito.search_cvs(account, db, search_params)
            new_cursor = results.get("meta", {}).get("cursor")
            resumes = results.get("resumes", [])

            if not resumes:
                logger.info(f"Ничего не найдено для '{vacancy.title}', сброс курсора.")
                await redis.delete(cursor_key)
                return

            if new_cursor:
                await redis.set(cursor_key, str(new_cursor), ex=604800)

            opened_count = 0
            MAX_OPEN_PER_VACANCY = 5

            for cv in resumes:
                if opened_count >= MAX_OPEN_PER_VACANCY:
                    break

                resume_id = str(cv.get("id"))

                # 1. ПРОВЕРКА: Есть ли кандидат уже в базе? (Бесплатно)
                cand_stmt = select(Candidate).filter_by(platform_user_id=resume_id)
                exists = await db.scalar(cand_stmt)
                if exists:
                    continue

                # 2. БЕЗОПАСНОЕ СПИСАНИЕ КВОТЫ (АТОМАРНО)
                # Уменьшаем лимит только если он больше 0
                quota_stmt = (
                    update(AvitoSearchQuota)
                    .where(and_(
                        AvitoSearchQuota.account_id == account.id,
                        AvitoSearchQuota.remaining_limits > 0
                    ))
                    .values(remaining_limits=AvitoSearchQuota.remaining_limits - 1)
                    .returning(AvitoSearchQuota.remaining_limits)
                )
                
                res = await db.execute(quota_stmt)
                new_limit = res.scalar()

                if new_limit is None:
                    logger.warning(f"🛑 Лимиты аккаунта {account.id} исчерпаны во время прогона.")
                    break

                try:
                    # 3. ПЛАТНОЕ ПОЛУЧЕНИЕ КОНТАКТОВ В API АВИТО
                    logger.info(f"💰 [SAFE SPEND] Открываем контакты {resume_id}. Осталось лимитов: {new_limit}")
                    contacts_data = await avito.get_resume_contacts(account, db, resume_id)
                    
                    contacts_list = contacts_data.get("contacts", [])
                    chat_id = next((c["value"] for c in contacts_list if c["type"] == "chat_id"), None)
                    
                    if not chat_id:
                        # Если нет чата, это фиаско, но лимит Авито уже списал за открытие телефона
                        logger.warning(f"У резюме {resume_id} нет chat_id (только телефон?).")
                        # Продолжаем, лимит не возвращаем, так как Авито его уже "съел" за просмотр телефона
                        continue

                    # 4. ОБНОВЛЕНИЕ СТАТИСТИКИ
                    await self._update_daily_stats(db, account.id, vacancy.id)

                    # 5. ОТПРАВКА В ОЧЕРЕДЬ
                    await mq.publish("avito_inbound", {
                        "source": "avito_search_found",
                        "account_id": account.id,
                        "avito_user_id": account.auth_data.get("user_id"),
                        "payload": {
                            "search_full_name": contacts_data.get("name"),
                            "search_phone": next((c["value"] for c in contacts_list if c["type"] == "phone"), None),
                            "cv_data": cv
                        },
                        "chat_id": chat_id,
                        "resume_id": resume_id,
                        "vacancy_id": vacancy.id
                    })
                    
                    opened_count += 1
                    # Сохраняем списание и стату немедленно
                    await db.commit() 

                except Exception as api_err:
                    # 6. ВОЗВРАТ ЛИМИТА, если запрос к API Авито упал (например, таймаут)
                    logger.error(f"❌ Ошибка API Авито для {resume_id}, возвращаем лимит: {api_err}")
                    await db.execute(
                        update(AvitoSearchQuota)
                        .where(AvitoSearchQuota.account_id == account.id)
                        .values(remaining_limits=AvitoSearchQuota.remaining_limits + 1)
                    )
                    await db.commit()
                    continue

        except Exception as e:
            logger.error(f"Ошибка в процессе поиска по вакансии {vacancy.id}: {e}")

avito_search_service = AvitoSearchService()