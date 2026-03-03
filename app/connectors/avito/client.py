# app/connectors/avito/client.py
import logging
import httpx
import datetime
import asyncio
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import os
from app.utils.redis_lock import acquire_lock, release_lock, DistributedSemaphore 
from app.db.models import Account
from app.core.rabbitmq import mq
from app.utils.redis_lock import acquire_lock, release_lock
from app.utils.redis_lock import get_redis_client

logger = logging.getLogger("avito.client")
AVITO_CONCURRENCY_LIMIT = int(os.getenv("AVITO_CONCURRENCY_LIMIT", 5))
class AvitoClient:
    def __init__(self):
        self.base_url = "https://api.avito.ru"
        self.token_url = f"{self.base_url}/token"
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def close(self):
        if self._http_client and not self._http_client.is_closed:
            await self._http_client.aclose()
    
    async def _send_alert(self, text: str):
        try: await mq.publish("tg_alerts", {"type": "system", "text": text})
        except: logger.error("Не удалось отправить алерт")

    # --- АВТОРИЗАЦИЯ С БЛОКИРОВКОЙ ---
    
    async def get_token(self, account: Account, db: AsyncSession) -> str:
        auth_data = account.auth_data or {}
        now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
        
        if auth_data.get("access_token") and auth_data.get("expires_at", 0) > (now_ts + 300):
            return auth_data["access_token"]
        
        lock_key = f"token_lock:{account.id}"
        if not await acquire_lock(lock_key, timeout=20):
            logger.info(f"⏳ Обновление токена для {account.id} уже в процессе. Ожидание...")
            await asyncio.sleep(3)
            await db.refresh(account)
            return await self.get_token(account, db)

        try:
            logger.info(f"🔑 Обновление токена для аккаунта {account.name} (ID: {account.id})")
            
            client_id = auth_data.get("client_id")
            client_secret = auth_data.get("client_secret")
            if not client_id or not client_secret:
                raise ValueError("В базе данных не найдены credentials (ID/Secret)")

            payload = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            }
            
            resp = await self.http_client.post(self.token_url, data=payload)
            resp.raise_for_status()
            token_data = resp.json()

            new_auth = dict(auth_data)
            new_auth.update({
                "access_token": token_data["access_token"],
                "expires_at": now_ts + token_data["expires_in"]
            })
            
            account.auth_data = new_auth
            await db.commit()
            
            logger.info(f"✅ Токен успешно получен для аккаунта {account.id}")
            return token_data["access_token"]
        except Exception as e:
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА АВТОРИЗАЦИИ Avito для аккаунта {account.name}: {e}"
            logger.error(error_msg, exc_info=True)
            await self._send_alert(error_msg)
            raise
        finally:
            await release_lock(lock_key)

    # --- УНИВЕРСАЛЬНЫЙ ЗАПРОС С РЕТРАЯМИ ---

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10),
        retry=retry_if_exception_type(httpx.HTTPError),
        reraise=True
    )
    async def _request(self, method: str, path: str, account: Account, db: AsyncSession, **kwargs) -> Any:
        url = f"{self.base_url}{path}"
        token = await self.get_token(account, db)
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        
        try:
            # === НАЧАЛО ИЗМЕНЕНИЙ ===
            # Ограничиваем количество одновременных запросов ко всему API Avito
            async with DistributedSemaphore(name="avito_api_global", limit=AVITO_CONCURRENCY_LIMIT):
                resp = await self.http_client.request(method, url, headers=headers, **kwargs)
            # === КОНЕЦ ИЗМЕНЕНИЙ ===
            
            
            if resp.status_code == 401:
                logger.warning(f"⚠️ 401 Unauthorized для {account.id}. Сброс токена и повтор...")
                auth = dict(account.auth_data)
                auth["expires_at"] = 0
                account.auth_data = auth
                await db.commit()
                
                token = await self.get_token(account, db)
                headers["Authorization"] = f"Bearer {token}"
                async with DistributedSemaphore(name="avito_api_global", limit=AVITO_CONCURRENCY_LIMIT):
                    resp = await self.http_client.request(method, url, headers=headers, **kwargs)

            resp.raise_for_status()
            return resp.json()
            
        except httpx.HTTPStatusError as e:
            error_msg = f"❌ API Error {e.response.status_code} на {url}: {e.response.text}"
            logger.error(error_msg)
            # Если это последняя попытка ретрая, шлем алерт
            if getattr(e.request, 'extensions', {}).get('retry_attempt') == 3:
                await self._send_alert(error_msg)
            raise

    # --- ВЕБХУКИ (MESSENGER API V3) ---

    async def check_and_register_webhooks(self, account: Account, db: AsyncSession, target_url: str):
        if not account.auth_data.get("user_id"):
            try:
                me = await self._request("GET", "/core/v1/accounts/self", account, db)
                auth = dict(account.auth_data)
                auth["user_id"] = str(me["id"])
                account.auth_data = auth
                await db.commit()
                logger.info(f"👤 Получен UserID для аккаунта {account.id}: {me['id']}")
            except Exception as e:
                logger.error(f"Не удалось получить self info для {account.id}: {e}")

        try:
            subs_data = await self._request("POST", "/messenger/v1/subscriptions", account, db)
            subscriptions = subs_data.get("subscriptions", [])
            
            is_active = any(s.get("url") == target_url for s in subscriptions)
            
            if not is_active:
                logger.info(f"🔌 Регистрация Messenger Webhook для {account.id} -> {target_url}")
                await self._request(
                    "POST", "/messenger/v3/webhook", account, db, json={"url": target_url}
                )
                logger.info(f"✅ Вебхук успешно привязан")
            else:
                logger.debug(f"👌 Вебхук для аккаунта {account.id} уже настроен")
        except Exception as e:
            error_msg = f"❌ Ошибка регистрации вебхука для {account.name}: {e}"
            logger.error(error_msg)
            await self._send_alert(error_msg)

    # --- ПОЛЛИНГ ОТКЛИКОВ (JOB API V1) ---

    async def get_new_applications(self, account: Account, db: AsyncSession) -> List[Dict]:
        redis = get_redis_client()
        cursor_key = f"avito_cursor:{account.id}"
        
        # 1. Пытаемся взять курсор из Redis
        cursor = await redis.get(cursor_key)
        
        # Точка отсчета — 24 часа назад (в формате ISO или как требует API)
        # Для updatedAtFrom Авито обычно ждет YYYY-MM-DD
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        yesterday_str = (now_utc - datetime.timedelta(hours=24)).strftime("%Y-%m-%d")
        
        params = {
            "limit": 100
        }

        # Если курсора нет — берем за последние сутки
        if cursor:
            params["cursor"] = cursor
        else:
            params["updatedAtFrom"] = yesterday_str

        try:
            # 2. Получаем список ID (используем ключ 'applies' из доки)
            resp_ids = await self._request("GET", "/job/v1/applications/get_ids", account, db, params=params)
            
            app_list = resp_ids.get("applies", []) # Исправлено с 'applications' на 'applies'
            if not app_list:
                return []

            # 3. Собираем только те ID, которые не старше 24 часов (на случай, если API вернуло лишнее)
            # Примечание: В ответе get_ids обычно нет даты, только ID. 
            # Поэтому фильтрацию по времени лучше сделать после получения деталей.
            application_ids = [str(item["id"]) for item in app_list]

            # 4. Получаем детали откликов
            details_resp = await self._request(
                "POST", "/job/v1/applications/get_by_ids", account, db, json={"ids": application_ids}
            )
            
            full_applications = details_resp.get("applications", [])

            # 5. Фильтруем результат строго по времени (updated_at >= now - 24h)
            # Авито возвращает timestamps в секундах или ISO строки в зависимости от версии
            cutoff_timestamp = (now_utc - datetime.timedelta(hours=24)).timestamp()
            
            filtered_apps = []
            for app in full_applications:
                # В Job API обычно поле updatedAt в секундах
                app_updated = app.get("updated_at", 0)
                if app_updated >= cutoff_timestamp:
                    filtered_apps.append(app)

            # 6. Сохраняем новый курсор в Redis (ставим TTL 2 дня, чтобы не копились вечно)
            new_cursor = resp_ids.get("cursor")
            if new_cursor:
                await redis.set(cursor_key, new_cursor, ex=172800) # 48 часов

            return filtered_apps

        except Exception as e:
            error_msg = f"❌ Ошибка полинга откликов (Account {account.id}): {e}"
            logger.error(error_msg)
            # Важно: не бросаем исключение выше, чтобы один упавший аккаунт не вешал весь цикл
            return []

    # --- ОТПРАВКА СООБЩЕНИЙ (MESSENGER API V1) ---

    async def send_message(self, account: Account, db: AsyncSession, chat_id: str, text: str, user_id: str = "me"):
        # Если в базе нет user_id, используем "me"
        avito_user_id = account.auth_data.get("user_id", user_id)
        path = f"/messenger/v1/accounts/{avito_user_id}/chats/{chat_id}/messages"
        payload = {"message": {"text": text}, "type": "text"}
        return await self._request("POST", path, account, db, json=payload)

    async def get_vacancy_details(self, account: Account, db: AsyncSession, vacancy_id: int) -> Dict:
        path = f"/job/v2/vacancies/{vacancy_id}"
        params = {"fields": "title,description,addressDetails"}
        return await self._request("GET", path, account, db, params=params)

    async def get_chat_context(self, account: Account, db: AsyncSession, chat_id: str) -> Dict:
        user_id = account.auth_data.get("user_id")
        path = f"/messenger/v2/accounts/{user_id}/chats/{chat_id}"
        return await self._request("GET", path, account, db)
    
    async def get_chat_messages(self, user_id: str, chat_id: str, account: Account, db: AsyncSession, limit: int = 20):
        path = f"/messenger/v3/accounts/{user_id}/chats/{chat_id}/messages"
        data = await self._request("GET", path, account, db, params={"limit": limit})
        return data.get("messages", [])

    async def get_job_details(self, vacancy_id: str, account: Account, db: AsyncSession):
        path = "/job/v2/vacancies/batch"
        # 1. Убираем ограничение по fields или добавляем все нужные, включая 'params' и 'salary'
        payload = {
            "ids": [int(vacancy_id)],
            "fields": ["title", "description", "addressDetails", "salary", "url"],
            "params": [
                "address", "experience", "schedule", "employment", 
                "payout_frequency", "age_preferences", "bonuses"
            ]
        }
        
        data = await self._request("POST", path, account, db, json=payload)
        
        if not data or len(data) == 0:
            raise ValueError("Vacancy not found")
            
        vac = data[0]
        
        # 2. Формируем расширенный текст со всеми деталями
        full_description_text = self._format_vacancy_full_text(vac)

        from dataclasses import dataclass
        @dataclass
        class VacDTO:
            title: str
            description: str # Здесь теперь будет "красивый" полный текст
            city: str
            raw_json: dict   # Добавим на всякий случай и сырые данные

        return VacDTO(
            title=vac.get("title", "Без названия"),
            description=full_description_text,
            city=vac.get("addressDetails", {}).get("city", "Не указан"),
            raw_json=vac 
        )

    def _format_vacancy_full_text(self, vac: dict) -> str:
        """Вспомогательный метод для превращения JSON Авито в читаемый текст с заголовками"""
        lines = []
        
        # Заголовок и ссылка
        lines.append(f"📋 ВАКАНСИЯ: {vac.get('title')}")
        if vac.get('url'):
            lines.append(f"🔗 Ссылка: https://www.avito.ru{vac.get('url')}")
        lines.append("")

        # Зарплата
        salary = vac.get('salary')
        if isinstance(salary, (int, float)):
            lines.append(f"💰 Зарплата: {salary} руб.")
        elif isinstance(salary, dict):
            lines.append(f"💰 Зарплата: от {salary.get('from')} до {salary.get('to')} руб.")
        
        # Локация
        addr = vac.get('addressDetails', {})
        lines.append(f"📍 Адрес: {addr.get('city', '')}, {addr.get('address', '')}")
        lines.append("")

        # Характеристики (params)
        params = vac.get('params', {})
        if params:
            lines.append("🏗 УСЛОВИЯ И ТРЕБОВАНИЯ:")
            mapping = {
                "experience": "Опыт",
                "schedule": "График",
                "employment": "Занятость",
                "payout_frequency": "Выплаты",
                "age_preferences": "Предпочтения",
                "bonuses": "Бонусы"
            }
            for key, label in mapping.items():
                val = params.get(key)
                if val:
                    if isinstance(val, list): val = ", ".join(map(str, val))
                    lines.append(f"  • {label}: {val}")
            lines.append("")

        # Основное описание
        lines.append("📝 ОПИСАНИЕ:")
        lines.append(vac.get('description', 'Нет описания'))
        
        return "\n".join(lines)
    
    async def search_resumes(self, account: Account, db: AsyncSession, params: dict) -> dict:
        """
        Поиск резюме по параметрам (GET /job/v1/resumes/)
        """
        path = "/job/v1/resumes/"
        return await self._request("GET", path, account, db, params=params)
        
        return await self._request("GET", path, account, db, params=params)
    async def get_resume_details(self, account: Account, db: AsyncSession, resume_id: str) -> Dict:
        """
        Запрос к /job/v2/resumes/{resume_id}
        """
        path = f"/job/v2/resumes/{resume_id}"
        # Запрашиваем все поля для максимального профиля
        return await self._request("GET", path, account, db)
    async def search_cvs(self, account: Account, db: AsyncSession, params: Dict[str, Any]) -> Dict[str, Any]:
        """Поиск резюме по параметрам"""
        path = "/job/v1/resumes/"
        return await self._request("GET", path, account, db, params=params)

    async def get_resume_contacts(self, account: Account, db: AsyncSession, resume_id: str) -> Dict[str, Any]:
        """Получение контактов резюме, включая chat_id"""
        path = f"/job/v1/resumes/{resume_id}/contacts/"
        return await self._request("GET", path, account, db)
    
    async def delete_message(self, account: Account, db: AsyncSession, chat_id: str, message_id: str):
        """
        Удаляет сообщение в чате Авито. 
        Внимание: удалять можно только свои сообщения и не позднее 1 часа с момента отправки.
        """
        user_id = account.auth_data.get("user_id")
        path = f"/messenger/v1/accounts/{user_id}/chats/{chat_id}/messages/{message_id}"
        
        try:
            # Метод POST, тело пустое
            return await self._request("POST", path, account, db, json={})
        except Exception as e:
            # Не бросаем ошибку, так как сообщения старше часа просто не удалятся
            logger.warning(f"⚠️ Не удалось удалить сообщение {message_id}: {e}")
            return None
        
    # --- ДОБАВИТЬ В AvitoClient ---
    
    async def get_item_details(self, item_id: str, account: Account, db: AsyncSession):
        """
        Получение данных через Core API (Resources).
        Работает для обычных объявлений (Услуги, Товары).
        """
        # Используем путь, который сработал в curl
        path = "/core/v1/items"
        params = {"ids": str(item_id)}
        
        try:
            # Делаем запрос
            data = await self._request("GET", path, account, db, params=params)
            
            # В этом методе данные лежат в resources
            resources = data.get("resources", [])
            if not resources:
                raise ValueError(f"Объявление {item_id} не найдено в API")
            
            item = resources[0] # Берем первое из списка

            # Чистим город из адреса (Ставропольский край, Ставрополь... -> Ставрополь)
            full_address = item.get("address", "")
            city = "Не указан"
            if full_address:
                parts = [p.strip() for p in full_address.split(",")]
                # Обычно город — это второй элемент после края, либо первый
                city = parts[1] if len(parts) > 1 else parts[0]

            from dataclasses import dataclass
            @dataclass
            class ItemDTO:
                title: str
                description: str
                city: str
                raw_json: dict

            # Формируем описание из того, что есть (название + цена)
            price = item.get("price", "Не указана")
            description = (
                f"📦 ОБЪЯВЛЕНИЕ: {item.get('title')}\n"
                f"💰 Цена: {price} руб.\n"
                f"📍 Адрес: {full_address}\n"
                f"🔗 Ссылка: {item.get('url')}\n\n"
                f"⚠️ Описание не получено через API, будет добавлено вручную."
            )

            return ItemDTO(
                title=item.get("title", "Объявление"),
                description=description,
                city=city,
                raw_json=item
            )
        except Exception as e:
            logger.error(f"❌ Ошибка Core API для item {item_id}: {e}")
            raise e

avito = AvitoClient()