# scheduler.py
import asyncio
import logging
import datetime
import signal
from sqlalchemy import select, and_
from app.db.models import Dialogue, AnalyticsEvent
from zoneinfo import ZoneInfo
from app.connectors.avito.avito_search import avito_search_service
from app.core.config import settings
from app.core.rabbitmq import mq
from app.db.session import AsyncSessionLocal, engine
from app.db.models import Dialogue, InterviewReminder
from app.services.knowledge_base import kb_service
from sqlalchemy.orm import selectinload

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Scheduler")

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

class Scheduler:
    def __init__(self):
        self.is_running = True

    async def start(self):
        logger.info("🚀 Планировщик задач запущен")
        await mq.connect()

        # Запускаем параллельные циклы
        await asyncio.gather(
            self._loop_silence_reminders(),      # Напоминания молчунам
            self._loop_interview_reminders(),    # Напоминания перед собесом
            self._loop_kb_refresh(),             # Обновление промпта (раз в 3 мин)
            self._loop_candidate_search()        # Активный поиск кандидатов
        )

    async def stop(self):
        logger.info("🛑 Остановка планировщика...")
        self.is_running = False

    
    # --- 1. ЛОГИКА МОЛЧУНОВ ---
    async def _loop_silence_reminders(self):
        """Проверка кандидатов, которые замолчали (с учетом часовых поясов и тихого часа)"""
        
        
        while self.is_running:
            try:
                # 1. Проверка глобального включения
                if not settings.reminders.silence.enabled:
                    await asyncio.sleep(60)
                    continue

                qt_cfg = settings.reminders.silence.quiet_time

                async with AsyncSessionLocal() as db:
                    now_utc = datetime.datetime.now(datetime.timezone.utc)
                    
                    # Загружаем диалоги + кандидатов (чтобы достать timezone из профиля)
                    stmt = (
                        select(Dialogue)
                        .options(selectinload(Dialogue.candidate))
                        .where(
                            and_(
                                Dialogue.status.in_(['in_progress', 'timed_out', 'new']),
                                Dialogue.reminder_level < len(settings.reminders.silence.levels)
                            )
                        )
                    )
                    result = await db.execute(stmt)
                    dialogues = result.scalars().all()

                    for dialogue in dialogues:
                        # --- А. ОПРЕДЕЛЯЕМ ЧАСОВОЙ ПОЯС КАНДИДАТА ---
                        profile = dialogue.candidate.profile_data or {}
                        tz_name = profile.get("timezone", qt_cfg.default_timezone)
                        
                        try:
                            candidate_tz = ZoneInfo(tz_name)
                        except Exception:
                            candidate_tz = ZoneInfo(qt_cfg.default_timezone)

                        # --- Б. ПРОВЕРКА ТИХОГО ЧАСА ---
                        if qt_cfg.enabled:
                            # Узнаем время в локации кандидата прямо сейчас
                            now_candidate = datetime.datetime.now(candidate_tz).time()
                            
                            # Парсим границы тихого часа из конфига
                            start_q = datetime.datetime.strptime(qt_cfg.start, "%H:%M").time()
                            end_q = datetime.datetime.strptime(qt_cfg.end, "%H:%M").time()

                            is_quiet = False
                            # Если интервал ночной (например, с 20:30 до 09:00)
                            if start_q > end_q:
                                if now_candidate >= start_q or now_candidate <= end_q:
                                    is_quiet = True
                            # Если интервал внутри одного дня (например, с 00:00 до 07:00)
                            else:
                                if start_q <= now_candidate <= end_q:
                                    is_quiet = True
                            
                            if is_quiet:
                                # Просто переходим к следующему диалогу, не отправляя задачу в Engine
                                continue

                        # --- В. СТАНДАРТНАЯ ЛОГИКА ПРОВЕРКИ МОЛЧАНИЯ ---
                        # Напоминаем только если последнее сообщение было от БОТА
                        if not dialogue.history or dialogue.history[-1].get("role") != "assistant":
                            continue
                        
                        last_ts = dialogue.last_message_at.replace(tzinfo=datetime.timezone.utc)
                        silence_minutes = (now_utc - last_ts).total_seconds() / 60
                        
                        reminder_cfg = None
                        new_level = dialogue.reminder_level

                        # Проверяем, пора ли переходить на следующий уровень
                        next_level_idx = dialogue.reminder_level
                        if next_level_idx < len(settings.reminders.silence.levels):
                            next_config = settings.reminders.silence.levels[next_level_idx]
                            
                            if silence_minutes >= next_config.delay_minutes:
                                reminder_cfg = next_config
                                new_level = next_level_idx + 1

                        if reminder_cfg:
                            logger.info(f"⏰ Напоминание! Диалог {dialogue.id}, уровень {new_level}, пояс {tz_name}")
                            
                            # Отправляем задачу в Engine
                            await mq.publish("engine_tasks", {
                                "dialogue_id": dialogue.id,
                                "trigger": "reminder",
                                "reminder_text": reminder_cfg.text,
                                "new_level": new_level,
                                "stop_bot": reminder_cfg.stop_bot
                            })

                            # Обновляем уровень в БД сразу, чтобы не слать дубли в следующем цикле
                            dialogue.reminder_level = new_level
                            
                            if reminder_cfg.stop_bot:
                                dialogue.status = 'timed_out'
                                logger.info(f"zzz Диалог {dialogue.id} -> timed_out.")
                                db.add(AnalyticsEvent(
                                    account_id=dialogue.account_id,
                                    job_context_id=dialogue.vacancy_id,
                                    dialogue_id=dialogue.id,
                                    event_type='timed_out',
                                    event_data={"final_level": new_level, "tz": tz_name}
                                ))
                    
                    await db.commit()

            except Exception as e:
                error_msg = f"❌ Ошибка в цикле молчунов Scheduler:\n{str(e)}"
                logger.error(error_msg, exc_info=True)
                try:
                    await mq.publish("tg_alerts", {"type": "system", "text": error_msg, "alert_type": "admin_only"})
                except: pass
            
            
            await asyncio.sleep(30)

    # --- 2. НАПОМИНАНИЯ ПЕРЕД СОБЕСЕДОВАНИЕМ ---
    async def _loop_interview_reminders(self):
        """Проверка таблицы InterviewReminder и отправка напоминаний"""
        from sqlalchemy.orm import selectinload # Убедись, что этот импорт есть в начале файла
        
        while self.is_running:
            try:
                # 1. Проверяем, включены ли напоминания в конфиге
                if not settings.reminders.interview.enabled:
                    await asyncio.sleep(30)
                    continue

                async with AsyncSessionLocal() as db:
                    now_utc = datetime.datetime.now(datetime.timezone.utc)
                    
                    # Загружаем напоминания, которые пора отправить
                    stmt = (
                        select(InterviewReminder)
                        .options(
                            selectinload(InterviewReminder.dialogue)
                            .selectinload(Dialogue.vacancy)
                        )
                        .where(
                            and_(
                                InterviewReminder.status == 'pending',
                                InterviewReminder.scheduled_at <= now_utc
                            )
                        )
                    )
                    result = await db.execute(stmt)
                    reminders = result.scalars().all()

                    for rem in reminders:
                        # 2. Ищем конфиг по ID (теперь через поиск в списке items)
                        reminder_cfg = next(
                            (item for item in settings.reminders.interview.items if item.id == rem.reminder_type), 
                            None
                        )
                        
                        if not reminder_cfg:
                            logger.warning(f"⚠️ Конфигурация для напоминания '{rem.reminder_type}' не найдена в config.yaml")
                            rem.status = 'error' # Помечаем ошибкой, чтобы не крутилось вечно
                            continue

                        if rem.dialogue:
                            dialogue = rem.dialogue
                            vacancy_title = dialogue.vacancy.title if dialogue.vacancy else "Вакансия"
                            
                            # 3. Достаем данные из метаданных (которые сохранил Engine)
                            meta = dialogue.metadata_json or {}
                            i_date_raw = meta.get("interview_date", "не указана")
                            i_time = meta.get("interview_time", "не указано")

                            # Красивое форматирование даты (из 2026-02-15 в 15.02.2026)
                            display_date = i_date_raw
                            try:
                                if i_date_raw != "не указана":
                                    dt_obj = datetime.datetime.strptime(i_date_raw, "%Y-%m-%d")
                                    display_date = dt_obj.strftime("%d.%m.%Y")
                            except:
                                pass

                            # 4. Форматируем текст
                            try:
                                formatted_text = reminder_cfg.text.format(
                                    interview_date=display_date,
                                    interview_time=i_time,
                                    vacancy_title=vacancy_title
                                )
                                
                                # 5. Публикуем в Engine для отправки
                                await mq.publish("engine_tasks", {
                                    "dialogue_id": rem.dialogue_id,
                                    "trigger": "reminder",
                                    "reminder_text": formatted_text
                                })
                                
                                rem.status = 'sent'
                                logger.info(f"✅ Отправлено напоминание '{rem.reminder_type}' для диалога {dialogue.id}")
                            except Exception as format_e:
                                logger.error(f"❌ Ошибка форматирования текста напоминания: {format_e}")
                                rem.status = 'error'
                        
                        rem.processed_at = now_utc
                    
                    await db.commit()
            except Exception as e:
                error_msg = f"❌ Ошибка в цикле собеседований Scheduler:\n{str(e)}"
                logger.error(error_msg, exc_info=True)
                try:
                    await mq.publish("tg_alerts", {
                        "type": "system", "text": error_msg, "alert_type": "admin_only"
                    })
                except: pass
            
            await asyncio.sleep(30) # Проверка каждые 30 секунд

    # --- 4. АКТИВНЫЙ ПОИСК КАНДИДАТОВ ---
    # --- 4. АКТИВНЫЙ ПОИСК КАНДИДАТОВ ---
    async def _loop_candidate_search(self):
        """Периодический поиск новых резюме на Авито под активные вакансии"""
        while self.is_running:
            try:
                # ПРОВЕРКА ИЗ ВАШЕГО КОНФИГА:
                if not settings.features.enable_outbound_search:
                    logger.info("🔍 Активный поиск (outbound) отключен в настройках.")
                    await asyncio.sleep(600) # Спим 10 минут и проверяем снова
                    continue

                logger.info("🔍 Запуск цикла активного поиска кандидатов...")
                await avito_search_service.discover_and_propose()
                logger.info("✅ Цикл активного поиска завершен.")

            except Exception as e:
                error_msg = f"❌ Ошибка в цикле поиска Scheduler:\n{str(e)}"
                logger.error(error_msg, exc_info=True)
                try:
                    await mq.publish("tg_alerts", {"type": "system", "text": error_msg, "alert_type": "admin_only"})
                except: pass
            
            await asyncio.sleep(900)

    # --- 3. ОБНОВЛЕНИЕ БАЗЫ ЗНАНИЙ ---
    async def _loop_kb_refresh(self):
        """Обновление промптов каждые 3 минуты"""
        while self.is_running:
            try:
                logger.info("🔄 Обновление библиотеки промптов из Google Docs...")
                await kb_service.refresh_cache()
            except Exception as e:
                error_msg = f"❌ Ошибка обновления базы знаний (Google Docs):\n{str(e)}"
                logger.error(error_msg)
                # Отправка алерта
                try:
                    await mq.publish("tg_alerts", {
                        "type": "system",
                        "text": error_msg,
                        "alert_type": "admin_only"
                    })
                except: pass
            await asyncio.sleep(180)

async def main():
    scheduler = Scheduler()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(scheduler.stop()))
    try:
        await scheduler.start()
    finally:
        await mq.close()
        await engine.dispose()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass