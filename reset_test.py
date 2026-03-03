import asyncio
import sys
import logging
from sqlalchemy import select, delete
from sqlalchemy.orm import selectinload

from app.db.session import AsyncSessionLocal
from app.db.models import (
    Account, Dialogue, Candidate, LlmLog, 
    AnalyticsEvent, InterviewReminder, InterviewFollowup
)
from app.connectors.avito.client import avito
from app.utils.redis_lock import get_redis_client

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("reset_tool")

async def full_reset(chat_id: str):
    async with AsyncSessionLocal() as db:
        # 1. Находим диалог и связанные сущности
        stmt = select(Dialogue).filter_by(external_chat_id=chat_id).options(
            selectinload(Dialogue.llm_logs),
            selectinload(Dialogue.reminders),
            selectinload(Dialogue.followups)
        )
        dialogue = await db.scalar(stmt)

        if not dialogue:
            logger.error(f"❌ Диалог {chat_id} не найден в базе.")
            return

        account = await db.get(Account, dialogue.account_id)
        candidate_id = dialogue.candidate_id

        # --- ЧАСТЬ 1: УДАЛЕНИЕ В АВИТО ---
        logger.info(f"🧹 Попытка удалить сообщения бота в Авито (chat_id: {chat_id})...")
        if dialogue.history:
            # Выбираем только сообщения ассистента (бота)
            bot_messages = [m for m in dialogue.history if m.get("role") == "assistant"]
            for msg in bot_messages:
                m_id = msg.get("message_id")
                if m_id and not m_id.startswith("no_msg_"):
                    await avito.delete_message(account, db, chat_id, m_id)
            logger.info(f"✅ Отправлено {len(bot_messages)} запросов на удаление в API.")

        # --- ЧАСТЬ 2: ОЧИСТКА БАЗЫ ДАННЫХ ---
        logger.info(f"🧨 Начинаем зачистку БД для диалога ID: {dialogue.id}")

        try:
            # 1. Удаляем логи LLM
            await db.execute(delete(LlmLog).where(LlmLog.dialogue_id == dialogue.id))
            logger.info("- Логи LLM удалены")

            # 2. Удаляем аналитические события
            await db.execute(delete(AnalyticsEvent).where(AnalyticsEvent.dialogue_id == dialogue.id))
            logger.info("- События аналитики удалены")

            # 3. Напоминания и фоллоуапы (хотя там есть cascade, удалим для верности)
            await db.execute(delete(InterviewReminder).where(InterviewReminder.dialogue_id == dialogue.id))
            await db.execute(delete(InterviewFollowup).where(InterviewFollowup.dialogue_id == dialogue.id))
            logger.info("- Запланированные задачи (reminders/followups) удалены")

            # 4. Удаляем сам Диалог
            await db.delete(dialogue)
            logger.info(f"- Диалог {dialogue.id} удален")

            # 5. Удаляем Кандидата (профиль)
            if candidate_id:
                # Проверяем, нет ли у этого кандидата других диалогов (вдруг тест был на разных аккаунтах)
                other_dialogues = await db.scalar(
                    select(Dialogue).filter(Dialogue.candidate_id == candidate_id, Dialogue.id != dialogue.id)
                )
                if not other_dialogues:
                    candidate = await db.get(Candidate, candidate_id)
                    if candidate:
                        await db.delete(candidate)
                        logger.info(f"- Профиль кандидата {candidate_id} полностью стерт")
                else:
                    logger.info("- Профиль кандидата оставлен, так как связан с другими диалогами")

            # 6. Чистим Redis Lock (чтобы бот не "тупил" 15 секунд ожидания пачки сообщений)
            redis = get_redis_client()
            await redis.delete(f"debounce_lock:{chat_id}")
            logger.info("- Redis lock сброшен")

            await db.commit()
            logger.info("✨ БАЗА ДАННЫХ ПРИВЕДЕНА В ПЕРВОЗДАННЫЙ ВИД.")

        except Exception as e:
            await db.rollback()
            logger.error(f"💥 Ошибка при очистке БД: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python reset_test.py <EXTERNAL_CHAT_ID>")
        sys.exit(1)
    
    asyncio.run(full_reset(sys.argv[1]))