import logging
from typing import Optional, Dict, Any
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BufferedInputFile
from sqlalchemy import select

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.db.models import TelegramUser

logger = logging.getLogger(__name__)

def esc(text: Any) -> str:
    """Экранирование спецсимволов для MarkdownV2 (упрощенное)"""
    return str(text).replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`').replace('>', '\\>')

async def _get_recipients(alert_type: str) -> list[int]:
    """Вспомогательная функция для получения ID получателей из БД"""
    async with AsyncSessionLocal() as session:
        if alert_type in ["balance", "all"]:
            # Все пользователи бота
            stmt = select(TelegramUser.telegram_id)
        else:
            # Только админы
            stmt = select(TelegramUser.telegram_id).where(TelegramUser.role == 'admin')
        
        result = await session.execute(stmt)
        return list(result.scalars().all())

async def send_system_alert(message_text: str, alert_type: str = "admin_only"):
    """
    Отправляет системное уведомление (ошибки, баланс, анонсы).
    """
    recipients = await _get_recipients(alert_type)
    if not recipients:
        return

    async with Bot(token=settings.TELEGRAM_BOT_TOKEN) as bot:
        for chat_id in recipients:
            try:
                await bot.send_message(chat_id=chat_id, text=message_text)
            except Exception as e:
                logger.warning(f"Ошибка отправки алерта в {chat_id}: {e}")

async def send_verification_alert(
    dialogue_id: int,
    external_chat_id: str,
    db_data: Dict[str, Any],
    llm_data: Dict[str, Any],
    history_text: Optional[str] = None,
    reasoning: str = "не указано"
):
    """
    Алерт о несовпадении анкетных данных (например, возраст или гражданство).
    """
    # Используем твой ID как основной для инцидентов или шлем всем админам
    admin_id = 1975808643 
    
    alert_text = (
        f"🚨 *INCIDENT: Ошибка верификации данных*\n\n"
        f"Диалог ID: `{dialogue_id}`\n"
        f"Avito Chat ID: `{esc(external_chat_id)}`\n\n"
        f"📉 *Данные в БД:* {esc(db_data)}\n"
        f"🤖 *Deep Check LLM:* {esc(llm_data)}\n\n"
        f"🧐 *Обоснование:* _{esc(reasoning)}_\n\n"
        f"⛔ *Данные в БД НЕ! обновлены на основе Deep Check.*"
    )

    async with Bot(
        token=settings.TELEGRAM_BOT_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    ) as bot:
        try:
            await bot.send_message(chat_id=admin_id, text=alert_text)
            
            if history_text:
                file = BufferedInputFile(
                    history_text.encode('utf-8'), 
                    filename=f"verify_error_{external_chat_id}.txt"
                )
                await bot.send_document(chat_id=admin_id, document=file, caption="📜 История для анализа")
        except Exception as e:
            logger.error(f"Ошибка отправки алерта верификации: {e}")

async def send_hallucination_alert(
    dialogue_id: int,
    external_chat_id: str,
    user_said: str,
    llm_suggested: str,
    corrected_val: str,
    history_text: Optional[str] = None,
    reasoning: str = "не указано"
):
    """
    Алерт о галлюцинации или ошибке извлечения (даты, телефоны и т.д.).
    """
    admin_id = 1975808643

    alert_text = (
        f"📅 *INCIDENT: Ошибка извлечения (Галлюцинация)*\n\n"
        f"Диалог ID: `{dialogue_id}`\n"
        f"Avito Chat: `{esc(external_chat_id)}`\n\n"
        f"👤 *Кандидат:* _{esc(user_said)}_\n"
        f"🤖 *LLM:* `{esc(llm_suggested)}`\n"
        f"✅ *Аудитор исправил:* `{esc(corrected_val)}`\n\n"
        f"🧐 *Обоснование:* _{esc(reasoning)}_\n\n"
        f"🔄 *Диалог отправлен на перегенерацию.*"
    )

    async with Bot(
        token=settings.TELEGRAM_BOT_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    ) as bot:
        try:
            await bot.send_message(chat_id=admin_id, text=alert_text)
            
            if history_text:
                file = BufferedInputFile(
                    history_text.encode('utf-8'), 
                    filename=f"hallucination_{external_chat_id}.txt"
                )
                await bot.send_document(chat_id=admin_id, document=file, caption="📜 История диалога")
        except Exception as e:
            logger.error(f"Ошибка отправки алерта галлюцинации: {e}")