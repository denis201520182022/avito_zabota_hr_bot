# engine_worker.py
import asyncio
import json
import logging
import signal
from aio_pika import IncomingMessage
from app.core.rabbitmq import mq
from app.core.engine import dispatcher
from app.db.session import engine
from app.services.llm import cleanup_llm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EngineWorker")

async def on_engine_task(message: IncomingMessage):
    """
    Обработка задачи ИИ.
    Используем ignore_processed=True, чтобы задача не удалялась из очереди при возникновении ошибки.
    """
    async with message.process(ignore_processed=True):
        # 1. Декодируем сообщение
        try:
            body_raw = message.body.decode()
            task_data = json.loads(body_raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.error("❌ Критическая ошибка: Не удалось распарсить JSON в engine_tasks. Сообщение удалено.")
            await message.reject(requeue=False)
            return

        # 2. Обработка логики
        diag_id = task_data.get('dialogue_id', 'unknown')
        try:
            logger.info(f"🧠 [Engine] Обработка ИИ-логики диалога ID: {diag_id}")
            
            await dispatcher.process_engine_task(task_data)
            
            # Если выполнение дошло до этой точки — подтверждаем успех
            await message.ack()
            
        except Exception as e:
            # Логируем ошибку (твоя исходная логика)
            error_msg = f"❌ Ошибка в Движке (Engine):\nДиалог ID: `{diag_id}`\nТекст ошибки: {str(e)}"
            logger.error(error_msg, exc_info=True)

            # --- ОТПРАВКА АЛЕРТА АДМИНАМ (твоя исходная логика) ---
            try:
                await mq.publish("tg_alerts", {
                    "type": "system",
                    "text": error_msg,
                    "alert_type": "admin_only"
                })
            except Exception as mq_err:
                logger.error(f"Не удалось отправить системный алерт из Engine: {mq_err}")

            # --- ВОЗВРАТ В ОЧЕРЕДЬ ---
            # Добавляем паузу в 1 секунду, чтобы не зацикливать ошибку слишком быстро
            # (например, если OpenAI временно недоступен или лимиты превышены)
            logger.info(f"♻️ Возвращаем задачу диалога {diag_id} в очередь для повторной попытки...")
            await asyncio.sleep(1)
            await message.nack(requeue=True)

async def main():
    await mq.connect()
    channel = mq.channel
    # Оставляем prefetch_count=10, чтобы не перегружать API ИИ
    await channel.set_qos(prefetch_count=10)

    engine_queue = await channel.get_queue("engine_tasks")
    await engine_queue.consume(on_engine_task)

    logger.info("👷 Engine Worker (Brain) запущен.")
    
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: stop_event.set())

    await stop_event.wait()
    
    # Закрытие ресурсов
    await mq.close()
    await engine.dispose()
    await cleanup_llm()
    logger.info("👋 Engine Worker остановлен.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass