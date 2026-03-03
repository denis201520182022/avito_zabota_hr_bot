# connector_worker.py
import asyncio
import json
import logging
import signal
from aio_pika import IncomingMessage
from app.core.rabbitmq import mq
from app.connectors.avito import avito_connector
from app.db.session import engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ConnectorWorker")

async def on_avito_inbound(message: IncomingMessage):
    """
    Обработка входящего события от Авито. 
    Используем ignore_processed=True для ручного управления подтверждением (ACK/NACK).
    """
    async with message.process(ignore_processed=True):
        # 1. Сначала пытаемся распарсить JSON
        try:
            body = json.loads(message.body.decode())
        except json.JSONDecodeError:
            logger.error("❌ Критическая ошибка: Некорректный JSON в очереди avito_inbound. Сообщение отброшено.")
            await message.reject(requeue=False)
            return

        # 2. Обрабатываем событие
        try:
            logger.info(f"📥 [Connector] Унификация события от Avito (Source: {body.get('source')})")
            await avito_connector.process_avito_event(body)
            
            # Если всё прошло успешно - подтверждаем выполнение
            await message.ack()
            
        except Exception as e:
            # Логируем ошибку
            error_msg = f"❌ Ошибка в Унификаторе (Avito):\n{str(e)}"
            logger.error(error_msg, exc_info=True)
            
            # --- ОТПРАВКА АЛЕРТА В TG ВОРКЕР (твоя исходная логика) ---
            try:
                alert_payload = {
                    "type": "system",
                    "text": error_msg,
                    "alert_type": "admin_only"
                }
                await mq.publish("tg_alerts", alert_payload)
            except Exception as amqp_err:
                logger.error(f"Не удалось отправить алерт в очередь: {amqp_err}")

            # --- ВОЗВРАТ В ОЧЕРЕДЬ ---
            # Делаем небольшую паузу, чтобы не перегружать систему мгновенными повторами при сбое БД
            logger.info("♻️ Возвращаем задачу в очередь RabbitMQ (requeue=True)...")
            await asyncio.sleep(1) 
            await message.nack(requeue=True)

async def main():
    await mq.connect()
    channel = mq.channel
    # Унификатор быстрый, можно брать много задач (prefetch_count=50)
    await channel.set_qos(prefetch_count=50) 

    inbound_queue = await channel.get_queue("avito_inbound")
    await inbound_queue.consume(on_avito_inbound)

    logger.info("👷 Connector Worker (Unificator) запущен.")
    
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: stop_event.set())

    await stop_event.wait()
    await mq.close()
    await engine.dispose()
    logger.info("👋 Connector Worker остановлен.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass