# app/core/rabbitmq.py
import json
import aio_pika
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class RabbitMQManager:
    def __init__(self):
        self.url = os.getenv("RABBITMQ_URL", "amqp://user:password@localhost:5672/")
        self.connection = None
        self.channel = None

    async def connect(self):
        """Установка соединения и создание основных очередей"""
        if not self.connection or self.connection.is_closed:
            self.connection = await aio_pika.connect_robust(self.url)
            self.channel = await self.connection.channel()
            
            # Объявляем основные очереди (чтобы они создались при первом запуске)
            await self.channel.declare_queue("avito_inbound", durable=True)
            await self.channel.declare_queue("engine_tasks", durable=True)
            await self.channel.declare_queue("outbound_messages", durable=True)
            await self.channel.declare_queue("integrations", durable=True)
            await self.channel.declare_queue("tg_alerts", durable=True)
            await self.channel.declare_queue("tg_notifications", durable=True)
            
            logger.info("✅ Успешное подключение к RabbitMQ и инициализация очередей")

    async def publish(self, queue_name: str, message: dict):
        """Отправка сообщения в очередь"""
        if not self.channel:
            await self.connect()
            
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message, ensure_ascii=False).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=queue_name
        )

    async def close(self):
        if self.connection:
            await self.connection.close()

# Глобальный экземпляр для импорта
mq = RabbitMQManager()