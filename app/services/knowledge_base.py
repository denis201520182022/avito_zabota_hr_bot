# app/services/knowledge_base.py
import logging
import re
import os
import json
import asyncio
from typing import Dict, Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import redis.asyncio as redis
from app.core.rabbitmq import mq
import json

from app.core.config import settings

logger = logging.getLogger("knowledge_base")

class KnowledgeBaseService:
    def __init__(self):
        self.doc_url = settings.knowledge_base.prompt_doc_url
        self.creds_path = settings.knowledge_base.credentials_json
        self.ttl = settings.knowledge_base.cache_ttl
        
        # Инициализация Redis клиента
        self.redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
        self.cache_key = f"{settings.bot_id}:prompt_library"

    def _extract_doc_id(self, url: str) -> str:
        """Извлекает ID документа из полной ссылки Google Docs"""
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
        if match:
            return match.group(1)
        raise ValueError(f"Не удалось извлечь Google Doc ID из ссылки: {url}")

    async def get_library(self) -> Dict[str, str]:
        """
        Основной метод получения промптов. 
        Сначала проверяет Redis, если данных нет — загружает из Google.
        """
        try:
            # 1. Пытаемся достать из кэша
            cached_data = await self.redis_client.get(self.cache_key)
            if cached_data:
                logger.debug("📚 Библиотека промптов загружена из кэша Redis")
                return json.loads(cached_data)

            # 2. Если в кэше пусто, идем в Google
            logger.info("🌀 Кэш пуст или просрочен. Загрузка данных из Google Docs...")
            library = await self.refresh_cache()
            return library

        except Exception as e:
            logger.error(f"❌ Ошибка при получении библиотеки промптов: {e}")
            return {}

    async def refresh_cache(self) -> Dict[str, str]:
        """Принудительное обновление данных в Redis из Google Docs"""
        library = await self._fetch_from_google()
        
        if library:
            # Сохраняем в Redis с установленным в конфиге TTL
            await self.redis_client.setex(
                self.cache_key, 
                self.ttl, 
                json.dumps(library, ensure_ascii=False)
            )
            logger.info(f"✅ Кэш Redis обновлен. Загружено блоков: {len(library)}")
            return library  # Возвращаем данные, если всё ок
        
        # Если библиотека пуста (этот код теперь достижим)
        error_msg = "⚠️ ПРЕДУПРЕЖДЕНИЕ KB: Библиотека промптов пуста после загрузки из Google Docs или ошибка доступа!"
        logger.warning(error_msg)
        try:
            await mq.publish("tg_alerts", {
                "type": "system",
                "text": error_msg,
                "alert_type": "admin_only"
            })
        except: pass
        return {}

    async def _fetch_from_google(self) -> Dict[str, str]:
        """Чтение и парсинг документа через Google API"""
        try:
            doc_id = self._extract_doc_id(self.doc_url)
            
            if not os.path.exists(self.creds_path):
                error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА KB: Файл ключей {self.creds_path} не найден!"
                logger.error(error_msg)
                try:
                    await mq.publish("tg_alerts", {
                        "type": "system",
                        "text": error_msg,
                        "alert_type": "admin_only"
                    })
                except: pass
                return {}

            # Авторизация
            creds = Credentials.from_service_account_file(
                self.creds_path, 
                scopes=['https://www.googleapis.com/auth/documents.readonly']
            )
            
            # Google Discovery API работает в блокирующем режиме, 
            # поэтому запускаем тяжелый вызов в отдельном потоке, чтобы не вешать event loop
            service = await asyncio.to_thread(build, 'docs', 'v1', credentials=creds, cache_discovery=False)
            document = await asyncio.to_thread(service.documents().get(documentId=doc_id).execute)
            
            content = document.get('body').get('content')
            full_text = ''
            for value in content:
                if 'paragraph' in value:
                    elements = value.get('paragraph').get('elements')
                    for elem in elements:
                        full_text += elem.get('textRun', {}).get('content', '')

            # Парсинг блоков по маркерам #TAG#
            prompt_library = {}
            # Ищем все вхождения тегов (например #ROLE#, #FAQ#)
            markers = re.findall(r"(#\w+#)", full_text)
            # Разрезаем текст по этим тегам
            parts = re.split(r"(#\w+#)", full_text)

            current_marker = None
            for part in parts:
                clean_part = part.strip()
                if not clean_part:
                    continue
                
                if clean_part in markers:
                    current_marker = clean_part
                elif current_marker:
                    prompt_library[current_marker] = clean_part
                    current_marker = None
            
            return prompt_library

        except Exception as e:
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА KB: Не удалось загрузить данные из Google Docs!\nURL: {self.doc_url}\nОшибка: {e}"
            logger.error(error_msg, exc_info=True)
            
            # Шлем алерт через RabbitMQ
            try:
                await mq.publish("tg_alerts", {
                    "type": "system",
                    "text": error_msg,
                    "alert_type": "admin_only"
                })
            except: pass
            
            return {}

# Экземпляр сервиса
kb_service = KnowledgeBaseService()


