import yaml
import os
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Literal

load_dotenv()

# --- Вспомогательные схемы для YAML ---

class SilenceReminderLevel(BaseModel):
    delay_minutes: int
    text: str
    stop_bot: bool = False

class QuietTimeConfig(BaseModel):
    enabled: bool
    start: str
    end: str
    default_timezone: str

class SilenceConfig(BaseModel):
    enabled: bool
    quiet_time: QuietTimeConfig
    levels: List[SilenceReminderLevel]

class InterviewReminderItem(BaseModel):
    id: str
    type: Literal["fixed_time", "relative"]
    days_before: Optional[int] = 0
    at_time: Optional[str] = None
    minutes_before: Optional[int] = None
    text: str

class InterviewConfig(BaseModel):
    enabled: bool
    items: List[InterviewReminderItem]

class RemindersConfig(BaseModel):
    silence: SilenceConfig
    interview: InterviewConfig

class GoogleSheetsConfig(BaseModel):
    spreadsheet_url: str
    calendar_sheet_name: str
    candidates_sheet_name: str

class KBConfig(BaseModel):
    prompt_doc_url: str
    credentials_json: str
    cache_ttl: int

class FeaturesConfig(BaseModel):
    enable_outbound_search: bool
    vacancy_description_source: str
    send_tg_interview_cards: bool

class LLMConfig(BaseModel):
    main_model: str
    smart_model: str
    temperature: float
    max_tokens: int
    request_timeout: int

class MessagesConfig(BaseModel):
    initial_greeting: str
    qualification_failed_farewell: str

# --- Главный класс настроек ---

class Settings(BaseModel):
    bot_id: str
    bot_role_name: str
    
    channels: Dict[str, List[str]]
    llm: LLMConfig
    features: FeaturesConfig
    knowledge_base: KBConfig
    google_sheets: GoogleSheetsConfig
    reminders: RemindersConfig
    messages: MessagesConfig

    # --- ПАРАМЕТРЫ ИЗ .ENV ---
    # Все переменные окружения должны быть здесь, чтобы Pydantic их увидел
    
    DATABASE_URL: str = Field(default_factory=lambda: os.getenv("DATABASE_URL", ""))
    RABBITMQ_URL: str = Field(default_factory=lambda: os.getenv("RABBITMQ_URL", ""))
    REDIS_URL: str = Field(default_factory=lambda: os.getenv("REDIS_URL", ""))
    TELEGRAM_BOT_TOKEN: str = Field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
    AVITO_WEBHOOK_SECRET: str = Field(default_factory=lambda: os.getenv("AVITO_WEBHOOK_SECRET", "")) # <--- ДОБАВЛЕНО
    OPENAI_API_KEY: str = Field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))             # <--- ДОБАВЛЕНО

    @classmethod
    def load(cls, path: str = "config.yaml"):
        if not Path(path).exists():
            raise FileNotFoundError(f"Файл конфигурации не найден: {path}")
            
        with open(path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
            
        return cls(**config_data)

# Создаем глобальный объект настроек
try:
    settings = Settings.load()
except Exception as e:
    print(f"❌ Ошибка загрузки конфигурации: {e}")
    raise