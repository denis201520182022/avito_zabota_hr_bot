import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool, create_engine
from alembic import context
from dotenv import load_dotenv

# Добавляем корневую директорию проекта в путь поиска,
# чтобы импорты из app.* работали корректно
sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), '..')))

# Подгружаем переменные из .env
load_dotenv()

# Импортируем метаданные наших моделей
from app.db.models import Base

# Это объект конфигурации Alembic
config = context.config

# Настройка логирования
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Передаем метаданные моделей для автогенерации (alembic revision --autogenerate)
target_metadata = Base.metadata

def get_url():
    """
    Получает URL базы данных из .env и меняет asyncpg на psycopg2,
    так как Alembic работает в синхронном режиме.
    """
    url = os.getenv("DATABASE_URL")
    if url and "asyncpg" in url:
        return url.replace("asyncpg", "psycopg2")
    return url

def run_migrations_offline() -> None:
    """Запуск миграций в 'offline' режиме (генерация SQL-скрипта)."""
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    """Запуск миграций в 'online' режиме (применение к живой БД)."""

    # Создаем обычный синхронный движок для миграций
    connectable = create_engine(get_url())

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()