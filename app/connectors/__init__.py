# app/connectors/__init__.py
from .avito.client import avito
# Сюда потом добавишь: from .hh.client import hh_connector

# Словарь-реестр всех коннекторов
CONNECTORS = {
    "avito": avito,
    # "hh": hh_connector,
}

def get_connector(platform: str):
    """Возвращает нужный коннектор по имени платформы"""
    connector = CONNECTORS.get(platform)
    if not connector:
        raise ValueError(f"Connector for platform '{platform}' not found")
    return connector