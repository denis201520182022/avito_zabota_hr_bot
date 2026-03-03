# tg_bot/keyboards.py

from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List, Any

# --- Основные Reply-клавиатуры ---

user_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="⚙️ Баланс")],
        [KeyboardButton(text="❓ Помощь")]
    ],
    resize_keyboard=True
)

admin_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="⚙️ Баланс и Тариф")],
        [KeyboardButton(text="👤 Управление пользователями")],
        # В контексте Avito можно оставить "рекрутеров", если логика сохраняется
        # или переименовать в "Управление аккаунтами", если нужно. 
        # Оставляю как было в HH боте:
        [KeyboardButton(text="👨‍💼 Управление рекрутерами")],
        [KeyboardButton(text="❓ Помощь")]
    ],
    resize_keyboard=True,
    input_field_placeholder="Выберите действие:"
)

# --- Inline-клавиатуры для Статистики ---

# Главное меню статистики (упрощено: сразу предлагает действия, т.к. подразделов больше нет)
stats_main_menu_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="👁️ Посмотреть за 7 дней (текст)", callback_data="view_stats_7days")],
        [InlineKeyboardButton(text="📥 Выгрузить в Excel", callback_data="export_excel_start")],
        # Кнопка "Назад" здесь не нужна, если это корневое сообщение после нажатия Reply-кнопки
    ]
)

# Кнопка возврата (если провалились куда-то глубоко)
back_to_stats_main_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_back_to_main")]]
)

# Быстрые диапазоны для Excel
export_date_options_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="📅 Последние 7 дней", callback_data="export_range_7")],
        [InlineKeyboardButton(text="📅 Последние 14 дней", callback_data="export_range_14")],
        [InlineKeyboardButton(text="📅 Последние 30 дней", callback_data="export_range_30")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_fsm")]
    ]
)

# Функция-хелпер (если понадобится генерация одной кнопки)
def create_stats_export_keyboard(period: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📥 Скачать Excel", callback_data=f"export_stats_{period}")]
        ]
    )

# --- Остальные Inline-клавиатуры (Управление и Настройки) ---

cancel_fsm_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_fsm")]]
)

role_choice_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [
            InlineKeyboardButton(text="Пользователь 🧑‍💻", callback_data="set_role_user"),
            InlineKeyboardButton(text="Администратор ✨", callback_data="set_role_admin")
        ]
    ]
)

limits_menu_keyboard = InlineKeyboardMarkup(
    inline_keyboard=[
        [InlineKeyboardButton(text="⚙️ Установить баланс", callback_data="set_limit")],
        [InlineKeyboardButton(text="💰 Установить тарифы", callback_data="set_tariff")],
        [InlineKeyboardButton(text="🔎 Изменить лимиты поиска", callback_data="set_search_limit")]
    ]
)

limit_options_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="1000"), KeyboardButton(text="5000"), KeyboardButton(text="10000")],
        [KeyboardButton(text="❌ Отмена")]
    ],
    resize_keyboard=True,
    one_time_keyboard=True
)

# --- Универсальная клавиатура управления (Users/Recruiters) ---

BUTTON_TEXTS = {
    "add_user": "➕ Добавить пользователя",
    "del_user": "➖ Удалить пользователя",
    "add_recruiter": "➕ Добавить рекрутера", # Или "аккаунт"
    "del_recruiter": "➖ Удалить рекрутера",
    "update_recruiter": "🔄 Обновить рекрутера",
}

def create_management_keyboard(items: List[Any], *actions: str) -> InlineKeyboardMarkup:
    """
    Создает клавиатуру действий. 
    items здесь не используется для генерации кнопок (обычно список выводится текстом),
    а actions определяют, какие кнопки добавить внизу.
    """
    builder = InlineKeyboardBuilder()
    for action in actions:
        text = BUTTON_TEXTS.get(action, action.replace('_', ' ').capitalize())
        builder.button(text=text, callback_data=action)
    builder.adjust(2)
    return builder.as_markup()