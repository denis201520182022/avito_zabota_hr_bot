# tg_bot/handlers/admin.py

import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified
from aiogram.utils.formatting import Text, Bold, Italic, Code
import io
import datetime
from aiogram.types import BufferedInputFile
from sqlalchemy.orm.attributes import flag_modified
from app.db.models import (
    Account, 
    JobContext, 
    Candidate, 
    AvitoSearchQuota, 
    AvitoSearchStat
)

from app.db.models import TelegramUser, Account, AppSettings, Dialogue
from app.tg_bot.filters import AdminFilter
from app.tg_bot.keyboards import (
    create_management_keyboard,
    role_choice_keyboard,
    cancel_fsm_keyboard,
    limits_menu_keyboard,
    admin_keyboard
)

logger = logging.getLogger(__name__)
router = Router()

# Применяем фильтр админа на весь роутер
router.message.filter(AdminFilter())
router.callback_query.filter(AdminFilter())

# --- FSM Состояния ---

class UserManagement(StatesGroup):
    add_id = State()
    add_name = State()
    add_role = State()
    del_id = State()

class AccountManagement(StatesGroup):
    """Управление аккаунтами Авито (вместо старых рекрутеров)"""
    add_name = State()
    add_client_id = State()
    add_client_secret = State()
    add_tg_chat_id = State()
    add_topic_id = State()
    update_id = State()
    update_name = State()
    update_client_id = State()
    update_client_secret = State()
    update_tg_chat_id = State()
    update_topic_id = State()
    del_id = State()
    # Состояния для обновления можно будет добавить по аналогии

class SettingsManagement(StatesGroup):
    set_balance = State()
    set_cost_dialogue = State()
    set_search_balance = State()

# --- Обработчики отмены ---

@router.message(Command("cancel"))
@router.message(F.text.casefold() == "отмена")
async def cancel_command_handler(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Нет активных действий для отмены.")
        return
    await state.clear()
    await message.answer("Действие отменено.", reply_markup=admin_keyboard)

@router.callback_query(F.data == "cancel_fsm")
async def cancel_callback_handler(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("Действие отменено.")
    await callback.answer()

# --- УПРАВЛЕНИЕ БАЛАНСОМ И ТАРИФАМИ ---


# tg_bot/handlers/admin.py (ВАШ ФАЙЛ)

@router.message(F.text == "⚙️ Баланс и Тариф")
async def limits_menu(message: Message, session: AsyncSession):
    # Тянем настройки
    settings = await session.get(AppSettings, 1)
    if not settings:
        await message.answer("❌ Не удалось загрузить настройки.")
        return

    # Тянем квоты поиска
    quota_stmt = select(AvitoSearchQuota).join(Account)
    quotas = (await session.execute(quota_stmt)).scalars().all()

    stats = settings.stats or {}
    costs = settings.costs or {}

    quota_lines = []
    if quotas:
        for q in quotas:
            quota_lines.extend([f"- {q.account.name}: ", Bold(str(q.remaining_limits)), " шт.\n"])
    else:
        quota_lines.append(Italic("Квоты не настроены.\n"))

    # Собираем контент с ИСПРАВЛЕННЫМ синтаксисом
    content = Text(
        Bold("📊 Управление балансом:"), "\n\n",
        "Текущий баланс: ", Bold(f"{settings.balance:.2f}"), " руб.\n\n",
        
        Bold("📈 История затрат (всего):"), "\n",
        # ВОТ ЗДЕСЬ БЫЛА ОШИБКА -> добавлена запятая в конце строки
        "- Потрачено на диалоги: ", Bold(f"{stats.get('spent_on_dialogues', 0):.2f}"), " руб.\n", 
        
        "\n💰 ", Bold("Тарифы:"), "\n",
        "Новый диалог: ", Bold(f"{costs.get('dialogue', 0):.2f}"), " руб.\n\n",

        Bold("🔎 Лимиты поиска (контакты):"), "\n",
        *quota_lines,
        "\n",
        
        "🔔 Уведомление при балансе < ", Bold(f"{settings.low_balance_threshold:.2f}"), " руб."
    )
    
    # Для редактирования сообщения в будущем, лучше использовать message.answer
    await message.answer(**content.as_kwargs(), reply_markup=limits_menu_keyboard)

    await message.answer(**content.as_kwargs(), reply_markup=limits_menu_keyboard)
@router.callback_query(F.data == "set_limit")
async def start_set_balance(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SettingsManagement.set_balance)
    await callback.message.answer("Введите новую сумму общего баланса в рублях (например: 5000):")
    await callback.answer()

@router.message(SettingsManagement.set_balance)
async def process_set_balance(message: Message, state: FSMContext, session: AsyncSession):
    try:
        new_balance = float(message.text.replace(',', '.'))
        if new_balance < 0: raise ValueError
    except (ValueError, TypeError):
        await message.answer("❌ Сумма должна быть числом. Попробуйте еще раз.")
        return

    stmt = select(AppSettings).where(AppSettings.id == 1)
    result = await session.execute(stmt)
    settings = result.scalar_one()
    
    settings.balance = new_balance
    
    if new_balance >= settings.low_balance_threshold:
        settings.low_limit_notified = False

    await session.commit()
    await state.clear()
    await message.answer(f"✅ Баланс обновлен: {new_balance:.2f} руб.", reply_markup=admin_keyboard)

@router.callback_query(F.data == "set_tariff")
async def start_set_cost_dialogue(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SettingsManagement.set_cost_dialogue)
    await callback.message.answer("Введите стоимость создания ОДНОГО ДИАЛОГА (в рублях):")
    await callback.answer()

@router.message(SettingsManagement.set_cost_dialogue)
async def process_set_cost_dialogue(message: Message, state: FSMContext, session: AsyncSession):
    try:
        val = float(message.text.replace(',', '.'))
        
        stmt = select(AppSettings).where(AppSettings.id == 1)
        result = await session.execute(stmt)
        settings = result.scalar_one()
        
        # Обновляем значение в JSONB словаре
        if settings.costs is None: settings.costs = {}
        settings.costs["dialogue"] = val
        
        # Сообщаем SQLAlchemy, что JSON поле изменилось
        flag_modified(settings, "costs")
        
        await session.commit()
        await state.clear()
        await message.answer(f"✅ Тариф обновлен: {val:.2f} руб. за диалог.", reply_markup=admin_keyboard)
    except Exception as e:
        logger.error(f"Ошибка при смене тарифа: {e}")
        await message.answer("❌ Ошибка в числе. Попробуйте еще раз.")


@router.callback_query(F.data == "set_search_limit")
async def start_set_search_limit(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SettingsManagement.set_search_balance)
    await callback.message.answer(
        "Введите ID аккаунта и новый лимит через пробел.\n"
        "Пример: `1 100` (где 1 - ID аккаунта, 100 - количество контактов)",
        parse_mode="Markdown"
    )
    await callback.answer()

@router.message(SettingsManagement.set_search_balance)
async def process_set_search_limit(message: Message, state: FSMContext, session: AsyncSession):
    try:
        parts = message.text.split()
        if len(parts) != 2:
            raise ValueError
        
        acc_id, new_limit = int(parts[0]), int(parts[1])
        
        # Ищем или создаем запись квоты
        stmt = select(AvitoSearchQuota).filter_by(account_id=acc_id)
        quota = await session.execute(stmt)
        quota = quota.scalar_one_or_none()
        
        if not quota:
            # Если записи еще нет, создаем новую
            quota = AvitoSearchQuota(account_id=acc_id, remaining_limits=new_limit)
            session.add(quota)
        else:
            quota.remaining_limits = new_limit
            
        await session.commit()
        await state.clear()
        await message.answer(f"✅ Лимит поиска для аккаунта ID {acc_id} установлен: {new_limit} шт.", reply_markup=admin_keyboard)
    
    except (ValueError, IndexError):
        await message.answer("❌ Неверный формат. Введите `ID ЛИМИТ` (например: `1 50`).")
    except Exception as e:
        logger.error(f"Ошибка при установке лимита поиска: {e}")
        await message.answer("❌ Произошла ошибка. Проверьте ID аккаунта.")


# --- 1. УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ ---

@router.message(F.text == "👤 Управление пользователями")
async def user_management_menu(message: Message, session: AsyncSession):
    stmt = select(TelegramUser)
    result = await session.execute(stmt)
    users = result.scalars().all()
    
    content_parts = [Bold("👥 Список пользователей:"), "\n\n"]
    if not users:
        content_parts.append(Italic("В системе пока нет пользователей."))
    else:
        for u in users:
            role_emoji = "✨" if u.role == 'admin' else "🧑‍💻"
            content_parts.extend([
                f"{role_emoji} ", Bold(u.username), 
                " (ID: ", Code(str(u.telegram_id)), ") - Роль: ", Italic(u.role), "\n"
            ])
            
    content_parts.append("\nВыберите действие:")
    content = Text(*content_parts)
    
    # Клавиатура управления (кнопки "Добавить" и "Удалить")
    await message.answer(
        **content.as_kwargs(), 
        reply_markup=create_management_keyboard([], "add_user", "del_user")
    )

@router.callback_query(F.data == "add_user")
async def start_add_user(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserManagement.add_id)
    content = Text("Введите Telegram ID нового пользователя.")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
    await callback.answer()

@router.message(UserManagement.add_id)
async def process_add_user_id(message: Message, state: FSMContext, session: AsyncSession):
    if not message.text or not message.text.isdigit():
        content = Text("❌ ID должен быть числом. Попробуйте еще раз.")
        await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
        return
    
    user_id = int(message.text)
    
    # Проверка на существование
    stmt = select(TelegramUser).where(TelegramUser.telegram_id == user_id)
    result = await session.execute(stmt)
    if result.scalar_one_or_none():
        content = Text("⚠️ Пользователь с ID ", Code(str(user_id)), " уже существует. Действие отменено.")
        await message.answer(**content.as_kwargs())
        await state.clear()
        return
        
    await state.update_data(user_id=user_id)
    await state.set_state(UserManagement.add_name)
    content = Text("Отлично. Теперь введите имя пользователя (например, ", Code("Иван Рекрутер"), ").")
    await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)

@router.message(UserManagement.add_name)
async def process_add_user_name(message: Message, state: FSMContext):
    if not message.text:
        content = Text("❌ Имя не может быть пустым. Попробуйте еще раз.")
        await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
        return
        
    await state.update_data(user_name=message.text)
    await state.set_state(UserManagement.add_role)
    await message.answer("Имя принято. Теперь выберите роль:", reply_markup=role_choice_keyboard)

@router.callback_query(UserManagement.add_role)
async def process_add_user_role(callback: CallbackQuery, state: FSMContext, session: AsyncSession):
    role = "admin" if callback.data == "set_role_admin" else "user"
    user_data = await state.get_data()
    
    new_user = TelegramUser(
        telegram_id=user_data['user_id'], 
        username=user_data['user_name'], 
        role=role
    )
    session.add(new_user)
    await session.commit()
    
    await state.clear()
    logger.info(f"Админ {callback.from_user.id} добавил пользователя {user_data['user_id']} с ролью {role}")
    
    content = Text(
        "✅ ", Bold("Успех!"), " Пользователь ", 
        Bold(user_data['user_name']), " добавлен с ролью ", Italic(role), "."
    )
    await callback.message.edit_text(**content.as_kwargs())

@router.callback_query(F.data == "del_user")
async def start_del_user(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserManagement.del_id)
    content = Text("Введите Telegram ID пользователя для удаления.")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
    await callback.answer()

@router.message(UserManagement.del_id)
async def process_del_user_id(message: Message, state: FSMContext, session: AsyncSession):
    if not message.text or not message.text.isdigit():
        content = Text("❌ ID должен быть числом. Попробуйте еще раз.")
        await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
        return
        
    user_id_to_delete = int(message.text)
    
    # Не даем удалить самого себя
    if message.from_user.id == user_id_to_delete:
        await message.answer("🤔 Вы не можете удалить самого себя. Действие отменено.")
        await state.clear()
        return
        
    stmt = select(TelegramUser).where(TelegramUser.telegram_id == user_id_to_delete)
    result = await session.execute(stmt)
    user_to_delete = result.scalar_one_or_none()
    
    if not user_to_delete:
        content = Text("⚠️ Пользователь с ID ", Code(str(user_id_to_delete)), " не найден. Действие отменено.")
        await message.answer(**content.as_kwargs())
        await state.clear()
        return
        
    deleted_username = user_to_delete.username
    deleted_id = user_to_delete.telegram_id
    
    await session.delete(user_to_delete)
    await session.commit()
    
    await state.clear()
    logger.info(f"Админ {message.from_user.id} удалил пользователя {deleted_id}")
    
    content = Text("✅ Пользователь ", Bold(deleted_username), " (ID: ", Code(str(deleted_id)), ") был удален.")
    await message.answer(**content.as_kwargs())


# --- 3. УПРАВЛЕНИЕ АККАУНТАМИ (АВИТО) ---

@router.message(F.text == "👨‍💼 Управление рекрутерами")
async def account_management_menu(message: Message, session: AsyncSession):
    """Список подключенных аккаунтов Авито"""
    stmt = select(Account).where(Account.platform == 'avito')
    result = await session.execute(stmt)
    accounts = result.scalars().all()

    content_parts = [Bold("💼 Подключенные аккаунты Авито:"), "\n\n"]
    if not accounts:
        content_parts.append(Italic("Список пуст. Добавьте свой первый аккаунт."))
    else:
        for acc in accounts:
            content_parts.extend(["- ", Bold(acc.name), " (ID: ", Code(str(acc.id)), ")\n"])
    
    content_parts.append("\nВыберите действие:")
    content = Text(*content_parts)
    
    await message.answer(
        **content.as_kwargs(),
        reply_markup=create_management_keyboard([], "add_recruiter", "del_recruiter")
    )

# --- ДОБАВЛЕНИЕ АККАУНТА ---

@router.callback_query(F.data == "add_recruiter")
async def start_add_account(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AccountManagement.add_name)
    content = Text("Шаг 1/5: Введите название аккаунта (например: ", Italic("Авито Основной"), ").")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
    await callback.answer()

@router.message(AccountManagement.add_name)
async def process_add_acc_name(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Название не может быть пустым.")
        return
    await state.update_data(name=message.text)
    await state.set_state(AccountManagement.add_client_id)
    await message.answer("Шаг 2/5: Теперь введите ваш Avito ", Bold("Client ID"), ".")

@router.message(AccountManagement.add_client_id)
async def process_add_client_id(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Client ID не может быть пустым.")
        return
    await state.update_data(client_id=message.text.strip())
    await state.set_state(AccountManagement.add_client_secret)
    await message.answer("Шаг 3/5: Теперь введите ваш Avito ", Bold("Client Secret"), ".")

@router.message(AccountManagement.add_client_secret)
async def process_add_client_secret(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Client Secret не может быть пустым.")
        return
    await state.update_data(client_secret=message.text.strip())
    await state.set_state(AccountManagement.add_tg_chat_id)
    await message.answer("Шаг 4/5: Введите ID Telegram-чата для уведомлений (начинается с -100...).")

@router.message(AccountManagement.add_tg_chat_id)
async def process_add_tg_chat(message: Message, state: FSMContext):
    chat_id_str = message.text.strip()
    if not chat_id_str.lstrip('-').isdigit():
        await message.answer("❌ ID чата должен быть числом.")
        return
    
    # Гарантируем наличие минуса для групповых чатов
    if not chat_id_str.startswith('-'):
        chat_id_str = f"-{chat_id_str}"
        
    await state.update_data(tg_chat_id=int(chat_id_str))
    await state.set_state(AccountManagement.add_topic_id)
    await message.answer("Шаг 5/5: Введите ID темы (Topic ID) для уведомлений о квалифицированных кандидатах.")

@router.message(AccountManagement.add_topic_id)
async def process_add_topic_id(message: Message, state: FSMContext, session: AsyncSession):
    if not message.text.isdigit():
        await message.answer("❌ Topic ID должен быть числом.")
        return
    
    topic_id = int(message.text)
    data = await state.get_data()
    
    # Создаем новый аккаунт
    new_account = Account(
        platform='avito',
        name=data['name'],
        # Храним токены/ключи в auth_data
        auth_data={
            "client_id": data['client_id'],
            "client_secret": data['client_secret'],
            "access_token": None,
            "refresh_token": None
        },
        # Храним настройки уведомлений в settings
        settings={
            "tg_chat_id": data['tg_chat_id'],
            "topic_qualified_id": topic_id
        },
        is_active=True
    )
    
    session.add(new_account)
    await session.commit()
    await state.clear()
    
    logger.info(f"Админ {message.from_user.id} добавил аккаунт Авито: {data['name']}")
    await message.answer(f"✅ Аккаунт <b>{data['name']}</b> успешно добавлен и настроен!", parse_mode="HTML", reply_markup=admin_keyboard)





@router.callback_query(F.data == "update_recruiter")
async def start_update_account(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AccountManagement.update_id)
    content = Text("Введите ID аккаунта, данные которого вы хотите обновить.")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
    await callback.answer()

@router.message(AccountManagement.update_id)
async def process_update_acc_id(message: Message, state: FSMContext, session: AsyncSession):
    if not message.text.isdigit():
        await message.answer("❌ ID должен быть числом.", reply_markup=cancel_fsm_keyboard)
        return

    acc_id = int(message.text)
    stmt = select(Account).where(Account.id == acc_id)
    result = await session.execute(stmt)
    account = result.scalar_one_or_none()

    if not account:
        await message.answer(f"⚠️ Аккаунт с ID `{acc_id}` не найден. Действие отменено.")
        await state.clear()
        return

    await state.update_data(acc_id=acc_id)
    await state.set_state(AccountManagement.update_name)
    content = Text(
        "Вы обновляете аккаунт: ", Bold(account.name), "\n\n",
        "Шаг 1/5: Введите новое название (или отправьте старое)."
    )
    await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)

@router.message(AccountManagement.update_name)
async def process_update_name(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Название не может быть пустым.")
        return
    await state.update_data(name=message.text)
    await state.set_state(AccountManagement.update_client_id)
    await message.answer("Шаг 2/5: Введите новый ", Bold("Client ID"), ".")

@router.message(AccountManagement.update_client_id)
async def process_update_client_id(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Не может быть пустым.")
        return
    await state.update_data(client_id=message.text.strip())
    await state.set_state(AccountManagement.update_client_secret)
    await message.answer("Шаг 3/5: Введите новый ", Bold("Client Secret"), ".")

@router.message(AccountManagement.update_client_secret)
async def process_update_client_secret(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("❌ Не может быть пустым.")
        return
    await state.update_data(client_secret=message.text.strip())
    await state.set_state(AccountManagement.update_tg_chat_id)
    await message.answer("Шаг 4/5: Введите новый ID Telegram-чата.")

@router.message(AccountManagement.update_tg_chat_id)
async def process_update_tg_chat(message: Message, state: FSMContext):
    chat_id_str = message.text.strip()
    if not chat_id_str.lstrip('-').isdigit():
        await message.answer("❌ ID должен быть числом.")
        return
    
    if not chat_id_str.startswith('-'):
        chat_id_str = f"-{chat_id_str}"
        
    await state.update_data(tg_chat_id=int(chat_id_str))
    await state.set_state(AccountManagement.update_topic_id)
    await message.answer("Шаг 5/5: Введите новый Topic ID для уведомлений.")

@router.message(AccountManagement.update_topic_id)
async def process_update_final(message: Message, state: FSMContext, session: AsyncSession):
    if not message.text.isdigit():
        await message.answer("❌ ID темы должен быть числом.")
        return
    
    data = await state.get_data()
    stmt = select(Account).where(Account.id == data['acc_id'])
    result = await session.execute(stmt)
    account = result.scalar_one()

    # Обновляем поля
    account.name = data['name']
    
    # Обновляем JSONB поля
    account.auth_data["client_id"] = data['client_id']
    account.auth_data["client_secret"] = data['client_secret']
    
    account.settings["tg_chat_id"] = data['tg_chat_id']
    account.settings["topic_qualified_id"] = int(message.text)

    # Важно: сообщаем SQLAlchemy об изменениях внутри словарей
    from sqlalchemy.orm.attributes import flag_modified
    flag_modified(account, "auth_data")
    flag_modified(account, "settings")

    await session.commit()
    await state.clear()

    logger.info(f"Админ {message.from_user.id} обновил аккаунт {account.name}")
    await message.answer(f"✅ Данные аккаунта <b>{account.name}</b> успешно обновлены!", parse_mode="HTML", reply_markup=admin_keyboard)


# --- УДАЛЕНИЕ АККАУНТА (Завершение) ---

@router.callback_query(F.data == "del_recruiter")
async def start_del_account(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AccountManagement.del_id)
    content = Text("Введите ID аккаунта (внутренний) для удаления из списка.")
    await callback.message.edit_text(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
    await callback.answer()

@router.message(AccountManagement.del_id)
async def process_del_account_id(message: Message, state: FSMContext, session: AsyncSession):
    if not message.text or not message.text.isdigit():
        content = Text("❌ ID должен быть числом. Попробуйте еще раз.")
        await message.answer(**content.as_kwargs(), reply_markup=cancel_fsm_keyboard)
        return

    acc_id = int(message.text)
    
    # Поиск аккаунта
    stmt = select(Account).where(Account.id == acc_id)
    result = await session.execute(stmt)
    account_to_delete = result.scalar_one_or_none()

    if not account_to_delete:
        content = Text("⚠️ Аккаунт с ID ", Code(str(acc_id)), " не найден. Действие отменено.")
        await message.answer(**content.as_kwargs())
        await state.clear()
        return

    deleted_name = account_to_delete.name
    await session.delete(account_to_delete)
    await session.commit()
    
    await state.clear()
    logger.info(f"Админ {message.from_user.id} удалил аккаунт {acc_id}")

    content = Text("✅ Аккаунт ", Bold(deleted_name), " (ID: ", Code(str(acc_id)), ") удален.")
    await message.answer(**content.as_kwargs())


# --- ПОТАЙНАЯ КОМАНДА ДЛЯ ПОЛНОЙ ИСТОРИИ (Авито) ---

@router.message(Command("dump_avito"))
async def secret_dump_handler(message: Message, session: AsyncSession):
    """
    Выгружает сырой лог диалога из БД в текстовый файл.
    Использование: /dump_avito [avito_chat_id]
    """
    # Замените ID на свой, если нужно ограничить доступ
    # if message.from_user.id != 1975808643: 
    #     return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: `/dump_avito [avito_chat_id]`", parse_mode="Markdown")
        return

    chat_id = args[1]
    
    # Ищем диалог по external_chat_id (авито чат id)
    stmt = select(Dialogue).where(Dialogue.external_chat_id == chat_id)
    result = await session.execute(stmt)
    dialogue = result.scalar_one_or_none()
    
    if not dialogue:
        await message.answer(f"❌ Диалог с ID `{chat_id}` не найден.")
        return

    # Формируем текстовый лог
    log_content = []
    log_content.append(f"=== ПОЛНЫЙ ДАМП ДИАЛОГА (АВИТО) ===")
    log_content.append(f"External Chat ID: {dialogue.external_chat_id}")
    log_content.append(f"Status: {dialogue.status}")
    log_content.append(f"Current State: {dialogue.current_state}")
    log_content.append(f"Created At: {dialogue.created_at}")
    
    stats = dialogue.usage_stats or {}
    log_content.append(f"Usage: {stats.get('tokens', 0)} tokens (${stats.get('total_cost', 0)})")
    log_content.append("="*35 + "\n")

    # 1. Обрабатываем основную историю из JSONB
    history = dialogue.history or []
    for msg in history:
        role = str(msg.get('role', 'unknown')).upper()
        # В авито-боте мы обычно храним время в timestamp_utc
        ts = msg.get('timestamp_utc', 'no_time')
        content = msg.get('content', '')
        
        # Инфо о состоянии, если оно сохранялось в историю
        state_info = f" [State: {msg.get('state')}]" if msg.get('state') else ""
        
        log_content.append(f"[{ts}] [{role}]{state_info}")
        log_content.append(f"TEXT: {content}")
        
        # Если есть извлеченные данные (результат скрининга)
        ext = msg.get('extracted_data')
        if ext:
            log_content.append(f"EXTRACTED DATA: {ext}")
            
        log_content.append("-" * 25)

    # 2. Метаданные диалога (результаты квалификации)
    meta = dialogue.metadata_json or {}
    if meta:
        log_content.append("\n" + "#"*10 + " METADATA " + "#"*10)
        for key, val in meta.items():
            log_content.append(f"{key}: {val}")

    final_text = "\n".join(log_content)

    # Создаем и отправляем файл
    file_buffer = io.BytesIO(final_text.encode('utf-8'))
    input_file = BufferedInputFile(file_buffer.getvalue(), filename=f"dump_avito_{chat_id}.txt")

    await message.answer_document(
        document=input_file, 
        caption=f"📄 Полный дамп диалога `{chat_id}`"
    )