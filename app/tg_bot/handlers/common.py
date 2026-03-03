# tg_bot/handlers/common.py

import logging
from datetime import date, datetime, timedelta

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, cast, Date, select
import io
import pandas as pd
from datetime import date, datetime, timedelta
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.fsm.context import FSMContext
from sqlalchemy import select, cast, Date, func
from sqlalchemy.orm import selectinload
from app.db.models import Dialogue, AnalyticsEvent, Account, JobContext
from datetime import date, timedelta
from aiogram.utils.formatting import Text, Bold, Italic
from app.db.models import AnalyticsEvent

from app.db.models import TelegramUser
from app.tg_bot.keyboards import (
    user_keyboard, 
    admin_keyboard, 
    stats_main_menu_keyboard, 
    export_date_options_keyboard,
    back_to_stats_main_keyboard
)

logger = logging.getLogger(__name__)
router = Router()

# Состояния для FSM (экспорт Excel)
class ExportStates(StatesGroup):
    waiting_for_range = State()



async def _build_7day_stats_content(session: AsyncSession) -> Text:
    # 1. Определяем диапазон дат
    today = date.today()
    start_date = today - timedelta(days=6)
    
    # 2. Собираем агрегированную статистику за один запрос
    # Группируем по дате и типу события
    stmt = (
        select(
            cast(AnalyticsEvent.created_at, Date).label('day'),
            AnalyticsEvent.event_type,
            func.count(AnalyticsEvent.id).label('count')
        )
        .where(AnalyticsEvent.created_at >= start_date)
        .group_by('day', AnalyticsEvent.event_type)
    )
    
    result = await session.execute(stmt)
    raw_data = result.all()
    
    # 3. Структурируем данные: { дата: { тип_события: кол-во } }
    stats_map = {}
    for row in raw_data:
        day = row.day
        if day not in stats_map:
            stats_map[day] = {}
        stats_map[day][row.event_type] = row.count

    # 4. Формируем текстовый отчет
    content_parts = [
        Bold("📊 Статистика за последние 7 дней:"), "\n", 
        Italic("(на основе событий системы)"), "\n\n"
    ]
    
    has_any_data = False
    # Идем по дням от сегодня назад
    for i in range(7):
        current_day = today - timedelta(days=i)
        day_stats = stats_map.get(current_day, {})
        
        # Считаем показатели согласно твоей шпаргалке
        leads = day_stats.get('lead_created', 0)
        
        if leads == 0 and not day_stats: # Если за день вообще нет событий
            continue
            
        has_any_data = True
        
        qualified = day_stats.get('qualified', 0)
        # Отказы = бот + сам кандидат
        rejected = day_stats.get('rejected_by_bot', 0) + day_stats.get('rejected_by_candidate', 0)
        timed_out = day_stats.get('timed_out', 0)
        
        # В работе = Всего откликов - (Подошло + Отказано + Молчуны)
        in_progress = leads - (qualified + rejected + timed_out)
        if in_progress < 0: in_progress = 0 # На случай, если события разнесены по дням

        day_str = current_day.strftime('%d.%m (%a)')
        content_parts.extend([
            Bold(f"📅 {day_str}"), "\n",
            f"   Откликов: ", Bold(str(leads)), "\n",
            f"   - Подошло: ", Bold(str(qualified)), "\n",
            f"   - Отказов: ", Bold(str(rejected)), "\n",
            f"   - Молчуны: ", Bold(str(timed_out)), "\n",
            f"   - В работе: ", Bold(str(in_progress)), "\n",
            "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
        ])

    if not has_any_data:
        return Text("📊 Данных за последние 7 дней не найдено.")

    return Text(*content_parts)

# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ EXCEL ---

async def generate_and_send_excel(message: Message, start_date: date, end_date: date, session: AsyncSession, state: FSMContext):
    msg_wait = await message.answer("⏳ Формирую детальный отчет по данным Авито...")
    
    # 1. СБОР ДИАЛОГОВ
    stmt = (
        select(Dialogue)
        .options(
            selectinload(Dialogue.account),
            selectinload(Dialogue.vacancy)
        )
        .where(
            cast(Dialogue.created_at, Date) >= start_date,
            cast(Dialogue.created_at, Date) <= end_date
        )
    )
    result = await session.execute(stmt)
    dialogues = result.scalars().all()

    if not dialogues:
        await msg_wait.edit_text("🤷 За этот период откликов не найдено.")
        await state.clear()
        return

    # 2. СБОР СОБЫТИЙ ДЛЯ ЭТИХ ДИАЛОГОВ (чтобы посчитать таймауты и отказы)
    diag_ids = [d.id for d in dialogues]
    event_stmt = select(AnalyticsEvent).where(AnalyticsEvent.dialogue_id.in_(diag_ids))
    event_result = await session.execute(event_stmt)
    events = event_result.scalars().all()
    
    # Группируем события по диалогам для быстрого доступа
    events_map = {}
    for e in events:
        if e.dialogue_id not in events_map:
            events_map[e.dialogue_id] = set()
        events_map[e.dialogue_id].add(e.event_type)

    # 3. ОБРАБОТКА ДАННЫХ (report_map)
    report_map = {}
    
    for d in dialogues:
        dt = d.created_at.strftime("%d.%m.%Y")
        acc_name = d.account.name if d.account else "Не указан"
        city = d.vacancy.city if d.vacancy else "Не указан"
        vac_title = d.vacancy.title if d.vacancy else "Не указана"
        key = (dt, acc_name, city, vac_title)

        if key not in report_map:
            report_map[key] = {
                "отклики_всего": 0, "не_вступили": 0, "начали_диалог": 0,
                "собес": 0, "отказался_кд": 0, "отказали_мы": 0, "молчуны": 0
            }

        m = report_map[key]
        d_events = events_map.get(d.id, set())
        
        # 1. Отклики всегда +1 (так как диалог существует)
        m["отклики_всего"] += 1
        
        # 2. Проверка контакта через событие 'first_contact'
        has_contact = 'first_contact' in d_events
        
        if has_contact:
            m["начали_диалог"] += 1
        else:
            m["не_вступили"] += 1
            
        # 3. Собеседования
        if 'qualified' in d_events:
            m["собес"] += 1
        
        # 4. Отказы
        if 'rejected_by_candidate' in d_events:
            m["отказался_кд"] += 1
        if 'rejected_by_bot' in d_events:
            m["отказали_мы"] += 1
            
        # 5. Молчуны (был первый контакт, но потом случился таймаут)
        if 'timed_out' in d_events and has_contact:
            m["молчуны"] += 1

    # 4. ФОРМИРОВАНИЕ СТРОК
    rows = []
    for (dt, acc, cit, vac), m in report_map.items():
        rows.append({
            "Дата": dt, "Рекрутер": acc, "Город": cit, "Вакансия": vac,
            "Отклики": m["отклики_всего"], 
            "Не вступили": m["не_вступили"],
            "Начали диалог": m["начали_диалог"], 
            "Собес": m["собес"],
            "Отказался КД": m["отказался_кд"], 
            "Отказали мы": m["отказали_мы"], 
            "Молчуны": m["молчуны"],
            "Отказы всего": m["отказался_кд"] + m["отказали_мы"]
        })

    df_base = pd.DataFrame(rows)
    df_base['dt_obj'] = pd.to_datetime(df_base['Дата'], format='%d.%m.%Y')
    df_base = df_base.sort_values(['dt_obj', 'Рекрутер']).drop(columns=['dt_obj'])

    # 5. СВОДНЫЕ ТАБЛИЦЫ
    def create_summary(groupby_col):
        s = df_base.groupby(groupby_col).agg({
            'Отклики': 'sum', 'Не вступили': 'sum', 'Начали диалог': 'sum', 
            'Собес': 'sum', 'Отказался КД': 'sum', 'Отказали мы': 'sum', 
            'Молчуны': 'sum', 'Отказы всего': 'sum'
        }).reset_index()
        
        s['Собес/отклик %'] = (s['Собес'] / s['Отклики']).fillna(0)
        s['Молчуны/Диалог %'] = (s['Молчуны'] / s['Начали диалог']).fillna(0)
        s['Отказы/Диалог %'] = (s['Отказы всего'] / s['Начали диалог']).fillna(0)
        
        total = s.sum(numeric_only=True)
        total[groupby_col] = 'ИТОГО'
        t_resp = total['Отклики'] if total['Отклики'] > 0 else 1
        t_dial = total['Начали диалог'] if total['Начали диалог'] > 0 else 1
        total['Собес/отклик %'] = total['Собес'] / t_resp
        total['Молчуны/Диалог %'] = total['Молчуны'] / t_dial
        total['Отказы/Диалог %'] = total['Отказы всего'] / t_dial
        
        return pd.concat([s, pd.DataFrame([total])], ignore_index=True)

    # Листы
    df_date = create_summary('Дата')
    df_acc = create_summary('Рекрутер')
    df_city = create_summary('Город')
    df_vac = create_summary('Вакансия')

    # 6. СОХРАНЕНИЕ В EXCEL
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_date.to_excel(writer, index=False, sheet_name='Свод по датам')
        df_acc.to_excel(writer, index=False, sheet_name='Свод по рекрутерам')
        df_city.to_excel(writer, index=False, sheet_name='Свод по городам')
        df_vac.to_excel(writer, index=False, sheet_name='Свод по вакансиям')
        df_base.to_excel(writer, index=False, sheet_name='Общий отчет')

        workbook = writer.book
        num_fmt = workbook.add_format({'border': 1, 'align': 'center'})
        perc_fmt = workbook.add_format({'num_format': '0%', 'border': 1, 'align': 'center'})
        
        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            ws.freeze_panes(1, 0)
            ws.set_column('A:Z', 15, num_fmt)
            # Применяем проценты к колонкам с %
            current_cols = df_date.columns if 'Свод' in sheet_name else df_base.columns
            for i, col in enumerate(current_cols):
                if '%' in col:
                    ws.set_column(i, i, 18, perc_fmt)

    output.seek(0)
    await message.answer_document(
        BufferedInputFile(output.read(), filename=f"Report_Avito_{start_date}_{end_date}.xlsx"),
        caption=f"📈 Детальная статистика ({start_date} - {end_date})"
    )
    await msg_wait.delete()
    await state.clear()


@router.message(CommandStart())
async def handle_start(message: Message, session: AsyncSession):
    """
    Обработка команды /start. 
    Проверяет права доступа и выдает соответствующую клавиатуру.
    """
    if not message.from_user:
        return

    # Ищем пользователя в БД
    stmt = select(TelegramUser).where(TelegramUser.telegram_id == message.from_user.id)
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()

    if not user:
        await message.answer("❌ Нет доступа. Обратитесь к администратору.")
        return

    # Выбираем клавиатуру в зависимости от роли
    kb = admin_keyboard if user.role == 'admin' else user_keyboard
    
    await message.answer(
        f"👋 Привет, {message.from_user.first_name or 'Коллега'}!\n"
        f"Бот готов к работе.", 
        reply_markup=kb
    )




# --- МЕНЮ СТАТИСТИКИ ---

@router.message(F.text == "📊 Статистика")
async def stats_main_menu(message: Message):
    """Открывает меню выбора статистики"""
    await message.answer(
        "Выберите действие:", 
        reply_markup=stats_main_menu_keyboard
    )


@router.callback_query(F.data == "stats_back_to_main")
async def stats_back_to_main(callback: CallbackQuery):
    """Возврат в главное меню статистики (если будем делать вложенность)"""
    await callback.message.edit_text(
        "Выберите действие:", 
        reply_markup=stats_main_menu_keyboard
    )
    await callback.answer()




@router.callback_query(F.data == "view_stats_7days")
async def view_text_stats(callback: CallbackQuery, session: AsyncSession):
    """
    Выводит реальную текстовую статистику за 7 дней, 
    используя таблицу AnalyticsEvent.
    """
    # 1. Вызываем функцию подсчета (она вернет объект Text)
    content = await _build_7day_stats_content(session)
    
    # 2. Редактируем сообщение, распаковывая форматирование через as_kwargs()
    # parse_mode указывать не нужно, as_kwargs сам подставит нужный (HTML или MarkdownV2)
    await callback.message.edit_text(
        **content.as_kwargs(), 
        reply_markup=back_to_stats_main_keyboard
    )
    
    # 3. Отвечаем на колбэк, чтобы убрать "часики" в телеграме
    await callback.answer()



@router.callback_query(F.data == "export_excel_start")
async def export_start(callback: CallbackQuery, state: FSMContext):
    """Начало сценария выгрузки Excel"""
    await state.set_state(ExportStates.waiting_for_range)
    await callback.message.answer(
        "За какой период выгрузить данные?\n\n"
        "Выберите кнопку или пришлите диапазон вручную:\n"
        "<code>01.12.2025 - 15.12.2025</code>",
        reply_markup=export_date_options_keyboard,
        parse_mode="HTML"
    )
    await callback.answer()

@router.callback_query(ExportStates.waiting_for_range, F.data.startswith("export_range_"))
async def export_range_quick(callback: CallbackQuery, state: FSMContext, session: AsyncSession):
    days_count = int(callback.data.split("_")[-1])
    end_date = date.today()
    start_date = end_date - timedelta(days=days_count-1)
    await generate_and_send_excel(callback.message, start_date, end_date, session, state)
    await callback.answer()

@router.message(ExportStates.waiting_for_range)
async def export_range_manual(message: Message, state: FSMContext, session: AsyncSession):
    try:
        parts = message.text.split("-")
        start_date = datetime.strptime(parts[0].strip(), "%d.%m.%Y").date()
        end_date = datetime.strptime(parts[1].strip(), "%d.%m.%Y").date()
        if (end_date - start_date).days > 60:
            await message.answer("❌ Ошибка: период не может превышать 60 дней.")
            return
        await generate_and_send_excel(message, start_date, end_date, session, state)
    except Exception:
        await message.answer("❌ Неверный формат. Пример: 01.12.2025 - 10.12.2025")


@router.callback_query(F.data == "cancel_fsm")
async def cancel_fsm(callback: CallbackQuery, state: FSMContext):
    """Отмена любого ввода"""
    await state.clear()
    await callback.message.delete()
    await callback.answer("❌ Отменено")