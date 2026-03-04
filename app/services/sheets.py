# app/services/sheets.py
import logging
import re
import asyncio
import json
from typing import List, Dict, Optional, Any
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import datetime  # <--- ДОБАВИТЬ ЭТО
from datetime import datetime as dt_obj # Для удобства
from app.core.config import settings
from app.core.rabbitmq import mq 

logger = logging.getLogger("google_sheets")

class GoogleSheetsService:
    def __init__(self):
        self.spreadsheet_url = settings.google_sheets.spreadsheet_url
        self.creds_path = settings.knowledge_base.credentials_json
        self.calendar_sheet = settings.google_sheets.calendar_sheet_name
        self.candidates_sheet = settings.google_sheets.candidates_sheet_name
        
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self._spreadsheet_id = self._extract_id(self.spreadsheet_url)

    def _extract_id(self, url: str) -> str:
        match = re.search(r"/d/([a-zA-Z0-9-_]+)", url)
        if match: return match.group(1)
        raise ValueError(f"Не удалось извлечь ID таблицы из ссылки: {url}")

    def _date_to_ru_human(self, date_iso: str) -> str:
        """Переводит '2026-03-02' -> 'понедельник, 2 марта'"""
        try:
            dt = datetime.datetime.strptime(date_iso, "%Y-%m-%d")
            weekdays = ["понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье"]
            months = ["января", "февраля", "марта", "апреля", "мая", "июня", 
                      "июля", "августа", "сентября", "октября", "ноября", "декабря"]
            return f"{weekdays[dt.weekday()]}, {dt.day} {months[dt.month-1]}"
        except:
            return date_iso

    def _ru_human_to_iso(self, human_date: str) -> str:
        """Переводит 'понедельник, 2 марта' -> '2026-03-02'"""
        try:
            # Предполагаем текущий год, так как в таблице его нет
            year = datetime.datetime.now().year
            months = ["января", "февраля", "марта", "апреля", "мая", "июня", 
                      "июля", "августа", "сентября", "октября", "ноября", "декабря"]
            
            # Убираем день недели: '2 марта'
            parts = human_date.split(", ")[-1].split(" ")
            day = int(parts[0])
            month = months.index(parts[1]) + 1
            return f"{year}-{month:02d}-{day:02d}"
        except:
            return human_date


    async def _send_critical_alert(self, error_text: str, payload: Optional[Dict] = None):
        """
        Отправка алерта с подробным дампом данных, которые не удалось обработать.
        """
        try:
            full_message = f"🚨 **ОШИБКА GOOGLE SHEETS**\n\n**Проблема:** {error_text}"
            
            if payload:
                # Форматируем данные в JSON блок, чтобы удобно было копировать
                data_dump = json.dumps(payload, indent=2, ensure_ascii=False)
                full_message += f"\n\n**Данные для ручного ввода:**\n```json\n{data_dump}\n```"

            await mq.publish("tg_alerts", {
                "type": "system",
                "text": full_message,
                "alert_type": "admin_only"
            })
        except Exception as e:
            logger.error(f"Не удалось отправить алерт: {e}")

    def _get_service(self):
        creds = Credentials.from_service_account_file(self.creds_path, scopes=self.scopes)
        return build('sheets', 'v4', credentials=creds, cache_discovery=False)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(Exception),
        reraise=True
    )
    async def _execute_google_call(self, func, *args, **kwargs):
        return await asyncio.to_thread(func(*args, **kwargs).execute)

    async def _get_all_calendar_rows(self) -> List[List[str]]:
        try:
            service = await asyncio.to_thread(self._get_service)
            range_name = f"'{self.calendar_sheet}'!A2:F500"
            result = await self._execute_google_call(
                service.spreadsheets().values().get,
                spreadsheetId=self._spreadsheet_id, range=range_name
            )
            return result.get('values', [])
        except Exception as e:
            await self._send_critical_alert(f"Сбой чтения календаря: {e}")
            raise e

    # --- МЕТОДЫ ДЛЯ КАЛЕНДАРЯ ---

    async def book_slot(self, target_date: str, target_time: str, candidate_name: str) -> bool:
        """Занимает слот"""
        return await self._update_slot_status(target_date, target_time, "Занято", candidate_name)

    async def release_slot(self, target_date: str, target_time: str, candidate_name: str) -> bool:
        """Освобождает слот конкретного рекрутера"""
        if not target_date or not target_time or not candidate_name: return False
        return await self._update_slot_status(target_date, target_time, "Свободно", candidate_name)


    async def _update_slot_status(self, target_date: str, target_time: str, status: str, name: str) -> bool:
        context = {"date": target_date, "time": target_time, "status": status, "candidate": name}
        
        try:
            search_date = self._date_to_ru_human(target_date)
            rows = await self._get_all_calendar_rows()
            for idx, row in enumerate(rows):
                if len(row) >= 2 and row[0].strip() == search_date and row[1].strip() == target_time:
                    row_number = idx + 2
                    service = await asyncio.to_thread(self._get_service)
                    
                    # --- ЛОГИКА БРОНИРОВАНИЯ ---
                    if status == "Занято":
                        roman_busy = len(row) > 2 and row[2].strip() == "Занято"
                        nastya_busy = len(row) > 4 and row[4].strip() == "Занято"
                        
                        if not roman_busy:
                            target_range = f"'{self.calendar_sheet}'!C{row_number}:D{row_number}"
                        elif not nastya_busy:
                            target_range = f"'{self.calendar_sheet}'!E{row_number}:F{row_number}"
                        else:
                            return False # Оба заняты

                        body = {'values': [["Занято", name]]}
                        await self._execute_google_call(
                            service.spreadsheets().values().update,
                            spreadsheetId=self._spreadsheet_id, range=target_range,
                            valueInputOption="RAW", body=body
                        )
                        return True
                    
                    # --- ЛОГИКА ОСВОБОЖДЕНИЯ (release_slot) ---
                    else:
                        # Проверяем, в какой колонке записано имя кандидата
                        is_at_roman = len(row) > 3 and row[3].strip() == name
                        is_at_nastya = len(row) > 5 and row[5].strip() == name
                        
                        if is_at_roman:
                            target_range = f"'{self.calendar_sheet}'!C{row_number}:D{row_number}"
                        elif is_at_nastya:
                            target_range = f"'{self.calendar_sheet}'!E{row_number}:F{row_number}"
                        else:
                            return False # Кандидат не найден

                        await self._execute_google_call(
                            service.spreadsheets().values().update,
                            spreadsheetId=self._spreadsheet_id, range=target_range,
                            valueInputOption="RAW", body={'values': [["", ""]]}
                        )
                        return True
            return False
        except Exception as e:
            await self._send_critical_alert(f"Ошибка обновления слота: {e}", context)
            return False

    # --- МЕТОДЫ ДЛЯ КАНДИДАТОВ ---

    async def append_candidate(self, data: Dict[str, Any]):
        """Добавляет строку в общую таблицу кандидатов"""
        try:
            service = await asyncio.to_thread(self._get_service)
            row_values = [
                data.get("full_name", ""),
                data.get("phone", ""),
                data.get("vacancy", ""),
                data.get("chat_link", ""),
                data.get("interview_dt", "Не назначено"),
                data.get("status", "Квалифицирован")
            ]
            await self._execute_google_call(
                service.spreadsheets().values().append,
                spreadsheetId=self._spreadsheet_id,
                range=f"'{self.candidates_sheet}'!A:F",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={'values': [row_values]}
            )
        except Exception as e:
            # Отправляем в алерт все данные кандидата, которые не записались
            await self._send_critical_alert(
                f"Ошибка записи кандидата в общий список: {e}", 
                {"sheet": "Кандидаты", "data": data}
            )

    async def get_available_slots(self, target_date: str) -> List[str]:
        search_date = self._date_to_ru_human(target_date)
        try:
            rows = await self._get_all_calendar_rows()
            available_times = []
            for row in rows:
                if len(row) < 2: continue
                if row[0].strip() == search_date:
                    # Слот свободен, если в C или D пусто
                    roman_free = len(row) <= 2 or not row[2].strip()
                    nastya_free = len(row) <= 4 or not row[4].strip()
                    
                    if roman_free or nastya_free:
                        available_times.append(row[1].strip())
            return available_times
        except Exception:
            return []

    async def get_all_slots_map(self) -> Dict[str, List[str]]:
        try:
            rows = await self._get_all_calendar_rows()
            slots_map = {}
            for row in rows:
                if len(row) < 2: continue
                
                # ПЕРЕВОДИМ ИЗ ТАБЛИЦЫ В ISO
                d_iso = self._ru_human_to_iso(row[0].strip())
                t = row[1].strip()
                
                roman_free = len(row) <= 2 or not row[2].strip()
                nastya_free = len(row) <= 4 or not row[4].strip()
                
                if roman_free or nastya_free:
                    if d_iso not in slots_map: slots_map[d_iso] = []
                    slots_map[d_iso].append(t)
            return slots_map
        except Exception:
            return {}

    

sheets_service = GoogleSheetsService()