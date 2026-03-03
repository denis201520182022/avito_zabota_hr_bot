# app/utils/pii_masker.py
import re
from typing import Tuple, Optional

# --- РЕГУЛЯРНЫЕ ВЫРАЖЕНИЯ ---
# Паттерн для поиска ФИО (Фамилия Имя Отчество с большой буквы)
FIO_PATTERN = re.compile(
    r'\b([А-ЯЁ][а-яё]+(?:-[А-ЯЁ][а-яё]+)?)\s+([А-ЯЁ][а-яё]+)\s+(([А-ЯЁ][а-яё]+))?\b'
)

# Паттерн для поиска телефонов в различных форматах (+7, 8, скобки, тире)
PHONE_PATTERN = re.compile(
    r'(?:\+7|8)?[ \-.(]*(\d{3})[ \-.)]*(\d{3})[ \-.]*(\d{2})[ \-.]*(\d{2})\b'
)

# Токены маскировки
FIO_MASK_TOKEN = "[ФИО ЗАМАСКИРОВАНО]"
PHONE_MASK_TOKEN = "[ТЕЛЕФОН ЗАМАСКИРОВАН]"


def extract_and_mask_pii(text: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Извлекает ФИО и номер телефона, а затем полностью заменяет их на токены в тексте.
    Возвращает: (замаскированный_текст, извлеченное_фио, извлеченный_телефон)
    
    Используется для защиты персональных данных перед отправкой в OpenAI.
    """
    if not text:
        return "", None, None

    extracted_fio = None
    extracted_phone = None
    masked_text = text
    
    # --- 1. Извлечение и маскировка телефона ---
    phone_match = PHONE_PATTERN.search(masked_text)
    if phone_match:
        # Очищаем от мусора, оставляем только цифры
        full_phone_digits = "".join(filter(str.isdigit, phone_match.group(0)))
        
        # Нормализуем номер до международного формата (11 цифр, начинается с 7)
        if len(full_phone_digits) == 11 and full_phone_digits.startswith('8'):
            extracted_phone = '7' + full_phone_digits[1:]
        elif len(full_phone_digits) == 10:
            extracted_phone = '7' + full_phone_digits
        else:
            extracted_phone = full_phone_digits

        # Заменяем всё найденное вхождение на токен
        masked_text = PHONE_PATTERN.sub(PHONE_MASK_TOKEN, masked_text, count=1)

    # --- 2. Извлечение и маскировка ФИО ---
    fio_match = FIO_PATTERN.search(masked_text)
    if fio_match:
        # Извлекаем найденное ФИО целиком
        extracted_fio = fio_match.group(0).strip()
        
        # Заменяем всё найденное вхождение на токен
        masked_text = FIO_PATTERN.sub(FIO_MASK_TOKEN, masked_text, count=1)

    return masked_text, extracted_fio, extracted_phone