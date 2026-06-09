"""Разбор и применение масок времени @t согласно спецификации Diasoft."""
import re
from typing import Optional
import datetime


# ---------------------------------------------------------------------------
# Маски времени @t — форматирование временных значений
# ---------------------------------------------------------------------------
# Формат: @t <Тип>[<Разделитель>]['b']['t']
#
# Типы времени (число 1-4):
#   1 — чч:мм          (HH:MM)
#   2 — ччмм           (HHMM, без разделителей)
#   3 — чч:мм[am/pm]   (HH:MMam/pm, 12-часовой формат)
#   4 — чч:мм:сс       (HH:MM:SS)
#
# Параметры:
#   <Разделитель> — символ разделителя (переопределяет дефолтный для типа):
#     .  → .
#     ,  → ,
#     -  → -
#     _  → : (двоеточие по умолчанию)
#     '  → '
#     `  → `
#     =  → =
#   b / B — пробелы вместо ведущих нулей
#   t / T — trim (удалить ведущие и оконечные заполнители)


# Карта символов разделителей
_SEP_MAP = {
    '.': '.',
    ',': ',',
    '-': '-',
    '_': ':',
    "'": "'",
    '`': '`',
    '=': '=',
}

# Дефолтные разделители для каждого типа времени
_DEFAULT_SEPS = {
    1: ':',   # чч:мм
    2: '',    # ччмм (без разделителей)
    3: ':',   # чч:ммam/pm
    4: ':',   # чч:мм:сс
}


def parse_t_mask(mask: str) -> dict:
    """
    Разбирает маску времени @t и возвращает словарь параметров.

    Args:
        mask: Маска вида @t... (уже проверено, что начинается с @t)

    Returns:
        Словарь с ключами:
            time_type      — int, тип времени (1-4)
            separator      — str, символ разделителя
            blank_zero     — bool, пробелы вместо ведущих нулей
            trim           — bool, удалить заполнители
    """
    # Убираем префикс @t
    m = mask[2:].strip()

    if not m:
        return {
            'time_type': 1,
            'separator': ':',
            'blank_zero': False,
            'trim': False,
        }

    # --- Читаем <Тип> ---
    type_match = re.match(r'^(\d+)', m)
    if not type_match:
        return {
            'time_type': 1,
            'separator': ':',
            'blank_zero': False,
            'trim': False,
        }

    time_type = int(type_match.group(1))
    if time_type < 1 or time_type > 4:
        time_type = 1  # дефолт
    m = m[len(type_match.group(1)):]

    # --- Дефолтный разделитель для типа ---
    separator = _DEFAULT_SEPS.get(time_type, ':')

    # --- Опциональный разделитель (переопределение) ---
    if m and m[0] in _SEP_MAP:
        separator = _SEP_MAP[m[0]]
        m = m[1:]

    # --- Флаг blank_zero: b / B ---
    blank_zero = False
    if m and m[0] in ('b', 'B'):
        blank_zero = True
        m = m[1:]

    # --- Флаг trim: t / T ---
    trim = False
    if m and m[0] in ('t', 'T'):
        trim = True
        m = m[1:]

    return {
        'time_type': time_type,
        'separator': separator,
        'blank_zero': blank_zero,
        'trim': trim,
    }


def _parse_time_value(value) -> Optional[tuple]:
    """
    Парсит значение времени в кортеж (hour, minute, second).

    Поддерживаемые форматы входного значения:
      - datetime.time
      - datetime.datetime
      - Строка "HH:MM:SS", "HH:MM", "HHMMSS", "HHMM"
      - Число (считается как секунды от начала дня)

    Args:
        value: Значение времени

    Returns:
        Кортеж (hour, minute, second) или None при ошибке
    """
    # datetime.time
    if isinstance(value, datetime.time):
        return (value.hour, value.minute, value.second)

    # datetime.datetime
    if isinstance(value, datetime.datetime):
        return (value.hour, value.minute, value.second)

    # Строка
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        # Пробуем форматы с датой и временем (извлекаем только время)
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M',
                     '%Y/%m/%d %H:%M:%S', '%Y/%m/%d %H:%M',
                     '%d.%m.%Y %H:%M:%S', '%d.%m.%Y %H:%M',
                     '%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M'):
            try:
                t = datetime.datetime.strptime(s, fmt).time()
                return (t.hour, t.minute, t.second)
            except ValueError:
                continue

        # HHMMSS / HHMM (без разделителей)
        digits = re.sub(r'[^\d]', '', s)
        if len(digits) == 6:
            h, m, sec = int(digits[:2]), int(digits[2:4]), int(digits[4:6])
            if 0 <= h <= 23 and 0 <= m <= 59 and 0 <= sec <= 59:
                return (h, m, sec)
        elif len(digits) == 4:
            h, m = int(digits[:2]), int(digits[2:4])
            if 0 <= h <= 23 and 0 <= m <= 59:
                return (h, m, 0)

        # Чистое число (секунды)
        try:
            total_seconds = float(s)
            h = int(total_seconds // 3600) % 24
            m = int((total_seconds % 3600) // 60)
            sec = int(total_seconds % 60)
            return (h, m, sec)
        except ValueError:
            pass

        return None

    # Числовой тип
    if isinstance(value, (int, float)):
        total_seconds = float(value)
        h = int(total_seconds // 3600) % 24
        m = int((total_seconds % 3600) // 60)
        sec = int(total_seconds % 60)
        return (h, m, sec)

    return None


def _to_12_hour(hour: int) -> tuple:
    """
    Конвертирует 24-часовой формат в 12-часовой.

    Args:
        hour: Час в 24-часовом формате (0-23)

    Returns:
        Кортеж (hour_12, am_pm) где hour_12 в диапазоне 1-12
    """
    if hour == 0:
        return (12, 'am')  # полуночь
    elif hour < 12:
        return (hour, 'am')
    elif hour == 12:
        return (12, 'pm')  # полдень
    else:
        return (hour - 12, 'pm')


def apply_t_mask(value, params: dict) -> str:
    """
    Применяет разобранные параметры маски времени @t к значению.

    Args:
        value: Значение времени (str, int, float, datetime.time, datetime.datetime)
        params: Словарь параметров из parse_t_mask()

    Returns:
        Отформатированная строка времени
    """
    time_tuple = _parse_time_value(value)
    if time_tuple is None:
        return str(value)

    hour, minute, second = time_tuple
    time_type = params['time_type']
    separator = params['separator']
    blank_zero = params['blank_zero']
    trim = params['trim']

    result = ''

    if time_type == 1:
        # чч:мм
        result = f"{hour:02d}{separator}{minute:02d}"

    elif time_type == 2:
        # ччмм (без разделителей)
        result = f"{hour:02d}{minute:02d}"

    elif time_type == 3:
        # чч:мм[am/pm]
        hour_12, am_pm = _to_12_hour(hour)
        result = f"{hour_12:02d}{separator}{minute:02d}{am_pm}"

    elif time_type == 4:
        # чч:мм:сс
        result = f"{hour:02d}{separator}{minute:02d}{separator}{second:02d}"

    # blank_zero: заменяем ведущие нули на пробелы
    if blank_zero:
        parts = re.split(r'(?<!\d)', result)
        formatted_parts = []
        for part in parts:
            if part.isdigit():
                stripped = part.lstrip('0')
                if stripped:
                    padded = ' ' * (len(part) - len(stripped)) + stripped
                else:
                    padded = ' ' * len(part)
                formatted_parts.append(padded)
            elif part.lower() in ('am', 'pm'):
                formatted_parts.append(part)
            elif separator and part == separator:
                formatted_parts.append(separator)
            else:
                formatted_parts.append(part)
        result = ''.join(formatted_parts)

    # trim: удаляем ведущие и оконечные пробелы
    if trim:
        result = result.strip()

    return result


def format_time_with_mask(value, mask: str) -> str:
    """
    Удобная обёртка: разбирает маску @t и применяет её к значению за один вызов.

    Args:
        value: Значение времени
        mask: Маска форматирования вида @t...

    Returns:
        Отформатированная строка
    """
    params = parse_t_mask(mask)
    return apply_t_mask(value, params)