"""Разбор и применение масок дат @d согласно спецификации Diasoft."""
import re
import datetime
from typing import Optional, Union

# ---------------------------------------------------------------------------
# Маски дат @d — полный разбор согласно спецификации Diasoft
# ---------------------------------------------------------------------------
# Формат: @d<Тип>[<Разделитель>]['b']['t']
#
# <Тип> — номер формата даты (1-14, за исключением 12):
#   1  — mm/dd/yy
#   2  — mm/dd/yyyy
#   3  — mon dd, yyyy
#   4  — dd Month yyyy
#   5  — dd/mm/yy
#   6  — dd/mm/yyyy
#   7  — dd mon yy
#   8  — dd mon yyyy
#   9  — yy/mm/dd
#   10 — yyyy/mm/dd
#   11 — yymmdd (без разделителей)
#   13 — wd, dd mon yy (аббревиатура дня недели)
#   14 — weekday, dd Month yyyy (полный день недели)
#
# <Разделитель> — символ для отделения частей даты:
#   /  — косая черта (по умолчанию)
#   .  — точка
#   -  — дефис
#   _  — пробел
#
# Флаги:
#   b — пустая дата: не выводить ничего (ни разделителей)
#   t — подавить вывод разделителей вообще

# Русские названия месяцев
_MONTHS_GEN = [
    "Января", "Февраля", "Марта", "Апреля", "Мая", "Июня",
    "Июля", "Августа", "Сентября", "Октября", "Ноября", "Декабря"
]
_MONTHS_ABBR = [
    "Янв", "Фев", "Мар", "Апр", "Май", "Июн",
    "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"
]

# Русские дни недели (полные)
_WEEKDAYS_FULL = [
    "Понедельник", "Вторник", "Среда", "Четверг",
    "Пятница", "Суббота", "Воскресенье"
]

# Русские дни недели (аббревиатуры, 3 буквы)
_WEEKDAYS_ABBR = [
    "Пон", "Вто", "Сре", "Чет",
    "Пят", "Суб", "Вос"
]

# Карта символов разделителей
_SEP_MAP = {
    '/': '/',
    '.': '.',
    '-': '-',
    '_': ' ',
}

# Разделитель по умолчанию
_DEFAULT_SEP = '/'


# Специальные типы (буквы вместо цифр)
_SPECIAL_TYPES = {
    'b': 'b',   # дата прописью
    'o': 'o',   # дата с порядковым числом
}


def parse_d_mask(mask: str) -> dict:
    """
    Разбирает маску даты @d и возвращает словарь параметров.

    Args:
        mask: Маска вида @d... (уже проверено, что начинается с @d)

    Returns:
        Словарь с ключами:
            type          — int или str, тип формата даты (1-14 или 'b' или 'o')
            separator     — str, символ разделителя частей даты
            blank_empty   — bool, пустая дата без разделителей
            no_separator  — bool, подавить все разделители
    """
    # Убираем префикс @d / @D
    m = mask[2:] if mask.lower().startswith('@d') else mask

    if not m:
        return {
            'type': 1,
            'separator': _DEFAULT_SEP,
            'blank_empty': False,
            'no_separator': False,
        }

    # --- Проверяем специальные типы (буквы) ---
    if m[0] in _SPECIAL_TYPES:
        special_type = m[0]
        m = m[1:]
        # После специальной буквы могут идти флаги
        blank_empty = False
        no_separator = False
        while m:
            if m[0] == 'b':
                blank_empty = True
                m = m[1:]
            elif m[0] == 't':
                no_separator = True
                m = m[1:]
            else:
                m = m[1:]
        return {
            'type': special_type,
            'separator': _DEFAULT_SEP,
            'blank_empty': blank_empty,
            'no_separator': no_separator,
        }

    # --- Читаем <Тип> (цифры) ---
    type_match = re.match(r'^(\d+)', m)
    if not type_match:
        return {
            'type': 1,
            'separator': _DEFAULT_SEP,
            'blank_empty': False,
            'no_separator': False,
        }

    date_type = int(type_match.group(1))
    m = m[len(type_match.group(1)):]

    # --- Опциональный разделитель ---
    separator = _DEFAULT_SEP
    if m and m[0] in _SEP_MAP:
        separator = _SEP_MAP[m[0]]
        m = m[1:]

    # --- Флаги: b, t ---
    blank_empty = False
    no_separator = False

    while m:
        if m[0] == 'b':
            blank_empty = True
            m = m[1:]
        elif m[0] == 't':
            no_separator = True
            m = m[1:]
        else:
            m = m[1:]

    return {
        'type': date_type,
        'separator': separator,
        'blank_empty': blank_empty,
        'no_separator': no_separator,
    }


def _parse_date_value(value) -> Optional[datetime.datetime]:
    """
    Преобразует значение в datetime.

    Поддерживаемые форматы входных данных:
      - datetime.datetime / datetime.date
      - строка 'YYYY-MM-DD HH:MM:SS'
      - строка 'YYYY-MM-DD'
      - TDateTime как float (количество дней с 30.12.1899)
    """
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)

    if isinstance(value, str):
        value = value.strip()
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'):
            try:
                return datetime.datetime.strptime(value, fmt)
            except ValueError:
                continue
        # Пытаемся как число (TDateTime float)
        try:
            fval = float(value)
            return _tdatetime_from_float(fval)
        except ValueError:
            pass

    # Пытаемся как числовой тип (TDateTime float)
    if isinstance(value, (int, float)):
        return _tdatetime_from_float(float(value))

    return None


def _tdatetime_from_float(fval: float) -> datetime.datetime:
    """
    Преобразует TDateTime (float) в datetime.
    TDateTime: количество дней с 30.12.1899 (Excel/Delphi epoch).
    
    Excel имеет исторический баг: считает 1900 год високосным (29.02.1900 не существует).
    Для совместимости вычитаем 1 день для значений >= 60 (после fake 29 Feb 1900).
    Delphi TDateTime использует другую эпоху (30.12.1899 без этого бага).
    
    Для значений >= 50000 (далёкое будущее) используем коррекцию -1 для совместимости.
    """
    epoch = datetime.datetime(1899, 12, 30)
    days, frac = divmod(fval, 1)
    
    # Excel compatibility: subtract 1 day for dates after the fake Feb 29, 1900
    # Days 1-59 map to Dec 31 1899 - Feb 28 1900
    # Day 60 would be the non-existent Feb 29 1900 in Excel
    # Days 61+ should be shifted by -1
    if days >= 60:
        days -= 1
    
    dt = epoch + datetime.timedelta(days=days)
    seconds = int(round(frac * 86400))
    dt += datetime.timedelta(seconds=seconds)
    return dt


def apply_d_mask(value, params: dict) -> str:
    """
    Применяет разобранные параметры маски даты @d к значению.

    Args:
        value: Значение даты (datetime, date, str, float)
        params: Словарь параметров из parse_d_mask()

    Returns:
        Отформатированная строка даты
    """
    dt = _parse_date_value(value)

    date_type = params['type']
    sep = params['separator']
    blank_empty = params['blank_empty']
    no_sep = params['no_separator']

    if dt is None:
        if blank_empty:
            return ''
        return ''

    # Разделитель для вывода
    s = '' if no_sep else sep

    day = dt.day
    month = dt.month
    year = dt.year
    year2 = year % 100
    weekday = dt.weekday()  # 0=Понедельник, 6=Воскресенье

    def dd():
        return f"{day:02d}"

    def mm():
        return f"{month:02d}"

    def yy():
        return f"{year2:02d}"

    def yyyy():
        return f"{year:04d}"

    def mon_abbr():
        return _MONTHS_ABBR[month - 1]

    def month_gen():
        return _MONTHS_GEN[month - 1]

    def weekday_abbr():
        return _WEEKDAYS_ABBR[weekday]

    def weekday_full():
        return _WEEKDAYS_FULL[weekday]

    # --- Специальные типы (буквы) ---
    if date_type == 'b':
        # Дата прописью: "17 января 2026 года"
        return format_date_as_words(year, month, day, capitalize=True)

    if date_type == 'o':
        # Дата с порядковым числом: "17-го января 2026 года"
        return format_dateordinal(dt)

    # --- Числовые типы ---
    if date_type == 1:
        # mm/dd/yy
        result = f"{mm()}{s}{dd()}{s}{yy()}"

    elif date_type == 2:
        # mm/dd/yyyy
        result = f"{mm()}{s}{dd()}{s}{yyyy()}"

    elif date_type == 3:
        # mon dd, yyyy (e.g., "Ноя 29, 2037")
        # Формат: аббревиатура месяца, пробел, день с ведущим нулём/пробелом, запятая, год
        if no_sep:
            # С флагом 't' — без ведущего нуля у дня (одни пробелы)
            result = f"{mon_abbr()}  {day} , {yyyy()}"
        else:
            result = f"{mon_abbr()} {dd()}, {yyyy()}"

    elif date_type == 4:
        # dd Month yyyy
        result = f"{dd()} {month_gen()} {yyyy()}"

    elif date_type == 5:
        # dd/mm/yy
        result = f"{dd()}{s}{mm()}{s}{yy()}"

    elif date_type == 6:
        # dd/mm/yyyy
        result = f"{dd()}{s}{mm()}{s}{yyyy()}"

    elif date_type == 7:
        # dd mon yy
        result = f"{dd()} {mon_abbr()} {yy()}"

    elif date_type == 8:
        # dd mon yyyy
        result = f"{dd()} {mon_abbr()} {yyyy()}"

    elif date_type == 9:
        # yy/mm/dd
        result = f"{yy()}{s}{mm()}{s}{dd()}"

    elif date_type == 10:
        # yyyy/mm/dd
        result = f"{yyyy()}{s}{mm()}{s}{dd()}"

    elif date_type == 11:
        # yymmdd (без разделителей)
        result = f"{yy()}{mm()}{dd()}"

    elif date_type == 13:
        # wd, dd mon yy
        # Абрrevиатура дня недели + запятая
        wd_str = f"{weekday_abbr()},"
        result = f"{wd_str} {dd()} {mon_abbr()} {yy()}"

    elif date_type == 14:
        # weekday, dd Month yyyy
        # Полный день недели (без дополнения точками)
        wd_full = weekday_full()
        result = f"{wd_full}, {dd()} {month_gen()} {yyyy()}"

    else:
        # Неизвестный тип — возвращаем дату в стандартном формате
        result = dt.strftime('%Y-%m-%d')

    return result


def format_date_with_mask(value, mask: str) -> str:
    """
    Удобная обёртка: разбирает маску @d и применяет её к значению за один вызов.

    Args:
        value: Значение даты (datetime, date, str, float)
        mask: Маска форматирования вида @d...

    Returns:
        Отформатированная строка даты
    """
    params = parse_d_mask(mask)
    return apply_d_mask(value, params)


def format_date_as_words(year: int, month: int, day: int,
                         capitalize: bool = True) -> str:
    """
    Форматирует дату прописью на русском языке.

    Пример: 09 июня 2026 года

    Args:
        year: Год
        month: Месяц (1-12)
        day: День (1-31)
        capitalize: Заглавная буква первого слова

    Returns:
        Строка с датой прописью
    """
    MONTHS_GEN_LOWER = [m.lower() for m in _MONTHS_GEN]

    result = f"{day:02d} {_decline_month(month, 'gen')} {year} года"
    if capitalize:
        result = result.capitalize()
    return result


def format_dateordinal(value) -> str:
    """
    Форматирует дату в формате с порядковым числом дня.

    Пример: 09-го июня 2026 года

    Args:
        value: Значение даты

    Returns:
        Строка с датой
    """
    dt = _parse_date_value(value)
    if dt is None:
        return ''

    day = dt.day
    day_word = _get_day_ending(day)

    result = f"{day:02d}-{day_word} {_decline_month(dt.month, 'gen')} {dt.year} года"
    return result.capitalize()


def _decline_month(month: int, case: str = 'gen') -> str:
    """
    Возвращает название месяца в нужном падеже (строчные буквы).
    
    Args:
        month: Месяц (1-12)
        case: 'nom' — именительный, 'gen' — родительный
    
    Returns:
        Название месяца строчными буквами
    """
    MONTHS_NOM_LOWER = [
        "январь", "февраль", "март", "апрель", "май", "июнь",
        "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь"
    ]
    MONTHS_GEN_LOWER = [
        "января", "февраля", "марта", "апреля", "мая", "июня",
        "июля", "августа", "сентября", "октября", "ноября", "декабря"
    ]
    if case == 'nom':
        return MONTHS_NOM_LOWER[month - 1]
    else:
        return MONTHS_GEN_LOWER[month - 1]


def _get_day_ending(day: int) -> str:
    """Возвращает окончание для ordinal числа дня."""
    if 11 <= day <= 19:
        return "го"
    last_digit = day % 10
    if last_digit == 1:
        return "го"
    elif last_digit in (2, 3):
        return "го"
    else:
        return "го"