"""Разбор и применение числовых масок @n согласно спецификации Diasoft."""
import re
from typing import Optional

# ---------------------------------------------------------------------------
# Числовые маски @n — полный разбор согласно спецификации Diasoft
# ---------------------------------------------------------------------------
# Формат: @n [-]['_'] <Ширина> ['.'|',']<Дробная> ['b'|'B'] [<Сдвиг>] [<Разделитель>]
#
# Параметры:
#   -               — показать знак минуса для отрицательных чисел
#   _ (после @n)    — без разделителей тысяч (цифры идут сплошь)
#   <Ширина>        — общая длина поля (включая знак, точку, разделители)
#   .N / ,N         — N знаков после запятой; . → точка как десятичный разделитель,
#                     , → запятая как десятичный разделитель
#   b / B           — вместо ведущих нулей ставить пробелы
#   <Сдвиг>:
#     t / T         — trim (удалить ведущие и оконечные пробелы)
#     <             — прижать к левому краю
#     >             — выборка цифр с правого края (подавление разделителей)
#   <Разделитель>   — символ разделителя тысяч:
#     .  → .        ,  → ,        _  → пробел
#     '  → '        `  → `        =  → =        -  → -
#
# Переполнение → поле заполняется '*'
# По умолчанию отрицательные числа выводятся без знака.
# По умолчанию пустота заполняется '.' (для нулевого заполнения — '0').


# Карта символов разделителей тысяч
_THOUSANDS_SEP_MAP = {
    '.': '.',
    ',': ',',
    '_': ' ',
    "'": "'",
    '`': '`',
    '=': '=',
    '-': '-',
}


def parse_n_mask(mask: str) -> dict:
    """
    Разбирает числовую маску @n и возвращает словарь параметров.

    Args:
        mask: Маска вида @n... (уже проверено, что начинается с @n)

    Returns:
        Словарь с ключами:
            show_sign       — bool, показывать ли минус
            no_thousands    — bool, подавить разделители тысяч
            width           — int, общая ширина поля
            decimals        — int, кол-во знаков после запятой (0 = без дробной)
            decimal_sep     — str, десятичный разделитель ('.' или ',')
            blank_zero      — bool, пробелы вместо ведущих нулей
            shift           — str|None: 'trim', 'left', 'right'
            thousands_sep   — str|None: символ разделителя тысяч
            zero_fill       — bool, заполнение нулями вместо точек
    """
    # Убираем префикс @n
    m = mask[2:]

    show_sign = False
    no_thousands = False
    zero_fill = False

    # --- Флаг знака '-' ---
    if m.startswith('-'):
        show_sign = True
        m = m[1:]

    # --- Флаг подавления разделителей '_' (сразу после @n/-) ---
    if m.startswith('_'):
        no_thousands = True
        m = m[1:]

    # --- Читаем <Ширина> ---
    width_match = re.match(r'^(\d+)', m)
    if not width_match:
        # Ширина обязательна; если не найдена — возвращаем параметры по умолчанию
        return {
            'show_sign': False,
            'no_thousands': False,
            'width': 0,
            'decimals': 0,
            'decimal_sep': '.',
            'blank_zero': False,
            'shift': None,
            'thousands_sep': None,
            'zero_fill': False,
        }

    width_str = width_match.group(1)
    # Если ширина начинается с 0 — это флаг zero_fill
    if width_str.startswith('0'):
        zero_fill = True
        width_str = width_str.lstrip('0') or '0'
    width = int(width_str)
    m = m[len(width_str):]

    # --- Дробная часть: .N или ,N ---
    # .N  → decimal_sep='.', auto thousands_sep=','   (US стиль)
    # ,N  → decimal_sep=',', auto thousands_sep='.'   (EU стиль)
    decimals = 0
    decimal_sep = None
    frac_marker = None          # '.' или ',' — помним, какой маркер был
    thousands_sep_explicit = False  # был ли разделитель тысяч указан явно
    thousands_sep = None
    frac_match = re.match(r'^([.,])(\d+)', m)
    if frac_match:
        frac_marker = frac_match.group(1)
        decimals = int(frac_match.group(2))
        # Маркер дробной части напрямую определяет десятичный разделитель:
        #   .N → decimal_sep='.' (точка), auto thousands_sep=','
        #   ,N → decimal_sep=',' (запятая), auto thousands_sep='.'
        decimal_sep = frac_marker
        if frac_marker == ',':
            thousands_sep = '.'     # авто-разделитель тысяч (EU стиль)
        else:
            thousands_sep = ','     # авто-разделитель тысяч (US стиль)
        m = m[len(frac_match.group(0)):]
    else:
        thousands_sep = None
        decimal_sep = None   # будет определена ниже

    # --- Флаг blank_zero: b / B ---
    blank_zero = False
    if m and m[0] in ('b', 'B'):
        blank_zero = True
        m = m[1:]

    # --- Сдвиг: t/T, <, > ---
    shift = None
    if m:
        if m[0] in ('t', 'T'):
            shift = 'trim'
            m = m[1:]
        elif m[0] == '<':
            shift = 'left'
            m = m[1:]
        elif m[0] == '>':
            shift = 'right'
            m = m[1:]

    # --- Последний символ: разделитель ---
    # Правило зависит от маркера дробной части:
    #   .N  + символ → символ заменяет decimal_sep
    #       ИСКЛЮЧЕНИЕ: '_' при .N означает no_thousands (подавить разделители тысяч)
    #   ,N  + символ → символ заменяет thousands_sep (через карту)
    #   без дроби + символ → символ заменяет thousands_sep (через карту)
    if m and m[0] in _THOUSANDS_SEP_MAP:
        ch = m[0]
        mapped = _THOUSANDS_SEP_MAP[ch]
        if frac_marker == '.':
            if ch == '_':
                # '_' при маркере '.' означает подавление разделителей тысяч
                no_thousands = True
            else:
                # При маркере '.' последний символ заменяет десятичный разделитель
                decimal_sep = mapped
        else:
            # При маркере ',' или без дробной части → разделитель тысяч
            thousands_sep = mapped
            thousands_sep_explicit = True
        m = m[1:]

    # Если decimal_sep не был определён, но есть дробные знаки → запятая по умолчанию
    if decimal_sep is None and decimals > 0:
        decimal_sep = ','
    # Если decimal_sep не был определён и нет дробных → None (не используется)
    elif decimal_sep is None:
        decimal_sep = ','

    return {
        'show_sign': show_sign,
        'no_thousands': no_thousands,
        'width': width,
        'decimals': decimals,
        'decimal_sep': decimal_sep,
        'blank_zero': blank_zero,
        'shift': shift,
        'thousands_sep': thousands_sep,
        'zero_fill': zero_fill,
        '_frac_marker': frac_marker,
        '_thousands_sep_explicit': thousands_sep_explicit,
    }


def apply_n_mask(value: str, params: dict) -> str:
    """
    Применяет разобранные параметры числовой маски @n к значению.

    Алгоритм:
    1. Преобразует значение в число.
    2. Округляет до нужного количества десятичных знаков.
    3. Форматирует целую часть с разделителями тысяч.
    4. Соединяет целую и дробную части десятичным разделителем.
    5. Добавляет знак минуса при необходимости.
    6. Обрабатывает переполнение (звёздочки) или дополняет до ширины.
    7. Применяет сдвиг.

    Args:
        value: Строковое представление числа
        params: Словарь параметров из parse_n_mask()

    Returns:
        Отформатированная строка
    """
    try:
        num_val = float(value)
    except (ValueError, TypeError):
        return value

    show_sign = params['show_sign']
    no_thousands = params['no_thousands']
    width = params['width']
    decimals = params['decimals']
    decimal_sep = params['decimal_sep']
    blank_zero = params['blank_zero']
    shift = params['shift']
    thousands_sep_char = params['thousands_sep']
    zero_fill = params['zero_fill']

    # Определяем знак
    is_negative = num_val < 0
    abs_val = abs(num_val)

    # По умолчанию отрицательные числа выводятся БЕЗ знака (согласно спецификации)
    if not show_sign and is_negative:
        # Просто берём модуль, без минуса
        pass

    # Округляем до нужного количества знаков
    rounded = round(abs_val, decimals)

    # Разделяем на целую и дробную части
    integer_part = int(rounded)
    if decimals > 0:
        # Получаем дробную часть как строку
        formatted_full = f"{rounded:.{decimals}f}"
        decimal_part = formatted_full.split('.')[1]
    else:
        decimal_part = ''

    # Форматируем целую часть
    int_str = str(integer_part)

    # Добавляем разделители тысяч ТОЛЬКО если явно указан символ разделителя
    if not no_thousands and thousands_sep_char is not None and len(int_str) > 3:
        sep = thousands_sep_char
        groups = []
        while len(int_str) > 3:
            groups.append(int_str[-3:])
            int_str = int_str[:-3]
        groups.append(int_str)
        int_formatted = sep.join(reversed(groups))
    else:
        int_formatted = int_str

    # Собираем число
    if decimals > 0:
        number_str = f"{int_formatted}{decimal_sep}{decimal_part}"
    else:
        number_str = int_formatted

    # Добавляем знак минуса
    if is_negative:
        if show_sign:
            number_str = f"-{number_str}"
        # иначе — без знака (по умолчанию)

    # --- Обработка ширины и сдвига ---
    fill_char = '0' if zero_fill else '.'

    if width == 0:
        # Ширина 0 — автоширина, просто возвращаем число
        return number_str

    # --- blank_zero для нуля: если значение равно 0 и blank_zero → все пробелы ---
    if blank_zero and rounded == 0:
        final = ' ' * width
        # Trim всё ещё может применяться
        if shift == 'trim':
            final = final.strip(' ')
        return final

    current_len = len(number_str)

    # --- Сдвиг right: обработка ДО проверки переполнения ---
    # Right shift: извлечь ВСЕ цифры из исходного числа (включая дробные, игнорируя decimals),
    # взять последние width цифр; если меньше — дополнить fill_char слева
    if shift == 'right':
        # Получаем все цифры из исходного значения (до округления по decimals)
        raw_digits = re.sub(r'[^\d]', '', value)
        if len(raw_digits) >= width:
            return raw_digits[-width:]
        else:
            # Right shift всегда заполняется нулями слева
            return raw_digits.rjust(width, '0')

    # --- Стандартная обработка ширины (без right shift) ---
    # При decimals > 0 не дополняем до ширины — возвращаем число как есть
    if decimals > 0:
        final = number_str
    elif current_len > width:
        # ПЕРЕПОЛНЕНИЕ — заполняем '*'
        final = '*' * width
    elif current_len < width:
        padding = width - current_len

        if blank_zero:
            # blank_zero: заполняем пробелами вместо ведущих нулей/точек
            padded_number = number_str.rjust(width, fill_char)
            # Заменяем все ведущие fill_char на пробелы
            result_chars = list(padded_number)
            for i in range(len(result_chars)):
                if result_chars[i] == fill_char:
                    result_chars[i] = ' '
                else:
                    break  # остановились на первой значащей цифре
            final = ''.join(result_chars)
        else:
            # Обычное заполнение точками (или нулями при zero_fill) слева от числа
            final = number_str.rjust(width, fill_char)
    else:
        final = number_str

    # --- Применение сдвига к результату ---
    if shift == 'trim':
        # Удалить ведущие и оконечные точки-заполнители (и пробелы при blank_zero)
        strip_chars = ' .' if blank_zero else '.'
        final = final.strip(strip_chars)
    elif shift == 'left':
        # Прижать к левому краю: убрать ведущие fill_char
        final = final.lstrip(fill_char)

    return final


def format_with_n_mask(value: str, mask: str) -> str:
    """
    Удобная обёртка: разбирает маску @n и применяет её к значению за один вызов.

    Args:
        value: Строковое представление числа
        mask: Маска форматирования вида @n...

    Returns:
        Отформатированная строка
    """
    params = parse_n_mask(mask)
    return apply_n_mask(value, params)