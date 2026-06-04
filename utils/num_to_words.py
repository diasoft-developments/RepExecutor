"""
Преобразование чисел в текст прописью по формам из TWRDigitField.

Пример:
    number_to_words(150, ["целая", "целой", "целых"], ["", "", ""])
    → "150 целых"
"""

import math
from loguru import logger


def _get_number_word_form(number: int, forms: list) -> str:
    """
    Выбирает правильную форму слова в зависимости от числа.
    
    Правила русского языка:
    - 1 целая (1, 21, 101, ...)
    - 2-4 целой (2-4, 22-24, 102-104, ...)
    - 5-20 целых (5-20, 25-30, 105-110, ...)
    """
    if not forms:
        return ""

    # Нормализуем формы: всегда 3 элемента
    while len(forms) < 3:
        forms.append(forms[-1] if forms else "")

    abs_num = abs(number)
    last_two = abs_num % 100
    last_one = abs_num % 10

    if 11 <= last_two <= 19:
        # 11-19 всегда множественное число (целых)
        return forms[2]
    elif last_one == 1:
        # 1, 21, 31, ... (целая)
        return forms[0]
    elif 2 <= last_one <= 4:
        # 2-4, 22-24, ... (целой)
        return forms[1]
    else:
        # 0, 5-9, ... (целых)
        return forms[2]


def number_to_words(number: float, int_parts: list, frac_parts: list = None) -> str:
    """
    Преобразует число в текст прописью по формам из TWRDigitField.
    
    Args:
        number: Число для преобразования
        int_parts: Формы для целой части ["целая", "целой", "целых"]
        frac_parts: Формы для дробной части (копеек), если есть
        
    Returns:
        Строка прописью: "150 целых"
        
    Examples:
        >>> number_to_words(1, ["целая", "целой", "целых"])
        '1 целая'
        >>> number_to_words(5, ["целая", "целой", "целых"])
        '5 целых'
        >>> number_to_words(150, ["целая", "целой", "целых"])
        '150 целых'
    """
    if number is None:
        return ""
    
    try:
        number = float(number)
    except (ValueError, TypeError):
        logger.warning(f"Не удалось преобразовать число: {number}")
        return str(number)

    if math.isnan(number) or math.isinf(number):
        return ""

    int_value = int(math.floor(abs(number)))
    form = _get_number_word_form(int_value, int_parts)
    
    # Форматируем число с разделителями тысяч
    int_str = f"{int_value:,}".replace(",", " ")
    
    result = f"{int_str} {form}".strip()
    
    # Если есть дробная часть
    if frac_parts and len(frac_parts) >= 3:
        frac_value = round((abs(number) - int_value) * 100)
        if frac_value > 0:
            frac_form = _get_number_word_form(frac_value, frac_parts)
            result += f" {frac_value:02d} {frac_form}"
    
    return result


def format_number_with_words(number: float, 
                              int_parts: list, 
                              frac_parts: list = None,
                              include_digits: bool = True) -> str:
    """
    Форматирует число с возможностью включения цифр и текста.
    
    Args:
        number: Число
        int_parts: Формы слов для целой части
        frac_parts: Формы слов для дробной части
        include_digits: Включать ли цифровое значение
        
    Returns:
        Отформатированная строка
    """
    words = number_to_words(number, int_parts, frac_parts)
    
    if include_digits and number is not None:
        try:
            number = float(number)
            return f"{words}"
        except (ValueError, TypeError):
            pass
    
    return words