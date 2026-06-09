"""
Преобразование чисел в текст прописью по формам из TWRDigitField.

Поддерживает все параметры TWRDigitField:
- Digits, FracPartDefinition, LeadingZero, TrailingZero
- NoZeroFrac, IntOnly, FracPartDigital

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
        return forms[2]
    elif last_one == 1:
        return forms[0]
    elif 2 <= last_one <= 4:
        return forms[1]
    else:
        return forms[2]


def _get_scale_word(group: int, scale_index: int) -> str:
    """
    Возвращает правильное название разряда с учётом склонения.
    
    Для тысяч используется женский род и три формы склонения:
    - 1 тысяча, 2-4 тысячи, 5-20 тысяч
    
    Для миллионов/миллиардов/триллионов — мужской род:
    - 1 миллион, 2-4 миллиона, 5-20 миллионов
    """
    if scale_index == 0:
        return ""
    
    # Три формы для каждого разряда
    scales = {
        1: ("тысяча", "тысячи", "тысяч"),
        2: ("миллион", "миллиона", "миллионов"),
        3: ("миллиард", "миллиарда", "миллиардов"),
        4: ("триллион", "триллиона", "триллионов"),
    }
    
    if scale_index not in scales:
        return ""
    
    sing, dual, plur = scales[scale_index]
    form = _get_number_word_form(group, [sing, dual, plur])
    return form


def _digits_to_words(n: int) -> str:
    """
    Преобразует целое число >= 0 в русское словесное представление прописью.
    
    Примеры:
        0 → "ноль"
        1 → "один"
        25 → "двадцать пять"
        100 → "сто"
        1_000_000 → "один миллион"
        1_000_001 → "один миллион один"
    """
    if n == 0:
        return "ноль"

    # Единицы (с гендерными формами для сотен/тысяч)
    ones_m = ["", "один", "два", "три", "четыре", "пять", "шесть", "семь", "восемь", "девять"]
    ones_n = ["", "одна", "две", "три", "четыре", "пять", "шесть", "семь", "восемь", "девять"]

    # Десятки
    tens = ["", "", "двадцать", "тридцать", "сорок", "пятьдесят", 
            "шестьдесят", "семьдесят", "восемьдесят", "девяносто"]

    # Особые случаи 11-19
    teens = ["десять", "одиннадцать", "двенадцать", "тринадцать", "четырнадцать",
             "пятнадцать", "шестнадцать", "семнадцать", "восемнадцать", "девятнадцать"]

    # Сотни
    hundreds = ["", "сто", "двести", "триста", "четыреста", "пятьсот", 
                "шестьсот", "семьсот", "восемьсот", "девятьсот"]

    def _convert_group(group: int, use_female: bool) -> list:
        """Конвертирует группу из 3 цифр (0-999) в список слов."""
        if group == 0:
            return []

        parts = []
        h = group // 100
        remainder = group % 100

        if h > 0:
            parts.append(hundreds[h])

        if remainder == 0:
            pass
        elif remainder < 10:
            word = ones_n[remainder] if use_female else ones_m[remainder]
            parts.append(word)
        elif remainder < 20:
            parts.append(teens[remainder - 10])
        else:
            t = remainder // 10
            o = remainder % 10
            if t > 1:
                parts.append(tens[t])
            if o > 0:
                parts.append(ones_m[o])

        return parts

    result_parts = []
    scale_index = 0

    while n > 0:
        group = n % 1000
        n //= 1000

        if group > 0:
            # Для тысяч используем женский род (одна тысяча, две тысячи)
            use_female = (scale_index == 1)
            group_words = _convert_group(group, use_female)
            
            # Добавить название разряда
            scale_word = _get_scale_word(group, scale_index)
            if scale_word:
                group_words.append(scale_word)
            
            result_parts.insert(0, group_words)
        scale_index += 1

    all_words = []
    for part in result_parts:
        all_words.extend(part)

    return " ".join(all_words)


def num_to_russian_words(n: int) -> str:
    """
    Обёртка для преобразования числа в русское словесное представление.
    
    Args:
        n: Целое число >= 0
        
    Returns:
        Строка прописью: "сто двадцать три"
    """
    if n < 0:
        return "минус " + _digits_to_words(abs(n))
    return _digits_to_words(n)


def number_to_words(
    number: float,
    int_parts: list,
    frac_parts: list = None,
    digits: int = 0,
    frac_part_definition: bool = True,
    leading_zero: bool = True,
    trailing_zero: bool = True,
    no_zero_frac: bool = False,
    int_only: bool = False,
    frac_part_digital: bool = False,
) -> str:
    """
    Преобразует число в текст прописью по формам из TWRDigitField.
    
    Args:
        number: Число для преобразования
        int_parts: Формы для целой части ["целая", "целой", "целых"]
        frac_parts: Формы для дробной части ["копейка", "копейки", "копеек"]
        digits: Количество значащих цифр (0 = не ограничено)
        frac_part_definition: True = печатать "десятых"/"сотых", False = использовать FracParts
        leading_zero: Печатать "Ноль" для целой части 0
        trailing_zero: Печатать незначащие нули в дробной части
        no_zero_frac: Скрывать нулевую дробную часть
        int_only: Печатать только целую часть
        frac_part_digital: Печатать дробную часть цифрами (не прописью)
        
    Returns:
        Строка прописью: "150 целых"
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

    negative = number < 0
    abs_number = abs(number)
    int_value = int(math.floor(abs_number))

    # Определяем, выводить ли целую часть прописью
    # Прописью выводим, когда дробная часть тоже прописью (frac_part_definition=True и frac_part_digital=False)
    # или когда frac_part_definition=False, frac_parts указаны и frac_part_digital=False
    int_as_words = False
    if frac_part_definition and not frac_part_digital:
        int_as_words = True
    elif not frac_part_definition and frac_parts and len(frac_parts) >= 3 and not frac_part_digital:
        int_as_words = True

    # --- Целая часть ---
    form = _get_number_word_form(int_value, int_parts)

    # Если целая часть == 0 и LeadingZero=False — не печатаем
    if int_value == 0 and not leading_zero:
        int_part_text = ""
    elif int_as_words:
        # Выводим целую часть прописью с большой буквы
        int_words = num_to_russian_words(int_value)
        # Capitalize первое слово
        if int_words:
            int_words = int_words[0].upper() + int_words[1:]
        int_part_text = f"{int_words} {form}".strip()
    else:
        int_str = f"{int_value:,}".replace(",", " ")
        int_part_text = f"{int_str} {form}".strip()

    # --- Дробная часть ---
    if int_only:
        return int_part_text

    # Вычисляем дробную часть через строковое представление
    # чтобы избежать проблем с плавающей точкой
    num_str = f"{abs_number:.10f}".rstrip('0')
    if '.' in num_str:
        frac_str = num_str.split('.')[1]
    else:
        frac_str = ""
    
    # Определяем количество дробных цифр
    # Если digits задан явно (digits > 0), используем его; иначе определяем автоматически
    if digits > 0:
        actual_frac_digits = digits
    else:
        actual_frac_digits = len(frac_str) if frac_str else 0
    
    # Определяем масштаб и слово для определения
    if frac_part_definition:
        if actual_frac_digits >= 2:
            scale = 100
            frac_def_word = "сотых"
        elif actual_frac_digits == 1:
            scale = 10
            frac_def_word = "десятых"
        else:
            scale = 100
            frac_def_word = "сотых"
    else:
        scale = 100
        frac_def_word = ""

    frac_raw = abs_number - int_value
    frac_value = round(frac_raw * scale)

    # NoZeroFrac: если дробная часть == 0 — пропускаем её entirely
    if no_zero_frac and frac_value == 0:
        return int_part_text

    # TrailingZero=False: если дробная часть == 0 — пропускаем
    if not trailing_zero and frac_value == 0:
        return int_part_text

    # Формируем дробную часть
    if frac_part_definition:
        # Печатаем "XX сотых" или "X десятых"
        if frac_part_digital:
            frac_text = f"{frac_value:0{scale//10+1 if scale >= 10 else 2}d} {frac_def_word}"
        else:
            frac_words = num_to_russian_words(frac_value)
            frac_text = f"{frac_words} {frac_def_word}"
    elif frac_parts and len(frac_parts) >= 3:
        # Используем FracParts (копейка/копейки/копеек)
        frac_form = _get_number_word_form(frac_value, frac_parts)
        if frac_part_digital:
            frac_text = f"{frac_value:02d} {frac_form}"
        else:
            frac_words = num_to_russian_words(frac_value)
            frac_text = f"{frac_words} {frac_form}"
    else:
        frac_text = ""

    # Собираем результат
    parts = [p for p in [int_part_text, frac_text] if p]
    result = " ".join(parts)

    return result


def format_number_with_words(
    number: float,
    int_parts: list,
    frac_parts: list = None,
    include_digits: bool = True,
    digits: int = 0,
    frac_part_definition: bool = True,
    leading_zero: bool = True,
    trailing_zero: bool = True,
    no_zero_frac: bool = False,
    int_only: bool = False,
    frac_part_digital: bool = False,
) -> str:
    """
    Форматирует число с текстом прописью.
    
    Args:
        number: Число
        int_parts: Формы слов для целой части
        frac_parts: Формы слов для дробной части
        include_digits: Включать ли цифровое значение (сейчас всегда включено в тексте)
        digits: Количество значащих цифр
        frac_part_definition: Использовать определение дробных частей
        leading_zero: Печатать "Ноль" для целой части 0
        trailing_zero: Печатать незначащие нули
        no_zero_frac: Скрывать нулевую дробную часть
        int_only: Только целая часть
        frac_part_digital: Дробная часть цифрами
        
    Returns:
        Отформатированная строка
    """
    return number_to_words(
        number, int_parts, frac_parts,
        digits, frac_part_definition, leading_zero, trailing_zero,
        no_zero_frac, int_only, frac_part_digital,
    )