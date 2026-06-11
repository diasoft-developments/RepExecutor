"""Форматирование полей отчётов: TWRDigitField (число прописью) и TWRField (маски)."""
import datetime
import re
from typing import Optional

from loguru import logger

# ---------------------------------------------------------------------------
# Точные импорты из моделей – TWRDigitField, TWRField
# ---------------------------------------------------------------------------
from parsers.wrd_parser import TWRDigitField, TWRField
from parsers.n_mask import parse_n_mask, apply_n_mask
from parsers.d_mask import parse_d_mask, apply_d_mask, format_date_with_mask
from parsers.t_mask import parse_t_mask, apply_t_mask, format_time_with_mask
from parsers.n_to_words import format_number_with_words as _format_number_with_words, number_to_words


# Карта символов разделителей тысяч (копия из n_mask для внутреннего использования)
_THOUSANDS_SEP_MAP = {
    '.': '.',
    ',': ',',
    '_': ' ',
    "'": "'",
    '`': '`',
    '=': '=',
    '-': '-',
}


# ---------------------------------------------------------------------------
# Внутренние обёртки над n_mask (для обратной совместимости)
# ---------------------------------------------------------------------------
def _parse_n_mask(mask: str) -> dict:
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
    return parse_n_mask(mask)


def _apply_n_mask(value: str, params: dict) -> str:
    """
    Применяет разобранные параметры числовой маски @n к значению.

    Args:
        value: Строковое представление числа
        params: Словарь параметров из _parse_n_mask()

    Returns:
        Отформатированная строка
    """
    return apply_n_mask(value, params)


# ---------------------------------------------------------------------------
# Применение одной маски форматирования
# ---------------------------------------------------------------------------
def _apply_single_mask(value: str, mask: str) -> str:
    """
    Применяет одну маску форматирования к значению.

    Поддерживаемые маски:
      - @n...       → числовая (полный набор параметров)
      - @d{width}.  → дата/число: фиксированное кол-во цифр
      - @s<w        → строковая: обрезка/дополнение до ширины W

    Args:
        value: Исходное строковое значение
        mask: Маска форматирования

    Returns:
        Отформатированная строка
    """
    if not mask:
        return value

    mask_stripped = mask.strip()

    # --- Числовая маска @n ---
    if mask_stripped.startswith('@n') or mask_stripped.startswith('@N'):
        params = _parse_n_mask(mask_stripped)
        return _apply_n_mask(value, params)

    # --- Дата/числовая маска @d ---
    # Поддерживаем два формата:
    #   1) @d{width}. — упрощённый (6 цифр, 8 цифр) — ТОЛЬКО когда width >= 4
    #      и точка НЕ является разделителем Diasoft
    #   2) @d<Тип>[<Разделитель>]['b']['t'] — полный синтаксис Diasoft
    if mask_stripped.startswith('@d') or mask_stripped.startswith('@D'):
        # Проверяем, является ли это старым форматом @d{width}.
        # Старый формат: @d6., @d8., @d10. и т.д. — ширина >= 4 и точка в конце
        # НО если после числа идёт разделитель Diasoft (./-_), это новый формат
        # Ключевое отличие: в старом формате точка — последний символ маски
        # а в новом формате после точки могут идти флаги b/t
        # Для надёжности: если число < 4 (тип даты 1-14), всегда новый формат
        # Если число >= 4 и маска заканчивается на точку — проверяем:
        #   - если после точки есть ещё что-то (флаги) → новый формат
        #   - если точка последняя → старый формат ТОЛЬКО если число не совпадает с типом даты
        date_match = re.match(r'^@[dD](\d+)\.$', mask_stripped)
        if date_match:
            width_or_type = int(date_match.group(1))
            # Типы дат Diasoft: 1-14 (кроме 12). Если число попадает в этот диапазон — новый формат.
            if 1 <= width_or_type <= 14 and width_or_type != 12:
                # Это новый формат Diasoft (тип даты без дополнительных параметров)
                return format_date_with_mask(value, mask_stripped)
            # Старый упрощённый формат
            w = int(date_match.group(1))
            try:
                if isinstance(value, (datetime.date, datetime.datetime)):
                    if w == 6:
                        return value.strftime('%d%m%y')
                    elif w == 8:
                        return value.strftime('%d%m%Y')
                    else:
                        return value.strftime(f'%0{w}d')
                else:
                    parsed = datetime.datetime.strptime(str(value).strip(), '%Y-%m-%d %H:%M:%S')
                    if w == 6:
                        return parsed.strftime('%d%m%y')
                    elif w == 8:
                        return parsed.strftime('%d%m%Y')
                    else:
                        return str(parsed).zfill(w)
            except (ValueError, TypeError):
                try:
                    num_val = int(float(value))
                    return str(num_val).zfill(w)
                except (ValueError, TypeError):
                    return str(value).zfill(w)
        else:
            # Полный синтаксис Diasoft: @d<Тип>[<Разделитель>]['b']['t']
            return format_date_with_mask(value, mask_stripped)

    # --- Маска времени: @t<Typ>[b] ---
    # Тип 1: hh:mm  |  Тип 2: hhmm  |  Тип 3: hh:mmam/pm  |  Тип 4: hh:mm:ss
    if mask_stripped.startswith('@t') or mask_stripped.startswith('@T'):
        return format_time_with_mask(value, mask_stripped)

    # --- Строковая маска: @s<w ---
    str_match = re.match(r'^@s<(\d+)$', mask_stripped)
    if str_match:
        max_width = int(str_match.group(1))
        return value[:max_width].ljust(max_width)

    # Маска не распознана — возвращаем исходное значение
    return value


# ---------------------------------------------------------------------------
# Публичная обёртка: format_number_with_words(fields, field_name, number)
# Для удобства тестирования и внешнего API
# ---------------------------------------------------------------------------
def format_number_with_words(digit_fields: dict, field_name: str, number: float) -> str:
    """
    Форматирует число прописью с использованием параметров из TWRDigitField.

    Args:
        digit_fields: Словарь {field_name: TWRDigitField} от parse_twr_digit_fields()
        field_name: Имя поля (FieldName) для поиска параметров
        number: Число для форматирования

    Returns:
        Строка с числом прописью, или исходное число если поле не найдено
    """
    field = digit_fields.get(field_name)
    if field is None:
        logger.warning(f"Поле '{field_name}' не найдено в digit_fields")
        return str(number)

    # Извлекаем формы
    int_forms = [f.replace('#', '') for f in field.IntParts] if field.IntParts else None
    frac_forms = [f.replace('#', '') for f in field.FracParts] if field.FracParts else None

    # Определяем количество дробных знаков
    if not field.FracPartDefinition:
        frac_forms = None

    return _format_number_with_words(number, int_forms, frac_parts=frac_forms)


# ---------------------------------------------------------------------------
# Digit field processing
# ---------------------------------------------------------------------------
def _apply_digit_fields(data_rows: list, digit_fields: dict) -> list:
    """
    Применяет TWRDigitField к данным: преобразует числовые поля в прописью.

    Для каждого поля из digit_fields находит соответствующий столбец в данных
    (по совпадению имени или по NamePart) и добавляет новый столбец с суффиксом
    _words, содержащий текст прописью.

    Args:
        data_rows: Список строк данных (SimpleNamespace)
        digit_fields: Словарь {field_name: TWRDigitField}

    Returns:
        Модифицированный список строк данных
    """
    if not data_rows or not digit_fields:
        return data_rows

    # Получаем имена столбцов из первой строки
    column_names = list(vars(data_rows[0]).keys())

    for field_name, digit_field in digit_fields.items():
        # Ищем целевой столбец: сначала по DataField, потом по FieldName
        target_col = None

        # Приоритет 1: DataField (явное имя поля в SQL/CSV)
        if digit_field.DataField and digit_field.DataField in column_names:
            target_col = digit_field.DataField
        # Приоритет 2: точное совпадение FieldName
        elif field_name in column_names:
            target_col = field_name
        # Приоритет 3: поиск по DataField как подстроке
        elif digit_field.DataField:
            for col in column_names:
                if digit_field.DataField in col:
                    target_col = col
                    break
        # Приоритет 4: поиск по FieldName как подстроке
        else:
            for col in column_names:
                if field_name in col:
                    target_col = col
                    break

        if not target_col:
            logger.warning(
                f"Столбец для DigitField '{field_name}' "
                f"(DataField='{digit_field.DataField}') не найден в данных. "
                f"Доступные столбцы: {column_names}"
            )
            continue

        # Имя нового столбца берём из FieldName (наименование поля в итоговом CSV)
        new_col = digit_field.FieldName if digit_field.FieldName else field_name

        # Защита от перезаписи существующего столбца
        if new_col in column_names and new_col != target_col:
            new_col = f"{new_col}_words"

        # Готовим формы из TWRDigitField
        int_forms = list(digit_field.IntParts)  # ["мелкая", "мелкой", "мелких"]
        frac_forms = list(digit_field.FracParts) if digit_field.FracParts else None  # ["копейка", "копейки", "копеек"]

        logger.debug(f"Применяем DigitField '{field_name}' → новый столбец '{new_col}' (источник: {target_col})")
        logger.debug(f"  int_forms={int_forms}, frac_forms={frac_forms}")

        # Обрабатываем каждую строку
        for row in data_rows:
            raw_value = getattr(row, target_col, None)

            if raw_value is None or raw_value == '':
                setattr(row, new_col, '')
                continue

            try:
                # Пытаемся преобразовать в число
                num_value = float(raw_value)
                words = _format_number_with_words(num_value, int_forms, frac_parts=frac_forms)
                setattr(row, new_col, words)
            except (ValueError, TypeError):
                # Если не число — оставляем как есть
                setattr(row, new_col, str(raw_value))

    return data_rows


# ---------------------------------------------------------------------------
# TWR field mask processing
# ---------------------------------------------------------------------------
def _apply_twr_masks(data_rows: list, twr_fields: dict) -> list:
    """
    Применяет TWRField (маски форматирования) к данным.

    Поддерживаемые маски:
      - @n...   → числовое: полный набор параметров (ширина, дробь, разделители и т.д.)
      - @d6.    → дата/число: 6 цифр (например, 040626)
      - @s<W    → строковое: ширина W, обрезка/дополнение пробелами

    Для каждого поля из twr_fields находит целевой столбец в данных
    и модифицирует значения согласно маске.

    Args:
        data_rows: Список строк данных (SimpleNamespace)
        twr_fields: Словарь {field_name: TWRField}

    Returns:
        Модифицированный список строк данных
    """
    if not data_rows or not twr_fields:
        return data_rows

    column_names = list(vars(data_rows[0]).keys())

    for field_name, twr_field in twr_fields.items():
        # Ищем целевой столбец по FieldName
        target_col = None

        # Приоритет 1: точное совпадение FieldName
        if field_name in column_names:
            target_col = field_name
        # Приоритет 2: регистронезависимое точное совпадение
        elif not any(ci.lower() == field_name.lower() for ci in column_names):
            for col in column_names:
                if col.lower() == field_name.lower():
                    target_col = col
                    break
        # Приоритет 3: поиск по FieldName как подстроке (регистронезависимо)
        if not target_col:
            for col in column_names:
                if field_name.lower() in col.lower():
                    target_col = col
                    break

        if not target_col:
            logger.warning(f"Столбец для TWRField '{field_name}' не найден. Доступные: {column_names}")
            continue

        mask = twr_field.Mask

        # Если маска не указана, определяем тип из данных и применяем дефолтную
        has_mask = bool(mask and mask.strip())

        logger.debug(f"Применяем TWRField '{field_name}' → столбец '{target_col}', маска='{mask}'")

        for row in data_rows:
            raw_value = getattr(row, target_col, None)

            if raw_value is None or raw_value == '':
                setattr(row, target_col, '')
                continue

            try:
                # Если маска не указана, проверяем является ли значение числовым или датой
                effective_mask = mask
                if not has_mask:
                    str_value = str(raw_value).strip()
                    
                    # Сначала пытаемся распознать как дату
                    parsed_date = None
                    for date_format in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'):
                        try:
                            parsed_date = datetime.datetime.strptime(str_value, date_format)
                            break
                        except ValueError:
                            continue
                    
                    if parsed_date is not None:
                        # Дата без маски — выводим в формате dd/MM/yyyy
                        formatted = parsed_date.strftime('%d/%m/%Y')
                        logger.debug(f"Поле '{field_name}' без маски, дата → формат dd/MM/yyyy: {formatted}")
                        setattr(row, target_col, formatted)
                        continue
                    
                    # Пытаемся определить, является ли значение числовым
                    test_val = str_value.replace(',', '.')
                    try:
                        float_val = float(test_val)
                        # Целое число — оставляем как есть
                        if float_val == int(float_val):
                            logger.debug(f"Поле '{field_name}' без маски, целое число → оставляем как есть")
                            continue
                        # Дробное число — применяем дефолтную маску @n0.3_ (3 знака после запятой)
                        effective_mask = '@n0.3_'
                        logger.debug(f"Поле '{field_name}' без маски, дробное значение → дефолтная маска @n0.3_")
                    except ValueError:
                        # Не числовое — оставляем как есть
                        continue

                formatted = _apply_single_mask(str(raw_value), effective_mask)
                setattr(row, target_col, formatted)
            except Exception as e:
                effective_mask_log = effective_mask if not has_mask else mask
                logger.warning(f"Ошибка применения маски '{effective_mask_log}' к значению '{raw_value}': {e}")
                setattr(row, target_col, str(raw_value))

    return data_rows