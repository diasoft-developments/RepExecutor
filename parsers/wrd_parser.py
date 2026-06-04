import re
import os
from typing import Optional
from loguru import logger

from .diasoft_macros import parse_diasoft_macros
from .wrd_params import inject_report_params
from utils.logger import log_execution


class TWRField:
    """
    Представляет поле TWRField из Delphi-формы DiaSoft.
    
    Свойства:
    - FieldName: имя объекта в DFM
    - DisplayLabel: отображаемая метка
    - Mask: маска форматирования (например '@n20.2_', '@d6.', '@n8.2')
      
    Форматы масок:
    - @n<w>.<d>[_] - числовой: ширина w, дробные d, _ = заполнитель
    - @d<fmt>.   - дата
    - @s<w>      - строковый
    """
    def __init__(self, field_name: str = "", display_label: str = "", mask: str = ""):
        self.FieldName = field_name
        self.DisplayLabel = display_label
        self.Mask = mask
    
    @property
    def mask_type(self) -> str:
        """Возвращает тип маски: 'n' (число), 'd' (дата), 's' (строка) или ''"""
        if self.Mask and self.Mask.startswith('@'):
            return self.Mask[1].lower() if len(self.Mask) > 1 else ''
        return ''
    
    @property
    def is_numeric(self) -> bool:
        return self.mask_type == 'n'
    
    @property
    def decimal_places(self) -> int:
        """Количество дробных знаков для числовых масок (@n20.2_ -> 2)"""
        if self.is_numeric:
            m = re.match(r'@n\d+\.(\d+)', self.Mask, re.IGNORECASE)
            return int(m.group(1)) if m else 2
        return 0
    
    @property
    def total_width(self) -> int:
        """Общая ширина поля для числовых масок (@n20.2_ -> 20)"""
        if self.is_numeric:
            m = re.match(r'@n(\d+)\.\d+', self.Mask, re.IGNORECASE)
            return int(m.group(1)) if m else 0
        return 0
    
    def format_value(self, value) -> str:
        """Форматирует значение согласно маске"""
        if value is None or str(value).strip() == '':
            return ''
        
        if self.is_numeric:
            try:
                num = float(value)
                decimals = self.decimal_places
                return f"{num:.{decimals}f}"
            except (ValueError, TypeError):
                return str(value)
        
        if self.mask_type == 'd':
            # Для дат пока возвращаем как есть
            return str(value)
        
        return str(value)
    
    def __repr__(self):
        return (f"TWRField(FieldName='{self.FieldName}', Mask='{self.Mask}')")


class TWRDigitField:
    """
    Представляет поле TWRDigitField из Delphi-формы DiaSoft.
    
    Свойства:
    - FieldName: имя объекта в DFM (используется для формирования имени выходного столбца)
    - DataField: имя поля в SQL/CSV для числового значения (исходный столбец)
    - IntParts[0..2]: формы слов для целой части (мелкая, мелкой, мелких)
    - FracParts[0..2]: формы слов для дробной части (копейка, копейки, копеек)
    - IncludeDigits: показывать ли цифровое значение
    """
    def __init__(self, field_name: str = "", data_field: str = "", 
                 int_parts: list = None, 
                 frac_parts: list = None,
                 include_digits: bool = True):
        self.FieldName = field_name
        self.DataField = data_field
        self.IntParts = int_parts or ["", "", ""]
        self.FracParts = frac_parts or ["", "", ""]
        self.IncludeDigits = include_digits
    
    def __repr__(self):
        return (f"TWRDigitField(FieldName='{self.FieldName}', DataField='{self.DataField}', "
                f"IntParts={self.IntParts}, FracParts={self.FracParts})")


@log_execution()
def parse_twr_fields(text: str) -> dict:
    """
    Извлекает все TWRField (с масками) из DFM-текста .wrd файла.
    
    Пример блока:
        object Qty: TWRField
            FieldName = 'Qty'
            DisplayLabel = 'Qty'
            Mask = '@n20.2_'
        end
    
    Returns:
        Словарь {FieldName: TWRField} с объектами TWRField
    """
    fields = {}
    
    # Паттерн для поиска блоков TWRField (но не TWRDigitField!)
    # Используем негативный lookahead чтобы исключить TWRDigitField
    field_pattern = re.compile(
        r'object\s+(\w+)\s*:\s*TWRField\b\s*\n((?:.*?\n)*?)\s*end\s*\n',
        re.DOTALL
    )
    
    for match in field_pattern.finditer(text):
        object_name = match.group(1)
        block = match.group(0)
        
        # Исключаем TWRDigitField (он уже обрабатывается отдельно)
        if 'TWRDigitField' in block:
            continue
        
        # Извлекаем FieldName
        fn_match = re.search(r"FieldName\s*=\s*'([^']*)'", block)
        field_name = fn_match.group(1) if fn_match else object_name
        
        # Извлекаем DisplayLabel
        dl_match = re.search(r"DisplayLabel\s*=\s*'([^']*)'", block)
        display_label = dl_match.group(1) if dl_match else field_name
        
        # Извлекаем Mask
        mask_match = re.search(r"Mask\s*=\s*'([^']*)'", block)
        mask = mask_match.group(1) if mask_match else ""
        
        fields[field_name] = TWRField(field_name, display_label, mask)
    
    return fields


@log_execution()
def parse_twr_digit_fields(text: str) -> dict:
    """
    Извлекает все TWRDigitField из DFM-текста .wrd файла.
    
    Пример блока:
        object MGRWr: TWRDigitField
            FieldName = 'MGRWr'
            DisplayLabel = 'MGRWr'
            IntPart.Strings = (
              #1094#1077#1083#1072#1103
              #1094#1077#1083#1086#1081
              #1094#1077#1083#1099#1093)
            FracPart.Strings = (
              ''
              ''
              '')
            Digits = 3
            DataField = 'MGR'
        end
    
    Returns:
        Словарь {FieldName: TWRDigitField} с объектами TWRDigitField
    """
    fields = {}
    
    # Паттерн для поиска блоков TWRDigitField
    # Ищем от "object Name: TWRDigitField" до соответствующего "end"
    field_pattern = re.compile(r'object\s+(\w+)\s*:\s*TWRDigitField\s*\n((?:.*?\n)*?)\s*end\s*\n', re.DOTALL)
    
    for match in field_pattern.finditer(text):
        object_name = match.group(1)  # e.g., 'MGRWr'
        block = match.group(0)  # весь блок включая заголовок и end
        
        # Извлекаем FieldName (из объявления объекта)
        field_name = object_name
        
        # Извлекаем DataField
        df_match = re.search(r"DataField\s*=\s*'([^']*)'", block)
        data_field = df_match.group(1) if df_match else ""
        
        # Извлекаем IncludeDigits
        id_match = re.search(r"IncludeDigits\s*=\s*(True|False)", block, re.IGNORECASE)
        include_digits = id_match.group(1).lower() == 'true' if id_match else True
        
        # Извлекаем IntParts (может быть IntPart или IntParts)
        int_parts = _extract_string_array(block, "IntPart")
        if all(p == "" for p in int_parts):
            int_parts = _extract_string_array(block, "IntParts")
        
        # Извлекаем FracParts (может быть FracPart или FracParts)
        frac_parts = _extract_string_array(block, "FracPart")
        if all(p == "" for p in frac_parts):
            frac_parts = _extract_string_array(block, "FracParts")
        
        if data_field:
            fields[field_name] = TWRDigitField(field_name, data_field, int_parts, frac_parts, include_digits)
    
    return fields


def _extract_string_array(block: str, array_name: str) -> list:
    """
    Извлекает массив строк из DFM блока. Поддерживает форматы:
    
    Формат 1: ArrayName = ( ... )
      IntPart = (
      #109#108#107...#0#0#0
      #109#108#107...#0#0#0
      #109#108#107...#0#0#0)
    
    Формат 2: ArrayName.Strings = ( ... )
      IntPart.Strings = (
        #1094#1077#1083#1072#1103
        #1094#1077#1083#1086#1081
        #1094#1077#1083#1099#1093)
    
    Важно: закрывающая ')' может быть сразу после последней строки или на отдельной строке.
    """
    # Попробуем формат "ArrayName.Strings = (...)"
    # Закрывающая ')' может быть сразу после последнего элемента или на новой строке
    pattern_strings = re.compile(
        rf'{array_name}\.Strings\s*=\s*\(\s*\n'
        r'([\s\S]*?)'
        r'\)\s*',
        re.MULTILINE
    )
    
    # Попробуем формат "ArrayName = (...)"
    pattern_direct = re.compile(
        rf'{array_name}\s*=\s*\(\s*\n'
        r'([\s\S]*?)'
        r'\)\s*',
        re.MULTILINE
    )
    
    match = pattern_strings.search(block) or pattern_direct.search(block)
    if not match:
        return ["", "", ""]
    
    inner = match.group(1)
    
    # Разбиваем на строки и обрабатываем каждую
    lines = [l.strip() for l in inner.split('\n') if l.strip()]
    results = []
    
    for line in lines:
        # Убрать закрывающую скобку если она прилипла к концу строки
        line = re.sub(r'\)\s*$', '', line).strip()
        if not line:
            continue
            
        if line.startswith('#'):
            # Строка с Unicode кодами: #1094#1077#1083...
            decoded = _decode_delphi_string(line)
            results.append(decoded)
        elif line == "''":
            # Пустая строка в Delphi формате
            results.append("")
        elif line.startswith("'"):
            # Обычная строка в кавычках
            m = re.match(r"'([^']*)'", line)
            if m:
                results.append(m.group(1))
            else:
                results.append("")
        else:
            results.append("")
        
        if len(results) >= 3:
            break
    
    return results if results else ["", "", ""]


def _decode_delphi_string(s: str) -> str:
    """
    Декодирует Delphi-строку формата #NNN#NNN...#0#0#0
    в читаемый текст (cp1251).
    """
    numbers = re.findall(r'#(\d+)', s)
    result = []
    for num in numbers:
        code = int(num)
        if code == 0:
            break
        result.append(chr(code))
    return "".join(result)

@log_execution()
def decode_bytes(b: bytes) -> str:
    for enc in ("utf-8", "cp1251", "latin1"):
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode("utf-8", errors="replace")

@log_execution()
def delphi_de_serializer(text):
    """
    Разбирает контент SQL.Strings построчно.
    Склеивает текст только если строка в DFM заканчивается на '+'.
    """
    raw_lines = text.splitlines()
    final_result = []
    
    for line in raw_lines:
        clean_line = line.strip()
        if not clean_line:
            continue
            
        # Определяем, есть ли символ склейки '+' в самом конце строки DFM
        is_concat = clean_line.endswith('+')
        
        # Текст для парсинга (убираем '+' если он есть)
        to_parse = clean_line[:-1].strip() if is_concat else clean_line
        
        # Разбираем литералы '...' и коды #NNN в текущей строке
        parsed_part = ""
        i = 0
        while i < len(to_parse):
            char = to_parse[i]
            if char == "'":
                i += 1
                start = i
                while i < len(to_parse):
                    if to_parse[i] == "'" and i + 1 < len(to_parse) and to_parse[i+1] == "'":
                        i += 2 # Экранированная кавычка
                    elif to_parse[i] == "'":
                        parsed_part += to_parse[start:i].replace("''", "'")
                        i += 1
                        break
                    else:
                        i += 1
            elif char == "#":
                m = re.match(r'#(\d+)', to_parse[i:])
                if m:
                    parsed_part += chr(int(m.group(1)))
                    i += len(m.group(0))
                else:
                    i += 1
            else:
                i += 1
        
        final_result.append(parsed_part)
        
        # Если плюса в конце строки DFM не было, значит это логический конец строки SQL
        if not is_concat:
            final_result.append("\n")
            
    return "".join(final_result)

@log_execution()
def heal_sql(sql):
    """
    Минимальная правка макросов, если они были разорваны внутри кавычек.
    """
    # Склеиваем макросы типа %InstitutionID!, если перенос все же случился
    # sql = re.sub(r'([%@#])\n', r'\1', sql)
    # sql = re.sub(r'\n([%@#!])', r'\1', sql)
    
    # Убираем тройные переносы строк
    sql = re.sub(r'\n\s*\n\s*\n', '\n\n', sql)
    
    return sql

@log_execution()
def parse_wrd_text(text: str, params: dict):
    sql_text = None
    docname = None
    digit_fields = {}
    twr_fields = {}

    # Поиск блока SQL.Strings
    match_sql = re.search(r'SQL\.Strings\s*=\s*\((.*?)\)\s*\w+\s*=', text, re.DOTALL | re.IGNORECASE)
    if not match_sql:
        match_sql = re.search(r'SQL\.Strings\s*=\s*\((.*)\)', text, re.DOTALL | re.IGNORECASE)
    
    if match_sql:
        raw_inside = match_sql.group(1)
        
        # Шаг 1: Десериализация (строго по наличию '+')
        sql_text = delphi_de_serializer(raw_inside)
        
        # Шаг 2: Легкая косметика макросов
        sql_text = heal_sql(sql_text)
        
        # Шаг 3: Применение внешних макросов системы        
        sql_text = parse_diasoft_macros(sql_text)
        
        # Шаг 4: Применение параметров
        sql_text = inject_report_params(sql_text, params)

    # Поиск DocName
    match_name = re.search(r"DocName\s*[=:]\s*'([^']*)'", text, re.IGNORECASE)
    if match_name:
        docname = match_name.group(1)

    # Парсинг TWRDigitField
    digit_fields = parse_twr_digit_fields(text)
    
    # Парсинг TWRField (поля с масками форматирования)
    twr_fields = parse_twr_fields(text)

    return sql_text, docname, digit_fields, twr_fields

@log_execution()
def save_sql(input_file):
    if not os.path.exists(input_file): return
    with open(input_file, 'rb') as f:
        raw = f.read()
        try:
            content = raw.decode('cp1251')
        except:
            content = raw.decode('utf-8', errors='replace')

    sql, docname, digit_fields, twr_fields = parse_wrd_text(content)
    if sql:
        out_base = os.path.splitext(docname)[0] if docname else os.path.splitext(input_file)[0]
        out_name = re.sub(r'[\\/*?:"<>|]', "", out_base) + ".sql"
        with open(out_name, 'w', encoding='utf-8') as f_out:
            f_out.write(sql.strip())
        logger.info(f"Выполнено: {out_name}")

# if __name__ == "__main__":
#     import glob
#     for f in glob.glob("*.wrd") + glob.glob("*.dfm"):
#         save_sql(f)