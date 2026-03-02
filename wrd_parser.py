import re
import os
from loguru import logger

from diasoft_macros import parse_diasoft_macros
from params import inject_report_params
from utils import log_execution

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

    return sql_text, docname

@log_execution()
def save_sql(input_file):
    if not os.path.exists(input_file): return
    with open(input_file, 'rb') as f:
        raw = f.read()
        try:
            content = raw.decode('cp1251')
        except:
            content = raw.decode('utf-8', errors='replace')

    sql, docname = parse_wrd_text(content)
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