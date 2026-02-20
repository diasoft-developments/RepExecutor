import re


def decode_bytes(b: bytes) -> str:
    for enc in ("utf-8", "cp1251", "latin1"):
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode("utf-8", errors="replace")



def parse_wrd_text(text: str):
    lower = text.lower()
    idx = lower.find('sql.strings')
    sql_text = None
    docname = None

    if idx != -1:
        start = text.find('(', idx)
        if start != -1:
            i = start + 1
            depth = 1
            in_single = False
            buf = []

            while i < len(text) and depth > 0:
                ch = text[i]

                if ch == "'":
                    if in_single and i + 1 < len(text) and text[i + 1] == "'":
                        buf.append("''")
                        i += 2
                        continue

                    in_single = not in_single
                    buf.append(ch)
                    i += 1
                    continue

                if not in_single:
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                        if depth == 0:
                            break

                buf.append(ch)
                i += 1

            content = ''.join(buf)                  
                    
                        
            # 🔹 Склеиваем строки, разделённые +
            def glue_delphi_concat(content: str) -> str:
                """
                Склеивает конструкции вида:
                'первая часть' +
                'вторая часть' +
                'третья'
                → 'первая частьвторая частьтретья'
                (убирает кавычки до и после +, сам +, пробелы вокруг +)
                """
                def replacer(m: re.Match) -> str:
                    # весь кусок: 'aaa' + 'bbb' + 'ccc'
                    piece = m.group(0)
                    
                    # находим все строковые литералы внутри
                    literals = re.findall(r"'((?:[^']|'')*)'", piece)
                    
                    # склеиваем их содержимое, заменяя '' → '
                    joined_inner = ''.join(
                        literal.replace("''", "'") for literal in literals
                    )
                    
                    # возвращаем как одну строку (без внешних кавычек)
                    return joined_inner

                # Паттерн: цепочка из минимум одной + между строками
                # (?:'[^']*'(?:\s*\+\s*'[^']*')+)
                pattern = r"(?:'((?:[^']|'')*)'(?:\s*\+\s*'((?:[^']|'')*)')+)"

                # заменяем все такие цепочки
                glued = re.sub(pattern, replacer, content, flags=re.DOTALL)

                return glued
            content = glue_delphi_concat(content)
            # Пример использования в твоей функции parse_delphi_sql_strings
            # (после замены #NNNN и перед разбиением на lines)

            # content = re.sub(r"#(\d+)", lambda m: chr(int(m.group(1))), content)
            # Вот здесь главное действие:
            

            # 🔹 Склеивание строк, если строка заканчивается на +
            def merge_plus_lines(text: str) -> str:
                """
                Склеивает строки, которые заканчиваются на '+'.
                Убирает пробелы вокруг '+' и ведущие пробелы второй строки.
                Сохраняет отступ первой строки, лишние пробелы справа удаляет.
                """
                lines = text.splitlines()
                merged_lines = []
                buffer = ""
                
                for line in lines:
                    stripped = line.rstrip()
                    # Если строка заканчивается на плюс с пробелами
                    if re.search(r"\s*\+\s*$", stripped):
                        # убираем пробелы вокруг + и добавляем к буферу
                        stripped = re.sub(r"\s*\+\s*$", "", stripped)
                        buffer += stripped
                    else:
                        # удаляем ведущие пробелы второй строки перед склейкой
                        merged_lines.append(buffer + line.lstrip())
                        buffer = ""
                
                if buffer:
                    merged_lines.append(buffer)

                # удаляем лишние пробелы справа каждой строки
                merged_lines = [line.rstrip() for line in merged_lines]
                return "\n".join(merged_lines)
            
            content = merge_plus_lines(content)
                    
            # Шаг 2. Теперь убираем все одиночные строковые литералы (уже склеенные)
            # и собираем их в итоговый текст
            # sql_parts = []
            # for m in re.finditer(r"'((?:[^']|'')*)'", content):
            #     cleaned = m.group(1).replace("''", "'")
            #     sql_parts.append(cleaned)

            # full_sql_raw = ''.join(sql_parts)

            # Шаг 3. Очистка: разбиваем по строкам, убираем пустые
            # lines = [line.rstrip() for line in full_sql_raw.splitlines() if line.strip()]
            # content = '\n'.join(lines)


            
            # 2️⃣ убираем только внешние кавычки строк
            # но не трогаем остальной текст
            def unquote(m):
                return m.group(1).replace("''", "'")

            content = re.sub(r"'((?:[^']|'')*)'", unquote, content)
          
            # content = content.replace("#39", "'")  # Заменяем #39 на ' 
            # 3. Убираем лишние кавычки и экранирование
            # content = content.replace("''", "'")  # Заменяем '' на '
           
            # 2. Заменяем #NNNN на символы Unicode
            def replace_numeric(m):
                try:
                    return chr(int(m.group(1)))
                except:
                    return ""

            content = re.sub(r"#(\d+)", replace_numeric, content)   

            # сохранение отступов и пустых строк
            lines = content.splitlines()
            lines = [line.rstrip() for line in lines]
            sql_text = "\n".join(lines)
             

    m = re.search(r"DocName\s*[=:]\s*'([^']*)'", text, flags=re.I)
    if m:
        docname = m.group(1)

    return sql_text, docname
