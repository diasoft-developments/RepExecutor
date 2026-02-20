import os
import json
import re
import pyodbc
from dotenv import load_dotenv
from wrd_parser import decode_bytes, parse_wrd_text


def read_config(path=None):
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "config.json")

    if not os.path.exists(path):
        print(f"Файл конфигурации не найден: {path}")
        return None

    with open(path, "r", encoding="utf-8") as f:
        try:
            cfg = json.load(f)
            return cfg
        except Exception as e:
            print("Ошибка чтения config.json:", e)
            return None


def get_connection():
    load_dotenv()

    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER")

    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )

    try:
        conn = pyodbc.connect(connection_string)
        print("Подключение успешно")
        return conn
    except Exception as e:
        print("Ошибка подключения:", e)
        return None


def find_col_index(cols, name):
    for i, c in enumerate(cols):
        if c.lower() == name.lower():
            return i
    return None


def process_query_and_files(connection, report_name):
    sql = """
        select g.Path
              ,s.FileName
          from tSample s WITH (NOLOCK)
         inner join tSampleGroup g WITH (NOLOCK)
                 on s.Type = g.SampleGroupID
         where s.brief = ?
         order by s.Brief
    """

    cursor = connection.cursor()
    try:
        cursor.execute(sql, report_name)
    except Exception as e:
        print("Ошибка выполнения запроса:", e)
        cursor.close()
        return

    rows = cursor.fetchall()
    if not rows:
        print("Записи не найдены для report_name=", report_name)
        cursor.close()
        return

    cols = [d[0] for d in cursor.description]
    idx_path = find_col_index(cols, "Path")
    idx_fname = find_col_index(cols, "FileName")

    for r in rows:
        path_val = r[idx_path] if idx_path is not None else None
        fname_val = r[idx_fname] if idx_fname is not None else None

        if not path_val or not fname_val:
            print("Отсутствуют поля Path/FileName в результате, пропускаю запись")
            continue

        full_path = os.path.join(path_val, fname_val)
        try:
            with open(full_path, "rb") as f:
                data = f.read()
                print(f"Прочитан файл: {full_path} (байт: {len(data)})")

                ext = os.path.splitext(fname_val)[1].lstrip('.').lower()
                if ext == 'wrd':
                    text = decode_bytes(data)
                    sql_text, docname = parse_wrd_text(text)
                    print("--- Извлечённый SQL ---")
                    if sql_text:
                        print(sql_text)
                    else:
                        print("SQL не найден")
                    print("--- DocName ---")
                    if docname:
                        print(docname)
                    else:
                        print("DocName не найден")

                    # Сохранение SQL в файл с именем DocName (если найден)
                    if sql_text:
                        safe_name = None
                        if docname and docname.strip():
                            # удалить недопустимые в именах файлов символы
                            safe_name = re.sub(r'[<>:\\\"/\\|?*]', '_', docname).strip()
                        if not safe_name:
                            base = os.path.splitext(fname_val)[0]
                            safe_name = base
                        out_name = f"{safe_name}.sql"
                        out_path = os.path.join(os.path.dirname(__file__), out_name)
                        try:
                            with open(out_path, 'w', encoding='utf-8') as out_f:
                                out_f.write(sql_text)
                            print(f"SQL сохранён: {out_path}")
                            # print(sql_text)
                        except Exception as e:
                            print(f"Не удалось сохранить SQL в {out_path}:", e)
                else:
                    # для остальных просто показываем размер/байты
                    pass
        except Exception as e:
            print(f"Не удалось прочитать файл {full_path}:", e)

    cursor.close()


if __name__ == "__main__":
    cfg = read_config()
    if not cfg:
        print("Нужно добавить config.json с ключом 'report_name'.")
        raise SystemExit(1)
    print("Считали файл конфигурации")    

    report_name = cfg.get("report_name")
    if not report_name:
        print("В config.json отсутствует 'report_name'.")
        raise SystemExit(1)

    connection = get_connection()

    if connection:
        process_query_and_files(connection, report_name)
        connection.close()
