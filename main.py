import os
import json
import re
import pyodbc
from dotenv import load_dotenv
import win32com.client as win32
import csv
from loguru import logger
from wrd_parser import decode_bytes, parse_wrd_text
from params import inject_report_params


def read_config(path=None):
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "config.json")

    if not os.path.exists(path):
        logger.error(f"Файл конфигурации не найден: {path}")
        return None

    with open(path, "r", encoding="utf-8") as f:
        try:
            cfg = json.load(f)
            return cfg
        except Exception as e:
            logger.error("Ошибка чтения config.json:", e)
            return None


def get_connection():
    load_dotenv()

    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_DATABASE")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER")
    
    connection_string = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=no;"
    )

    try:
        conn = pyodbc.connect(connection_string)
        logger.info(f"Успешно подключено к базе данных: {database} на сервере {server})")
        return conn
    except Exception as e:
        logger.error("Ошибка подключения:", e)
        return None


def perform_mail_merge(template_path, data_csv_path, output_pdf_path):
    try:
        word = win32.Dispatch("Word.Application")
        word.Visible = False  # Не показывать Word

        doc = word.Documents.Open(template_path)
        doc.MailMerge.OpenDataSource(data_csv_path)

        # Выполнить слияние
        doc.MailMerge.Execute()

        # Сохранить как PDF
        doc.SaveAs(output_pdf_path, FileFormat=17)  # 17 - wdFormatPDF

        doc.Close(False)
        word.Quit()

        logger.info(f"Слияние выполнено, сохранено в {output_pdf_path}")
    except Exception as e:
        logger.error(f"Ошибка при слиянии: {e}")


def find_col_index(cols, name):
    for i, c in enumerate(cols):
        if c.lower() == name.lower():
            return i
    return None


def process_query_and_files(report_name, connection, cfg):
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
        logger.error("Ошибка выполнения запроса:", e)
        cursor.close()
        return

    rows = cursor.fetchall()
    if not rows:
        logger.warning("Записи не найдены для report_name=", report_name)
        cursor.close()
        return

    cols = [d[0] for d in cursor.description]
    idx_path = find_col_index(cols, "Path")
    idx_fname = find_col_index(cols, "FileName")

    for r in rows:
        path_val = r[idx_path] if idx_path is not None else None
        fname_val = r[idx_fname] if idx_fname is not None else None

        if not path_val or not fname_val:
            logger.warning("Отсутствуют поля Path/FileName в результате, пропускаю запись")
            continue

        full_path = os.path.join(path_val, fname_val)
        try:
            with open(full_path, "rb") as f:
                data = f.read()
                logger.info(f"Прочитан файл: {full_path} (байт: {len(data)})")

                ext = os.path.splitext(full_path)[1].lstrip('.').lower()
                if ext == 'wrd':
                    text = decode_bytes(data)
                    sql_text, docname = parse_wrd_text(text)
                    logger.info("--- Извлечённый SQL ---")
                    if sql_text:
                        logger.info(sql_text)
                    else:
                        logger.warning("SQL не найден")
                    logger.info("--- DocName ---")
                    if docname:
                        logger.info(docname)
                    else:
                        logger.warning("DocName не найден")

                    if sql_text:
                        # Заменить параметры в SQL
                        sql_text = inject_report_params(sql_text)
                        
                        # Если заданы marks, добавить delete и insert
                        marks = cfg.get('marks')
                        if marks:
                            delete_sql = """
                            delete pErrorLine from pErrorLine WITH (ROWLOCK INDEX=XIE0pErrorLine) where SPID = @@spid

                            delete pDSDiagnosticDataSet from pDSDiagnosticDataSet WITH (ROWLOCK INDEX=XPKpDSDiagnosticDataSet) where SPID in (@@spid)
                            
                            delete tDocMark from tDocMark WITH (ROWLOCK INDEX=XPKtDocMark) where SPID = @@spid
                            """
                            insert_sql = f"INSERT INTO tDocMark ( SPID, Type,ID) SELECT @@spid, {marks['Type']}, {marks['ID']}"
                            full_sql = f"{delete_sql}\n{insert_sql}\n{sql_text}"
                            logger.info("--- Добавлены команды для marks ---")
                            logger.info(full_sql)
                            sql_text = full_sql
                        else:
                            logger.info("--- SQL после замены параметров ---")
                            logger.info(sql_text)

                        # Сохранение полного SQL в файл
                        out_base = os.path.splitext(docname)[0] if docname else os.path.splitext(full_path)[0]
                        out_name = re.sub(r'[\\/*?:"<>|]', "", out_base) + ".sql"
                        with open(out_name, 'w', encoding='utf-8') as f_out:
                            f_out.write(sql_text.strip())
                            logger.info(f"SQL сохранён: {out_name}")

                        # Выполнить SQL
                        try:
                            cursor2 = connection.cursor()
                            cursor2.execute(sql_text)
                            data_rows = cursor2.fetchall()
                            data_cols = [d[0] for d in cursor2.description]
                            cursor2.close()

                            # Сохранить данные в CSV для mail merge
                            csv_path = f"{docname}_data.csv"
                            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                                writer = csv.writer(csvfile)
                                writer.writerow(data_cols)  # Заголовки
                                for row in data_rows:
                                    writer.writerow(row)

                            logger.info(f"Данные сохранены в {csv_path}")

                            # Предполагаем, что шаблон - docname.docx
                            template_path = f"{docname}.docx"
                            if os.path.exists(template_path):
                                output_pdf_path = f"{docname}_output.pdf"
                                perform_mail_merge(template_path, csv_path, output_pdf_path)
                            else:
                                logger.warning(f"Шаблон {template_path} не найден")

                        except Exception as e:
                            logger.error(f"Ошибка выполнения SQL или слияния: {e}")

        except Exception as e:
            logger.error(f"Не удалось прочитать файл {full_path}:", e)

    cursor.close()


if __name__ == "__main__":
    cfg = read_config()
    if not cfg:
        logger.error("Нужно добавить config.json с ключом 'report_name'.")
        raise SystemExit(1)
    logger.info("Считали файл конфигурации")    

    report_name = cfg.get("report_name")
    if not report_name:
        logger.error("В config.json отсутствует 'report_name'.")
        raise SystemExit(1)

    connection = get_connection()

    if connection:
        process_query_and_files(report_name, connection, cfg)
        connection.close()
