"""Подключение к базе данных и выполнение SQL-запросов."""
import datetime
import os
import re
from pathlib import Path
from types import SimpleNamespace
from loguru import logger
import pyodbc
from dotenv import load_dotenv
from utils.logger import log_execution


# custom errors -------------------------------------------------------------
class QueryResultEmpty(Exception):
    """Вызывается, когда SQL-запрос не возвращает строк."""


# connection ----------------------------------------------------------------
@log_execution()
def get_connection():
    """
    Создает и возвращает подключение к базе данных SQL Server.
    Параметры подключения извлекаются из переменных окружения (.env).

    Returns:
        pyodbc.Connection: Объект соединения или None в случае неудачи.
    """
    load_dotenv()

    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    driver = os.getenv("DB_DRIVER")
    trustServerCertificate = os.getenv("DB_TrustServerCertificate")

    connection_string = (
        f"DRIVER={driver};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate={trustServerCertificate};"
    )

    try:
        conn = pyodbc.connect(connection_string)
        logger.info(f"Успешно подключено к базе данных: {database} на сервере {server}")
        return conn
    except Exception as e:
        logger.error("Ошибка подключения:", e)
        return None


# sql execution -------------------------------------------------------------
@log_execution()
def execute_sql(connection, sql_text):
    """
    Выполняет SQL-запрос и возвращает результат в виде списка объектов SimpleNamespace.

    Args:
        connection: Активное соединение pyodbc.
        sql_text (str): Текст SQL-запроса.

    Returns:
        list[SimpleNamespace]: Список строк, где поля доступны через точку (row.FieldName).

    Raises:
        QueryResultEmpty: Если запрос не вернул ни одной строки.
    """
    cursor = connection.cursor()
    cursor.execute(sql_text)
    columns = [col[0] for col in cursor.description] if cursor.description else []
    rows = [SimpleNamespace(**dict(zip(columns, r))) for r in cursor.fetchall()]
    cursor.close()

    if not rows:
        raise QueryResultEmpty("SQL-запрос вернул пустой набор строк")

    return rows


# sql file saving -----------------------------------------------------------
@log_execution()
def save_sql_to_file(sql_text: str, full_path: str, docname: str | None = None) -> Path:
    """
    Сохраняет текст SQL-запроса в файл для отладки.

    Args:
        sql_text (str): Текст SQL для сохранения.
        full_path (str): Базовый путь директории для сохранения.
        docname (str | None): Имя документа для формирования имени файла.

    Returns:
        Path: Путь к созданному .sql файлу.
    """
    # базовое имя файла
    out_base = Path(docname).stem if docname else Path(full_path).stem

    # Создаем красивое имя: sql_ID_ДАТА_ВРЕМЯ.sql
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # убрать недопустимые символы
    safe_name = re.sub(r'[\\/*?:"<>|]', "", out_base) + "_" + now + ".sql"
    out_path = str(Path(full_path) / safe_name)

    try:
        with open(out_path, 'w', encoding='utf-8') as f_out:
            f_out.write(sql_text.strip())
        logger.info(f"SQL сохранён: {out_path}")
        return out_path
    except Exception as e:
        logger.exception(f"Не удалось сохранить SQL в файл {out_path}: {e}")
        raise


# helpers -------------------------------------------------------------------
def find_col_index(cols, name):
    """
    Ищет индекс колонки в списке по имени (без учета регистра).

    Args:
        cols (list): Список имен колонок.
        name (str): Искомое имя.

    Returns:
        int | None: Индекс колонки или None, если не найдена.
    """
    for i, c in enumerate(cols):
        if c.lower() == name.lower():
            return i
    return None