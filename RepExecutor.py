import configparser
import datetime
import os
import json
import re
import pyodbc
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv
from utils_logger import configure_logger, get_base_path, log_execution
from utils_system import log_drives, log_net_use, log_runtime_user
from utils_word import perform_mail_merge, submit_mail_merge_job
from wrd_parser import decode_bytes, parse_wrd_text
from types import SimpleNamespace
import tempfile, pandas as pd
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from typing import Optional, Dict, Any 


# custom errors -------------------------------------------------------------
class QueryResultEmpty(Exception):
    """Вызывается, когда SQL-запрос не возвращает строк."""


class CSVCreationError(Exception):
    """Вызывается при ошибке создания временного CSV-файла."""

# --------------------------------------------------------------------------
class MarksModel(BaseModel):
    """Модель данных для меток (Type и ID)."""
    Type: int
    ID: int

class ConfigModel(BaseModel):
    """Модель конфигурации запуска отчета."""
    report_name: str = Field(..., description="имя отчёта для выполнения")
    output_path: Path = Field(..., description="полный путь к файлу отчета")
    output_format: str = Field("PDF", description="формат отчета (например, PDF, XLSX)")
    marks: Optional[MarksModel] = None
    params: Optional[Dict[str, Any]] = None

    # Запрещаем лишние поля
    model_config = {"extra": "forbid"}

    @model_validator(mode="before")
    def pre_check_paths(cls, values):
        """
        Предварительная обработка данных перед валидацией.
        Обеспечивает корректную интерпретацию путей.
        """
        # Если пришла строка в output_path, Pydantic сам сконвертирует её в Path
        # благодаря аннотации типа, но здесь можно реализовать доп. логику
        return values

    @field_validator('output_path')
    @classmethod
    def check_output_dir_exists(cls, v: Path) -> Path:
        """
        Проверяет существование родительской директории для выходного файла
        и наличие прав на запись в неё.
        """
        # На вход уже пришел объект Path (Pydantic сконвертировал его)
        if not v.parent.exists():
            raise ValueError(f"Целевая директория не найдена: {v.parent}")

        # Проверка на права записи (опционально)
        if v.parent.exists() and not os.access(v.parent, os.W_OK):
            logger.warning(f"Возможно, отсутствуют права на запись в {v.parent}")

        return v


@log_execution()
def read_config(path: Optional[Path] = None) -> Optional[ConfigModel]:
    """
    Загружает и валидирует файл конфигурации JSON.
    
    Args:
        path (Optional[Path]): Путь к файлу конфигурации. Если не указан, ищется RepExecutor.json в корне.

    Returns:
        Optional[ConfigModel]: Объект валидированной конфигурации или None при ошибке.
    """

    if path is None:
        path = get_base_path() / "RepExecutor.json"

    path = Path(path)
    logger.info(f"Загрузка конфигурации из: {path}")

    if not path.exists():
        logger.error(f"Файл конфигурации не найден: {path}")
        return None

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Ошибка чтения JSON: {e}")
        return None

    try:
        # Pydantic автоматически вызовет валидатор check_output_dir_exists
        cfg = ConfigModel(**raw)

        # Дополнительная явная проверка (если не использовать валидаторы в модели):
        output_file = Path(cfg.output_path)
        if not output_file.parent.exists():
            logger.error(f"Целевая директория не существует: {output_file.parent}")
            return None

        logger.info("Конфигурация успешно проверена.")
        return cfg

    except ValidationError as ve:
        # Здесь поймаются и ошибки формата, и наша ошибка "Директория не существует"
        logger.error(f"Конфигурация RepExecutor.json не прошла валидацию: \n{ve}")
        return None

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
        F"TrustServerCertificate={trustServerCertificate};"
    )

    try:
        conn = pyodbc.connect(connection_string)
        logger.info(f"Успешно подключено к базе данных: {database} на сервере {server}")

        return conn
    except Exception as e:
        logger.error("Ошибка подключения:", e)
        return None

# helper functions -----------------------------------------------------------
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

@log_execution()
def rows_to_csv(rows, directory: str | None = None, base_name: str | None = None):
    """
    Сериализует список объектов в CSV-файл для Mail Merge.

    При передаче `directory` создаёт файл в указанной папке, иначе
    использует системный временный каталог.  Имя файла
    может быть стандартизировано, если указан `base_name`.

    Args:
        rows (list): Список объектов SimpleNamespace с данными.
        directory (str | None): Путь к каталогу для хранения CSV. Если `None`,
            используется временный файл OS.
        base_name (str | None): Базовое имя для файла (без расширения). Если
            указано, к нему будет добавлен штамп времени для уникальности.

    Returns:
        Path: Путь к созданному CSV-файлу.

    Raises:
        CSVCreationError: Если список пуст или возникла ошибка записи.
    """    
    if not rows:
        raise CSVCreationError("Нечего записывать, список строк пуст")

    try:
        # если указали базовое имя, формируем собственную длину пути,
        # иначе падаем на NamedTemporaryFile
        if base_name:
            now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_base = re.sub(r'[\\/*?:"<>|]', "", base_name)
            fname = f"{safe_base}_{now}.csv"
            if directory:
                Path(directory).mkdir(parents=True, exist_ok=True)
                csv_path = Path(directory) / fname
            else:
                csv_path = Path(tempfile.gettempdir()) / fname
            # записываем через pandas напрямую
            df = pd.DataFrame([vars(r) for r in rows])
            df.to_csv(csv_path, sep=";", index=False, encoding="utf-8-sig")
        else:
            kwargs = {
                "mode": "w",
                "suffix": ".csv",
                "delete": False,
                "encoding": "utf-8-sig",
                "newline": "",
            }
            if directory:
                Path(directory).mkdir(parents=True, exist_ok=True)
                kwargs["dir"] = directory

            with tempfile.NamedTemporaryFile(**kwargs) as f:
                csv_path = Path(f.name)

                # преобразуем namespace → dict
                df = pd.DataFrame([vars(r) for r in rows])
                df.to_csv(f, sep=";", index=False)
    except IOError as ioe:
        raise CSVCreationError(f"Ошибка при создании CSV: {ioe}") from ioe

    # проверка, что файл действительно создан и не пуст
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        raise CSVCreationError(f"CSV-файл не был создан или пуст: {csv_path}")

    return csv_path

# ---------------------------------------------------------------------------
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

@log_execution()
def process_query_and_files(connection, cfg, common_cfg):
    """
    Основная процедура обработки отчета. 
    1. Ищет метаданные шаблона в БД по report_name.
    2. Читает и парсит файл шаблона (.wrd).
    3. Подготавливает SQL (очистка временных таблиц, вставка меток).
    4. Выполняет итоговый SQL-запрос.
    5. Генерирует CSV с данными и выполняет Mail Merge в Word для создания PDF.

    Args:
        connection: Активное соединение с БД.
        cfg (ConfigModel): Валидированная конфигурация запуска.
        common_cfg (ConfigParser): Общие настройки из .ini файла.

    Returns:
        list[Path]: Список путей к сгенерированным файлам.
    """
    generated_files = []
    sql = """
        select g.Path     as Path
              ,s.FileName as FileName
              ,s.Brief    as RepBrief
              ,s.Name     as RepName
          from tSample s WITH (NOLOCK)
         inner join tSampleGroup g WITH (NOLOCK)
                 on s.Type = g.SampleGroupID
         where s.brief = ?
         order by s.Brief
    """

    cursor = connection.cursor()
    try:
        cursor.execute(sql, cfg.report_name)
    except Exception as e:
        logger.error("Ошибка выполнения запроса:", e)
        cursor.close()
        return

    rows = cursor.fetchall()
    if not rows:
        logger.warning("Записи не найдены для report_name=", cfg.report_name)
        cursor.close()
        return

    cols = [d[0] for d in cursor.description]
    idx_path  = find_col_index(cols, "Path")
    idx_fname = find_col_index(cols, "FileName")

    marks = cfg.marks
    params = cfg.params
    logger.debug('Сведения про RepExecutor.json')
    logger.debug(f'report_name:   {cfg.report_name}')
    logger.debug(f'output_path:   {cfg.output_path}')
    logger.debug(f'output_format: {cfg.output_format}')
    logger.debug(f"marks Type:    {marks.Type if marks else '-'}")
    logger.debug(f"marks ID:      {marks.ID if marks else '-'}\n")

    for r in rows:
        # Инициализация локальных переменных, чтобы значения не "утекали" между итерациями
        sql_text = None
        docname = None

        logger.debug('Сведения про отчет')
        logger.debug(f'RepBrief: {r.RepBrief}')
        logger.debug(f'RepName:  {r.RepName}')
        logger.debug(f'Path:     {r.Path}')
        logger.debug(f'FileName: {r.FileName}')

        path_val = r[idx_path] if idx_path is not None else None
        fname_val = r[idx_fname] if idx_fname is not None else None

        if not path_val or not fname_val:
            logger.warning("Отсутствуют поля Path/FileName в результате, пропускаю запись")
            continue

        full_path = Path(path_val) / fname_val               
        logger.debug(f"1 Пробуем открыть файл: {full_path}")
        logger.debug(f"Существует: {full_path.exists()}")
                
        full_path = full_path.resolve()
        logger.debug(f"2 Пробуем открыть файл: {full_path}")
        logger.debug(f"Существует: {full_path.exists()}")    
                           
        try:
            with open(full_path, "rb") as f:
                data = f.read()
                logger.info(f"Прочитан файл: {full_path} (байт: {len(data)})")

                ext = Path(full_path).suffix.lstrip('.').lower()
                logger.info(f"Тип отчета: {ext}")

                if ext == 'wrd':
                    text = decode_bytes(data)
                    sql_text, docname = parse_wrd_text(text, params)

                    if sql_text:
                        logger.info("SQL извлечён")
                    else:
                        logger.warning("SQL не найден")

                    if docname:
                        logger.info(f"DocName извлечён: {docname}")
                    else:
                        logger.warning("DocName не найден")

                if sql_text:
                    delete_sql = """
                    set nocount on;
                    
                    delete pErrorLine
                      from pErrorLine WITH (ROWLOCK INDEX=XIE0pErrorLine) 
                     where SPID = @@spid;
                        
                    delete pDSDiagnosticDataSet 
                      from pDSDiagnosticDataSet WITH (ROWLOCK INDEX=XPKpDSDiagnosticDataSet) 
                     where SPID = @@spid;
                        
                    delete pDSDiagnosticFields  
                      from pDSDiagnosticFields  WITH (ROWLOCK INDEX=XPKpDSDiagnosticFields)  
                     where SPID = @@spid;
                    
                    delete tDocMark 
                      from tDocMark WITH (ROWLOCK INDEX=XPKtDocMark) 
                     where SPID = @@spid;
                    """
                    insert_sql = ""

                    ID = None
                    if marks:
                        ID = marks.ID # или cfg.marks.ID
                        logger.debug(f'ObjectID: {ID}')
                        insert_sql = f"INSERT INTO tDocMark (SPID, Type, ID) SELECT @@spid, {marks.Type}, {marks.ID}"

                    full_sql = f"{delete_sql}\n{insert_sql}\n{sql_text}"
                    sql_text = full_sql

                    # Сохранение полного SQL в файл
                    sql_file_path = None
                    is_tmpsave  = common_cfg.getboolean("tmp", "save", fallback=False)
                    if is_tmpsave:
                        tmp_save_path = common_cfg.get("tmp", "path", fallback="")
                        filename = Path(full_path).stem
                        if ID:
                            filename = f"{filename}_{ID}"
                        sql_file_path = save_sql_to_file(sql_text, tmp_save_path, filename)

                    # Выполнить SQL
                    try:
                        # выполнять запрос и получить результат; execute_sql теперь выбросит
                        # QueryResultEmpty при отсутствии строк
                        data_rows = execute_sql(connection, sql_text)

                        # сохранить результирующие строки в CSV (может бросить CSVCreationError)
                        csv_dir = tmp_save_path if is_tmpsave and tmp_save_path else None
                        base = Path(full_path).stem
                        if ID:
                            base = f"{base}_{ID}"
                        csv_path = rows_to_csv(data_rows, csv_dir, base)  # имя стандартизировано
                        template_doc_path = str(Path(full_path).parent / docname)

                        logger.debug(f"csv_path: {csv_path}")
                        logger.info(f"template_path: {template_doc_path}")

                        if Path(template_doc_path).exists():

                            output_path_cfg = Path(cfg.output_path)
                            logger.info(f"ID: {ID}")

                            # если указан файл (есть расширение)
                            if output_path_cfg.suffix:
                                base_name = output_path_cfg.stem
                                output_pdf_path = output_path_cfg.parent / f"{base_name}.pdf"
                                logger.debug("Указано имя файла — формируем уникальное")
                            else:
                                # если указана только папка
                                output_path_cfg.mkdir(parents=True, exist_ok=True)
                                output_pdf_path = output_path_cfg / f"rep_{ID if ID else 'result'}.pdf"
                                logger.debug("Указан только путь — формируем имя файла")

                            output_pdf_path = str(output_pdf_path)

                            logger.debug(f"Итоговый output_pdf_path: {output_pdf_path}")
                            
                            should_delete_tmp = not is_tmpsave
                            try:
                                # with ThreadPoolExecutor(max_workers=2) as executor:
                                #     submit_mail_merge_job(template_doc_path, csv_path, output_pdf_path, common_cfg)
                                #     generated_files.append(Path(output_pdf_path))
                                    
                                future = submit_mail_merge_job(template_doc_path, csv_path, output_pdf_path, common_cfg)
                                generated_files.append(Path(output_pdf_path))
                                try:
                                    result = future.result()
                                except Exception as e:
                                    logger.error(f"Mail Merge завершился ошибкой: {e}")
                                    raise                                    

                                    logger.info(f"Mail Merge запущен в фоне: {output_pdf_path}")                                
                            finally:
                                # Проверяем настройку [tmp] save из INI: если False, удаляем временные файлы
                                if should_delete_tmp:   
                                    if sql_file_path:                                
                                        try:
                                            if Path(sql_file_path).exists():
                                                Path(sql_file_path).unlink()
                                                logger.debug(f"Временный SQL удалён (save=false): {sql_file_path}")
                                        except Exception:
                                            logger.exception(f"Не удалось удалить временный SQL: {sql_file_path}")
                                else:
                                    logger.debug(f"Сохраняем временные файлы (save=true): CSV={csv_path}, SQL={sql_file_path}")

                        else:
                            logger.warning(f"Шаблон {template_doc_path} не найден")

                    except (QueryResultEmpty, CSVCreationError) as err:
                        # критические ошибки обработки данных – логируем и переходим к следующему файлу
                        logger.error(err)
                        continue
                    except Exception as e:
                        logger.error(f"Ошибка выполнения SQL или слияния: {e}")

        except Exception as e:
            logger.error(f"Не удалось прочитать файл {full_path}: {e}")

    cursor.close()
    return generated_files


if __name__ == "__main__":
    # настройка логирования (из *.ini)
    configure_logger()
    
    log_runtime_user()
    log_drives()
    log_net_use()    

    # всегда загружаем и валидируем конфиг
    cfg = read_config()
    if not cfg:
        logger.error("Некорректный файл конфигурации, см. выше ошибки.")
        raise SystemExit(1)

    # подготовка tmp-настроек
    config = configparser.ConfigParser()
    config.read(get_base_path() / "RepExecutor.ini", encoding="utf-8")
    is_tmpsave = config.getboolean("tmp", "save", fallback=False)
    tmp_save_path = config.get("tmp", "path", fallback="") if is_tmpsave else None

    # резервируем файл конфигурации в tmp-папке, если нужно
    config_json = get_base_path() / "RepExecutor.json"
    if is_tmpsave and tmp_save_path and config_json.exists():
        try:
            Path(tmp_save_path).mkdir(parents=True, exist_ok=True)
            stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            dst = Path(tmp_save_path) / f"RepExecutor_{stamp}.json"
            import shutil
            shutil.copy2(config_json, dst)
            logger.info(f"Сохранена копия конфигурации: {dst}")
        except Exception:
            logger.exception("Ошибка при сохранении копии RepExecutor.json")

    connection = get_connection()

    if connection:
        process_query_and_files(connection, cfg, config)
        connection.close()

    # всегда удаляем исходный конфиг после обработки (копия, если надо, уже сделана)
    try:
        if config_json.exists():
            config_json.unlink()
            logger.info(f"Исходный RepExecutor.json удалён: {config_json}")
    except Exception:
        logger.exception("Не удалось удалить исходный RepExecutor.json")