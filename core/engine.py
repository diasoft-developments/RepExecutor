"""Основной движок обработки отчетов."""
import configparser
import datetime
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import List, Optional
import pandas as pd
from loguru import logger
from utils.logger import log_execution
from utils.system import get_base_path
from core.config import ConfigModel, read_config
from core.db_connector import (
    get_connection, execute_sql, save_sql_to_file,
    find_col_index, QueryResultEmpty
)
from parsers.wrd_parser import decode_bytes, parse_wrd_text
from parsers.wrd_field_formatter import _apply_digit_fields, _apply_twr_masks
from generators.word_generator import submit_mail_merge_job


# csv creation --------------------------------------------------------------
class CSVCreationError(Exception):
    """Вызывается при ошибке создания временного CSV-файла."""


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
            df.to_csv(csv_path, sep=";", index=False, encoding="utf-8-sig") # , float_format='%.2f'
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


# main processing -----------------------------------------------------------
@log_execution()
def process_query_and_files(connection, cfg: ConfigModel, common_cfg: configparser.ConfigParser) -> List[Path]:
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
    generated_files: List[Path] = []
    sql = """
         select case 
                  when isnull(g.Path, '') = ''
                  then 'T:\\7.2_003\\BIN\\Reports\\'
                  else g.Path
                end as Path
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
        return generated_files

    rows = cursor.fetchall()
    if not rows:
        logger.warning("Записи не найдены для report_name=", cfg.report_name)
        cursor.close()
        return generated_files

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

        if not full_path.exists():
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
                    sql_text, docname, digit_fields, twr_fields = parse_wrd_text(text, params)

                    if sql_text:
                        logger.info("SQL - извлечён")
                    else:
                        logger.warning("SQL не найден")

                    if docname:
                        logger.info(f"DocName - извлечён [{docname}]")
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
                        ID = marks.ID
                        logger.debug(f'ObjectID: {ID}')
                        insert_sql = f"INSERT INTO tDocMark (SPID, Type, ID) SELECT @@spid, {marks.Type}, {marks.ID}"

                    full_sql = f"{delete_sql}\n{insert_sql}\n{sql_text}"
                    sql_text = full_sql

                    # Сохранение полного SQL в файл
                    sql_file_path = None
                    is_tmpsave = common_cfg.getboolean("tmp", "save", fallback=False)
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

                        # Применяем TWRDigitField: преобразуем числовые поля в прописью
                        if digit_fields:
                            data_rows = _apply_digit_fields(data_rows, digit_fields)
                        
                        # Применяем TWRField: форматируем значения по маскам (@n20.2_, @d6., @s<w>)
                        if twr_fields:
                            data_rows = _apply_twr_masks(data_rows, twr_fields)
                        
                        # сохранить результирующие строки в CSV (может бросить CSVCreationError)
                        csv_dir = tmp_save_path if is_tmpsave and tmp_save_path else None
                        base = Path(full_path).stem
                        
                        if ID:
                            base = f"{base}_{ID}"
                        csv_path = rows_to_csv(data_rows, csv_dir, base)  # имя стандартизировано
                        template_doc_path = str(Path(full_path).parent / docname)

                        logger.debug(f"ID: {ID}")
                        logger.debug(f"csv_path: {csv_path}")
                        logger.debug(f"template_path: {template_doc_path}")

                        if Path(template_doc_path).exists():
                            output_path_cfg = Path(cfg.output_path)
                            
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
                                future = submit_mail_merge_job(template_doc_path, csv_path, output_pdf_path, common_cfg)
                                generated_files.append(Path(output_pdf_path))
                                try:
                                    result = future.result()
                                except Exception as e:
                                    logger.error(f"Mail Merge завершился ошибкой: {e}")
                                    raise

                            finally:
                                # Проверяем настройку [tmp] save из INI: если False, удаляем временные файлы
                                if should_delete_tmp:
                                    tmp_files_to_clean = [Path(csv_path), sql_file_path]
                                    for tmp_f in tmp_files_to_clean:
                                        if tmp_f:
                                            try:
                                                if tmp_f.exists():
                                                    tmp_f.unlink()
                                                    logger.debug(f"Временный файл удалён (save=false): {tmp_f}")
                                            except Exception:
                                                logger.exception(f"Не удалось удалить временный файл: {tmp_f}")
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


# standalone entry point ----------------------------------------------------
def run(executor_json_path: Optional[Path] = None):
    """
    Точка входа для запуска обработчика из командной строки или внешнего скрипта.

    Args:
        executor_json_path: Путь к RepExecutor.json. Если None, ищется в корне проекта.
    """
    from utils.system import log_runtime_user, log_drives, log_net_use

    # настройка логирования (из *.ini)
    from utils.logger import configure_logger
    configure_logger(str(get_base_path() / "RepExecutor.ini"))

    log_runtime_user()
    log_drives()
    log_net_use()

    # всегда загружаем и валидируем конфиг
    cfg = read_config(executor_json_path)
    if not cfg:
        logger.error("Некорректный файл конфигурации, см. выше ошибки.")
        raise SystemExit(1)

    # подготовка настроек
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


if __name__ == "__main__":
    run()