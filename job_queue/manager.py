"""Менеджер очереди задач из БД."""
import json
from typing import Optional

import pyodbc
from dotenv import load_dotenv
from loguru import logger

from utils.logger import log_execution


# ==============================
# РАБОТА С ОЧЕРЕДЬЮ БД
# ==============================

@log_execution()
def get_next_job_from_queue(connection: pyodbc.Connection) -> Optional[dict]:
    """
    Получает первую задачу из очереди со статусом NEW и обновляет её статус на IN_PROGRESS.

    Returns:
        dict: Словарь с данными задачи или None если очередь пуста.
    """
    sql = """
    SET NOCOUNT ON;

    ;WITH cte AS (
        SELECT TOP (1) *
          FROM rdbReportQueue WITH (UPDLOCK, READPAST, ROWLOCK)
         WHERE Status = 'NEW'
         ORDER BY Priority DESC, CreatedAt
    )
    UPDATE cte
       SET Status    = 'IN_PROGRESS',
           StartedAt = SYSDATETIME()
    OUTPUT inserted.*;
    """

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()

        if not row:
            logger.debug("Очередь пуста, нет задач со статусом NEW")
            return None

        columns = [col[0] for col in cursor.description]
        cursor.close()
        job = dict(zip(columns, row))

        logger.info(f"Получена задача {job['ID']} из очереди")
        return job

    except Exception as e:
        logger.exception(f"Ошибка при получении задачи из очереди: {e}")
        return None


@log_execution()
def mark_job_done(connection: pyodbc.Connection, job_id: int) -> None:
    """Обновляет статус задачи на DONE."""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SET ANSI_NULLS ON;
            SET ANSI_PADDING ON;
            SET ANSI_WARNINGS ON;
            SET ARITHABORT ON;
            SET CONCAT_NULL_YIELDS_NULL ON;
            SET NUMERIC_ROUNDABORT OFF;
            SET QUOTED_IDENTIFIER ON;
        """)
        cursor.execute("""     
        UPDATE rdbReportQueue
           SET Status     = 'DONE',
               FinishedAt = SYSDATETIME()
          from rdbReportQueue with (updlock)     
         WHERE ID = ?  
        """, job_id)
        cursor.close()

        logger.success(f"Задача {job_id} отмечена как DONE")
    except Exception as e:
        logger.exception(f"Ошибка при обновлении статуса DONE для {job_id}: {e}")


@log_execution()
def mark_job_error(connection: pyodbc.Connection, job_id: int, 
                    error_message: str) -> None:
    """Обновляет статус задачи на ERROR с сообщением об ошибке."""
    try:
        cursor = connection.cursor()
        # Обрезаем сообщение до 8000 символов (ограничение БД)
        error_msg_truncated = error_message[:8000]
        cursor.execute("""
            SET ANSI_NULLS ON;
            SET ANSI_PADDING ON;
            SET ANSI_WARNINGS ON;
            SET ARITHABORT ON;
            SET CONCAT_NULL_YIELDS_NULL ON;
            SET NUMERIC_ROUNDABORT OFF;
            SET QUOTED_IDENTIFIER ON;
        """)
        cursor.execute("""                   
            UPDATE rdbReportQueue
               SET Status       = 'ERROR',
                   FinishedAt   = SYSDATETIME(),
                   ErrorMessage = ?
              from rdbReportQueue with (updlock)          
             WHERE ID = ?
        """, error_msg_truncated, job_id)
        cursor.close()

        logger.error(f"Задача {job_id} отмечена как ERROR: {error_msg_truncated}")
    except Exception as e:
        logger.exception(f"Ошибка при обновлении статуса ERROR для {job_id}: {e}")


def convert_job_to_config(job: dict) -> dict:
    """
    Преобразует запись задачи из очереди в формат ConfigModel.

    Args:
        job: Словарь с данными из rdbReportQueue

    Returns:
        dict: Данные в формате ConfigModel
    """
    config_data = {
        "report_name": job.get("ReportName"),
        "output_path": job.get("OutputPath"),
        "output_format": job.get("OutputFormat", "PDF"),
        "params": {}
    }

    # Парсим JSON параметры если они есть
    if job.get("ParametersJson"):
        try:
            config_data["params"] = json.loads(job["ParametersJson"])
        except json.JSONDecodeError:
            logger.warning(
                f"Не удалось распарсить ParametersJson для задачи {job.get('ID')}"
            )

    # Добавляем marks если присутствуют ObjectID и ObjectType
    if job.get("ObjectID") and job.get("ObjectType"):
        config_data["marks"] = {
            "Type": int(job["ObjectType"]),
            "ID": int(job["ObjectID"])
        }

    return config_data