"""
Queue Worker — обработчик задач из очереди rdbReportQueue.

Запускается как обычный Python-скрипт (без API-сервера) и обрабатывает
задачи из SQL-таблицы rdbReportQueue одна за другой.

Режимы:
    python queue_worker.py              # одна задача и выход
    python queue_worker.py --loop       # бесконечный цикл
    python queue_worker.py --loop --interval 10   # цикл с паузой 10с между проверками
"""
import argparse
import configparser
import sys
import time
from pathlib import Path

from loguru import logger

from utils.logger import configure_logger
from utils.system import get_base_path, log_runtime_user, log_drives, log_net_use
from core.db_connector import get_connection
from core.engine import process_query_and_files
from job_queue.manager import (
    get_next_job_from_queue,
    mark_job_done,
    mark_job_error,
    convert_job_to_config,
)


def process_one_job(connection) -> bool:
    """
    Берёт одну задачу из очереди и обрабатывает её.

    Returns:
        True, если задача была успешно обработана;
        False, если очередь пуста.
    """
    job = get_next_job_from_queue(connection)
    if job is None:
        return False

    job_id = job.get("ID")
    report_name = job.get("ReportName", "unknown")
    logger.info(f"Начата обработка задачи {job_id}: report={report_name}")

    # Загружаем общие настройки из INI
    common_cfg = configparser.ConfigParser()
    common_cfg.read(get_base_path() / "RepExecutor.ini", encoding="utf-8")

    try:
        # Преобразуем запись из БД в формат ConfigModel
        config_data = convert_job_to_config(job)

        # Импортируем ConfigModel и создаём экземпляр
        from core.config import ConfigModel
        cfg = ConfigModel(**config_data)

        # Выполняем обработку
        generated_files = process_query_and_files(connection, cfg, common_cfg)

        # Отмечаем задачу как выполненную
        mark_job_done(connection, job_id)

        if generated_files:
            logger.success(
                f"Задача {job_id} завершена, создано файлов: {len(generated_files)}"
            )
        else:
            logger.warning(f"Задача {job_id} завершена, файлы не были созданы")

        return True

    except Exception as e:
        error_msg = str(e)
        logger.exception(f"Ошибка обработки задачи {job_id}: {error_msg}")
        mark_job_error(connection, job_id, error_msg)
        return True


def run_worker(loop: bool = False, interval: int = 5):
    """
    Основной цикл worker-а.

    Args:
        loop: Если True, работает бесконечно, обрабатывая все задачи.
        interval: Пауза в секундах между проверками очереди (только в режиме loop).
    """
    # Настройка логирования
    configure_logger(str(get_base_path() / "RepExecutor.ini"))

    log_runtime_user()
    log_drives()
    log_net_use()

    logger.info("=" * 60)
    logger.info("Queue Worker запущен")
    if loop:
        logger.info(f"Режим: бесконечный цикл, интервал={interval}с")
    else:
        logger.info("Режим: одна задача")
    logger.info("=" * 60)

    connection = get_connection()
    if not connection:
        logger.error("Не удалось подключиться к БД. Завершение.")
        sys.exit(1)

    try:
        jobs_processed = 0

        while True:
            result = process_one_job(connection)

            if not result:
                # Очередь пуста
                if loop:
                    logger.info(f"Очередь пуста, через {interval}с проверяю снова ...")
                    time.sleep(interval)
                    continue
                else:
                    logger.info("Очередь пуста, задач для обработки нет.")
                    break

            jobs_processed += 1

            if not loop:
                logger.info(f"Обработана 1 задача. Завершение (для цикла используйте --loop).")
                break

            if loop:
                logger.info(f"Обработано задач: {jobs_processed}. Проверяю очередь снова ...")
                time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("\nПолучен сигнал прерывания (Ctrl+C). Завершение.")
    finally:
        connection.close()
        logger.info("Соединение с БД закрыто.")


def main():
    parser = argparse.ArgumentParser(
        description="Queue Worker — обработчик задач из rdbReportQueue"
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Бесконечный цикл обработки задач",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Пауза в секундах между проверками очереди (по умолчанию: 5)",
    )

    args = parser.parse_args()
    run_worker(loop=args.loop, interval=args.interval)


if __name__ == "__main__":
    main()