import json
import configparser
from pathlib import Path
import os
from loguru import logger
from utils_logger import get_base_path, log_execution


def load_ini_config(path: str = "RepExecutor.ini") -> configparser.ConfigParser:
    """Загружает INI конфигурацию приложения."""
    config = configparser.ConfigParser()
    config.read(path, encoding="utf-8")
    return config

@log_execution()
def create_config_file(job_data: dict, config_path: str) -> bool:
    """
    Создает JSON конфигурационный файл для RepExecutor на основе данных задачи.

    Args:
        job_data: Словарь с данными задачи из БД или API.
        config_path: Путь для сохранения конфигурационного файла.

    Returns:
        bool: True если файл успешно создан, иначе False.
    """
    try:
        config_data = {
            "report_name": job_data.get("report_name"),
            "output_path": job_data.get("output_path"),
            "output_format": job_data.get("output_format", "PDF"),
            "params": job_data.get("params", {})
        }

        # Добавляем marks если присутствуют ObjectID и ObjectType
        if job_data.get("object_id") and job_data.get("object_type"):
            config_data["marks"] = {
                "Type": int(job_data["object_type"]),
                "ID": int(job_data["object_id"])
            }

        # Создаем директорию если её нет
        Path(config_path).parent.mkdir(parents=True, exist_ok=True)

        # Записываем JSON
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=4, ensure_ascii=False)

        # Проверка
        if not os.path.exists(config_path) or os.path.getsize(config_path) == 0:
            logger.error(f"Файл конфигурации не был создан или пуст: {config_path}")
            return False

        logger.info(f"Файл конфигурации успешно создан: {config_path}")
        return True

    except Exception as e:
        logger.exception(f"Ошибка создания конфигурационного файла: {e}")
        return False