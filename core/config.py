"""Управление конфигурацией."""
import json
import configparser
from pathlib import Path
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from loguru import logger
from utils.logger import log_execution
from utils.system import get_base_path


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
        return values

    @field_validator('output_path')
    @classmethod
    def check_output_dir_exists(cls, v: Path) -> Path:
        """
        Проверяет существование родительской директории для выходного файла
        и наличие прав на запись в неё.
        """
        import os
        if not v.parent.exists():
            raise ValueError(f"Целевая директория не найдена: {v.parent}")

        if v.parent.exists() and not os.access(v.parent, os.W_OK):
            logger.warning(f"Возможно, отсутствуют права на запись в {v.parent}")

        return v


@log_execution()
def load_ini_config(path: str = "RepExecutor.ini") -> configparser.ConfigParser:
    """Загружает INI конфигурацию приложения."""
    config = configparser.ConfigParser()
    config.read(path, encoding="utf-8")
    return config


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
        cfg = ConfigModel(**raw)

        output_file = Path(cfg.output_path)
        if not output_file.parent.exists():
            logger.error(f"Целевая директория не существует: {output_file.parent}")
            return None

        logger.info("Конфигурация успешно проверена")
        return cfg

    except ValidationError as ve:
        logger.error(f"Конфигурация RepExecutor.json не прошла валидацию: \n{ve}")
        return None


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
        import os
        if not os.path.exists(config_path) or os.path.getsize(config_path) == 0:
            logger.error(f"Файл конфигурации не был создан или пуст: {config_path}")
            return False

        logger.info(f"Файл конфигурации успешно создан: {config_path}")
        return True

    except Exception as e:
        logger.exception(f"Ошибка создания конфигурационного файла: {e}")
        return False