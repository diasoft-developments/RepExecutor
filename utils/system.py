import os
import sys
from pathlib import Path
import subprocess
import getpass
import platform
import win32api
import win32file

from loguru import logger


def get_base_path() -> Path:
    """
    Возвращает базовый путь проекта.

    Поддерживает два режима:
    1. Собранный exe (PyInstaller) — путь к исполняемому файлу.
    2. Разработка — корневая директория репозитория.

    Ищет маркер-файл RepExecutor.ini (или config/RepExecutor.ini) для
    определения правильного базового пути.
    """
    # PyInstaller: sys._MEIPASS или dirname(sys.executable)
    if getattr(sys, 'frozen', False):
        base = Path(sys.executable).resolve().parent
    else:
        # Разработка: стартуем от текущего файла и поднимаемся вверх
        base = Path(__file__).resolve().parent.parent

    # Проверяем, что это действительно корень проекта
    # Ищем RepExecutor.ini в корне или в config/
    candidate_paths = [base / "RepExecutor.ini", base / "config" / "RepExecutor.ini"]
    for candidate in candidate_paths:
        if candidate.is_file():
            return candidate.parent if "config" in candidate.parts else base

    # Fallback: возвращаем найденный base
    return base


def log_runtime_user():
    """Логирование информации о текущем пользователе."""
    try:
        user = getpass.getuser()
    except Exception:
        user = os.environ.get("USERNAME", "UNKNOWN")

    logger.debug(f"Текущий пользователь: {user}")
    logger.debug(f"OS user (env USERNAME): {os.environ.get('USERNAME')}")
    logger.debug(f"Домашний каталог: {Path.home()}")
    logger.debug(f"Платформа: {platform.platform()}")


def log_drives():
    """Логирование информации о логических дисках."""
    logger.debug("=== Логические диски ===")
    drives = win32api.GetLogicalDriveStrings().split('\x00')[:-1]

    for drive in drives:
        drive_type = win32file.GetDriveType(drive)
        types = {
            win32file.DRIVE_FIXED: "FIXED",
            win32file.DRIVE_REMOTE: "REMOTE",
            win32file.DRIVE_REMOVABLE: "REMOVABLE",
            win32file.DRIVE_CDROM: "CDROM",
            win32file.DRIVE_RAMDISK: "RAMDISK",
        }

        logger.debug(f"{drive} -> {types.get(drive_type, 'UNKNOWN')}")


def log_net_use():
    """Логирование сетевых подключений."""
    logger.debug("=== Сетевые диски ===")
    result = subprocess.run(
        ["net", "use"],
        capture_output=True,
        text=True
    )
    logger.debug(result.stdout)