import os
from pathlib import Path
import subprocess

from loguru import logger
import getpass
import getpass
import platform
import win32api
import win32file      


def log_runtime_user():
    try:
        user = getpass.getuser()
    except Exception:
        user = os.environ.get("USERNAME", "UNKNOWN")

    logger.debug(f"Текущий пользователь: {user}")
    logger.debug(f"OS user (env USERNAME): {os.environ.get('USERNAME')}")
    logger.debug(f"Домашний каталог: {Path.home()}")
    logger.debug(f"Платформа: {platform.platform()}")


def log_drives():
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
    logger.debug("=== Сетевые диски ===")
    result = subprocess.run(
        ["net", "use"],
        capture_output=True,
        text=True
    )
    logger.debug(result.stdout)     