import configparser
from functools import wraps
from pathlib import Path
import sys
import time

from loguru import logger

def get_base_path():
    # если запущено как exe
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent
    # если обычный .py
    return Path(__file__).parent

def configure_logger() -> str:
    """Настройка логирования на файл RepExecutor.ini

    Уровень возвращается для того, чтобы его можно было использовать
    при добавлении консольного логгера, если параметр командной строки
    не переопределяет его.
    """        
    ini_path = get_base_path() / "RepExecutor.ini"
    config = configparser.ConfigParser()
    config.read(ini_path, encoding="utf-8")
    log_path = config.get("log", "path", fallback="")
    file_name = config.get("log", "file", fallback="RepExecutor.log")
    log_file = Path(log_path) / file_name if log_path else Path(file_name)

    level = config.get("log", "level", fallback="INFO").upper()

    logger.remove()
    logger.add(
        log_file,
        level=level,
        rotation=config.get("log", "rotation", fallback="10 MB"),
        retention=config.get("log", "retention", fallback="7 days"),
        compression=config.get("log", "compression", fallback="zip"),
        encoding="utf-8",
        # format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {extra.get(func_name, function)} | {message}"
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {function} | {message}"
    )
    logger.debug(f"configure_logger Load")
    return level

# def log_execution(level="DEBUG"):
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             logger.log(level, f"{func.__name__} | BEGIN")
#             start = time.perf_counter()
#             try:
#                 return func(*args, **kwargs)
#             finally:
#                 elapsed = time.perf_counter() - start
#                 logger.log(level, f"{func.__name__} | END | {elapsed:.3f}s")
#         return wrapper
#     return decorator

def log_execution(level="DEBUG"):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Создаем "заплатку", которая подменяет имя функции в объекте record
            patched_logger = logger.patch(lambda record: record.update(function=func.__name__))
            
            patched_logger.log(level, "BEGIN")
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                elapsed = time.perf_counter() - start
                patched_logger.log(level, f"END | {elapsed:.3f}s")
        return wrapper
    return decorator