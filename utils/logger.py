import configparser
import functools
import sys
from pathlib import Path
from loguru import logger


def configure_logger(ini_path: str = "RepExecutor.ini") -> None:
    """
    Настраивает логгер на основе секции [log] из INI-файла.

    Args:
        ini_path: Путь к конфигурационному .ini файлу.
    """
    config = configparser.ConfigParser()
    found = config.read(ini_path, encoding="utf-8")

    if not found:
        # Fallback: читаем из вложенного config/
        alt = Path("config") / "RepExecutor.ini"
        if alt.exists():
            config.read(str(alt), encoding="utf-8")

    level = config.get("log", "level", fallback="INFO").upper()
    log_file = config.get("log", "file", fallback="app.log")
    rotation = config.get("log", "rotation", fallback="10 MB")

    setup_logger(log_file=log_file, level=level, rotation=rotation)


def setup_logger(log_file: str = "app.log", level: str = "INFO", rotation: str = "10 MB") -> None:
    """
    Настраивает логгер loguru.
    
    Args:
        log_file: Путь к файлу логов
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR)
        rotation: Правило ротации файлов
    """
    # Убираем все дефолтные хендлеры
    logger.remove()
    
    # Консоль
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<7}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    # Файл
    log_path = Path(log_file)
    if log_path.parent.exists():
        logger.add(
            log_file,
            level=level,
            rotation=rotation,
            retention="7 days",
            compression="zip",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:<7} | {name}:{function}:{line} - {message}"
        )
    else:
        logger.warning(f"Директория для логов не найдена: {log_path.parent}, логирование в файл отключено")


def log_execution(func=None, *, log_args: bool = True, log_result: bool = True):
    """
    Декоратор для логирования выполнения функции.
    
    Args:
        func: Функция для логирования
        log_args: Логировать аргументы
        log_result: Логировать результат
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            func_name = fn.__qualname__
            
            # Лог входных данных
            if log_args and args or kwargs:
                safe_args = []
                for a in args:
                    s = str(a)
                    if len(s) > 200:
                        s = s[:200] + "..."
                    safe_args.append(s)
                safe_kwargs = {}
                for k, v in kwargs.items():
                    s = str(v)
                    if len(s) > 200:
                        s = s[:200] + "..."
                    safe_kwargs[k] = s
                logger.debug(f"[{func_name}] вызов с args={safe_args}, kwargs={safe_kwargs}")
            
            # Лог начала
            logger.debug(f"[{func_name}] начало выполнения")
            
            try:
                result = fn(*args, **kwargs)
                
                # Лог результата
                if log_result:
                    s = str(result)
                    if len(s) > 200:
                        s = s[:200] + "..."
                    logger.debug(f"[{func_name}] завершено успешно, результат: {s}")
                
                return result
            except Exception as e:
                logger.exception(f"[{func_name}] ошибка: {e}")
                raise
        return wrapper
    
    if func is not None:
        return decorator(func)
    return decorator