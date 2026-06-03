"""
RepExecutor — CLI точка входа для обработки отчетов.

Использование:
    python RepExecutor.py [--config <путь>]
"""
import argparse
import sys
from pathlib import Path

from utils.system import get_base_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="RepExecutor — обработка отчетов на основе JSON-конфигурации"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Путь к файлу RepExecutor.json (по умолчанию ищется в корне проекта)",
    )
    args = parser.parse_args()

    # Определяем путь к конфигу
    config_path = args.config
    if config_path is None:
        config_path = get_base_path() / "RepExecutor.json"

    if not config_path.exists():
        print(f"[ERROR] Конфигурация не найдена: {config_path}", file=sys.stderr)
        sys.exit(1)

    # Импортируем и запускаем движок
    from core.engine import run
    run(config_path)


if __name__ == "__main__":
    main()