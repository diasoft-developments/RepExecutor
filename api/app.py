"""Главное приложение FastAPI для RepExecutor."""
import datetime
import sys
from contextlib import asynccontextmanager
from pathlib import Path

# Добавляем корень проекта в sys.path для корректных импортов
# при запуске через `python api/app.py`
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from api.routes import router
from utils.logger import configure_logger
from utils.system import get_base_path


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Инициализация при старте и завершении приложения."""
    # Инициализация
    configure_logger(str(get_base_path() / "RepExecutor.ini"))
    logger.info("RepExecutor API запущен")
    yield
    # Завершение
    logger.info("RepExecutor API остановлен")


# Создание приложения
app = FastAPI(
    title="RepExecutor API",
    description="""
REST API для автоматизированной генерации отчетов на основе данных из Диасофт.

## Возможности

### Обработка отчетов
- **POST /api/execute** — выполнить отчет по переданной конфигурации (JSON)
- **POST /api/execute_from_queue** — взять следующую задачу из очереди в БД Диасофт

### Управление задачами
- **GET /api/queue** — получить историю всех обработанных задач за сессию
- **GET /api/queue/{task_id}** — получить статус конкретной задачи
- **DELETE /api/queue** — очистить историю задач

### Системные операции
- **GET /api/health** — проверка доступности сервера
- **POST /api/shutdown** — остановить сервер

## Форматы выходных файлов

Поддерживается один формат генерации:
- **PDF** — через шаблон Word (.docx) с последующей конвертацией
- ~~**DOCX** — заполнение шаблона Word с параметрами~~ (не реализовано)
- ~~**XLSX** — генерация таблицы Excel с данными~~ (не реализовано)

## Конфигурация

Настройки подключения к БД и путей хранятся в:
- `RepExecutor.ini` — параметры соединения, пути к шаблонам
- `RepExecutor.json` — конфигурация текущего отчета (создается автоматически)

## Пример использования

```bash
# Выполнить отчет по конфигурации
curl -X POST http://localhost:8000/api/execute \\
  -H "Content-Type: application/json" \\
  -d '{
    "config": {
            "report_name": "report",
            "output_path": "c:\\Tasks\\10002479017.pdf",
            "output_format": "PDF",
            "marks": {
                "Type": 3,
                "ID": 10002479017
            },
            "params": {
                "Podpisant": 10000000026,
                "Controler": 10000000026,
                "InstitutionID": 2000
            }
        }
  }'

# Взять задачу из очереди БД
curl -X POST http://localhost:8000/api/execute_from_queue

# Проверить здоровье сервера
curl http://localhost:8000/api/health
```
""",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение маршрутов (префикс /api уже задан в router)
app.include_router(router)


def run_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = True):
    """Запуск сервера."""
    logger.info(f"Запуск API на {host}:{port}")
    uvicorn.run(
        "api.app:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info",
    )


if __name__ == "__main__":
    run_server()