"""Главное приложение FastAPI для RepExecutor."""
import datetime
from contextlib import asynccontextmanager
from pathlib import Path

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
    description="API для автоматизированной обработки отчетов Diasoft",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение маршрутов
app.include_router(router, prefix="/api")


def run_server(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
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