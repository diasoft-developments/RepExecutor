"""Маршруты API для RepExecutor."""
import datetime
import shutil
from pathlib import Path
from typing import Optional
import uvicorn
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel, Field
from core.config import read_config
from core.engine import process_query_and_files
from core.db_connector import get_connection
from utils.system import get_base_path


router = APIRouter()


# --- Pydantic models for API validation ---
class ExecutorConfig(BaseModel):
    """Модель валидации входящей конфигурации (RepExecutor.json)."""
    report_name: str = Field(..., description="Имя отчета")
    output_path: str = Field(..., description="Путь к выходному файлу/папке")
    output_format: str = Field("pdf", description="Формат вывода")
    marks: Optional[dict] = Field(None, description="Метки документа")
    params: Optional[dict] = Field(None, description="Параметры запроса")


class ApiRequest(BaseModel):
    """Запрос API для запуска обработки."""
    config: ExecutorConfig
    json_path: Optional[str] = Field(None, description="Путь к файлу RepExecutor.json для сохранения")


class QueueItem(BaseModel):
    """Элемент очереди задач."""
    id: int
    config: ExecutorConfig
    status: str = "pending"
    created_at: str
    result: Optional[str] = None
    error: Optional[str] = None


# --- In-memory task queue (for single-instance protection) ---
_task_queue: list[QueueItem] = []
_next_id: int = 1


@router.get("/health")
def health_check():
    """Проверка здоровья API."""
    return {"status": "ok", "timestamp": datetime.datetime.now().isoformat()}


@router.post("/execute", status_code=200)
def execute_report(request: ApiRequest):
    """
    Запустить обработку отчета.

    1. Сохраняет конфиг в RepExecutor.json (опционально в указанную папку).
    2. Подключается к БД.
    3. Выполняет обработку.
    4. Возвращает результат.
    """
    global _next_id

    # Создаем задачу в очереди
    task = QueueItem(
        id=_next_id,
        config=request.config,
        status="processing",
        created_at=datetime.datetime.now().isoformat(),
    )
    _next_id += 1
    _task_queue.append(task)

    logger.info(f"Задача #{task.id} создана: report={request.config.report_name}")

    try:
        # Сохраняем конфиг как RepExecutor.json, если указан путь
        if request.json_path:
            json_dir = Path(request.json_path).parent
            json_dir.mkdir(parents=True, exist_ok=True)
            json_path = Path(request.json_path)
        else:
            json_path = get_base_path() / "RepExecutor.json"

        # Формируем dict из Pydantic модели и сохраняем
        import json
        config_data = request.config.model_dump()
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Конфиг сохранен: {json_path}")

        # Читаем и валидируем конфиг через read_config
        cfg = read_config(json_path)
        if not cfg:
            raise HTTPException(status_code=400, detail="Некорректный файл конфигурации")

        # Общие настройки
        import configparser
        common_cfg = configparser.ConfigParser()
        common_cfg.read(get_base_path() / "RepExecutor.ini", encoding="utf-8")

        # Подключение и обработка
        connection = get_connection()
        if not connection:
            raise HTTPException(status_code=500, detail="Не удалось подключиться к базе данных")

        try:
            generated_files = process_query_and_files(connection, cfg, common_cfg)

            # Удаляем конфиг после обработки
            try:
                if json_path.exists():
                    json_path.unlink()
                    logger.info(f"RepExecutor.json удален: {json_path}")
            except Exception:
                logger.exception("Не удалось удалить RepExecutor.json")

            task.status = "completed"
            task.result = [str(p) for p in generated_files]

            return JSONResponse(
                content={
                    "task_id": task.id,
                    "status": task.status,
                    "generated_files": task.result,
                },
                status_code=200,
            )

        finally:
            connection.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Ошибка при обработке задачи #{task.id}")
        task.status = "failed"
        task.error = str(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/queue", status_code=200)
def get_queue():
    """Возвращает текущую очередь задач."""
    return {
        "queue_length": len(_task_queue),
        "tasks": [
            {
                "id": t.id,
                "status": t.status,
                "report_name": t.config.report_name,
                "created_at": t.created_at,
                "result": t.result,
                "error": t.error,
            }
            for t in _task_queue
        ],
    }


@router.get("/queue/{task_id}", status_code=200)
def get_task_status(task_id: int):
    """Возвращает статус конкретной задачи."""
    for t in _task_queue:
        if t.id == task_id:
            return {
                "id": t.id,
                "status": t.status,
                "report_name": t.config.report_name,
                "created_at": t.created_at,
                "result": t.result,
                "error": t.error,
            }
    raise HTTPException(status_code=404, detail=f"Задача #{task_id} не найдена")


@router.delete("/queue", status_code=200)
def clear_queue():
    """Очищает историю задач."""
    global _task_queue
    _task_queue.clear()
    return {"message": "Очередь очищена"}