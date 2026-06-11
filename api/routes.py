"""Маршруты API для RepExecutor."""
import datetime
import json
import os
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from loguru import logger
from pydantic import BaseModel, Field

from core.config import read_config, ConfigModel, load_ini_config
from core.engine import process_query_and_files
from core.db_connector import get_connection
from job_queue.manager import (
    get_next_job_from_queue,
    mark_job_done,
    mark_job_error,
    convert_job_to_config,
)
from utils.system import get_base_path


router = APIRouter(
    prefix="/api",
    tags=["RepExecutor API"],
    responses={
        400: {"description": "Некорректный запрос"},
        404: {"description": "Ресурс не найден"},
        500: {"description": "Внутренняя ошибка сервера"},
    },
)


# --- Pydantic models for API validation ---
class ExecutorConfig(BaseModel):
    """Конфигурация отчета для обработки.

    Определяет параметры одного отчета: имя, путь вывода, формат,
    метки документа и параметры SQL-запроса. Соответствует структуре
    файла `RepExecutor.json`.
    """
    model_config = {"json_schema_extra": {
        "examples": [
            {
                "report_name": "Отчет_по_актам",
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
        ]
    }}
    report_name: str = Field(
        ...,
        description="Название отчета. Используется для логирования и идентификации задачи.",
        min_length=1,
        examples=["Отчет_по_актам"],
    )
    output_path: str = Field(
        ...,
        description="Полный путь к выходному файлу или папке, куда будет сохранён результат.",
        examples=["C:\\Output\\report.pdf"],
    )
    output_format: str = Field(
        "pdf",
        description="Формат выходного файла. Поддерживаемые значения: pdf, ~~docx, xlsx.~~",
        examples=["pdf", "~~docx~~", "~~xlsx~~"],
    )
    marks: Optional[dict] = Field(
        None,
        description="Словарь меток документа для идентификации типа отчета и объекта. Например: Type — тип документа, ID — идентификатор записи.",
        examples=[{"Type": 3, "ID": 10002479017}],
    )
    params: Optional[dict] = Field(
        None,
        description="Дополнительные параметры отчета: Podpisant — подписант, Controler — контролер, InstitutionID — идентификатор учреждения и др.",
        examples=[{"Podpisant": 10000000026, "Controler": 10000000026, "InstitutionID": 2000}],
    )


class ApiRequest(BaseModel):
    """Тело запроса для эндпоинта POST /execute.

    Содержит конфигурацию отчета и опциональный путь к файлу
    RepExecutor.json, куда конфиг будет сохранён перед обработкой.
    """
    model_config = {"json_schema_extra": {
        "examples": [
            {
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
                },
                "json_path": "C:\\Temp\\RepExecutor.json",
            }
        ]
    }}
    config: ExecutorConfig = Field(..., description="Конфигурация отчета для обработки.")
    json_path: Optional[str] = Field(
        None,
        description="Путь к файлу RepExecutor.json для сохранения конфигурации перед обработкой. Если не указан, используется файл по умолчанию.",
        examples=["C:\\Temp\\RepExecutor.json"],
    )


class QueueItem(BaseModel):
    """Элемент внутренней истории задач.

    Хранится в памяти на время работы сервера и используется для
    отслеживания статуса выполненных запросов.
    """
    id: int = Field(..., description="Уникальный идентификатор задачи.")
    config: ExecutorConfig = Field(..., description="Конфигурация отчета.")
    status: str = Field(
        "pending",
        description="Текущий статус задачи: pending, processing, completed, failed, idle.",
    )
    created_at: str = Field(..., description="Время создания задачи в формате ISO 8601.")
    result: Optional[str] = Field(None, description="Результат выполнения (путь к сгенерированным файлам).")
    error: Optional[str] = Field(None, description="Текст ошибки, если задача завершилась неудачей.")


# --- In-memory task history ---
_task_queue: list[QueueItem] = []
_next_id: int = 1


@router.get(
    "/health",
    summary="Проверка здоровья сервера",
    description="Возвращает текущий статус API и временную метку. Используется для мониторинга доступности сервиса.",
    responses={
        200: {
            "description": "Сервер работает корректно",
            "content": {
                "application/json": {
                    "example": {
                        "status": "ok",
                        "timestamp": "2026-06-11T15:00:00.000000",
                    }
                }
            },
        }
    },
)
def health_check():
    """GET /api/health — проверка здоровья сервера."""
    return {"status": "ok", "timestamp": datetime.datetime.now().isoformat()}


@router.post(
    "/execute",
    status_code=200,
    summary="Выполнить обработку отчета",
    description="""Запускает обработку отчета по переданной конфигурации.

**Алгоритм работы:**
1. Сохраняет конфигурацию в файл `RepExecutor.json`.
2. Подключается к базе данных диасофт.
3. Выполняет SQL-запрос и обрабатывает данные (таблица очереди rdbReportQueue).
4. Генерирует выходной файл (PDF) по шаблону.
5. Удаляет временный файл конфигурации.
6. Возвращает список сгенерированных файлов.

 **Пример запроса:**
 ```json
 {
     "config": {
         "report_name": "report",
         "output_path": "c:\\\\Tasks\\\\10002479017.pdf",
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
     },
     "json_path": "C:\\\\Temp\\\\RepExecutor.json"
 }
 ```
""",
    responses={
        200: {
            "description": "Отчет успешно обработан",
            "content": {
                "application/json": {
                    "example": {
                        "task_id": 1,
                        "status": "completed",
                        "generated_files": ["C:\\Output\\report.pdf"],
                    }
                }
            },
        },
        400: {
            "description": "Некорректный файл конфигурации",
            "content": {
                "application/json": {
                    "example": {"detail": "Некорректный файл конфигурации"}
                }
            },
        },
        500: {
            "description": "Ошибка подключения к БД или обработки",
            "content": {
                "application/json": {
                    "example": {"detail": "Не удалось подключиться к базе данных"}
                }
            },
        },
    },
)
def execute_report(request: ApiRequest):
    """POST /api/execute — выполнить обработку отчета по конфигурации."""
    global _next_id

    # Создаем задачу во внутренней истории
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
        # Сохраняем конфиг как RepExecutor.json
        if request.json_path:
            json_dir = Path(request.json_path).parent
            json_dir.mkdir(parents=True, exist_ok=True)
            json_path = Path(request.json_path)
        else:
            json_path = get_base_path() / "RepExecutor.json"

        config_data = request.config.model_dump()
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)
        logger.info(f"Конфиг сохранен: {json_path}")

        # Читаем и валидируем конфиг
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
            raise HTTPException(
                status_code=500, detail="Не удалось подключиться к базе данных"
            )

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


@router.post(
    "/execute_from_queue",
    status_code=200,
    summary="Выполнить следующую задачу из очереди БД",
    description="""Берёт следующую ожидающую задачу из таблицы `rdbReportQueue` в базе данных и обрабатывает её.

**Особенности:**
- Вызывается явно по HTTP-запросу (нет фоновых процессов).
- Подходит для интеграции с внешними системами, которые добавляют задачи в очередь БД.
- Задача автоматически помечается как выполненная после успешной обработки.

**Возможные ответы:**
- `completed` — задача обработана, файлы сгенерированы.
- `no_jobs` — очередь пуста, задач для обработки нет.
""",
    responses={
        200: {
            "description": "Задача обработана или очередь пуста",
            "content": {
                "application/json": {
                    "examples": {
                        "completed": {
                            "summary": "Задача успешно выполнена",
                            "value": {
                                "task_id": 2,
                                "job_id": 42,
                                "status": "completed",
                                "report_name": "Отчет_по_актам",
                                "generated_files": ["C:\\Output\\report.pdf"],
                            },
                        },
                        "no_jobs": {
                            "summary": "Очередь пуста",
                            "value": {
                                "task_id": 2,
                                "status": "no_jobs",
                                "message": "Очередь БД пуста — нет задач для обработки",
                            },
                        },
                    }
                }
            },
        },
        500: {
            "description": "Ошибка подключения к БД или обработки задачи",
            "content": {
                "application/json": {
                    "example": {"detail": "Не удалось подключиться к базе данных"}
                }
            },
        },
    },
)
def execute_from_queue():
    """POST /api/execute_from_queue — обработать следующую задачу из очереди БД."""
    global _next_id

    # Создаем запись во внутренней истории
    task = QueueItem(
        id=_next_id,
        config=ExecutorConfig(
            report_name="queue_job",
            output_path="",
            output_format="pdf",
        ),
        status="processing",
        created_at=datetime.datetime.now().isoformat(),
    )
    _next_id += 1
    _task_queue.append(task)

    logger.info(f"Задача #{task.id}: получение из очереди БД...")

    try:
        # Подключаемся к БД
        connection = get_connection()
        if not connection:
            raise HTTPException(
                status_code=500, detail="Не удалось подключиться к базе данных"
            )

        try:
            # Получаем следующую задачу из очереди
            job = get_next_job_from_queue(connection)

            if job is None:
                task.status = "idle"
                task.result = "Нет задач в очереди"
                return JSONResponse(
                    content={
                        "task_id": task.id,
                        "status": "no_jobs",
                        "message": "Очередь БД пуста — нет задач для обработки",
                    },
                    status_code=200,
                )

            job_id = job.get("ID")
            report_name = job.get("ReportName", "unknown")
            task.config.report_name = report_name
            logger.info(f"Задача #{task.id}: обработка #{job_id} — {report_name}")

            # Преобразуем задачу в формат config
            config_data = convert_job_to_config(job)
            task.config.output_path = config_data.get("output_path", "")
            task.config.output_format = config_data.get("output_format", "pdf")

            # Создаем ConfigModel из dict
            cfg = ConfigModel(**config_data)

            # Загружаем общие настройки из INI
            common_cfg = load_ini_config(str(get_base_path() / "RepExecutor.ini"))

            # Запускаем обработку
            generated_files = process_query_and_files(connection, cfg, common_cfg)

            # Отмечаем как выполненную
            mark_job_done(connection, job_id)
            task.status = "completed"
            task.result = [str(p) for p in (generated_files or [])]

            logger.success(
                f"Задача #{task.id}: очередь #{job_id} выполнена, "
                f"файлов: {len(generated_files) if generated_files else 0}"
            )

            return JSONResponse(
                content={
                    "task_id": task.id,
                    "job_id": job_id,
                    "status": "completed",
                    "report_name": report_name,
                    "generated_files": task.result,
                },
                status_code=200,
            )

        finally:
            connection.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Ошибка при обработке задачи из очереди #{task.id}")
        task.status = "failed"
        task.error = str(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/queue",
    status_code=200,
    summary="Получить историю задач",
    description="""Возвращает список всех задач, обработанных за текущую сессию работы сервера.

**Важно:** история хранится в памяти и сбрасывается при перезапуске сервера или вызове `DELETE /api/queue`.
""",
    response_model=dict,
    responses={
        200: {
            "description": "Список задач возвращён успешно",
            "content": {
                "application/json": {
                    "example": {
                        "queue_length": 2,
                        "tasks": [
                            {
                                "id": 1,
                                "status": "completed",
                                "report_name": "Отчет_по_актам",
                                "created_at": "2026-06-11T15:00:00",
                                "result": ["C:\\Output\\report.pdf"],
                                "error": None,
                            }
                        ],
                    }
                }
            },
        },
    },
)
def get_queue():
    """GET /api/queue — получить внутреннюю историю задач."""
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


@router.get(
    "/queue/{task_id}",
    status_code=200,
    summary="Получить статус конкретной задачи",
    description="""Возвращает детальную информацию о задаче по её идентификатору.

**Параметры:**
- `task_id` — уникальный ID задачи, полученный при создании через `POST /api/execute`.
""",
    responses={
        200: {
            "description": "Информация о задаче найдена",
            "content": {
                "application/json": {
                    "example": {
                        "id": 1,
                        "status": "completed",
                        "report_name": "Отчет_по_актам",
                        "created_at": "2026-06-11T15:00:00",
                        "result": ["C:\\Output\\report.pdf"],
                        "error": None,
                    }
                }
            },
        },
        404: {
            "description": "Задача с указанным ID не найдена",
            "content": {
                "application/json": {
                    "example": {"detail": "Задача #999 не найдена"}
                }
            },
        },
    },
)
def get_task_status(task_id: int):
    """GET /api/queue/{task_id} — получить статус конкретной задачи."""
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


@router.delete(
    "/queue",
    status_code=200,
    summary="Очистить историю задач",
    description="""Удаляет все записи из внутренней истории задач.

**Внимание:** операция необратима. После очистки восстановить историю невозможно.
""",
    responses={
        200: {
            "description": "История задач успешно очищена",
            "content": {
                "application/json": {
                    "example": {"message": "История задач очищена"}
                }
            },
        },
    },
)
def clear_queue():
    """DELETE /api/queue — очистить внутреннюю историю задач."""
    global _task_queue
    _task_queue.clear()
    return {"message": "История задач очищена"}


@router.post(
    "/shutdown",
    status_code=200,
    summary="Остановить API сервер",
    description="""Немедленно останавливает процесс API сервера.

**Внимание:** это действие завершает процесс принудительно. Все незавершённые задачи будут потеряны.
""",
    responses={
        200: {
            "description": "Сервер остановлен",
            "content": {
                "application/json": {
                    "example": {"message": "Сервер остановлен"}
                }
            },
        },
    },
)
def shutdown_server():
    """POST /api/shutdown — остановить API сервер."""
    logger.info("Получен запрос на остановку сервера")
    os._exit(0)