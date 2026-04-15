import time
import tempfile
import os
import json
from pathlib import Path
from pydantic import ValidationError
from loguru import logger

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

# Импорты из существующего кода
from RepExecutor import ConfigModel, get_connection, process_query_and_files, configure_logger
from utils_logger import get_base_path, log_execution

# Импорты из новых модулей
from queue_manager import get_db_connection, get_next_job_from_queue, mark_job_done, mark_job_error, convert_job_to_config
from config_manager import load_ini_config, create_config_file

app = FastAPI(title="RepExecutor API", description="API для генерации отчетов Word", version="1.0.0")

api_metrics = {
    "total_requests": 0,
    "endpoint_counts": {},
    "report_requests": 0,
    "report_successes": 0,
    "report_failures": 0,
    "queue_jobs_processed": 0,
    "average_report_time_seconds": 0.0,
    "last_error": None,
}

# ==============================
# ИНИЦИАЛИЗАЦИЯ
# ==============================
@app.on_event("startup")
async def startup_event():
    """Настройка при запуске приложения."""
    configure_logger()
    logger.info("API RepExecutor запущен")

@app.middleware("http")
async def collect_metrics(request: Request, call_next):
    path = request.url.path
    api_metrics["total_requests"] += 1
    api_metrics["endpoint_counts"][path] = api_metrics["endpoint_counts"].get(path, 0) + 1
    response = await call_next(request)
    return response

@app.post("/generate-report")
async def generate_report(
    background_tasks: BackgroundTasks,
    config: ConfigModel | None = None,
    from_queue: bool = False
):
    """
    Генерация отчета в фоне. Клиент сразу получает подтверждение,
    а отчёт создается асинхронно.
    """
    job_id = None
    connection = None
    api_metrics["report_requests"] += 1
    
    # Подготовка конфигурации, подключение к БД и проверка INI, как у тебя
    try:
        if from_queue:
            connection = get_db_connection()
            if not connection:
                raise HTTPException(status_code=500, detail="Не удалось подключиться к БД")
            job = get_next_job_from_queue(connection)
            if not job:
                raise HTTPException(status_code=404, detail="Очередь пуста")
            job_id = job["ID"]
            config_dict = convert_job_to_config(job)
            config = ConfigModel(**config_dict)
        elif not config:
            raise HTTPException(status_code=400, detail="Необходимо передать config или from_queue=True")
    except ValidationError as ve:
        raise HTTPException(status_code=422, detail=f"Ошибка валидации: {ve}")
    
    # Фоновая задача
    def background_report_task(config: ConfigModel, connection, job_id: int | None):
        start_time = time.perf_counter()
        try:
            ini_path = get_base_path() / "RepExecutor.ini"
            config_parser = load_ini_config(str(ini_path))
            if connection is None:
                connection = get_connection()
            generated_files = process_query_and_files(connection, config, config_parser)
            
            if generated_files:
                elapsed = time.perf_counter() - start_time
                api_metrics["report_successes"] += 1
                api_metrics["average_report_time_seconds"] = (
                    (api_metrics["average_report_time_seconds"] * (api_metrics["report_successes"] - 1))
                    + elapsed
                ) / api_metrics["report_successes"]
                if job_id:
                    mark_job_done(connection, job_id)
                logger.success(f"Отчет {config.report_name} сгенерирован | время={elapsed:.3f}s")
            else:
                api_metrics["report_failures"] += 1
                api_metrics["last_error"] = f"Отчет не был сгенерирован для: {config.report_name}"
                if job_id:
                    mark_job_error(connection, job_id, api_metrics["last_error"])
                logger.warning(api_metrics["last_error"])
        except Exception as e:
            api_metrics["report_failures"] += 1
            api_metrics["last_error"] = str(e)
            if connection and job_id:
                mark_job_error(connection, job_id, str(e))
            logger.exception(f"Ошибка при генерации отчета: {e}")
        finally:
            if connection:
                connection.close()
    
    # Добавляем задачу в фон
    background_tasks.add_task(background_report_task, config, connection, job_id)
    
    return {"status": "accepted", "message": "Отчет создается в фоне", "report_name": config.report_name}

@app.post("/create-config")
async def create_config(job_data: dict):
    """
    Создает конфигурационный JSON файл на основе данных задачи.
    
    Ожидает JSON с полями:
    - report_name: str
    - output_path: str
    - output_format: str (опционально, по умолчанию "PDF")
    - params: dict (опционально)
    - object_id: int (опционально)
    - object_type: int (опционально)
    """
    try:
        logger.info(f"Запрос на создание конфига для отчета: {job_data.get('report_name')}")
        
        # Получаем путь для сохранения конфига из INI
        ini_path = get_base_path() / "RepExecutor.ini"
        if not ini_path.exists():
            raise HTTPException(status_code=500, detail="Файл RepExecutor.ini не найден")
        
        config_parser = load_ini_config(str(ini_path))
        config_path = config_parser.get("worker", "config_path", fallback=str(get_base_path() / "RepExecutor.json"))
        
        # Создаем конфигурационный файл
        if create_config_file(job_data, config_path):
            logger.info(f"Конфиг успешно создан: {config_path}")
            return {
                "status": "success",
                "message": "Конфигурационный файл успешно создан",
                "config_path": config_path
            }
        else:
            raise HTTPException(status_code=500, detail="Не удалось создать конфигурационный файл")

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Ошибка при создании конфига: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка создания конфига: {str(e)}")


@app.get("/stats")
async def api_stats():
    """Возвращает текущие метрики работы API."""
    return JSONResponse(content=api_metrics)


@app.get("/db-job/{job_id}")
async def get_db_job(job_id: int):
    """
    Получает задачу из очереди БД по ID и преобразует её в конфигурацию.
    """
    try:
        logger.info(f"Запрос на получение задачи из БД: {job_id}")
        
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Не удалось подключиться к базе данных")
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ID, ReportName, OutputPath, OutputFormat, ParametersJson, ObjectID, ObjectType
                  FROM rdbReportQueue with (Nolock)
                 WHERE ID = ?
            """, job_id)
            
            row = cursor.fetchone()
            cursor.close()
            
            if not row:
                raise HTTPException(status_code=404, detail=f"Задача {job_id} не найдена")
            
            columns = [col[0] for col in cursor.description]
            job = dict(zip(columns, row))
            
            # Преобразуем в формат конфигурации
            config_data = {
                "report_name": job.get("ReportName"),
                "output_path": job.get("OutputPath"),
                "output_format": job.get("OutputFormat", "PDF"),
                "params": json.loads(job.get("ParametersJson", "{}")) if job.get("ParametersJson") else {},
            }
            
            if job.get("ObjectID") and job.get("ObjectType"):
                config_data["marks"] = {
                    "Type": int(job["ObjectType"]),
                    "ID": int(job["ObjectID"])
                }
            
            logger.info(f"Получена задача {job_id} из БД")
            return config_data
        
        finally:
            conn.close()
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Ошибка при получении задачи из БД: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения задачи: {str(e)}")


@app.get("/")
async def root():
    """Корневой эндпоинт с информацией об API."""
    return {
        "message": "RepExecutor API запущен",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "POST /generate-report": "Генерировать отчет (параметры: config или from_queue=true)",
            "POST /generate-report?from_queue=true": "Получить задачу из очереди БД и сгенерировать отчет",
            "POST /create-config": "Создать конфигурационный файл",
            "GET /db-job/{job_id}": "Получить задачу из БД по ID",
            "GET /stats": "Получить статистику API"
        }
    }

# Добавьте в конец api.py
if __name__ == "__main__":
    import uvicorn
    import sys
    import os
    
    # Автоматический перезапуск при падении
    def run_with_restart():
        # while True:
            try:
                uvicorn.run(
                    app, 
                    host="192.168.10.140", 
                    port=8000,
                    reload=False,  # В production выключите reload
                    log_level="info"
                )
            except Exception as e:
                logger.error(f"Сервис упал с ошибкой: {e}")
                logger.info("Перезапуск через 5 секунд...")
                time.sleep(5)
    
    if os.name == 'nt':  # Windows
        # Запуск как служба
        run_with_restart()
    else:  # Linux
        uvicorn.run(app, host="0.0.0.0", port=8000)