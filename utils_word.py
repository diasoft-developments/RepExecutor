import shutil
import tempfile
import time
from pathlib import Path
from loguru import logger
import win32com.client as win32
from concurrent.futures import ThreadPoolExecutor

from utils_logger import log_execution

@log_execution()
def perform_mail_merge(template_doc_path, data_csv_path, output_pdf_path, delete_tmp=True):
    """
    Mail Merge с временным шаблоном.
    Работает безопасно для параллельных вызовов.
    """
    word = None
    doc = None
    merged_doc = None
    temp_template_path = None

    try:
        # Временная копия шаблона
        temp_dir = Path(tempfile.gettempdir())
        # Берём исходное расширение
        orig_ext = Path(template_doc_path).suffix
        # Формируем имя с сохранением расширения
        temp_template_path = temp_dir / f"{Path(template_doc_path).stem}_{int(time.time()*1000)}{orig_ext}"
        shutil.copy(template_doc_path, temp_template_path)
        logger.info(f"Временный шаблон создан: {temp_template_path}")

        # Запуск Word
        word = win32.DispatchEx("Word.Application")
        word.Visible = False
        word.DisplayAlerts = 0
        word.ScreenUpdating = False

        doc = word.Documents.Open(str(temp_template_path))
        merge = doc.MailMerge

        merge.OpenDataSource(
            Name=str(data_csv_path),
            ConfirmConversions=False,
            ReadOnly=True,
            LinkToSource=True,
            AddToRecentFiles=False,
            SubType=0
        )

        merge.Execute()
        merged_doc = word.ActiveDocument
        merged_doc.Fields.Update()
        merged_doc.ExportAsFixedFormat(str(output_pdf_path), 17)
        logger.info(f"PDF готов: {output_pdf_path}")
        return True

    except Exception:
        logger.exception("Ошибка при Mail Merge")
        raise

    finally:
        for d in [merged_doc, doc]:
            if d is not None:
                try:
                    d.Close(False)
                except Exception:
                    pass
        if word is not None:
            try:
                word.Quit()
            except Exception:
                pass
        if temp_template_path and temp_template_path.exists():
            try:
                temp_template_path.unlink()
                logger.info(f"Временный шаблон удален: {temp_template_path}")
            except Exception:
                logger.warning(f"Не удалось удалить временный шаблон: {temp_template_path}")
                                
        if delete_tmp:
            try:
                if data_csv_path and Path(data_csv_path).exists():
                    Path(data_csv_path).unlink()
                    logger.debug(f"Временный CSV удалён (save=false): {data_csv_path}")
            except Exception:
                logger.exception(f"Не удалось удалить временный CSV: {data_csv_path}")
            


def submit_mail_merge_job(template_doc_path, data_csv_path, output_pdf_path, delete_tmp=True):
    """
    Отправляем задачу в пул воркеров. Возвращаем Future.
    """
    with ThreadPoolExecutor(max_workers=2) as executor:
        # executor.shutdown(wait=True)  # дождаться завершения всех фоновых задач
        future = executor.submit(perform_mail_merge, template_doc_path, data_csv_path, output_pdf_path, delete_tmp)
    
    return future
