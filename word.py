from pathlib import Path
import re

from loguru import logger
import win32com.client as win32

from utils import log_execution

@log_execution()
def perform_mail_merge(template_doc_path, data_csv_path, output_pdf_path):
    ''' Выполняем слияние данных и экспорт в pdf'''
    logger.info("Запуск Word...")
    word = None
    doc = None
    merged_doc = None
    try:
        word = win32.DispatchEx("Word.Application")
        logger.info("Запустили Word")
        word.Visible = False
        word.DisplayAlerts = 0
        word.ScreenUpdating = False

        doc = word.Documents.Open(str(template_doc_path))
        merge = doc.MailMerge

        merge.OpenDataSource(
            Name=str(data_csv_path),
            ConfirmConversions=False,
            ReadOnly=True,
            LinkToSource=True,
            AddToRecentFiles=False,
            SubType=0 #win32.constants.wdMergeSubTypeOther
        )
        merge.Execute()
        logger.info("Выполнили merge.Execute")
        merged_doc = word.ActiveDocument

        merged_doc.Fields.Update()
        merged_doc.ExportAsFixedFormat(output_pdf_path, 17) #win32.constants.wdExportFormatPDF

        logger.info(f"PDF сохранен: {output_pdf_path}")
        merged_doc.Close(False)
        return True

    except Exception:
        logger.exception("Ошибка при выполнении Mail Merge в Word")
        raise

    finally:
        if merged_doc is not None:
            try:
                merged_doc.Close(False)
            except Exception:
                pass
        if doc is not None:
            try:
                doc.Close(False)
            except Exception:
                pass
        if word is not None:
            try:
                word.Quit()
            except Exception:
                pass
        