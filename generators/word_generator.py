import shutil
import tempfile
import time
import csv
import re
from pathlib import Path
from loguru import logger
import win32com.client as win32
from concurrent.futures import ThreadPoolExecutor

from utils.logger import log_execution

@log_execution()
def perform_mail_merge(template_doc_file, data_csv_file, output_pdf_file, common_cfg=None):
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
        orig_ext = Path(template_doc_file).suffix
        # Формируем имя с сохранением расширения
        temp_template_path = temp_dir / f"{Path(template_doc_file).stem}_{int(time.time()*1000)}{orig_ext}"
        shutil.copy(template_doc_file, temp_template_path)
        logger.debug(f"Временный шаблон создан: {temp_template_path}")
        
        backup_path = Path(common_cfg.get("backup", "path", fallback=""))
        if backup_path:
            backup_file = backup_path / Path(output_pdf_file).name
        else:
            backup_file = temp_dir / Path(output_pdf_file).name            

        # Запуск Word
        word = win32.DispatchEx("Word.Application")
        word.Visible = False
        word.DisplayAlerts = 0
        word.ScreenUpdating = False
        # word.AlertBeforeOverwriting = False
        
        doc = word.Documents.Open(str(temp_template_path))      
        # Валидация перед выполнением
        is_valid, missing = validate_mail_merge_fields(doc, data_csv_file)
        if not is_valid:
            err_msg = f"Невозможно выполнить слияние: отсутствуют поля {missing}"
            logger.error(err_msg)
            raise Exception(err_msg)             
        
        merge = doc.MailMerge
        merge.OpenDataSource(
            Name=str(data_csv_file),
            ConfirmConversions=False, 
            ReadOnly=True,
            LinkToSource=True,
            AddToRecentFiles=False,
            SubType=0
        )         

        merge.Execute()                
        merged_doc = word.ActiveDocument
        merged_doc.Fields.Update()                
        merged_doc.ExportAsFixedFormat(str(backup_file), 17)         
       
        shutil.copy2(str(backup_file), str(output_pdf_file))       
        # тут нужно подумать как лучше сделать, если сохраняю на сетевой диск? то word ругается на сертификат               
        # merged_doc.ExportAsFixedFormat(str(output_pdf_file), 17) 
        logger.info(f"PDF готов: {output_pdf_file}")
        return True

    except Exception as e:
        logger.exception(f"Ошибка при Mail Merge : {e}")
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
                                 
        if common_cfg:
            is_tmpsave  = common_cfg.getboolean("tmp", "save", fallback=False)
            if not is_tmpsave:
                try:
                    if data_csv_file and Path(data_csv_file).exists():
                        Path(data_csv_file).unlink()
                        logger.debug(f"Временный CSV удалён (save=false): {data_csv_file}")
                except Exception:
                    logger.exception(f"Не удалось удалить временный CSV: {data_csv_file}")
                    
        if not backup_path:
            try:
                if backup_file and Path(backup_file).exists():
                    Path(backup_file).unlink()
                    logger.debug(f"Временный CSV удалён (save=false): {backup_file}")
            except Exception:
                logger.exception(f"Не удалось удалить временный CSV: {backup_file}")

# Глобальный executor для всего скрипта
executor = ThreadPoolExecutor(max_workers=2)

def submit_mail_merge_job(template_doc_file, data_csv_file, output_pdf_file, common_cfg=None):
    """
    Отправляем задачу в пул воркеров. Возвращаем Future.
    """
    future = executor.submit(perform_mail_merge, template_doc_file, data_csv_file, output_pdf_file, common_cfg)
    return future

# def submit_mail_merge_job(template_doc_file, data_csv_file, output_pdf_file, common_cfg=None):
#     """
#     Отправляем задачу в пул воркеров. Возвращаем Future.
#     """
#     with ThreadPoolExecutor(max_workers=2) as executor:
#         # executor.shutdown(wait=True)  # дождаться завершения всех фоновых задач
#         future = executor.submit(perform_mail_merge, template_doc_file, data_csv_file, output_pdf_file, common_cfg)
    
#     return future

def _extract_field_name(field_code_text):
    """
    Извлекает чистое имя MERGEFIELD из текста кода поля.
    Пример входного текста: " MERGEFIELD  UdostUserName  \\* CHARFORMAT "
    """
    name = re.sub(r'^MERGEFIELD\s+', '', field_code_text, flags=re.IGNORECASE)
    
    # Всё после "\" — модификатор, убираем
    if '\\' in name:
        name = name.split('\\')[0]
    
    # Убираем кавычки и управляющие символы
    name = name.replace('"', '').strip()
    name = ''.join(c for c in name if ord(c) >= 32)
    name = name.strip()
    
    return name


def _get_all_story_ranges(doc):
    """
    Возвращает все StoryRanges документа, включая связанные (linked) области.
    
    Word Story Type константы:
      0 - wdMainTextStory
      1 - wdEvenPagesHeaderStory
      2 - wdPrimaryHeaderStory
      3 - wdOddPagesHeaderStory
      4 - wdEvenPagesFooterStory
      5 - wdPrimaryFooterStory
      6 - wdOddPagesFooterStory
      7 - wdFirstPageHeaderStory
      8 - wdFirstPageFooterStory
      9 - wdFootnotesStory
     10 - wdEndnotesStory
     11 - wdTextFramesStory
    """
    story_ranges = []
    
    # Перебираем все типы story ranges (0-11)
    for story_type in range(12):
        try:
            story = doc.StoryRanges(story_type)
            story_ranges.append(story)
            
            # Следующая связанная область (например, последующие секции)
            next_story = story.NextStoryRange
            while next_story is not None:
                story_ranges.append(next_story)
                next_story = next_story.NextStoryRange
        except Exception:
            # Некоторые story types могут быть недоступны для данного документа
            pass
    
    return story_ranges


def validate_mail_merge_fields(doc, data_csv_file):
    """
    Проверяет, что все поля слияния в шаблоне имеют соответствующие колонки в CSV.
    Ищет поля во всех StoryRanges (основной текст, колонтитулы, сноски и т.д.).
    """
    
    # Читаем заголовки CSV с правильным разделителем и кодировкой
    with open(data_csv_file, 'r', encoding='utf-8-sig') as f:  # utf-8-sig убирает BOM
        reader = csv.reader(f, delimiter=';')  # ← используем ; как разделитель
        csv_columns = {col.strip().lower() for col in next(reader)}
    
    logger.debug(f"Колонки в CSV: {csv_columns}")
    
    # Получаем поля слияния из всех StoryRanges документа
    merge_fields = set()
    
    # Получаем все story ranges (колонтитулы, основной текст, сноски и т.д.)
    story_ranges = _get_all_story_ranges(doc)
    logger.debug(f"Найдено StoryRanges: {len(story_ranges)}")
    
    for story_range in story_ranges:
        for field in story_range.Fields:
            if field.Type == 59:  # wdFieldMergeField = 59
                field_code = field.Code.Text.strip()
                field_name = _extract_field_name(field_code)
                
                if field_name:
                    logger.debug(f"Извлечённое поле: '{field_name}' из кода '{field_code}'")
                    merge_fields.add(field_name.lower())
    
    logger.debug(f"Все поля в шаблоне: {merge_fields}")
    

    # Находим несоответствия
    missing_fields = merge_fields - csv_columns
    # extra_columns = csv_columns - merge_fields
    
    if missing_fields:
        logger.warning(f"Поля в шаблоне, отсутствующие в CSV: {missing_fields}")
        return False, missing_fields
    
    return True, None

class WordGenerator:
    """Генератор Word-документов через mail merge."""
    
    @staticmethod
    @log_execution()
    def generate(template_doc_file: str, data_csv_file: str, output_pdf_file: str, common_cfg=None) -> bool:
        """
        Генерирует PDF из Word-шаблона используя mail merge.
        
        Args:
            template_doc_file: Путь к шаблону .doc
            data_csv_file: Путь к CSV с данными
            output_pdf_file: Путь к выходному PDF
            common_cfg: Конфигурация
            
        Returns:
            True при успешной генерации
        """
        return perform_mail_merge(template_doc_file, data_csv_file, output_pdf_file, common_cfg)
    
    @staticmethod
    @log_execution()
    def generate_async(template_doc_file: str, data_csv_file: str, output_pdf_file: str, common_cfg=None):
        """
        Асинхронно генерирует PDF из Word-шаблона.
        
        Args:
            template_doc_file: Путь к шаблону .doc
            data_csv_file: Путь к CSV с данными
            output_pdf_file: Путь к выходному PDF
            common_cfg: Конфигурация
            
        Returns:
            Future объект
        """
        return submit_mail_merge_job(template_doc_file, data_csv_file, output_pdf_file, common_cfg)