import shutil
import tempfile
import time
from pathlib import Path
from loguru import logger
import win32com.client as win32

from utils.logger import log_execution

@log_execution()
def perform_excel_generate(template_xls_file, data_dict, output_pdf_file, common_cfg=None):
    """
    Генерация PDF из Excel-шаблона с подстановкой данных.
    
    Args:
        template_xls_file: Путь к шаблону .xls/.xlsx
        data_dict: Словарь с данными для подстановки {имя_клетки: значение}
        output_pdf_file: Путь к выходному PDF
        common_cfg: Конфигурация
        
    Returns:
        True при успешной генерации
    """
    excel = None
    wb = None
    temp_template_path = None

    try:
        # Временная копия шаблона
        temp_dir = Path(tempfile.gettempdir())
        orig_ext = Path(template_xls_file).suffix
        temp_template_path = temp_dir / f"{Path(template_xls_file).stem}_{int(time.time()*1000)}{orig_ext}"
        shutil.copy(template_xls_file, temp_template_path)
        logger.debug(f"Временный шаблон Excel создан: {temp_template_path}")
        
        backup_path = Path(common_cfg.get("backup", "path", fallback="")) if common_cfg else Path("")
        if backup_path:
            backup_file = backup_path / Path(output_pdf_file).name
        else:
            backup_file = temp_dir / Path(output_pdf_file).name

        # Запуск Excel
        excel = win32.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.ScreenUpdating = False
        
        wb = excel.Workbooks.Open(str(temp_template_path))
        
        # Подставляем данные в именованные клетки
        if data_dict:
            for cell_name, value in data_dict.items():
                try:
                    ws = wb.ActiveSheet
                    range_obj = ws.Range(cell_name)
                    range_obj.Value = value
                    logger.debug(f"Установлено значение в {cell_name}: {value}")
                except Exception as e:
                    logger.warning(f"Не удалось установить значение в клетку {cell_name}: {e}")
        
        # Экспорт в PDF
        ws = wb.ActiveSheet
        ws.ExportAsFixedFormat(0, str(backup_file))  # 0 = xlPDF
        
        shutil.copy2(str(backup_file), str(output_pdf_file))
        logger.info(f"PDF готов (Excel): {output_pdf_file}")
        return True

    except Exception as e:
        logger.exception(f"Ошибка при генерации Excel: {e}")
        raise

    finally:
        if wb is not None:
            try:
                wb.Close(False)
            except Exception:
                pass
        if excel is not None:
            try:
                excel.Quit()
            except Exception:
                pass
        if temp_template_path and temp_template_path.exists():
            try:
                temp_template_path.unlink()
                logger.info(f"Временный шаблон Excel удален: {temp_template_path}")
            except Exception:
                logger.warning(f"Не удалось удалить временный шаблон Excel: {temp_template_path}")
                
        # Очистка бэкапа если не нужен
        if common_cfg:
            is_tmpsave = common_cfg.getboolean("tmp", "save", fallback=False)
            if not is_tmpsave and backup_path and not backup_path:
                try:
                    if backup_file and Path(backup_file).exists():
                        Path(backup_file).unlink()
                        logger.debug(f"Временный PDF удален: {backup_file}")
                except Exception:
                    logger.exception(f"Не удалось удалить временный PDF: {backup_file}")


class ExcelGenerator:
    """Генератор PDF из Excel-шаблонов."""
    
    @staticmethod
    @log_execution()
    def generate(template_xls_file: str, data_dict: dict, output_pdf_file: str, common_cfg=None) -> bool:
        """
        Генерирует PDF из Excel-шаблона с подстановкой данных.
        
        Args:
            template_xls_file: Путь к шаблону .xls/.xlsx
            data_dict: Словарь с данными {имя_клетки: значение}
            output_pdf_file: Путь к выходному PDF
            common_cfg: Конфигурация
            
        Returns:
            True при успешной генерации
        """
        return perform_excel_generate(template_xls_file, data_dict, output_pdf_file, common_cfg)