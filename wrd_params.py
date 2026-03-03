import re
from loguru import logger
from utils_logger import log_execution

# @log_execution()
# def load_params_from_config(config_path: str) -> dict:
#     with open(  config_path, "r", encoding="utf-8") as f:
#         config = json.load(f)
        
#     return config.get("params", {})

@log_execution()
def inject_report_params(sql: str, params: dict) -> str:
    """
    Заменяет %Param! на значение из словаря marks.
    Не заменяет параметры внутри однострочных SQL-комментариев (-- ...).
    """
    try:
        def replace_in_code(part: str) -> str:
            def replacer(match: re.Match) -> str:
                param_name = match.group(1)

                if param_name not in params:
                    raise ValueError(f"Параметр '{param_name}' не найден в конфигурации")

                value = params[param_name]

                if isinstance(value, str):
                    safe_value = value.replace("'", "''")
                    return f"'{safe_value}'"

                if value is None:
                    return "NULL"

                return str(value)

            return re.sub(r"%([A-Za-z0-9_]+)!", replacer, part)

        result_lines = []

        for line in sql.splitlines():
            if "--" in line:
                code_part, comment_part = line.split("--", 1)
                code_part = replace_in_code(code_part)
                result_lines.append(code_part + "--" + comment_part)
            else:
                result_lines.append(replace_in_code(line))

        return "\n".join(result_lines)

    except Exception:
        logger.exception("Ошибка обработки параметров SQL")
        raise