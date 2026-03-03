# RepExecutor

Скрипт автоматически обрабатывает отчеты в формате WRD (Delphi DFM), извлекает SQL-запросы, выполняет их на сервере Microsoft SQL Server, сохраняет результаты в CSV-файл для слияния данных, выполняет merge с шаблоном Microsoft Word указанным в образце и сохраняет итоговый отчет в формате PDF.

## Основное назначение

Скрипт предназначен для автоматизации процесса генерации отчетов на основе wrd шаблонов диасофт. 

### Настройки
- **RepExecutor.json** – параметры (`report_name`, `params`, `marks`).
- **RepExecutor.ini → [tmp]**
  - `save` (bool) – если `true`, CSV/SQL/копия JSON остаются в `path`; если `false`, файлы удаляются.
  - `path` – каталог для временных файлов (если `save=true`).

#### Пример RepExecutor.json
```json
{
  "report_name": "СЧЕТ",
  "output_path": "T:\\Reports\\result.pdf",
  "output_format": "PDF",
  "params": {
    "Podpisant": 10000000001,
    "Controler": 10000000001,
    "InstitutionID": 10000000001
  },
  "marks": {
    "Type": 3,
    "ID": 10000000002
  }
}
```

#### Пример RepExecutor.ini
```ini
[log]
level = DEBUG
path = C:\\Tasks\\Logs\\
rotation = 2 MB
retention = 7 days
compression = zip

[tmp]
save = true
path = C:\\Tasks\\tmp\\
```

После запуска исходный `RepExecutor.json` всегда удаляется, а при `save=true` сохраняется копия.

### Запуск
1. Заполните `.env` (DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD, DB_DRIVER, DB_TrustServerCertificate).
2. `python RepExecutor.py`.

### Зависимости
`pyodbc`, `pywin32`, `loguru`, `python-dotenv`, `pandas`, `pydantic`.

Логи ведутся через **loguru** (INFO/ERROR/DEBUG уровни).

Скрипт подходит для пакетной генерации PDF‑отчётов по шаблонам WRD. 