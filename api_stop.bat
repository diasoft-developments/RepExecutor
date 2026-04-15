@echo off
echo Поиск процесса API...

:: Найти PID процесса на порту 8000
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8000 ^| findstr LISTENING') do (
    set PID=%%a
    goto :found
)

:found
if defined PID (
    echo Найден процесс с PID: %PID%
    echo Останавливаем процесс...
    taskkill /F /PID %PID%
    echo Процесс остановлен
) else (
    echo Процесс API не найден
)

pause