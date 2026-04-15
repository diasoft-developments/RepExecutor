@echo off
:: check_process.bat

echo ========================================
echo Поиск API процесса
echo ========================================

:: Ищем процесс на порту 8000
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8000 ^| findstr LISTENING') do (
    set PID=%%a
    goto :found
)

echo ? API не запущен
pause
exit /b

:found
echo ? Найден процесс с PID: %PID%
echo.

:: Получаем информацию о процессе
tasklist /FI "PID eq %PID%" /FO TABLE

:: Проверяем, является ли процесс службой
sc query | findstr /i "RepExecutor" >nul
if %errorlevel% equ 0 (
    echo.
    echo ? ЗАПУЩЕН КАК СЛУЖБА
    echo.
    sc query RepExecutorAPI | findstr "STATE"
) else (
    echo.
    echo ?? ЗАПУЩЕН КАК ОБЫЧНОЕ ПРИЛОЖЕНИЕ
    echo    (не как служба)
)

:: Показываем полную командную строку
echo.
echo Командная строка:
wmic process where processid=%PID% get commandline

pause