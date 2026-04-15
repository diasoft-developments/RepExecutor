@echo off
echo Перезапуск API...

:: Останавливаем
call api_stop.bat

:: Ждем 3 секунды
timeout /t 3 /nobreak >nul

:: Запускаем заново
start /min cmd /c "api_start.bat"

echo API перезапущен
pause