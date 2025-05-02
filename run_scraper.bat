@echo off
REM Set console to UTF-8 mode
chcp 65001 > nul
set LOG_FILE=C:\Python\Python310\projects\BuyinScraping\scheduler_runs.log

echo ========================================== >> "%LOG_FILE%"
echo %date% %time% - Task started >> "%LOG_FILE%"

echo %date% %time% - Changing to project directory >> "%LOG_FILE%"
cd /d C:\Python\Python310\projects\BuyinScraping
if errorlevel 1 (
    echo %date% %time% - ERROR: Failed to change directory >> "%LOG_FILE%"
    exit /b 1
)

echo %date% %time% - Activating virtual environment >> "%LOG_FILE%"
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo %date% %time% - ERROR: Failed to activate virtual environment >> "%LOG_FILE%"
    exit /b 1
)

echo %date% %time% - Virtual environment activated successfully >> "%LOG_FILE%"
echo %date% %time% - Python path: >> "%LOG_FILE%"
where python >> "%LOG_FILE%"

echo %date% %time% - Starting main.py >> "%LOG_FILE%"
python -m main
if errorlevel 1 (
    echo %date% %time% - ERROR: Script failed with error code %errorlevel% >> "%LOG_FILE%"
    echo Error occurred while running the scraper
    exit /b 1
)

echo %date% %time% - Script completed successfully >> "%LOG_FILE%"
echo %date% %time% - Deactivating virtual environment >> "%LOG_FILE%"
deactivate

echo %date% %time% - Task completed >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%" 