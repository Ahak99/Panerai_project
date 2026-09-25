@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

:START_PROCESS
echo ====================================================
echo [SYSTEM] Starting Panerai Daily Guard...
echo ====================================================

:: 1. Run the checker to see if today's data is missing
python src/checker.py > missing_countries.txt
set "MISSING_LIST="

:: Read the last line of the output file (which contains our comma-separated list)
for /f "tokens=*" %%i in (missing_countries.txt) do set "MISSING_LIST=%%i"

:: The checker exits with code 1 if data is missing
if %errorlevel% EQU 1 (
    echo.
    echo [SYSTEM] Data is missing or incomplete: %MISSING_LIST%
    echo [SYSTEM] Launching scraper for these countries...
    echo ====================================================

    :: 2. Launch the scraper for specific countries
    python src/scraper.py -c %MISSING_LIST%

    echo.
    echo [SYSTEM] Scraper execution finished.

    :: 3. Launch the transfer for those same countries
    echo [SYSTEM] Syncing new data to Databricks...
    python src/transfer.py -c %MISSING_LIST%

    echo.
    echo [SYSTEM] Transfer completed.
) else (
    echo.
    echo [SYSTEM] Today's data is already complete.
    echo [SYSTEM] No action needed.
)

echo ====================================================
echo [SYSTEM] Daily process finished successfully.
echo ====================================================
if exist missing_countries.txt del missing_countries.txt
pause
