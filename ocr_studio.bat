@echo off
title SeisWebLog OCR Studio
echo ===============================
echo   Starting OCR Studio
echo ===============================
REM Get the current directory path where the batch file resides
set "CURRENT_DIR=%~dp0"

ECHO "CURRENT DIR: %CURRENT_DIR%"
REM Activate virtual environment (must be inside project folder)
call myenv\Scripts\activate
ECHO "CURRENT DIR: %CURRENT_DIR%"
cd %CURRENT_DIR%
python -m ocr.run_ocr_v3
