@echo off
title SeisWebLog Survey Viewer
echo ===============================
echo   Starting Survey Viewer
echo ===============================
REM Get the current directory path where the batch file resides
set "CURRENT_DIR=%~dp0"

ECHO "CURRENT DIR: %CURRENT_DIR%"
REM Activate virtual environment (must be inside project folder)
call myenv\Scripts\activate
ECHO "CURRENT DIR: %CURRENT_DIR%"
cd %CURRENT_DIR%
python -m dataviewer.app

pause