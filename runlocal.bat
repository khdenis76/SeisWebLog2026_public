@echo off
title SeisWebLog Django Server
echo ===============================
echo   Starting SeisWebLog Server
echo ===============================
REM Get the current directory path where the batch file resides
set "CURRENT_DIR=%~dp0"

ECHO "CURRENT DIR: %CURRENT_DIR%"
REM Activate virtual environment (must be inside project folder)
call myenv\Scripts\activate
cd %CURRENT_DIR%
REM Run Django
python manage.py runserver 127.0.0.1:8005
cmd /k