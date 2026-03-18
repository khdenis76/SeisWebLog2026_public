@echo off
chcp 65001 >nul
color 1F
title SeisWebLog Local Server

echo.
echo ==============================================================
echo   ███████╗███████╗██╗███████╗██╗    ██╗███████╗██████╗ ██╗      ██████╗  ██████╗
echo   ██╔════╝██╔════╝██║██╔════╝██║    ██║██╔════╝██╔══██╗██║     ██╔═══██╗██╔════╝
echo   ███████╗█████╗  ██║███████╗██║ █╗ ██║█████╗  ██████╔╝██║     ██║   ██║██║  ███╗
echo   ╚════██║██╔══╝  ██║╚════██║██║███╗██║██╔══╝  ██╔══██╗██║     ██║   ██║██║   ██║
echo   ███████║███████╗██║███████║╚███╔███╔╝███████╗██████╔╝███████╗╚██████╔╝╚██████╔╝
echo   ╚══════╝╚══════╝╚═╝╚══════╝ ╚══╝╚══╝ ╚══════╝╚═════╝ ╚══════╝ ╚═════╝  ╚═════╝
echo.
echo                     S E I S W E B L O G
echo           Seismic Data Management ^& QC Platform
echo ==============================================================

REM Get the current directory path where the batch file resides
set "CURRENT_DIR=%~dp0"

echo CURRENT DIR: %CURRENT_DIR%

REM Activate virtual environment
call "%CURRENT_DIR%myenv\Scripts\activate"

cd /d "%CURRENT_DIR%"
echo.
echo Checking for model changes...
python manage.py makemigrations

echo.
echo Applying migrations if needed...
python manage.py migrate
REM Run Django
echo.
echo Starting Django server...
python manage.py runserver 127.0.0.1:8005

cmd /k