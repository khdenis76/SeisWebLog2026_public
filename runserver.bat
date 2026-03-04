@echo off
chcp 65001 >nul
color 2E
title SeisWebLog Network Server

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

REM Run Django
python manage.py runserver 172.21.77.154:8000

cmd /k