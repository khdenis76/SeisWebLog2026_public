@echo off
chcp 65001 >nul
color 1F
title SeisWebLog Local Server
@echo off
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
echo           Seismic Data Management & QC Platform
echo ==============================================================
echo.
REM Get the current directory path where the batch file resides
set "CURRENT_DIR=%~dp0"

ECHO "CURRENT DIR: %CURRENT_DIR%"
REM Activate virtual environment (must be inside project folder)
call myenv\Scripts\activate
cd %CURRENT_DIR%
REM Run Django
python manage.py runserver 127.0.0.1:8005
cmd /k