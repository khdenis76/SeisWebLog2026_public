color 2E
@echo off
title SeisWebLog Network Server
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
echo.REM Get the current directory path where the batch file resides
set "CURRENT_DIR=%~dp0"

ECHO "CURRENT DIR: %CURRENT_DIR%"
REM Activate virtual environment (must be inside project folder)
call myenv\Scripts\activate
cd %CURRENT_DIR%
REM Run Django
python manage.py runserver 172.21.77.154:8000
cmd /k