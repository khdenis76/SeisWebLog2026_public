@echo off
chcp 65001 >nul
title SeisWebLog Django Server
color 0A

echo ============================================================
echo   ███████╗████████╗██████╗     ███████╗██╗   ██╗███╗   ██╗ ██████╗
echo   ██╔════╝╚══██╔══╝██╔══██╗    ██╔════╝╚██╗ ██╔╝████╗  ██║██╔════╝
echo   █████╗     ██║   ██████╔╝    ███████╗ ╚████╔╝ ██╔██╗ ██║██║
echo   ██╔══╝     ██║   ██╔═══╝     ╚════██║  ╚██╔╝  ██║╚██╗██║██║
echo   ██║        ██║   ██║         ███████║   ██║   ██║ ╚████║╚██████╗
echo   ╚═╝        ╚═╝   ╚═╝         ╚══════╝   ╚═╝   ╚═╝  ╚═══╝ ╚═════╝
echo ------------------------------------------------------------
echo    FTP / FTPS / SFTP Folder Sync Utility
echo ------------------------------------------------------------
echo
echo    Author : Denis Khairutdinov
echo    Year   : 2026
echo    Email  : kh_denis@mail.ru

echo ============================================================
@echo off
title FTP Sync Utility - Denis Khairutdinov 2026

REM Get directory where this BAT file is located
set "CURRENT_DIR=%~dp0"

echo CURRENT DIR: %CURRENT_DIR%

REM Go to project root (BAT location)
cd /d "%CURRENT_DIR%"

REM Activate virtual environment
call myenv\Scripts\activate

REM Run module
python -m ftpsync.ftp_sync_gui_v2

pause