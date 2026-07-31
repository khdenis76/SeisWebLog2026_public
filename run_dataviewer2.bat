@echo off
setlocal
title SeisWebLog DataViewer 2.0
cd /d "%~dp0"
if not exist "myenv\Scripts\activate.bat" (
  echo Virtual environment not found: %CD%\myenv
  pause
  exit /b 1
)
call "myenv\Scripts\activate.bat"
set "PYTHONPATH=%CD%"
python -m dataviewer2.app %*
if errorlevel 1 pause
endlocal
