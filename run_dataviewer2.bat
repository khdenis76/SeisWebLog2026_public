@echo off
title SeisWebLog DataViewer 2 Diagnostic

cd /d "%~dp0"

call "myenv\Scripts\activate.bat"
set "PYTHONPATH=%CD%"

REM Force software rendering
set "QT_OPENGL=software"
set "QT_QUICK_BACKEND=software"
set "QSG_RHI_BACKEND=software"

REM Enable Python native-crash reporting
set "PYTHONFAULTHANDLER=1"

REM Create a fresh log
del "dataviewer2_console.log" 2>nul

python -X faulthandler -m dataviewer2.app > "dataviewer2_console.log" 2>&1

echo.
echo DataViewer exited with code: %ERRORLEVEL%
echo Log file:
echo %CD%\dataviewer2_console.log
echo.
pause