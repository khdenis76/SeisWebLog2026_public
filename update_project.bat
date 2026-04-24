@echo off
setlocal

set "WORKDIR=%~dp0"
cd /d "%WORKDIR%"

echo ============================================
echo SeisWebLog Project Updater
echo ============================================
echo.

if exist "%WORKDIR%myenv\Scripts\python.exe" (
    set "PYTHON_EXE=%WORKDIR%myenv\Scripts\python.exe"
) else (
    set "PYTHON_EXE=python"
)

echo Using Python: %PYTHON_EXE%
echo.

"%PYTHON_EXE%" "%WORKDIR%updater.py"
if errorlevel 1 (
    echo.
    echo Update failed.
    pause
    exit /b 1
)

echo.
echo Update finished successfully.
echo Starting runlocal.bat ...
echo.

if exist "%WORKDIR%runlocal.bat" (
    call "%WORKDIR%runlocal.bat"
) else (
    echo runlocal.bat not found.
    pause
)

endlocal
