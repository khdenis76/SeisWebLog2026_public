@echo off
setlocal

set "WORKDIR=%~dp0"
cd /d "%WORKDIR%"

echo ============================================
echo SeisWebLog Project Restore
echo ============================================
echo.

if exist "%WORKDIR%myenv\Scripts\python.exe" (
    set "PYTHON_EXE=%WORKDIR%myenv\Scripts\python.exe"
) else (
    set "PYTHON_EXE=python"
)

"%PYTHON_EXE%" "%WORKDIR%restore_project.py"
if errorlevel 1 (
    echo.
    echo Restore failed.
    pause
    exit /b 1
)

echo.
echo Restore finished successfully.
echo.

if exist "%WORKDIR%runlocal.bat" (
    call "%WORKDIR%runlocal.bat"
) else (
    pause
)

endlocal
