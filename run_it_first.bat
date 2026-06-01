@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM ============================================================
REM SeisWebLog installer - simple + GDAL unblock fix
REM Run from SeisWebLog project root folder.
REM ============================================================

title SeisWebLog Installer

echo ==========================================
echo   SeisWebLog - Installer
echo ==========================================
echo Working directory: %CD%
echo.

where py >nul 2>nul
if %errorlevel%==0 (
    set "PYLAUNCHER=py -3"
) else (
    set "PYLAUNCHER=python"
)

if not exist "myenv\Scripts\python.exe" (
    echo Creating virtual environment myenv...
    %PYLAUNCHER% -m venv myenv
    if errorlevel 1 goto FAIL
)

call "myenv\Scripts\activate.bat"
if errorlevel 1 goto FAIL

echo.
echo ==========================================
echo   Upgrading pip tools
echo ==========================================
python -m pip install --upgrade pip setuptools wheel packaging
if errorlevel 1 goto FAIL

echo.
echo ==========================================
echo   Checking GDAL
echo ==========================================
python -c "from osgeo import gdal; print('GDAL OK:', gdal.VersionInfo())" >nul 2>nul
if %errorlevel%==0 (
    echo GDAL already works.
) else (
    echo Installing GDAL from Windows geospatial wheels...
    python -m pip install --upgrade --prefer-binary --find-links https://github.com/cgohlke/geospatial-wheels/releases/download/v2025.10.25/ GDAL==3.11.4
    if errorlevel 1 goto FAIL
)

echo.
echo ==========================================
echo   Unblocking downloaded binary files
echo ==========================================
powershell -NoProfile -ExecutionPolicy Bypass -Command "Get-ChildItem -LiteralPath '%CD%\myenv' -Recurse -Include *.dll,*.pyd,*.exe -ErrorAction SilentlyContinue | Unblock-File -ErrorAction SilentlyContinue"

echo Testing GDAL import...
python -c "from osgeo import gdal; print('GDAL OK:', gdal.VersionInfo())"
if errorlevel 1 goto GDAL_BLOCKED

if exist requirements.txt (
    echo.
    echo ==========================================
    echo   Installing requirements.txt
    echo ==========================================
    python -m pip install -r requirements.txt
    if errorlevel 1 (
        echo Normal pip install failed. Retrying with trusted hosts...
        python -m pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host github.com -r requirements.txt
        if errorlevel 1 goto FAIL
    )
) else (
    echo requirements.txt not found, skipping.
)

echo.
echo ==========================================
echo   Checking Tesseract
echo ==========================================
where tesseract >nul 2>nul
if %errorlevel%==0 (
    tesseract --version
) else (
    echo Tesseract is not in PATH.
    echo If OCR is required, install Tesseract manually or with winget:
    echo winget install --id UB-Mannheim.TesseractOCR
)

echo.
echo ==========================================
echo   Checking MiKTeX
echo ==========================================
where pdflatex >nul 2>nul
if %errorlevel%==0 (
    pdflatex --version
) else (
    echo MiKTeX/pdflatex is not in PATH.
    echo If PDF reports are required, install MiKTeX manually or with winget:
    echo winget install --id MiKTeX.MiKTeX
)

echo.
echo ==========================================
echo   INSTALLATION COMPLETE
echo ==========================================
echo To activate later:
echo call myenv\Scripts\activate.bat
echo.
pause
exit /b 0

:GDAL_BLOCKED
echo.
echo ==========================================
echo   GDAL IS INSTALLED BUT WINDOWS BLOCKED IT
echo ==========================================
echo Windows Application Control / Smart App Control / company policy blocked GDAL binary files.
echo The installer already tried to run Unblock-File on myenv.
echo.
echo Try these steps:
echo 1. Move project to a trusted local folder, for example D:\SeisWebLog2026_develop
echo 2. Open PowerShell as Administrator
echo 3. Run:
echo    Get-ChildItem -Path "%CD%\myenv" -Recurse -Include *.dll,*.pyd,*.exe ^| Unblock-File
echo 4. If still blocked, disable/allow through Windows App Control or ask IT to allow this venv folder.
echo.
goto FAIL

:FAIL
echo.
echo ==========================================
echo   INSTALLATION FAILED
echo ==========================================
echo Check the error message above.
echo.
pause
exit /b 1
