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
    py -3.11 -c "import struct; raise SystemExit(0 if struct.calcsize('P') * 8 == 64 else 1)" >nul 2>nul
    if errorlevel 1 (
        echo ERROR: Python 3.11 64-bit is not installed.
        echo Download it from: https://www.python.org/downloads/release/python-3119/
        goto FAIL
    )
    set "PYLAUNCHER=py -3.11"
) else (
    python -c "import sys, struct; raise SystemExit(0 if sys.version_info[:2] == (3, 11) and struct.calcsize('P') * 8 == 64 else 1)" >nul 2>nul
    if errorlevel 1 (
        echo ERROR: Python 3.11 64-bit is not installed or is not in PATH.
        echo Download it from: https://www.python.org/downloads/release/python-3119/
        goto FAIL
    )
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
    echo Installing NumPy dependency...
    python -m pip install --upgrade numpy
    if errorlevel 1 goto FAIL

    echo Installing the precompiled GDAL 3.11.4 Windows wheel...
    python -m pip install --index-url https://gisidx.github.io/gwi --only-binary=:all: --no-deps GDAL==3.11.4
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
    where winget >nul 2>nul
    if !errorlevel!==0 (
        echo Installing Tesseract OCR with winget...
        winget install --id UB-Mannheim.TesseractOCR --exact --silent --accept-source-agreements --accept-package-agreements
        if errorlevel 1 (
            echo WARNING: Automatic Tesseract installation failed.
            echo OCR features will not be available until Tesseract is installed.
        ) else (
            REM Make a standard Tesseract installation visible immediately.
            if exist "%ProgramFiles%\Tesseract-OCR\tesseract.exe" set "PATH=%PATH%;%ProgramFiles%\Tesseract-OCR"
            where tesseract >nul 2>nul
            if !errorlevel!==0 (
                echo Tesseract installed successfully.
                tesseract --version
            ) else (
                echo Tesseract was installed, but it is not yet in this terminal PATH.
                echo Restart Windows Terminal before using OCR features.
            )
        )
    ) else (
        echo WARNING: winget is not available.
        echo Install or update App Installer from Microsoft Store, then run:
        echo winget install --id UB-Mannheim.TesseractOCR --exact
    )
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
    where winget >nul 2>nul
    if !errorlevel!==0 (
        echo Installing MiKTeX with winget...
        winget install --id MiKTeX.MiKTeX --exact --silent --accept-source-agreements --accept-package-agreements
        if errorlevel 1 (
            echo WARNING: Automatic MiKTeX installation failed.
            echo LaTeX PDF reports will not be available until MiKTeX is installed.
        ) else (
            REM Check the common per-user and all-users MiKTeX locations.
            if exist "%LOCALAPPDATA%\Programs\MiKTeX\miktex\bin\x64\pdflatex.exe" set "PATH=%PATH%;%LOCALAPPDATA%\Programs\MiKTeX\miktex\bin\x64"
            if exist "%ProgramFiles%\MiKTeX\miktex\bin\x64\pdflatex.exe" set "PATH=%PATH%;%ProgramFiles%\MiKTeX\miktex\bin\x64"
            where pdflatex >nul 2>nul
            if !errorlevel!==0 (
                echo MiKTeX installed successfully.
                pdflatex --version
            ) else (
                echo MiKTeX was installed, but pdflatex is not yet in this terminal PATH.
                echo Restart Windows Terminal before generating LaTeX PDF reports.
            )
        )
    ) else (
        echo WARNING: winget is not available.
        echo Install or update App Installer from Microsoft Store, then run:
        echo winget install --id MiKTeX.MiKTeX --exact
    )
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
