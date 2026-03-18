@echo off
setlocal

echo ==========================================
echo   SeisWebLog - Installer
echo ==========================================

REM --- Remember current directory ---
set "WORKDIR=%~dp0"
echo Working directory: %WORKDIR%

REM ==========================================
REM   PYTHON ENV SETUP FIRST
REM ==========================================

REM --- Check Python exists ---
where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python is not installed or not in PATH.
    pause
    exit /b 1
)

REM --- Create virtual environment if missing ---
if not exist "%WORKDIR%myenv\Scripts\activate.bat" (
    echo Virtual environment not found. Creating myenv...
    python -m venv "%WORKDIR%myenv"

    if %ERRORLEVEL% neq 0 (
        echo ERROR: Failed to create virtual environment.
        pause
        exit /b 1
    )
)

REM --- Activate virtual environment ---
echo Activating virtual environment...
call "%WORKDIR%myenv\Scripts\activate.bat"

if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to activate virtual environment.
    pause
    exit /b 1
)

REM --- Upgrade pip ---
echo Upgrading pip...
python -m pip install --upgrade pip

if %ERRORLEVEL% neq 0 (
    echo ERROR: Failed to upgrade pip.
    pause
    exit /b 1
)

REM --- Install requirements ---
if exist "%WORKDIR%requirements.txt" (
    echo Installing requirements...
    python -m pip install -r "%WORKDIR%requirements.txt"
) else (
    echo ERROR: requirements.txt not found in %WORKDIR%
    pause
    exit /b 1
)

if %ERRORLEVEL% neq 0 (
    echo ERROR: pip install failed.
    pause
    exit /b 1
)

echo Python environment ready.

REM ==========================================
REM   TESSERACT INSTALL (LAST STEP)
REM ==========================================

echo.
echo ==========================================
echo   Checking Tesseract
echo ==========================================

set "TESS_VERSION=5.3.0"
set "TESS_URL=https://github.com/UB-Mannheim/tesseract/releases/download/v%TESS_VERSION%/tesseract-ocr-w64-setup-%TESS_VERSION%.exe"
set "TEMP_DIR=%TEMP%\tesseract_install"
set "INSTALL_DIR=C:\Tesseract-OCR"

REM --- Check if already installed ---
where tesseract >nul 2>nul
if %ERRORLEVEL%==0 (
    echo Tesseract already installed:
    tesseract --version
    goto :DONE
)

if exist "%INSTALL_DIR%\tesseract.exe" (
    echo Tesseract found at %INSTALL_DIR%
    "%INSTALL_DIR%\tesseract.exe" --version
    goto :DONE
)

if exist "C:\Program Files\Tesseract-OCR\tesseract.exe" (
    echo Tesseract found in Program Files
    "C:\Program Files\Tesseract-OCR\tesseract.exe" --version
    goto :DONE
)

echo Tesseract not found. Installing...

REM --- Create temp folder ---
if not exist "%TEMP_DIR%" mkdir "%TEMP_DIR%"

echo Downloading Tesseract...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -Uri '%TESS_URL%' -OutFile '%TEMP_DIR%\tesseract.exe'"

if not exist "%TEMP_DIR%\tesseract.exe" (
    echo ERROR: Download failed.
    pause
    exit /b 1
)

echo Installing silently...
"%TEMP_DIR%\tesseract.exe" /S /D=%INSTALL_DIR%

if %ERRORLEVEL% neq 0 (
    echo ERROR: Installation failed.
    pause
    exit /b 1
)

timeout /t 2 >nul

REM --- Add to PATH for future sessions ---
setx PATH "%PATH%;%INSTALL_DIR%" >nul

REM --- Add to current session PATH too ---
set "PATH=%PATH%;%INSTALL_DIR%"

echo Verifying installation...
"%INSTALL_DIR%\tesseract.exe" --version

if %ERRORLEVEL% neq 0 (
    echo ERROR: Verification failed.
    pause
    exit /b 1
)

echo Tesseract installed successfully!

:DONE
echo.
echo ==========================================
echo   ALL DONE
echo ==========================================
echo.

pause