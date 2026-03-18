@echo off
setlocal

echo ==========================================
echo   SeisWebLog - Full Installer
echo ==========================================

REM --- Remember current directory ---
set "WORKDIR=%~dp0"
echo Working directory: %WORKDIR%

REM --- Tesseract config ---
set "TESS_VERSION=5.3.0"
set "TESS_URL=https://github.com/UB-Mannheim/tesseract/releases/download/v%TESS_VERSION%/tesseract-ocr-w64-setup-%TESS_VERSION%.exe"
set "TEMP_DIR=%TEMP%\tesseract_install"
set "INSTALL_DIR=C:\Tesseract-OCR"

echo.
echo Checking for existing Tesseract installation...

REM --- Check PATH first ---
where tesseract >nul 2>nul
if %ERRORLEVEL%==0 (
    echo Found Tesseract in PATH:
    tesseract --version
    goto :PYTHON_SETUP
)

REM --- Check known folders ---
if exist "%INSTALL_DIR%\tesseract.exe" (
    echo Found Tesseract at %INSTALL_DIR%
    "%INSTALL_DIR%\tesseract.exe" --version
    goto :PYTHON_SETUP
)

if exist "C:\Program Files\Tesseract-OCR\tesseract.exe" (
    echo Found Tesseract in C:\Program Files\Tesseract-OCR
    "C:\Program Files\Tesseract-OCR\tesseract.exe" --version
    goto :PYTHON_SETUP
)

echo Tesseract not found. Installing...

REM --- Create temp folder ---
if not exist "%TEMP_DIR%" mkdir "%TEMP_DIR%"

echo Downloading Tesseract...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -Uri '%TESS_URL%' -OutFile '%TEMP_DIR%\tesseract-setup.exe'"

if not exist "%TEMP_DIR%\tesseract-setup.exe" (
    echo ERROR: Failed to download Tesseract installer.
    pause
    exit /b 1
)

echo Installing Tesseract silently...
"%TEMP_DIR%\tesseract-setup.exe" /S /D=%INSTALL_DIR%

if %ERRORLEVEL% neq 0 (
    echo ERROR: Tesseract installer failed.
    pause
    exit /b 1
)

timeout /t 2 >nul

REM --- Add to PATH for future sessions ---
echo Adding Tesseract to PATH...
setx PATH "%PATH%;%INSTALL_DIR%" >nul

REM --- Also add for current session ---
set "PATH=%PATH%;%INSTALL_DIR%"

echo Verifying Tesseract installation...
"%INSTALL_DIR%\tesseract.exe" --version >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Tesseract installed, but verification failed.
    pause
    exit /b 1
)

echo Tesseract installed successfully.
"%INSTALL_DIR%\tesseract.exe" --version

:PYTHON_SETUP
echo.
echo ==========================================
echo   Python Environment Setup
echo ==========================================

REM --- Check venv exists ---
if not exist "%WORKDIR%myenv\Scripts\activate.bat" (
    echo ERROR: Virtual environment not found:
    echo %WORKDIR%myenv\Scripts\activate.bat
    pause
    exit /b 1
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
    echo ERROR: pip upgrade failed.
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
    echo ERROR: requirements installation failed.
    pause
    exit /b 1
)

echo.
echo ==========================================
echo   Installation completed successfully
echo ==========================================
echo.
pause