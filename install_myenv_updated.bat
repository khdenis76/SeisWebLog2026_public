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
REM   TESSERACT INSTALL
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
    goto :LATEX_CHECK
)

if exist "%INSTALL_DIR%\tesseract.exe" (
    echo Tesseract found at %INSTALL_DIR%
    "%INSTALL_DIR%\tesseract.exe" --version
    set "PATH=%PATH%;%INSTALL_DIR%"
    goto :LATEX_CHECK
)

if exist "C:\Program Files\Tesseract-OCR\tesseract.exe" (
    echo Tesseract found in Program Files
    "C:\Program Files\Tesseract-OCR\tesseract.exe" --version
    set "PATH=%PATH%;C:\Program Files\Tesseract-OCR"
    goto :LATEX_CHECK
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

:LATEX_CHECK
echo.
echo ==========================================
echo   Checking LaTeX (MiKTeX)
echo ==========================================

where pdflatex >nul 2>nul
if %ERRORLEVEL%==0 (
    echo LaTeX already installed.
    pdflatex --version
    goto :DONE_LATEX
)

set "MIKTEX_EXE=%TEMP%\basic-miktex-25.12-x64.exe"

echo Downloading MiKTeX installer...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"Invoke-WebRequest -Uri 'https://miktex.org/download/ctan/systems/win32/miktex/setup/windows-x64/basic-miktex-25.12-x64.exe' -OutFile '%MIKTEX_EXE%'"

if not exist "%MIKTEX_EXE%" (
    echo ERROR: MiKTeX download failed.
    pause
    exit /b 1
)

echo Installing MiKTeX...
start /wait "" "%MIKTEX_EXE%"

echo Updating PATH for current session...
set "PATH=%PATH%;%LOCALAPPDATA%\Programs\MiKTeX\miktex\bin\x64"

echo Verifying LaTeX installation...
where pdflatex >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo WARNING: MiKTeX installed, but pdflatex is not in PATH yet.
    echo Reopen terminal and test:
    echo pdflatex --version
) else (
    pdflatex --version
)

:DONE_LATEX
echo.
echo ==========================================
echo   ALL DONE
echo ==========================================
echo.
pause