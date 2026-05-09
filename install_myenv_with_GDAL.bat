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
REM   GDAL INSTALL FROM GOHLKE WHEEL
REM ==========================================

echo.
echo ==========================================
echo   Checking GDAL
echo ==========================================

REM --- Check if GDAL is already installed in this venv ---
python -c "from osgeo import gdal; print('GDAL already installed. Version:', gdal.VersionInfo())" >nul 2>nul
if %ERRORLEVEL%==0 (
    python -c "from osgeo import gdal; print('GDAL already installed. Version:', gdal.VersionInfo())"
    goto :GDAL_DONE
)

echo GDAL not found. Preparing wheel installation...

REM --- Detect Python tag, for example cp311 or cp312 ---
for /f %%i in ('python -c "import sys; print(f'cp{sys.version_info.major}{sys.version_info.minor}')"') do set "PY_TAG=%%i"

REM --- Detect 64-bit Python ---
for /f %%i in ('python -c "import struct; print(struct.calcsize('P') * 8)"') do set "PY_BITS=%%i"

if not "%PY_BITS%"=="64" (
    echo ERROR: GDAL wheel requires 64-bit Python. Current Python is %PY_BITS%-bit.
    pause
    exit /b 1
)

set "GDAL_VERSION=3.11.4"
set "GDAL_RELEASE=v2025.10.25"
set "GDAL_WHL=gdal-%GDAL_VERSION%-%PY_TAG%-%PY_TAG%-win_amd64.whl"
set "GDAL_URL=https://github.com/cgohlke/geospatial-wheels/releases/download/%GDAL_RELEASE%/%GDAL_WHL%"
set "GDAL_TEMP_DIR=%TEMP%\seisweblog_gdal_install"

if not exist "%GDAL_TEMP_DIR%" mkdir "%GDAL_TEMP_DIR%"

if exist "%GDAL_TEMP_DIR%\%GDAL_WHL%" del "%GDAL_TEMP_DIR%\%GDAL_WHL%" >nul 2>nul

echo Python tag: %PY_TAG%
echo Downloading GDAL wheel:
echo %GDAL_URL%

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%GDAL_URL%' -OutFile '%GDAL_TEMP_DIR%\%GDAL_WHL%' -UseBasicParsing } catch { Write-Host $_.Exception.Message; exit 1 }"

if not exist "%GDAL_TEMP_DIR%\%GDAL_WHL%" (
    echo ERROR: GDAL wheel download failed.
    echo Check if this wheel exists for your Python version:
    echo %GDAL_URL%
    pause
    exit /b 1
)

echo Installing GDAL wheel...
python -m pip install "%GDAL_TEMP_DIR%\%GDAL_WHL%"

if %ERRORLEVEL% neq 0 (
    echo ERROR: GDAL installation failed.
    pause
    exit /b 1
)

echo Verifying GDAL installation...
python -c "from osgeo import gdal; print('GDAL installed successfully. Version:', gdal.VersionInfo())"

if %ERRORLEVEL% neq 0 (
    echo ERROR: GDAL verification failed.
    pause
    exit /b 1
)

:GDAL_DONE

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