@echo off
setlocal EnableExtensions EnableDelayedExpansion

echo ==========================================
echo   SeisWebLog - Installer
echo ==========================================

set "WORKDIR=%~dp0"
echo Working directory: %WORKDIR%

REM ==========================================
REM PYTHON
REM ==========================================
where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python not found in PATH.
    pause
    exit /b 1
)

if not exist "%WORKDIR%myenv\Scripts\activate.bat" (
    echo Creating virtual environment...
    python -m venv "%WORKDIR%myenv"
)

call "%WORKDIR%myenv\Scripts\activate.bat"

echo Upgrading pip...
python -m pip install --upgrade pip

if not exist "%WORKDIR%requirements.txt" (
    echo ERROR: requirements.txt not found.
    pause
    exit /b 1
)

echo Installing Python requirements...
python -m pip install -r "%WORKDIR%requirements.txt"

echo Python ready.

REM ==========================================
REM TESSERACT
REM ==========================================
echo.
echo ==========================================
echo   Checking Tesseract
echo ==========================================

where tesseract >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Installing Tesseract...

    set "TESS_URL=https://github.com/UB-Mannheim/tesseract/releases/download/v5.3.0/tesseract-ocr-w64-setup-5.3.0.exe"
    set "TESS_TMP=%TEMP%\tesseract_setup.exe"

    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
        "Invoke-WebRequest -UseBasicParsing '%TESS_URL%' -OutFile '%TESS_TMP%'"

    if not exist "%TESS_TMP%" (
        echo ERROR: Tesseract download failed.
        pause
        exit /b 1
    )

    "%TESS_TMP%" /S
) else (
    echo Tesseract already installed.
)

REM ==========================================
REM MIKTEX
REM ==========================================
echo.
echo ==========================================
echo   Checking MiKTeX
echo ==========================================

where pdflatex >nul 2>nul
if %ERRORLEVEL% neq 0 (

    echo Installing MiKTeX...

    set "ZIP=%TEMP%\miktex.zip"
    set "DIR=%TEMP%\miktex"
    set "REPO=%TEMP%\miktex_repo"
    set "REPOURL=https://ftp.fau.de/ctan/systems/win32/miktex/tm/packages/"

    REM clean old files
    if exist "!ZIP!" del /f /q "!ZIP!"
    if exist "!DIR!" rmdir /s /q "!DIR!"

    echo Downloading MiKTeX setup utility...
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
        "Invoke-WebRequest -UseBasicParsing 'https://miktex.org/download/win/miktexsetup-x64.zip' -OutFile '!ZIP!'"

    if not exist "!ZIP!" (
        echo ERROR: MiKTeX setup download failed.
        pause
        exit /b 1
    )

    echo Extracting MiKTeX...
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
        "Expand-Archive -Path '!ZIP!' -DestinationPath '!DIR!' -Force"

    if not exist "!DIR!\miktexsetup_standalone.exe" (
        echo ERROR: miktexsetup not found.
        pause
        exit /b 1
    )

    if not exist "!REPO!" mkdir "!REPO!"

    echo Downloading MiKTeX packages...
    "!DIR!\miktexsetup_standalone.exe" ^
        --package-set=basic ^
        --remote-package-repository="!REPOURL!" ^
        --local-package-repository="!REPO!" ^
        download

    if !ERRORLEVEL! neq 0 (
        echo Retry download...
        "!DIR!\miktexsetup_standalone.exe" ^
            --package-set=basic ^
            --remote-package-repository="!REPOURL!" ^
            --local-package-repository="!REPO!" ^
            download

        if !ERRORLEVEL! neq 0 (
            echo ERROR: MiKTeX download failed.
            pause
            exit /b 1
        )
    )

    echo Installing MiKTeX...
    "!DIR!\miktexsetup_standalone.exe" ^
        --local-package-repository="!REPO!" ^
        --package-set=basic ^
        --shared=no ^
        --modify-path ^
        install

    if !ERRORLEVEL! neq 0 (
        echo ERROR: MiKTeX install failed.
        pause
        exit /b 1
    )

) else (
    echo MiKTeX already installed.
)

echo.
echo ==========================================
echo   INSTALL COMPLETE
echo ==========================================
pause