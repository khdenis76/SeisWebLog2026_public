@echo off
echo ================================
echo  SeisWebLog environment setup
echo ================================

REM --- Remember current directory ---
set "WORKDIR=%~dp0"
echo Working directory: %WORKDIR%

REM --- Check Python ---
python --version >nul 2>&1
IF ERRORLEVEL 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM --- Create virtual environment in CURRENT directory ---
IF NOT EXIST "%WORKDIR%\myenv" (
    echo Creating virtual environment in %WORKDIR%\myenv
    python -m venv "%WORKDIR%\myenv"
) ELSE (
    echo Virtual environment already exists in %WORKDIR%\myenv
)

REM --- Activate virtual environment ---
echo Activating virtual environment...
call "%WORKDIR%\myenv\Scripts\activate.bat"

REM --- Upgrade pip ---
echo Upgrading pip...
python -m pip install --upgrade pip

REM --- Install requirements ---
IF EXIST "%WORKDIR%\requirements.txt" (
    echo Installing requirements...
    python -m pip install -r "%WORKDIR%\requirements.txt"
) ELSE (
    echo ERROR: requirements.txt not found in %WORKDIR%
    pause
    exit /b 1
)

echo ================================
echo  Environment setup completed
echo ================================
echo To activate manually later use:
echo   %WORKDIR%\myenv\Scripts\activate
echo ================================
REM --- Ensure initial SQLite database exists ---
IF NOT EXIST "db.sqlite3" (
REM   echo db.sqlite3 not found. Copying from dummy_db...
    echo db.sqlite3 not found. run migration...
    python manage.py migrate
REM    IF EXIST "dummy_db\db.sqlite3" (
REM        copy "dummy_db\db.sqlite3" "db.sqlite3"
REM        echo Database copied successfully.
REM    ) ELSE (
REM        echo ERROR: dummy_db\db.sqlite3 not found!
REM        pause
REM        exit /b 1
REM    )
) ELSE (
    echo Database already exists. Skipping copy.
)

pause
