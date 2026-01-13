@echo off
echo ================================
echo  SeisWebLog environment setup
echo ================================

REM --- Check Python ---
python --version >nul 2>&1
IF ERRORLEVEL 1 (
    echo ERROR: Python is not installed or not in PATH
    pause
    exit /b 1
)

REM --- Create virtual environment ---
IF NOT EXIST myenv (
    echo Creating virtual environment...
    python -m venv myenv
) ELSE (
    echo Virtual environment already exists
)

REM --- Activate virtual environment ---
echo Activating virtual environment...
call myenv\Scripts\activate.bat

REM --- Upgrade pip ---
echo Upgrading pip...
python -m pip install --upgrade pip

REM --- Install requirements ---
IF EXIST requirements.txt (
    echo Installing requirements...
    pip install -r requirements.txt
) ELSE (
    echo ERROR: requirements.txt not found
    pause
    exit /b 1
)

echo ================================
echo  Environment setup completed
echo ================================
echo To activate manually later use:
echo   myenv\Scripts\activate
echo ================================

pause
