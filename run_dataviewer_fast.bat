@echo off
title SeisWebLog Survey Viewer

cd /d "%~dp0"

call "myenv\Scripts\activate.bat"

set "PYTHONPATH=%CD%"

python -m dataviewer_fast.app "%~1"

pause