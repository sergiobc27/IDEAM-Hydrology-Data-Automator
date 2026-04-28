@echo off
set PYTHONUTF8=1
set PYTHONPATH=%~dp0src
".venv\Scripts\python.exe" -m ideam_socrata.cli interactive
pause
