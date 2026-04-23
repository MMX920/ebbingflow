@echo off
title EbbingFlow Dual Console Starter
echo [SYSTEM] Starting EbbingFlow Backend Broadcast Server...
echo [SYSTEM] Access Interaction Hub at: http://localhost:8000
echo [SYSTEM] Access Data Monitor at: http://localhost:8000/monitor
echo.

echo [SYSTEM] Starting EbbingFlow Core Service...
set PY_EXE=python
if exist .venv\Scripts\python.exe set PY_EXE=.venv\Scripts\python.exe
if exist venv\Scripts\python.exe set PY_EXE=venv\Scripts\python.exe
%PY_EXE% api\server.py
pause
