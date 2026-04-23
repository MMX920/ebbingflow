@echo off
setlocal
cd /d %~dp0

echo ==========================================
echo   MemGraph AI - Data Purge Console
echo   !!! WARNING: Wiping All Data !!!
echo ==========================================

if not exist venv\Scripts\python.exe goto ERROR_VENV

:RUN_CLEAN
venv\Scripts\python.exe scripts\clear_all_data.py
goto END

:ERROR_VENV
echo [Error] venv not found in this folder.
goto END

:END
echo.
echo Process ended.
pause
