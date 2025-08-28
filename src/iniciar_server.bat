@echo off
REM === Ruta base del proyecto ===
cd /d "%~dp0"

REM === Activar entorno virtual ===
call ..\venv\Scripts\activate.bat

REM === Lanzar Waitress (WSGI) ===
waitress-serve --host=0.0.0.0 --port=8050 app_PROYECTO2:server

pause
