@echo off
setlocal
cd /d %~dp0
chcp 65001 >NUL
echo === Préparation (winutils + venv) ===
if not exist "venv\Scripts\python.exe" (
  echo ❌ venv absent. Lance d'abord: install.bat
  pause
  exit /b 1
)
call venv\Scripts\python get_winutils.py
set "HADOOP_HOME=%~dp0winutils"
set "PATH=%HADOOP_HOME%;%HADOOP_HOME%\bin;%PATH%"
set "PYSPARK_PYTHON=python"
set "PYSPARK_DRIVER_PYTHON=python"
if "%JAVA_HOME%"=="" (
  echo ⚠️  JAVA_HOME n'est pas défini. Assure-toi d'avoir Java 17 installé.
)
echo === Activation venv et lancement du pipeline ===
call venv\Scripts\activate
python -m src.main
echo.
echo ✅ WAX Pipeline terminé
pause
