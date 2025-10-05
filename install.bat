@echo off
setlocal
cd /d %~dp0
echo === Création du venv (Python 3.10) ===
py -3.10 -m venv venv || goto :error
echo === Upgrade pip ===
call venv\Scripts\python -m pip install --upgrade pip wheel setuptools || goto :error
echo === Installation des dépendances ===
call venv\Scripts\python -m pip install -r requirements.txt || goto :error
echo.
echo ✅ Installation OK
pause
exit /b 0
:error
echo ❌ Erreur pendant l'installation. Vérifie que 'py -3.10' est disponible.
pause
exit /b 1
