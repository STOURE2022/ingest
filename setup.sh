@echo off
title 🚀 Setup Environnement WAX Local - Spark + Delta
color 0a

echo ==============================================
echo   ⚙️  INITIALISATION DE L'ENVIRONNEMENT WAX
echo ==============================================
echo.

:: Étape 1 – Vérification du JDK
echo 🔍 Vérification de Java...
where java >nul 2>nul
if %errorlevel% neq 0 (
    echo ❌ Java non trouvé. Installe le JDK 17 avant de continuer.
    pause
    exit /b
)
for /f "tokens=2 delims==" %%j in ('java -XshowSettings:properties -version 2^>^&1 ^| findstr "java.home"') do set JAVA_HOME=%%j
setx JAVA_HOME "%JAVA_HOME%"
echo ✅ JAVA_HOME défini sur : %JAVA_HOME%
echo.

:: Étape 2 – Création de winutils si absent
set HADOOP_HOME=%cd%\winutils
if not exist "%HADOOP_HOME%\bin" mkdir "%HADOOP_HOME%\bin"
if not exist "%HADOOP_HOME%\bin\winutils.exe" (
    echo 📥 Téléchargement de winutils.exe...
    powershell -Command "Invoke-WebRequest -Uri https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.0/bin/winutils.exe -OutFile '%HADOOP_HOME%\bin\winutils.exe'"
)
setx HADOOP_HOME "%HADOOP_HOME%"
setx PATH "%PATH%;%JAVA_HOME%\bin;%HADOOP_HOME%\bin"
echo ✅ HADOOP_HOME défini sur : %HADOOP_HOME%
echo.

:: Étape 3 – Activation du venv
if not exist "venv" (
    echo 🐍 Création de l'environnement virtuel Python...
    python -m venv venv
)
call venv\Scripts\activate
echo ✅ Environnement Python activé
echo.

:: Étape 4 – Installation des dépendances
echo 📦 Installation des dépendances...
pip install --upgrade pip wheel setuptools >nul
pip install pyspark==3.5.3 pandas==2.2.3 numpy==1.26.4 openpyxl==3.1.2 delta-spark==3.2.0 python-dateutil==2.9.0 pytest==8.3.3 pytest-cov==5.0.0
echo ✅ Installation terminée
echo.

:: Étape 5 – Vérifications
echo 🧠 Vérification des variables d'environnement :
echo JAVA_HOME  = %JAVA_HOME%
echo HADOOP_HOME= %HADOOP_HOME%
echo PATH       = %PATH%
echo.

:: Étape 6 – Lancement du pipeline
echo 🚀 Lancement du pipeline Spark WAX...
python -m src.main
echo.

echo ==============================================
echo ✅ Configuration et exécution terminées !
echo ==============================================
pause
