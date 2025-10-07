import os
from typing import Dict, Any

def _make_widgets(dbutils):
    try:
        dbutils.widgets.text("zip_path", "dbfs:/FileStore/tables/wax_delta_from_historized.zip", "📦 ZIP Source")
        dbutils.widgets.text("excel_path", "dbfs:/FileStore/tables/custom_test2_secret_conf.xlsx", "📑 Excel Config")
        dbutils.widgets.text("extract_dir", "dbfs:/tmp/unzipped_wax_csvs", "📂 Dossier Extraction ZIP")
        dbutils.widgets.text("log_exec_path", "/mnt/logs/wax_execution_logs_delta", "📝 Logs Exécution (Delta)")
        dbutils.widgets.text("log_quality_path", "/mnt/logs/wax_data_quality_errors_delta", "🚦 Log Qualité (Delta)")
        dbutils.widgets.text("env", "dev", "🌍 Environnement")
        dbutils.widgets.text("version", "v1", "🔖 Version Pipeline")
    except Exception:
        pass

def get_databricks_config(dbutils) -> Dict[str, Any]:
    _make_widgets(dbutils)
    params = {k: dbutils.widgets.get(k) for k in [
        "zip_path", "excel_path", "extract_dir",
        "log_exec_path", "log_quality_path",
        "env", "version"
    ]}
    # chemin local utile si besoin (badRecords path en local)
    params["log_error_path"] = "/mnt/logs/wax_specific_errors_delta"
    return params

def get_local_config() -> Dict[str, Any]:
    base = os.path.dirname(os.path.abspath(__file__))
    root = os.path.abspath(os.path.join(base, ".."))
    params = {
        "zip_path": os.path.join(root, "data/input/wax_delta_from_historized.zip"),
        "excel_path": os.path.join(root, "data/input/custom_test2_secret_conf.xlsx"),
        "extract_dir": os.path.join(root, "data/temp/unzipped_wax_csvs"),
        "log_exec_path": os.path.join(root, "data/output/logs_execution_delta"),
        "log_quality_path": os.path.join(root, "data/output/logs_quality_delta"),
        "log_error_path": os.path.join(root, "data/output/logs_specific_delta"),
        "env": "local",
        "version": "v1",
    }
    for k in ["extract_dir", "log_exec_path", "log_quality_path", "log_error_path"]:
        os.makedirs(params[k], exist_ok=True)
    return params

def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_config(dbutils=None) -> Dict[str, Any]:
    if dbutils is not None or is_databricks():
        try:
            if dbutils is None:
                from pyspark.sql import SparkSession
                from pyspark.dbutils import DBUtils
                spark = SparkSession.builder.getOrCreate()
                dbutils = DBUtils(spark)
            return get_databricks_config(dbutils)
        except Exception:
            pass
    return get_local_config()

def print_config(params: Dict[str, Any]) -> None:
    print("✅ Paramètres chargés :")
    for k, v in params.items():
        print(f" - {k}: {v}")




"""
Module config.py
----------------
Gestion de la configuration du pipeline :
- via widgets Databricks (si disponibles)
- via valeurs par défaut ou dict local
- détection automatique de l’environnement (local vs Databricks)
"""
import os


# =========================================================
# 🧠 Détection automatique d’environnement
# =========================================================
def is_databricks_env():
    """Détecte si on est sur Databricks via une variable d’environnement."""
    return os.environ.get("DATABRICKS_RUNTIME_VERSION") is not None


# =========================================================
# ⚙️ Configuration Databricks (widgets)
# =========================================================
def get_databricks_config(dbutils):
    print("⚙️ Chargement config depuis widgets Databricks")
    return {
        "zip_path": dbutils.widgets.get("zip_path"),
        "excel_path": dbutils.widgets.get("excel_path"),
        "extract_dir": dbutils.widgets.get("extract_dir"),
        "log_exec_path": dbutils.widgets.get("log_exec_path"),
        "log_quality_path": dbutils.widgets.get("log_quality_path"),
        "env": dbutils.widgets.get("env"),
        "version": dbutils.widgets.get("version"),
    }


# =========================================================
# 💻 Configuration locale (VSCode / PyCharm)
# =========================================================
def get_local_config():
    print("⚙️ Chargement config locale (fallback)")
    base = os.getcwd()

    config = {
        "zip_path": os.path.join(base, "data", "input", "site_20251201_120001.zip"),
        "excel_path": os.path.join(base, "data", "input", "waxsite_config.xlsx"),
        "extract_dir": os.path.join(base, "data", "tmp", "unzipped"),
        "log_exec_path": os.path.join(base, "data", "logs", "execution"),
        "log_quality_path": os.path.join(base, "data", "logs", "quality"),
        "env": "dev",
        "version": "v1",
    }

    # 🧩 Création automatique des dossiers si manquants
    for d in [
        os.path.dirname(config["zip_path"]),
        config["extract_dir"],
        config["log_exec_path"],
        config["log_quality_path"],
    ]:
        if not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
            print(f"📁 Dossier créé : {d}")

    # ⚠️ Vérification des fichiers d’entrée
    if not os.path.exists(config["zip_path"]):
        print(f"⚠️ Fichier ZIP introuvable : {config['zip_path']}")
    if not os.path.exists(config["excel_path"]):
        print(f"⚠️ Fichier Excel introuvable : {config['excel_path']}")

    return config


# =========================================================
# 🧩 Sélection automatique selon l’environnement
# =========================================================
def get_config(dbutils=None):
    """Retourne la bonne configuration selon l’environnement."""
    try:
        if is_databricks_env() and dbutils is not None:
            return get_databricks_config(dbutils)
        else:
            return get_local_config()
    except Exception as e:
        print(f"⚠️ Erreur de chargement config : {e}")
        return get_local_config()


# =========================================================
# 🖨️ Affichage formaté
# =========================================================
def print_config(config: dict):
    print("\n===== CONFIGURATION =====")
    for k, v in config.items():
        print(f"{k:20} : {v}")
    print("=========================\n")
