"""
Module config.py
----------------
Gestion de la configuration du pipeline :
- via widgets Databricks (si disponibles)
- via valeurs par défaut ou dict local
"""
import os


def get_databricks_config(dbutils):
    print("⚙️ Chargement config depuis widgets Databricks")
    params = {
        "zip_path": dbutils.widgets.get("zip_path"),
        "excel_path": dbutils.widgets.get("excel_path"),
        "extract_dir": dbutils.widgets.get("extract_dir"),
        "log_exec_path": dbutils.widgets.get("log_exec_path"),
        "log_quality_path": dbutils.widgets.get("log_quality_path"),
        "env": dbutils.widgets.get("env"),
        "version": dbutils.widgets.get("version"),
    }
    return params


def get_local_config():
    print("⚙️ Chargement config locale (fallback)")
    base = os.getcwd()

    # 📁 Construction des chemins
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
    dirs_to_create = [
        os.path.dirname(config["zip_path"]),
        config["extract_dir"],
        config["log_exec_path"],
        config["log_quality_path"],
    ]
    for d in dirs_to_create:
        if not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
            print(f"📁 Dossier créé : {d}")

    # ⚠️ Vérification des fichiers d’entrée
    if not os.path.exists(config["zip_path"]):
        print(f"⚠️ Fichier ZIP introuvable : {config['zip_path']}")
    if not os.path.exists(config["excel_path"]):
        print(f"⚠️ Fichier Excel introuvable : {config['excel_path']}")

    return config


def get_config(dbutils=None):
    if dbutils is not None:
        try:
            return get_databricks_config(dbutils)
        except Exception as e:
            print(f"⚠️ Erreur dbutils, fallback en local : {e}")
            return get_local_config()
    return get_local_config()


def print_config(config: dict):
    print("\n===== CONFIGURATION =====")
    for k, v in config.items():
        print(f"{k:20} : {v}")
    print("=========================\n")
