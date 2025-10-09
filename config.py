# src/config.py
# --------------------------------------------------------------------------------------
# Chargement de la configuration (Databricks)
# via les widgets dbutils.widgets.
# Compatible avec le schéma utilisé dans ton notebook WAX.
# --------------------------------------------------------------------------------------

from typing import Dict


def load_config_from_widgets(dbutils) -> Dict[str, str]:
    """
    Charge la configuration depuis les widgets Databricks.
    Chaque widget doit être créé dans le notebook avant l'appel :
      dbutils.widgets.text("zip_path", "dbfs:/FileStore/tables/wax_delta_from_historized.zip")
      dbutils.widgets.text("excel_path", "dbfs:/FileStore/tables/custom_test2_secret_conf.xlsx")
      ...
    """
    print("⚙️ Chargement de la configuration depuis les widgets Databricks...")

    keys = [
        "zip_path",
        "excel_path",
        "extract_dir",
        "log_exec_path",
        "log_quality_path",
        "env",
        "version"
    ]

    params = {}
    for key in keys:
        try:
            params[key] = dbutils.widgets.get(key)
        except Exception:
            params[key] = None

    print("✅ Paramètres chargés :")
    for k, v in params.items():
        print(f"   • {k}: {v}")

    # Valeurs par défaut (fallbacks)
    params.setdefault("env", "dev")
    params.setdefault("version", "v1")

    return params


def create_default_widgets(dbutils) -> None:
    """
    Crée les widgets Databricks s’ils n’existent pas.
    À appeler au début du notebook si besoin.
    """
    defaults = {
        "zip_path": "dbfs:/FileStore/tables/wax_delta_from_historized.zip",
        "excel_path": "dbfs:/FileStore/tables/custom_test2_secret_conf.xlsx",
        "extract_dir": "dbfs:/tmp/unzipped_wax_csvs",
        "log_exec_path": "/mnt/logs/wax_execution_logs_delta",
        "log_quality_path": "/mnt/logs/wax_data_quality_errors_delta",
        "env": "dev",
        "version": "v1"
    }

    for key, default in defaults.items():
        try:
            dbutils.widgets.get(key)
        except Exception:
            dbutils.widgets.text(key, default, key.capitalize().replace("_", " "))

    print("✅ Widgets créés ou déjà existants.")


def get_param(dbutils, key: str, default=None):
    """
    Récupère un paramètre depuis les widgets avec valeur par défaut.
    """
    try:
        return dbutils.widgets.get(key)
    except Exception:
        return default
