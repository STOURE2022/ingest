# src/config_local.py
# --------------------------------------------------------------------------------------
# Configuration locale (exécution sur VSCode)
# Permet de tester le pipeline WAX sans Databricks.
# Les chemins pointent vers le dossier ./data et ./logs du projet local.
# --------------------------------------------------------------------------------------

import os
from typing import Dict
from environment import get_data_base_path, get_log_path


def load_config_local() -> Dict[str, str]:
    """
    Charge la configuration locale par défaut pour les tests sur VSCode.
    Les chemins sont relatifs à ./data et ./logs.
    """
    base_dir = get_data_base_path()
    log_exec = get_log_path("exec")
    log_quality = get_log_path("quality")

    os.makedirs(base_dir, exist_ok=True)
    os.makedirs(log_exec, exist_ok=True)
    os.makedirs(log_quality, exist_ok=True)

    params = {
        "zip_path": os.path.join(base_dir, "wax_delta_from_historized.zip"),
        "excel_path": os.path.join(base_dir, "custom_test2_secret_conf.xlsx"),
        "extract_dir": os.path.join(base_dir, "unzipped_wax_csvs"),
        "log_exec_path": log_exec,
        "log_quality_path": log_quality,
        "env": "local",
        "version": "v1"
    }

    print("⚙️ Configuration locale chargée :")
    for k, v in params.items():
        print(f"   • {k}: {v}")

    return params
