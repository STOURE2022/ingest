"""
config.py
----------
Gestion centralisée des paramètres du pipeline WAX :
- Lecture depuis Databricks widgets si disponibles
- Fallback sur des valeurs locales par défaut
"""

import os


def get_config(dbutils=None):
    """
    Récupère la configuration d'exécution :
    - si `dbutils` est fourni → lecture depuis les widgets Databricks
    - sinon → valeurs locales (mode test/CI/CD)

    Returns:
        dict: dictionnaire des paramètres du pipeline
    """
    print("⚙️ Chargement de la configuration...")

    # Lecture Databricks (prioritaire)
    if dbutils:
        print("📦 Mode Databricks détecté (widgets actifs).")
        params = {
            "env": dbutils.widgets.get("env") if dbutils.widgets.exists("env") else "dev",
            "version": dbutils.widgets.get("version") if dbutils.widgets.exists("version") else "v1",
            "zip_path": dbutils.widgets.get("zip_path"),
            "excel_path": dbutils.widgets.get("excel_path"),
            "extract_dir": dbutils.widgets.get("extract_dir"),
            "log_exec_path": dbutils.widgets.get("log_exec_path"),
            "log_quality_path": dbutils.widgets.get("log_quality_path"),
        }
        return params

    # Fallback local
    print("💻 Mode local détecté (aucun widget trouvé).")
    base_dir = os.getcwd()

    params = {
        "env": "dev",
        "version": "v1",
        "zip_path": os.path.join(base_dir, "data/wax_delta_from_historized.zip"),
        "excel_path": os.path.join(base_dir, "data/custom_test2_secret_conf.xlsx"),
        "extract_dir": os.path.join(base_dir, "tmp/unzipped_wax_csvs"),
        "log_exec_path": os.path.join(base_dir, "logs/wax_execution_logs_delta"),
        "log_quality_path": os.path.join(base_dir, "logs/wax_quality_logs_delta"),
    }

    print("✅ Configuration locale chargée avec succès.")
    return params
