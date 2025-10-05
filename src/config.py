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
