"""Configuration Management"""
import os
from typing import Dict, Any


def get_local_config() -> Dict[str, Any]:
    """Configuration pour mode local"""
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    params = {
        "zip_path": os.path.join(base, "data/input/site.zip"),
        "excel_path": os.path.join(base, "data/input/config.xlsx"),
        "extract_dir": os.path.join(base, "data/temp/unzipped"),
        "log_exec_path": os.path.join(base, "data/output/logs_execution"),
        "log_quality_path": os.path.join(base, "data/output/logs_quality"),
        "env": "local",
        "version": "v1"
    }
    
    for key in ["extract_dir", "log_exec_path", "log_quality_path"]:
        os.makedirs(params[key], exist_ok=True)
    
    return params


def get_databricks_config(dbutils) -> Dict[str, Any]:
    """Configuration pour Databricks"""
    dbutils.widgets.text("zip_path", "dbfs:/FileStore/tables/site.zip", "📦 ZIP")
    dbutils.widgets.text("excel_path", "dbfs:/FileStore/tables/config.xlsx", "📑 Excel")
    dbutils.widgets.text("extract_dir", "dbfs:/tmp/unzipped", "📂 Extract")
    dbutils.widgets.text("log_exec_path", "/mnt/logs/execution", "📝 Logs Exec")
    dbutils.widgets.text("log_quality_path", "/mnt/logs/quality", "🚦 Logs Quality")
    dbutils.widgets.text("env", "dev", "🌍 Env")
    dbutils.widgets.text("version", "v1", "📖 Version")
    
    return {k: dbutils.widgets.get(k) for k in [
        "zip_path", "excel_path", "extract_dir", 
        "log_exec_path", "log_quality_path", "env", "version"
    ]}


def get_config(dbutils=None) -> Dict[str, Any]:
    """Auto-détection de l'environnement"""
    if dbutils is not None or 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        print("🌐 Mode DATABRICKS")
        return get_databricks_config(dbutils)
    else:
        print("💻 Mode LOCAL")
        return get_local_config()


def print_config(params: Dict[str, Any]):
    """Affiche la configuration"""
    print("\n" + "="*60)
    print("📥 CONFIGURATION")
    print("="*60)
    for k, v in params.items():
        print(f"  {k:20s}: {v}")
    print("="*60 + "\n")
