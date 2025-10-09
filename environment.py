# src/environment.py
# --------------------------------------------------------------------------------------
# Détection d'environnement (Databricks vs Local) et gestion centralisée des chemins.
# Compatible Unity Catalog (DBX) et exécution locale (VSCode).
# --------------------------------------------------------------------------------------

import os
from typing import Dict


# =========================
# 1) Détection d'environnement
# =========================
def is_databricks() -> bool:
    """
    Retourne True si le code tourne dans un cluster Databricks.
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


# =========================
# 2) Chemins de base (data & logs)
# =========================
def get_data_base_path() -> str:
    """
    Répertoire de travail pour les données brutes (ZIP/Excel/extraction).
    - Databricks : 'dbfs:/FileStore/tables'
    - Local      : './data' (absolu)
    """
    if is_databricks():
        return "dbfs:/FileStore/tables"
    return os.path.abspath("./data")


def get_log_path(log_type: str) -> str:
    """
    Répertoire des logs (exec | quality).
    - Databricks : '/mnt/logs/wax_<type>_delta'
    - Local      : './logs/<type>'
    """
    log_type = log_type.strip().lower()
    if is_databricks():
        return f"/mnt/logs/wax_{log_type}_delta"
    return os.path.abspath(f"./logs/{log_type}")


def get_wax_storage_base(env: str = "dev", zone: str = "internal", version: str = "v1") -> str:
    """
    Base de stockage Delta des tables WAX.
    - Databricks : '/mnt/wax/{env}/{zone}/{version}'
    - Local      : './data/wax/{env}/{zone}/{version}'
    """
    if is_databricks():
        return f"/mnt/wax/{env}/{zone}/{version}"
    return os.path.abspath(f"./data/wax/{env}/{zone}/{version}")


# =========================
# 3) Aides chemin DBFS/Local
# =========================
def to_local_path(maybe_dbfs_path: str) -> str:
    """
    Convertit 'dbfs:/...' en chemin local '/dbfs/...' lorsqu'on est DANS Databricks driver.
    Ne fait rien en mode local.
    Utile pour openpyxl/zipfile qui exigent un chemin local.
    """
    if maybe_dbfs_path is None:
        return None
    if maybe_dbfs_path.startswith("dbfs:"):
        # En Databricks, /dbfs/ mirror existe côté driver
        return maybe_dbfs_path.replace("dbfs:", "/dbfs", 1)
    return maybe_dbfs_path


def ensure_dir(path: str) -> None:
    """
    Crée le dossier s'il n'existe pas (en local uniquement).
    Sur Databricks, préférer dbutils.fs.mkdirs pour les chemins DBFS/MNT (géré ailleurs).
    """
    if is_databricks():
        return
    os.makedirs(path, exist_ok=True)


# =========================
# 4) Catalog/Schema par défaut (Unity Catalog)
# =========================
def get_default_catalog_schema() -> Dict[str, str]:
    """
    Retourne le couple (catalog, schema) par défaut pour l'enregistrement des tables.
    - Databricks (Unity Catalog) : ('main', 'wax_obs') par défaut
    - Local (Hive metastore embarqué) : (None, 'wax_obs') → on n'utilise que le schema
    """
    if is_databricks():
        return {"catalog": "main", "schema": "wax_obs"}
    return {"catalog": None, "schema": "wax_obs"}


def fq_table_name(table: str, catalog: str | None, schema: str) -> str:
    """
    Construit le nom pleinement qualifié d'une table.
    - Avec UC : 'catalog.schema.table'
    - Sans UC : 'schema.table'
    """
    if catalog:
        return f"{catalog}.{schema}.{table}"
    return f"{schema}.{table}"


# =========================
# 5) Résolution centralisée des chemins à partir de PARAMS
# =========================
def resolve_paths(params: Dict[str, str]) -> Dict[str, str]:
    """
    Normalise les chemins d'entrée/sortie à partir de PARAMS.
    - Garantit que zip/excel/extract pointent vers le bon espace (dbfs:/... ou ./data/...)
    - Normalise les chemins de logs
    """
    env = params.get("env", "dev")
    version = params.get("version", "v1")
    zone = params.get("zone", params.get("output_zone", "internal"))

    # Data base
    data_base = get_data_base_path()

    # ZIP
    zip_path = params.get("zip_path")
    if not zip_path:
        # défaut : data_base/wax_delta_from_historized.zip
        zip_path = os.path.join(data_base, "wax_delta_from_historized.zip")

    # Excel
    excel_path = params.get("excel_path")
    if not excel_path:
        excel_path = os.path.join(data_base, "custom_test2_secret_conf.xlsx")

    # Dossier d'extraction
    extract_dir = params.get("extract_dir")
    if not extract_dir:
        extract_dir = os.path.join(data_base, "unzipped_wax_csvs")

    # Logs
    log_exec_path = params.get("log_exec_path") or get_log_path("exec")
    log_quality_path = params.get("log_quality_path") or get_log_path("quality")

    # Base Delta
    wax_base = get_wax_storage_base(env=env, zone=zone, version=version)

    return {
        **params,
        "zip_path": zip_path,
        "excel_path": excel_path,
        "extract_dir": extract_dir,
        "log_exec_path": log_exec_path,
        "log_quality_path": log_quality_path,
        "wax_base": wax_base,
    }
