"""Modes d'ingestion"""
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from delta_manager import save_delta, register_table_in_metastore
from utils import build_output_path


def apply_ingestion_mode(spark: SparkSession, df_raw: DataFrame, column_defs: pd.DataFrame,
                         table_name: str, ingestion_mode: str, params: dict,
                         env: str = None, zone: str = "internal", version: str = None,
                         parts: dict = None, file_name_received: str = None):
    """Applique le mode d'ingestion"""
    env = env or params.get("env", "dev")
    version = version or params.get("version", "v1")
    
    path_all = build_output_path(env, zone, f"{table_name}_all", version, parts)
    path_last = build_output_path(env, zone, f"{table_name}_last", version, parts)
    
    imode = (ingestion_mode or "").strip().upper()
    print(f"Mode ingestion: {imode}")
    
    # Toujours sauvegarder dans _all
    save_delta(spark, df_raw, path_all, params, mode="append", add_ts=True, 
              parts=parts, file_name_received=file_name_received)
    register_table_in_metastore(spark, f"{table_name}_all", path_all, if_exists="ignore")
    
    # Sauvegarder dans _last selon le mode
    if imode == "FULL_SNAPSHOT":
        save_delta(spark, df_raw, path_last, params, mode="overwrite", 
                  parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")
    
    elif imode == "DELTA_FROM_LOT":
        save_delta(spark, df_raw, path_last, params, mode="append", add_ts=True,
                  parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
    
    else:
        # Mode par défaut: append
        save_delta(spark, df_raw, path_last, params, mode="append", add_ts=True,
                  parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
    
    print(f"✅ Ingestion complète: {table_name}")
