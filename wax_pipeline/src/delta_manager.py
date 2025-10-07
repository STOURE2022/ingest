"""Gestion Delta Lake"""
import os
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp


def is_databricks() -> bool:
    """Détecte Databricks"""
    return 'DATABRICKS_RUNTIME_VERSION' in os.environ


def save_delta(spark: SparkSession, df: DataFrame, path: str, params: dict,
               mode: str = "append", add_ts: bool = False, parts: dict = None,
               file_name_received: str = None):
    """Sauvegarde en Delta/Parquet"""
    today = datetime.today()
    y = (parts or {}).get("yyyy", today.year)
    m = (parts or {}).get("mm", today.month)
    d = (parts or {}).get("dd", today.day)
    
    if add_ts:
        df = df.withColumn("FILE_PROCESS_DATE", current_timestamp())
    
    if file_name_received:
        df = df.withColumn("FILE_NAME_RECEIVED", lit(os.path.basename(file_name_received)))
    
    df = df.withColumn("yyyy", lit(y).cast("int"))
    df = df.withColumn("mm", lit(m).cast("int"))
    df = df.withColumn("dd", lit(d).cast("int"))
    
    fmt = "delta" if is_databricks() else "parquet"
    df.write.format(fmt).mode(mode).partitionBy("yyyy", "mm", "dd").save(path)
    print(f"✅ Sauvegardé: {path} ({mode})")


def register_table_in_metastore(spark: SparkSession, table_name: str, path: str, 
                                database: str = "wax_obs", if_exists: str = "ignore"):
    """Enregistre dans le metastore (Databricks uniquement)"""
    if not is_databricks():
        return
    
    full_name = f"{database}.{table_name}"
    
    if if_exists == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_name} USING DELTA LOCATION '{path}'")
    print(f"✅ Table enregistrée: {full_name}")
