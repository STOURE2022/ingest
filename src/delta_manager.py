import os
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp

def _is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def save_delta(spark: SparkSession, df: DataFrame, path: str, params: dict,
               mode: str = "append", add_ts: bool = False, parts: dict = None,
               file_name_received: str = None) -> None:
    today = datetime.today()
    y = int((parts or {}).get("yyyy", today.year))
    m = int((parts or {}).get("mm", today.month))
    d = int((parts or {}).get("dd", today.day))

    if add_ts:
        df = df.withColumn("FILE_PROCESS_DATE", current_timestamp())
    if file_name_received:
        import os as _os
        base = _os.path.splitext(_os.path.basename(file_name_received))[0]
        df = df.withColumn("FILE_NAME_RECEIVED", lit(base))

    # Drop duplicate columns (case-insensitive)
    seen, cols = set(), []
    for c in df.columns:
        cl = c.lower()
        if cl not in seen:
            cols.append(c); seen.add(cl)
    df = df.select(*cols)

    df = (df.withColumn("yyyy", lit(y).cast("int"))
            .withColumn("mm", lit(m).cast("int"))
            .withColumn("dd", lit(d).cast("int")))

    if _is_databricks():
        (df.write.format("delta").option("mergeSchema","true").mode(mode)
           .partitionBy("yyyy","mm","dd").save(path))
    else:
        import os as _os
        _os.makedirs(path, exist_ok=True)
        (df.write.format("parquet").mode(mode).partitionBy("yyyy","mm","dd").save(path))

def register_table_in_metastore(spark: SparkSession, table_name: str, path: str,
                                database: str = "wax_obs", if_exists: str = "ignore") -> None:
    if not _is_databricks():
        print("ℹ️ Metastore indisponible (mode local).")
        return
    full_name = f"{database}.{table_name}"
    exists = any(t.name == table_name for t in spark.catalog.listTables(database))
    if exists and if_exists == "ignore":
        return
    if exists and if_exists == "errorexists":
        raise RuntimeError(f"Table {full_name} existe déjà.")
    if exists and if_exists == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    elif exists and if_exists == "append":
        return
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_name} USING DELTA LOCATION '{path}'")
    print(f"✅ Table {full_name} enregistrée.")
