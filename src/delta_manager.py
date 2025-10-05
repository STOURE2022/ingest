"""
Module delta_manager.py
-----------------------
Sauvegarde Delta + enregistrement metastore.
"""
import os
from datetime import datetime
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable


def save_delta(df: DataFrame, path: str, mode: str = "append", add_ts: bool = False,
               parts: dict = None, file_name_received: str = None):
    today = datetime.today()
    y = int((parts or {}).get("yyyy", today.year))
    m = int((parts or {}).get("mm", today.month))
    d = int((parts or {}).get("dd", today.day))
    if add_ts:
        df = df.withColumn("FILE_PROCESS_DATE", F.current_timestamp())
    if file_name_received:
        base_name = os.path.splitext(os.path.basename(file_name_received))[0]
        df = df.withColumn("FILE_NAME_RECEIVED", F.lit(base_name))
    ordered = []
    if "FILE_NAME_RECEIVED" in df.columns: ordered.append("FILE_NAME_RECEIVED")
    if "FILE_PROCESS_DATE" in df.columns: ordered.append("FILE_PROCESS_DATE")
    df = df.select(ordered + [c for c in df.columns if c not in ordered])
    seen, cols = set(), []
    for c in df.columns:
        if c.lower() not in seen:
            cols.append(c);
            seen.add(c.lower())
    df = df.select(*cols)
    spark = df.sparkSession
    if DeltaTable.isDeltaTable(spark, path):
        schema = spark.read.format("delta").load(path).schema
        type_map = {f.name: f.dataType.simpleString() for f in schema.fields}
        yyyy_type, mm_type, dd_type = type_map.get("yyyy", "int"), type_map.get("mm", "int"), type_map.get("dd", "int")
    else:
        yyyy_type = mm_type = dd_type = "int"
    df = (df.withColumn("yyyy", F.lit(y).cast(yyyy_type))
          .withColumn("mm", F.lit(m).cast(mm_type))
          .withColumn("dd", F.lit(d).cast(dd_type)))
    (df.write.format("delta").option("mergeSchema", "true").mode(mode)
     .partitionBy("yyyy", "mm", "dd").save(path))
    print(f"✅ Delta sauvegardé dans {path} (mode={mode}, partitions={y}-{m:02d}-{d:02d})")


def register_table_in_metastore(spark, table_name: str, path: str, database: str = "wax_obs",
                                if_exists: str = "ignore"):
    full = f"{database}.{table_name}"
    exists = any(t.name == table_name for t in spark.catalog.listTables(database))
    if exists and if_exists == "ignore":
        print(f"⚠️ Table {full} existe déjà → ignorée");
        return
    elif exists and if_exists == "errorexists":
        raise Exception(f"❌ Table {full} existe déjà (stop)")
    elif exists and if_exists == "overwrite":
        print(f"♻️ Table {full} existe déjà → DROP avant CREATE");
        spark.sql(f"DROP TABLE IF EXISTS {full}")
    elif exists and if_exists == "append":
        print(f"➕ Table {full} existe déjà → append autorisé");
        return
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full} USING DELTA LOCATION '{path}'")
    print(f"✅ Table {full} enregistrée sur {path}")
