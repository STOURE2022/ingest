from datetime import datetime
import os
from typing import Dict, Optional, List

from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable

def build_output_path(env: str, zone: str, table_name: str, version: str) -> str:
    return f"/mnt/wax/{env}/{zone}/{version}/{table_name}"

def save_delta(df: DataFrame, path: str, *, mode: str = "append",
               add_ts: bool = False, parts: Optional[Dict] = None,
               file_name_received: Optional[str] = None) -> None:
    today = datetime.today()
    y = int((parts or {}).get("yyyy", today.year))
    m = int((parts or {}).get("mm", today.month))
    d = int((parts or {}).get("dd", today.day))

    if add_ts:
        df = df.withColumn("FILE_PROCESS_DATE", F.current_timestamp())

    if file_name_received:
        base = os.path.splitext(os.path.basename(file_name_received))[0]
        df = df.withColumn("FILE_NAME_RECEIVED", F.lit(base))

    ordered = []
    if "FILE_NAME_RECEIVED" in df.columns: ordered.append("FILE_NAME_RECEIVED")
    if "FILE_PROCESS_DATE" in df.columns: ordered.append("FILE_PROCESS_DATE")
    other = [c for c in df.columns if c not in ordered]
    df = df.select(ordered + other)

    # Remove duplicate columns (case-insensitive)
    seen, cols = set(), []
    for c in df.columns:
        cl = c.lower()
        if cl not in seen:
            cols.append(c)
            seen.add(cl)
    df = df.select(*cols)

    # Partition columns
    df = (df.withColumn("yyyy", F.lit(y).cast("int"))
             .withColumn("mm", F.lit(m).cast("int"))
             .withColumn("dd", F.lit(d).cast("int")))

    (df.write.format("delta").option("mergeSchema","true").mode(mode)
       .partitionBy("yyyy","mm","dd").save(path))

def register_table_in_metastore(spark, table_name: str, path: str,
                                *, database: str = "wax_obs", if_exists: str = "ignore") -> None:
    full = f"{database}.{table_name}"
    exists = any(t.name == table_name for t in spark.catalog.listTables(database))
    if exists and if_exists == "ignore": return
    if exists and if_exists == "errorexists":
        raise RuntimeError(f"Table {full} already exists")
    if exists and if_exists == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {full}")
    elif exists and if_exists == "append":
        return
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full} USING DELTA LOCATION '{path}'")

def apply_ingestion_mode(spark, df_raw: DataFrame, column_defs,
                         *, table_name: str, ingestion_mode: str,
                         env: str, zone: str, version: str, parts=None,
                         file_name_received=None):
    path_all  = build_output_path(env, zone, f"{table_name}_all", version)
    path_last = build_output_path(env, zone, f"{table_name}_last", version)

    specials = column_defs.copy()
    specials["Is Special lower"] = specials["Is Special"].astype(str).str.lower() if "Is Special" in specials.columns else ""
    merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist() if "Column Name" in specials.columns else []
    start_cols = specials[specials["Is Special lower"] == "isstartvalidity"]["Column Name"].tolist() if "Column Name" in specials.columns else []
    update_col = start_cols[0] if start_cols else None

    imode = (ingestion_mode or "").strip().upper()

    # Always append to _all with timestamp
    save_delta(df_raw, path_all, mode="append", add_ts=True, parts=parts, file_name_received=file_name_received)
    register_table_in_metastore(spark, f"{table_name}_all", path_all, if_exists="ignore")


    if imode == "FULL_SNAPSHOT":
        save_delta(df_raw, path_last, mode="overwrite", parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")
        return

    if imode in {"DELTA_FROM_FLOW", "DELTA_FROM_HISTORIZED"}:
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
        return

    if imode == "FULL_KEY_REPLACE":
        if not merge_keys:
            raise RuntimeError("FULL_KEY_REPLACE requires merge keys.")
        if DeltaTable.isDeltaTable(spark, path_last):
            target = DeltaTable.forPath(spark, path_last)
            # delete on keys present in incoming data
            cond = " OR ".join([f"target.{k} IN (SELECT DISTINCT {k} FROM updates)" for k in merge_keys])
            # Delta's Python API doesn't support subquery strings in delete; use join approach:
            updates_keys = df_raw.select(*merge_keys).dropDuplicates()
            target.alias("t").merge(
                updates_keys.alias("u"),
                " AND ".join([f"t.{k} = u.{k}" for k in merge_keys])
            ).whenMatchedDelete().execute()
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
        return

    if imode == "DELTA_FROM_NON_HISTORIZED":
        if not merge_keys:
            # fall back to overwrite if no keys
            save_delta(df_raw, path_last, mode="overwrite", add_ts=True, parts=parts, file_name_received=file_name_received)
            register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")
            return
        compare_col = update_col or "FILE_PROCESS_DATE"
        updates = (df_raw
                   .withColumn("FILE_PROCESS_DATE", F.current_timestamp())
                   .withColumn("yyyy", F.lit((parts or {}).get("yyyy")).cast("int"))
                   .withColumn("mm", F.lit((parts or {}).get("mm")).cast("int"))
                   .withColumn("dd", F.lit((parts or {}).get("dd")).cast("int")))

        if DeltaTable.isDeltaTable(spark, path_last):
            target = DeltaTable.forPath(spark, path_last)
            target_cols = [f.name for f in target.toDF().schema.fields]
        else:
            target_cols = updates.columns

        auto_cols = {"yyyy","mm","dd"}
        update_cols = [c for c in updates.columns if c in target_cols and c not in merge_keys and c not in auto_cols]
        insert_cols = [c for c in updates.columns if c in target_cols]

        cond = " AND ".join([f"target.{k}=updates.{k}" for k in merge_keys])

        if DeltaTable.isDeltaTable(spark, path_last):
            (target.alias("target").merge(updates.alias("updates"), cond)
             .whenMatchedUpdate(
                 condition=f"updates.{compare_col} > target.{compare_col}",
                 set={c: f"updates.{c}" for c in update_cols}
             ).whenNotMatchedInsert(
                 values={c: f"updates.{c}" for c in insert_cols}
             ).execute())
            register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
        else:
            save_delta(updates, path_last, mode="overwrite", add_ts=False, parts=parts, file_name_received=file_name_received)
            register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")
        return

    # Default: append
    save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=file_name_received)
    register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
