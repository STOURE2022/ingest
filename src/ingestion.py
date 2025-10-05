"""
Module ingestion.py
-------------------
Gère les modes d’ingestion.
"""
import time
from pyspark.sql import DataFrame, functions as F
from delta.tables import DeltaTable
from .delta_manager import save_delta, register_table_in_metastore
from .utils import build_output_path

def apply_ingestion_mode(df_raw: DataFrame, column_defs, table_name: str, ingestion_mode: str,
                         env: str, zone: str, version: str, parts=None, file_name_received=None, spark=None):
    start_time = time.time()
    path_all = build_output_path(env, zone, f"{table_name}_all", version, parts)
    path_last = build_output_path(env, zone, f"{table_name}_last", version, parts)
    specials = column_defs.copy()
    if "Is Special" in specials.columns:
        specials["Is Special lower"] = specials["Is Special"].astype(str).str.lower()
        merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist()
        update_cols = specials[specials["Is Special lower"] == "isstartvalidity"]["Column Name"].tolist()
    else:
        merge_keys, update_cols = [], []
    update_col = update_cols[0] if update_cols else None
    imode = (ingestion_mode or "").strip().upper()
    print(f"⚙️ Ingestion mode = {imode} pour {table_name}")
    save_delta(df_raw, path_all, mode="append", add_ts=True, parts=parts, file_name_received=file_name_received)
    register_table_in_metastore(spark, f"{table_name}_all", path_all, if_exists="ignore")
    if imode == "FULL_SNAPSHOT":
        save_delta(df_raw, path_last, mode="overwrite", parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")
    elif imode == "DELTA_FROM_FLOW":
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
    elif imode == "DELTA_FROM_NON_HISTORIZED":
        if not merge_keys:
            raise Exception(f"Impossible d’appliquer DELTA_FROM_NON_HISTORIZED sans merge keys ({table_name})")
        compare_col = update_col if update_col else "FILE_PROCESS_DATE"
        df_upd = df_raw.withColumn("FILE_PROCESS_DATE", F.current_timestamp())
        if DeltaTable.isDeltaTable(spark, path_last):
            target = DeltaTable.forPath(spark, path_last)
            target_cols = [f.name for f in target.toDF().schema.fields]
        else:
            target_cols = df_upd.columns
        auto_cols = ["FILE_PROCESS_DATE", "yyyy", "mm", "dd"]
        update_cols_clean = [c for c in df_upd.columns if c in target_cols and c not in merge_keys and c not in auto_cols]
        insert_cols_clean = [c for c in df_upd.columns if c in target_cols and c not in auto_cols]
        update_expr = {c: f"updates.{c}" for c in update_cols_clean}
        insert_expr = {c: f"updates.{c}" for c in insert_cols_clean}
        cond = " AND ".join([f"target.{k} = updates.{k}" for k in merge_keys])
        if DeltaTable.isDeltaTable(spark, path_last):
            (target.alias("target").merge(df_upd.alias("updates"), cond)
                .whenMatchedUpdate(condition=f"updates.{compare_col} > target.{compare_col}", set=update_expr)
                .whenNotMatchedInsert(values=insert_expr).execute())
            print(f"✅ Merge {table_name} sur {compare_col} / keys {merge_keys}")
        else:
            print(f"⚠️ Pas de DeltaTable pour {table_name}_last → création initiale (overwrite)")
            save_delta(df_upd, path_last, mode="overwrite", add_ts=True, parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
    elif imode == "DELTA_FROM_HISTORIZED":
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
    elif imode == "FULL_KEY_REPLACE":
        if not merge_keys:
            raise Exception(f"Mode FULL KEY REPLACE pour {table_name} → merge_keys obligatoires.")
        if DeltaTable.isDeltaTable(spark, path_last):
            target = DeltaTable.forPath(spark, path_last)
            cond = " OR ".join([f"{k} IN (" + ",".join([str(x) for x in df_raw.select(k).distinct().rdd.flatMap(lambda x: x).collect()]) + ")" for k in merge_keys])
            print(f"🔄 Suppression lignes existantes sur clés {merge_keys}")
            target.delete(cond)
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
    else:
        print(f"❌ Mode inconnu : {imode} → append par défaut")
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=file_name_received)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
    print(f"⏱️ Ingestion {table_name} terminée en {round(time.time()-start_time,2)}s")
