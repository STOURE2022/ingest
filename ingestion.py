"""
ingestion.py
-------------
Implémentation des modes d’ingestion WAX selon le notebook Databricks.

Modes supportés :
- FULL_SNAPSHOT
- DELTA_FROM_FLOW
- DELTA_FROM_NON_HISTORIZED
- DELTA_FROM_HISTORIZED
- FULL_KEY_REPLACE
- Fallback (append)
"""

import os
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta_manager import save_delta_table, register_table_in_metastore


# ==============================================================
# 1. OUTILS GÉNÉRIQUES
# ==============================================================

def build_output_path(env: str, zone: str, table_name: str, version: str, parts: dict = None):
    """Construit le chemin Delta Lake standardisé pour la table."""
    return f"/mnt/wax/{env}/{zone}/{version}/{table_name}"


# ==============================================================
# 2. SAUVEGARDE GÉNÉRALISÉE
# ==============================================================

def save_delta(
    spark,
    df: DataFrame,
    path: str,
    mode: str = "append",
    add_ts: bool = False,
    parts: dict = None,
    file_name_received: str = None
):
    """Sauvegarde Delta avec colonnes yyyy/mm/dd et FILE_PROCESS_DATE."""

    today = datetime.today()
    y = int((parts or {}).get("yyyy", today.year))
    m = int((parts or {}).get("mm", today.month))
    d = int((parts or {}).get("dd", today.day))

    if add_ts:
        df = df.withColumn("FILE_PROCESS_DATE", F.current_timestamp())

    if file_name_received:
        base_name = os.path.splitext(os.path.basename(file_name_received))[0]
        df = df.withColumn("FILE_NAME_RECEIVED", F.lit(base_name))

    # Ajout colonnes de partition
    df = (
        df.withColumn("yyyy", F.lit(y).cast("int"))
          .withColumn("mm", F.lit(m).cast("int"))
          .withColumn("dd", F.lit(d).cast("int"))
    )

    (
        df.write.format("delta")
        .option("mergeSchema", "true")
        .mode(mode)
        .partitionBy("yyyy", "mm", "dd")
        .save(path)
    )

    print(f"✅ Delta saved → {path} (mode={mode}, partitions={y}-{m:02d}-{d:02d})")


# ==============================================================
# 3. FONCTION PRINCIPALE D’INGESTION
# ==============================================================

def apply_ingestion_mode(
    spark,
    df_raw: DataFrame,
    column_defs,
    table_name: str,
    ingestion_mode: str,
    env: str,
    zone: str,
    version: str,
    parts: dict,
    FILE_NAME_RECEIVED: str
):
    """
    Applique le mode d’ingestion selon le paramétrage Excel.
    Inspirée directement du notebook Databricks WAX.
    """

    path_all = build_output_path(env, zone, f"{table_name}_all", version, parts)
    path_last = build_output_path(env, zone, f"{table_name}_last", version, parts)

    # Extraction des colonnes "Is Special"
    specials = column_defs.copy()
    specials["Is Special lower"] = specials["Is Special"].astype(str).str.lower()

    merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist()
    update_cols = specials[specials["Is Special lower"] == "isstartvalidity"]["Column Name"].tolist()
    update_col = update_cols[0] if update_cols else None

    imode = (ingestion_mode or "").strip().upper()
    print(f"\n🧩 Ingestion mode : {imode} | Table : {table_name}")

    # ===================================================================
    # 1️⃣ Enregistrement complet (table _all)
    # ===================================================================
    save_delta(df_raw, path_all, mode="append", add_ts=True, parts=parts, file_name_received=FILE_NAME_RECEIVED)
    register_table_in_metastore(spark, f"{table_name}_all", path_all, if_exists="ignore")

    # ===================================================================
    # 2️⃣ FULL_SNAPSHOT → réécriture complète
    # ===================================================================
    if imode == "FULL_SNAPSHOT":
        save_delta(df_raw, path_last, mode="overwrite", parts=parts, file_name_received=FILE_NAME_RECEIVED)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")

    # ===================================================================
    # 3️⃣ DELTA_FROM_FLOW → append simple
    # ===================================================================
    elif imode == "DELTA_FROM_FLOW":
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=FILE_NAME_RECEIVED)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")

    # ===================================================================
    # 4️⃣ DELTA_FROM_NON_HISTORIZED → MERGE (avec clé)
    # ===================================================================
    elif imode == "DELTA_FROM_NON_HISTORIZED":
        if not merge_keys:
            print("⚠️ Aucun merge_key défini — fallback overwrite.")
            save_delta(df_raw, path_last, mode="overwrite", add_ts=True, parts=parts, file_name_received=FILE_NAME_RECEIVED)
            register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")
            return

        fallback_col = "FILE_PROCESS_DATE"
        compare_col = update_col if update_col else fallback_col
        print(f"🕒 Colonne de comparaison temporelle : {compare_col}")

        # Conversion type si nécessaire
        if compare_col in df_raw.columns:
            dtype = str(df_raw.schema[compare_col].dataType)
            if dtype == "StringType":
                df_raw = df_raw.withColumn(compare_col, F.to_timestamp(compare_col))

        # Ajouter colonnes automatiques
        df_raw = (
            df_raw
            .withColumn("FILE_PROCESS_DATE", F.current_timestamp())
            .withColumn("yyyy", F.lit(parts.get("yyyy", datetime.today().year)).cast("int"))
            .withColumn("mm", F.lit(parts.get("mm", datetime.today().month)).cast("int"))
            .withColumn("dd", F.lit(parts.get("dd", datetime.today().day)).cast("int"))
        )

        if DeltaTable.isDeltaTable(spark, path_last):
            target = DeltaTable.forPath(spark, path_last)
            cond = " AND ".join([f"target.{k}=updates.{k}" for k in merge_keys])

            update_expr = {c: f"updates.{c}" for c in df_raw.columns if c not in merge_keys}
            insert_expr = update_expr.copy()

            (
                target.alias("target")
                .merge(df_raw.alias("updates"), cond)
                .whenMatchedUpdate(condition=f"updates.{compare_col} > target.{compare_col}", set=update_expr)
                .whenNotMatchedInsert(values=insert_expr)
                .execute()
            )
            print(f"✅ MERGE effectué sur {merge_keys} avec comparaison {compare_col}")
        else:
            print("⚙️ Première écriture Delta (table inexistante)")
            save_delta(df_raw, path_last, mode="overwrite", add_ts=True, parts=parts, file_name_received=FILE_NAME_RECEIVED)

        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")

    # ===================================================================
    # 5️⃣ DELTA_FROM_HISTORIZED → append historisé
    # ===================================================================
    elif imode == "DELTA_FROM_HISTORIZED":
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=FILE_NAME_RECEIVED)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")

    # ===================================================================
    # 6️⃣ FULL_KEY_REPLACE → suppression + remplacement
    # ===================================================================
    elif imode == "FULL_KEY_REPLACE":
        if not merge_keys:
            raise Exception(f"❌ Mode FULL_KEY_REPLACE : merge_keys manquantes pour {table_name}.")

        if DeltaTable.isDeltaTable(spark, path_last):
            target = DeltaTable.forPath(spark, path_last)

            # Suppression des lignes existantes sur les merge_keys
            conds = []
            for k in merge_keys:
                vals = [str(x[k]) for x in df_raw.select(k).distinct().collect()]
                conds.append(f"{k} IN ({','.join([f'\"{v}\"' for v in vals])})")
            cond = " OR ".join(conds)

            target.delete(cond)
            print(f"♻️ Suppression existante sur clés {merge_keys}")

            # Réinsertion
            save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=FILE_NAME_RECEIVED)
        else:
            print(f"⚠️ Table Delta inexistante — création neuve")
            save_delta(df_raw, path_last, mode="overwrite", add_ts=True, parts=parts, file_name_received=FILE_NAME_RECEIVED)

        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")

    # ===================================================================
    # 7️⃣ MODE INCONNU → fallback append
    # ===================================================================
    else:
        print(f"❌ Mode inconnu {imode} — fallback append.")
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, file_name_received=FILE_NAME_RECEIVED)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")

    print(f"✅ Ingestion terminée pour {table_name} ({imode})")
