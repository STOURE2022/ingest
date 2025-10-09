# src/delta_manager.py
# --------------------------------------------------------------------------------------
# Gestion des écritures Delta/Parquet :
#  - Sauvegarde append / overwrite / merge
#  - Enregistrement dans Unity Catalog (Databricks) ou Hive local
#  - Détection automatique de l'environnement
# --------------------------------------------------------------------------------------

import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, functions as F
from delta.tables import DeltaTable

from environment import is_databricks, ensure_dir, get_default_catalog_schema, fq_table_name


# ======================================================================================
# 1️⃣ - CONSTRUCTION DES CHEMINS DE SORTIE
# ======================================================================================
def build_output_path(env: str, zone: str, table_name: str, version: str, wax_base: str = None) -> str:
    """
    Construit le chemin Delta pour une table donnée.
    """
    if wax_base:
        base = wax_base
    elif is_databricks():
        base = f"/mnt/wax/{env}/{zone}/{version}"
    else:
        base = os.path.abspath(f"./data/wax/{env}/{zone}/{version}")

    path = os.path.join(base, table_name)
    return path


# ======================================================================================
# 2️⃣ - SAUVEGARDE DELTA / PARQUET
# ======================================================================================
def save_delta_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    mode: str = "append",
    add_ts: bool = False,
    file_name_received: str = None,
    parts: dict | None = None
):
    """
    Sauvegarde un DataFrame en Delta (Databricks) ou Parquet (local).
    - `parts` : dictionnaire contenant éventuellement {'yyyy','mm','dd'}
    - `add_ts` : ajoute FILE_PROCESS_DATE et FILE_NAME_RECEIVED
    """

    today = datetime.today()
    y = int((parts or {}).get("yyyy", today.year))
    m = int((parts or {}).get("mm", today.month))
    d = int((parts or {}).get("dd", today.day))

    if add_ts:
        df = df.withColumn("FILE_PROCESS_DATE", F.current_timestamp())

    if file_name_received:
        base_name = os.path.splitext(os.path.basename(file_name_received))[0]
        df = df.withColumn("FILE_NAME_RECEIVED", F.lit(base_name))

    # Partitionnement temporel
    df = (
        df.withColumn("yyyy", F.lit(y).cast("int"))
          .withColumn("mm", F.lit(m).cast("int"))
          .withColumn("dd", F.lit(d).cast("int"))
    )

    if not is_databricks():
        ensure_dir(path)

    print(f"💾 Sauvegarde Delta → {path} | mode={mode}")

    if is_databricks():
        df.write.format("delta").mode(mode).option("mergeSchema", "true") \
            .partitionBy("yyyy", "mm", "dd").save(path)
    else:
        df.write.mode(mode).parquet(path)

    print(f"✅ Table sauvegardée : {path}")


# ======================================================================================
# 3️⃣ - ENREGISTREMENT METASTORE / UNITY CATALOG
# ======================================================================================
def register_table_in_metastore(
    spark: SparkSession,
    table_name: str,
    path: str,
    if_exists: str = "ignore"
):
    """
    Enregistre la table dans Unity Catalog (Databricks) ou dans Hive local.
    """
    ns = get_default_catalog_schema()
    catalog, schema = ns["catalog"], ns["schema"]
    full_name = fq_table_name(table_name, catalog, schema)

    # Vérification existence
    try:
        existing = [t.name for t in spark.catalog.listTables(schema)]
        if table_name in existing and if_exists == "ignore":
            print(f"⚠️ Table déjà existante : {full_name}")
            return
        if table_name in existing and if_exists == "overwrite":
            print(f"♻️ Suppression de la table existante : {full_name}")
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    except Exception:
        pass

    # Création table
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_name}
            USING DELTA
            LOCATION '{path}'
        """)
        print(f"✅ Table enregistrée dans le metastore : {full_name}")
    except Exception as e:
        print(f"⚠️ Erreur création table {full_name} : {e}")


# ======================================================================================
# 4️⃣ - MERGE DELTA TABLE
# ======================================================================================
def merge_delta_table(
    spark: SparkSession,
    df_updates: DataFrame,
    path: str,
    merge_keys: list[str],
    compare_col: str = "FILE_PROCESS_DATE"
):
    """
    Effectue un MERGE Delta (upsert) entre le DataFrame et la table existante.
    """
    if not is_databricks():
        print("⚠️ Merge Delta disponible uniquement sur Databricks (DeltaTable).")
        save_delta_table(spark, df_updates, path, mode="append")
        return

    if not DeltaTable.isDeltaTable(spark, path):
        print("⚠️ Table cible non Delta — création initiale.")
        save_delta_table(spark, df_updates, path, mode="overwrite")
        return

    target = DeltaTable.forPath(spark, path)

    # Colonnes de mise à jour
    update_cols = [c for c in df_updates.columns if c not in merge_keys]
    update_expr = {c: f"updates.{c}" for c in update_cols}

    cond = " AND ".join([f"target.{k}=updates.{k}" for k in merge_keys])

    (target.alias("target")
        .merge(df_updates.alias("updates"), cond)
        .whenMatchedUpdate(
            condition=f"updates.{compare_col} > target.{compare_col}",
            set=update_expr
        )
        .whenNotMatchedInsert(values=update_expr)
        .execute()
    )

    print(f"✅ MERGE terminé sur {path} (keys={merge_keys})")
