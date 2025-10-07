"""
delta_manager.py
----------------
Gestion des écritures Delta/Parquet du pipeline WAX :
- Sauvegarde append / overwrite
- Merge Delta (upsert)
- Enregistrement Hive / Unity Catalog
Compatible Databricks et exécution locale.
"""

import os
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


# ==============================================================
# 1️⃣ Détection d'environnement
# ==============================================================

def is_databricks() -> bool:
    """Détermine si on exécute sur Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


# ==============================================================
# 2️⃣ Sauvegarde Delta ou Parquet
# ==============================================================

def save_delta_table(
    spark: SparkSession,
    df: DataFrame,
    params: dict,
    table_name: str,
    mode: str = "append",
    partition_cols: list = None,
):
    """
    Écrit un DataFrame dans Delta Lake ou Parquet (local).
    """
    base_path = params.get("base_output_path") or params.get("log_exec_path", "./data/output")
    output_path = os.path.join(base_path, table_name)

    if df is None or df.rdd.isEmpty():
        print(f"⚠️ Aucun enregistrement à sauvegarder pour {table_name}.")
        return

    print(f"💾 Sauvegarde [{table_name}] → {output_path} | mode={mode}")

    writer = df.write.mode(mode).option("mergeSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    if is_databricks():
        writer.format("delta").save(output_path)
        print(f"✅ Table Delta sauvegardée sur Databricks : {output_path}")
    else:
        writer.format("parquet").save(output_path)
        print(f"✅ Table Parquet sauvegardée localement : {output_path}")


# ==============================================================
# 3️⃣ Merge Delta
# ==============================================================

def merge_delta_table(spark, df_source, target_path, merge_keys):
    """
    Effectue un merge Delta (upsert) sur la table existante.
    """
    if not is_databricks():
        print("ℹ️ Merge ignoré (mode local sans Delta).")
        return

    if not DeltaTable.isDeltaTable(spark, target_path):
        print(f"⚙️ Table Delta inexistante → création initiale : {target_path}")
        df_source.write.format("delta").mode("overwrite").save(target_path)
        return

    delta_target = DeltaTable.forPath(spark, target_path)
    condition = " AND ".join([f"t.{k}=s.{k}" for k in merge_keys])

    (
        delta_target.alias("t")
        .merge(df_source.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ MERGE terminé sur {merge_keys} → {target_path}")


# ==============================================================
# 4️⃣ Enregistrement dans le métastore Hive / Unity Catalog
# ==============================================================

def register_table_in_metastore(spark, df, params, table_name, if_exists="ignore"):
    """
    Enregistre la table dans Hive ou Unity Catalog (Databricks uniquement).
    """
    if not is_databricks():
        print(f"ℹ️ Enregistrement Hive ignoré (mode local). Table: {table_name}")
        return

    env = params.get("env", "dev")
    catalog = params.get("catalog", f"{env}_wax_catalog")
    database = params.get("database", f"{env}_wax_db")
    full_name = f"{catalog}.{database}.{table_name}"

    print(f"🧩 Enregistrement Unity Catalog → {full_name}")

    try:
        if if_exists == "overwrite":
            spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        df.write.format("delta").mode("overwrite").saveAsTable(full_name)
        print(f"✅ Table {full_name} enregistrée avec succès dans Unity Catalog.")
    except Exception as e:
        print(f"⚠️ Erreur d’enregistrement Hive/Unity Catalog : {e}")
