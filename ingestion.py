# src/ingestion.py
# --------------------------------------------------------------------------------------
# Gestion des différents modes d’ingestion du pipeline WAX :
#  - FULL : réécriture complète de la table
#  - DELTA_FROM_FLOW : append incrémental
#  - FULL_KEY_REPLACE : remplacement par clé (merge/upsert)
# Compatible Databricks (Delta) et exécution locale (Parquet fallback)
# --------------------------------------------------------------------------------------

from pyspark.sql import SparkSession, DataFrame
from delta_manager import save_delta_table, merge_delta_table
from environment import is_databricks
from pyspark.sql import functions as F


# ======================================================================================
# 1️⃣ - INGESTION MODE MANAGER
# ======================================================================================
def apply_ingestion_mode(
    spark: SparkSession,
    df: DataFrame,
    mode: str,
    output_path: str,
    merge_keys: list[str] = None,
    compare_col: str = "FILE_PROCESS_DATE"
):
    """
    Applique le mode d’ingestion choisi.
    """

    mode = (mode or "").strip().upper()
    merge_keys = merge_keys or []

    print(f"🚀 Application du mode d’ingestion : {mode}")

    if mode == "FULL":
        # Réécriture complète de la table
        print("🧹 Suppression de l’ancienne table et réécriture complète...")
        save_delta_table(spark, df, output_path, mode="overwrite", add_ts=True)
        print("✅ Mode FULL terminé.")

    elif mode in ["DELTA_FROM_FLOW", "DELTA_APPEND"]:
        # Ajout incrémental (append)
        print("➕ Ajout incrémental (append)...")
        save_delta_table(spark, df, output_path, mode="append", add_ts=True)
        print("✅ Mode DELTA_FROM_FLOW terminé.")

    elif mode == "FULL_KEY_REPLACE":
        # Remplacement par clé (merge/upsert)
        if not merge_keys:
            print("⚠️ Aucun merge key défini, fallback en append.")
            save_delta_table(spark, df, output_path, mode="append", add_ts=True)
        else:
            print(f"🔄 Merge Delta sur les clés : {merge_keys}")
            merge_delta_table(spark, df, output_path, merge_keys, compare_col=compare_col)
        print("✅ Mode FULL_KEY_REPLACE terminé.")

    else:
        print(f"⚠️ Mode inconnu '{mode}', fallback sur append.")
        save_delta_table(spark, df, output_path, mode="append", add_ts=True)
        print("✅ Mode par défaut terminé.")
