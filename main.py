"""
main.py
--------
Point d’entrée du pipeline WAX modulaire.

Étapes :
1. Chargement de la configuration
2. Initialisation Spark
3. Extraction du ZIP et lecture Excel
4. Traitement des fichiers (boucle principale)
5. Génération des logs d’exécution et qualité
"""

import time
from pyspark.sql import SparkSession
from config import get_config
from file_processor import extract_zip_file, load_excel_config, process_files
from logging_manager import log_execution
from datetime import datetime


def init_spark(app_name="WAX_PIPELINE"):
    """
    Initialise la session Spark, compatible local / Databricks.
    """
    print("🚀 Initialisation SparkSession ...")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    print(f"✅ Spark initialisé : {spark.version}")
    return spark


def main():
    """
    Exécution complète du pipeline WAX.
    """
    start_pipeline = time.time()
    print("=" * 80)
    print("🧠 WAX DATA INGESTION PIPELINE - MODE MODULAIRE")
    print("=" * 80)

    # ==============================================================
    # 1️⃣ Chargement des paramètres
    # ==============================================================
    try:
        params = get_config()
        print("\n✅ Paramètres chargés :")
        for k, v in params.items():
            print(f" - {k}: {v}")
    except Exception as e:
        print(f"❌ Erreur chargement configuration : {e}")
        return

    # ==============================================================
    # 2️⃣ Initialisation Spark
    # ==============================================================
    spark = init_spark()

    # ==============================================================
    # 3️⃣ Extraction du ZIP
    # ==============================================================
    try:
        extract_dir_local = extract_zip_file(params["zip_path"], params["extract_dir"])
    except Exception as e:
        print(f"❌ Échec extraction ZIP : {e}")
        log_execution(
            spark,
            params,
            table_name="N/A",
            filename=params["zip_path"],
            ingestion_mode="N/A",
            row_count=0,
            column_count=0,
            masklist_applied=False,
            error_count=1,
            error_message=str(e),
            status="FAILED",
            start_time=start_pipeline
        )
        return

    # ==============================================================
    # 4️⃣ Lecture Excel (config tables & colonnes)
    # ==============================================================
    try:
        file_tables_df, file_columns_df = load_excel_config(params["excel_path"])
    except Exception as e:
        print(f"❌ Erreur lecture Excel : {e}")
        log_execution(
            spark,
            params,
            table_name="N/A",
            filename=params["excel_path"],
            ingestion_mode="N/A",
            row_count=0,
            column_count=0,
            masklist_applied=False,
            error_count=1,
            error_message=str(e),
            status="FAILED",
            start_time=start_pipeline
        )
        return

    # ==============================================================
    # 5️⃣ Traitement principal
    # ==============================================================
    try:
        process_files(spark, params, file_tables_df, file_columns_df)
    except Exception as e:
        print(f"❌ Échec pipeline principal : {e}")
        log_execution(
            spark,
            params,
            table_name="GLOBAL",
            filename="GLOBAL",
            ingestion_mode="N/A",
            row_count=0,
            column_count=0,
            masklist_applied=False,
            error_count=1,
            error_message=str(e),
            status="FAILED",
            start_time=start_pipeline
        )
        return

    # ==============================================================
    # 6️⃣ Fin du pipeline + log global
    # ==============================================================
    duration = round(time.time() - start_pipeline, 2)
    print("\n" + "=" * 80)
    print(f"✅ WAX PIPELINE TERMINÉ AVEC SUCCÈS EN {duration} sec.")
    print("=" * 80)

    log_execution(
        spark,
        params,
        table_name="GLOBAL",
        filename="PIPELINE",
        ingestion_mode="FULL_RUN",
        row_count=0,
        column_count=0,
        masklist_applied=False,
        error_count=0,
        error_message="Pipeline terminé avec succès",
        status="SUCCESS",
        start_time=start_pipeline
    )


if __name__ == "__main__":
    main()
