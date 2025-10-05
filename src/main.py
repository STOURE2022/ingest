"""
Module main.py
--------------
Point d’entrée du pipeline WAX.
"""

import os
import sys
import traceback
from pyspark.sql import SparkSession
from .config import get_config, print_config
from .file_processor import extract_zip_file, load_excel_config, process_files

import os



# ============================
# ✅ Configuration Windows - Hadoop
# ============================
if os.name == "nt":  # nt = Windows
    hadoop_path = "C:\\hadoop"
    os.environ["HADOOP_HOME"] = hadoop_path
    os.environ["hadoop.home.dir"] = hadoop_path
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_path, "bin")

    print(f"✅ HADOOP_HOME défini sur : {os.environ['HADOOP_HOME']}")
    print(f"✅ PATH mis à jour avec : {os.path.join(hadoop_path, 'bin')}")


def main(dbutils=None, spark=None):
    print("\n🚀 Lancement du pipeline WAX...\n")

    # ✅ Sécurité : SparkSession obligatoire
    if spark is None:
        print("⚠️ Avertissement : SparkSession n'était pas initialisée, création forcée...")
        spark = SparkSession.builder.appName("WAXPipeline_Fallback").getOrCreate()

    # 🔧 Chargement config (Databricks ou locale)
    config = get_config(dbutils)
    print_config(config)

    try:
        # 📦 Extraction ZIP
        extract_zip_file(config["zip_path"], config["extract_dir"])

        # 📑 Lecture Excel (File-Table, Field-Column)
        file_tables_df, file_columns_df = load_excel_config(config["excel_path"])

    except Exception as e:
        print(f"❌ Erreur préparation fichiers : {e}")
        traceback.print_exc()
        sys.exit(1)

    try:
        # ⚙️ Lancement du traitement principal
        process_files(
            spark=spark,
            file_tables_df=file_tables_df,
            file_columns_df=file_columns_df,
            params=config
        )

    except Exception as e:
        print(f"❌ Erreur processing tables : {e}")
        traceback.print_exc()
        sys.exit(1)

    print("\n✅ Pipeline WAX terminé avec succès ✅\n")


if __name__ == "__main__":
    try:
        # =========================================================
        # ⚙️  ENVIRONNEMENT LOCAL / DATARICKS COMPATIBLE
        # =========================================================
        # Force Spark à utiliser ton installation Python Windows
        os.environ["PYSPARK_PYTHON"] = "python"
        os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


        # =========================================================
        # 🔥 INITIALISATION SPARK AVEC DELTA LAKE
        # =========================================================
        spark = (
            SparkSession.builder
            .appName("WAXPipeline")
            # Ajout du support Delta Lake
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Optimisations utiles
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
        )

        print(f"✨ SparkSession active — version {spark.version}")

    except Exception as e:
        print(f"⚠️ Impossible d'initialiser Spark correctement : {e}")
        traceback.print_exc()
        spark = None

    # 🚀 Lancement du pipeline
    main(spark=spark)
