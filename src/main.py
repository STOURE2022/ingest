#!/usr/bin/env python3
import sys
import time
from pyspark.sql import SparkSession
from config import get_config, print_config
from file_processor import process_files

def initialize_spark(app_name: str = "WAX_Pipeline") -> SparkSession:
    try:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            print("✓ Session Spark existante détectée.")
            return spark
        print("💻 Création d'une session Spark (local)...")
        spark = (SparkSession.builder
                 .appName(app_name)
                 .config("spark.sql.adaptive.enabled", "true")
                 .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                 .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                 .config("spark.sql.shuffle.partitions", "200")
                 .master("local[*]").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        sys.exit(1)

def main():
    print("=" * 80)
    print("🚀 WAX PIPELINE - DÉMARRAGE")
    print("=" * 80)
    start_time = time.time()

    spark = initialize_spark()
    try:
        params = get_config(dbutils)  # Databricks
    except NameError:
        params = get_config()         # Local
    print_config(params)

    print("\n📦 Vérification des dépendances (openpyxl)...")
    try:
        import openpyxl  # noqa
        print("✓ openpyxl présent")
    except Exception:
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "-q"])
        print("✓ openpyxl installé")

    mode = "databricks" if params.get("env") != "local" else "local"
    print(f"\n🔧 Mode d'exécution: {mode.upper()}")
    process_files(spark, params, mode=mode)

    duration = time.time() - start_time
    print("\n" + "=" * 80)
    print("✅ WAX PIPELINE - SUCCÈS")
    print("=" * 80)
    print(f"⏱️  Durée totale: {duration:.2f} sec")
    print("=" * 80)

if __name__ == "__main__":
    main()
