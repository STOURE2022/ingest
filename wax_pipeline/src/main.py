#!/usr/bin/env python3
"""
WAX Pipeline - Point d'entrée principal

Usage:
    python src/main.py
"""

import sys
import time
from pyspark.sql import SparkSession
from config import get_config, print_config
from file_processor import process_files


def initialize_spark(app_name: str = "WAX_Pipeline") -> SparkSession:
    """Initialise Spark"""
    try:
        spark = SparkSession.getActiveSession()
        if spark:
            print("✅ Session Spark existante (Databricks)")
            return spark
    except:
        pass
    
    print("💻 Création session Spark (local)...")
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark initialisé")
    return spark


def main():
    """Fonction principale"""
    print("\n" + "="*70)
    print("🚀 WAX PIPELINE - DÉMARRAGE")
    print("="*70)
    
    start = time.time()
    
    # Initialiser Spark
    spark = initialize_spark()
    
    # Charger config
    try:
        params = get_config(dbutils)
    except NameError:
        params = get_config()
    
    print_config(params)
    
    # Installer openpyxl
    try:
        import openpyxl
    except ImportError:
        import subprocess
        print("📦 Installation openpyxl...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl", "-q"])
    
    # Traiter les fichiers
    try:
        mode = "databricks" if params.get("env") != "local" else "local"
        process_files(spark, params, mode=mode)
        
        duration = time.time() - start
        print("\n" + "="*70)
        print("✅ SUCCÈS")
        print("="*70)
        print(f"⏱️  Durée: {duration:.2f}s")
        
    except Exception as e:
        print("\n" + "="*70)
        print("❌ ÉCHEC")
        print("="*70)
        print(f"Erreur: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
