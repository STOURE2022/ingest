# src/main.py
# --------------------------------------------------------------------------------------
# Point d’entrée du pipeline WAX.
# Détection d’environnement (Databricks / Local)
# Chargement des paramètres
# Exécution du pipeline complet.
# --------------------------------------------------------------------------------------

import sys
from pyspark.sql import SparkSession
from environment import is_databricks
from config import get_databricks_config, get_local_config
from file_processor import process_files


# ======================================================================================
# 1️⃣ - INITIALISATION DE SPARK
# ======================================================================================
def create_spark_session(app_name: str = "WAX_Pipeline"):
    """
    Crée la session Spark adaptée à l’environnement :
    - Databricks : session existante
    - Local : session autonome avec Delta support
    """
    if is_databricks():
        print("💻 Exécution détectée sur Databricks")
        return SparkSession.getActiveSession()
    else:
        print("🏠 Exécution locale détectée (VSCode ou Terminal)")
        return (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
        )


# ======================================================================================
# 2️⃣ - MAIN EXECUTION
# ======================================================================================
def main():
    print("\n🚀 Lancement du pipeline WAX – Main Entry Point\n" + "=" * 80)

    spark = create_spark_session()

    # Chargement des paramètres selon l’environnement
    if is_databricks():
        try:
            import dbutils
        except ImportError:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)

        params = get_databricks_config(dbutils)
    else:
        params = get_local_config()

    # Vérification rapide
    print("\n✅ Paramètres chargés :")
    for k, v in params.items():
        print(f"  {k}: {v}")

    # Lancement du pipeline principal
    process_files(spark, params)

    print("\n🎉 Pipeline WAX terminé avec succès !")
    print("=" * 80)


# ======================================================================================
# 3️⃣ - ENTRY POINT
# ======================================================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Erreur inattendue dans le pipeline : {e}")
        sys.exit(1)
