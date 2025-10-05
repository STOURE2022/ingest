# Databricks notebook source
# MAGIC %md
# MAGIC # WAX Pipeline - Point d'entrée principal
# MAGIC
# MAGIC Pipeline ETL pour ingestion de données CSV/Excel dans Delta Lake
# MAGIC
# MAGIC **Architecture**: Databricks Asset Bundle
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Paramètres du Notebook (Widgets)

# COMMAND ----------

# Création des widgets Databricks
dbutils.widgets.text("zip_path", "dbfs:/FileStore/tables/site_20251201_120001.zip", "📦 Source ZIP")
dbutils.widgets.text("excel_path", "dbfs:/FileStore/tables/waxsite_config.xlsx", "📑 Config Excel")
dbutils.widgets.text("extract_dir", "dbfs:/tmp/unzipped_wax_csvs", "📂 Extract Dir")
dbutils.widgets.text("log_exec_path", "/mnt/logs/wax_execution_logs_delta", "📝 Logs Exec")
dbutils.widgets.text("log_quality_path", "/mnt/logs/wax_data_quality_errors", "🚦 Logs Quality")
dbutils.widgets.text("env", "dev", "🌍 Environment")
dbutils.widgets.text("version", "v1", "🔖 Version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import du module WAX Pipeline

# COMMAND ----------

# Installation du package (si nécessaire)
# %pip install /dbfs/jfrog/wax_pipeline-1.0.0-py3-none-any.whl

# Import du module principal
from src.main import main
from src.config import get_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuration

# COMMAND ----------

# Récupération de la configuration depuis les widgets
params = get_config(dbutils)

print("📥 Configuration chargée:")
for k, v in params.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exécution du Pipeline

# COMMAND ----------

import time
start_time = time.time()

try:
    # Exécution du pipeline principal
    print("🚀 Démarrage du WAX Pipeline...")
    main()

    duration = round(time.time() - start_time, 2)
    print(f"✅ Pipeline terminé avec succès en {duration}s")

except Exception as e:
    duration = round(time.time() - start_time, 2)
    print(f"❌ Erreur lors de l'exécution du pipeline après {duration}s")
    print(f"Erreur: {str(e)}")

    # Log de l'erreur
    import traceback
    traceback.print_exc()

    # Échec du notebook
    dbutils.notebook.exit(f"FAILED: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Résultats et Dashboards

# COMMAND ----------

# Affichage des logs d'exécution
try:
    log_exec = spark.read.format("delta").load(params["log_exec_path"])
    display(
        log_exec
        .select("tableName", "fileName", "ingestionMode", "status", "errorCount", "env", "duration", "ts")
        .orderBy("ts", ascending=False)
        .limit(50)
    )
except Exception as e:
    print(f"⚠️ Impossible de charger les logs d'exécution: {e}")

# COMMAND ----------

# Affichage des logs de qualité
try:
    log_quality = spark.read.format("delta").load(params["log_quality_path"])
    display(
        log_quality
        .select("table_name", "filename", "line_id", "column_name", "error_message", "error_count", "zone", "env", "log_ts")
        .orderBy("log_ts", ascending=False)
        .limit(100)
    )
except Exception as e:
    print(f"⚠️ Impossible de charger les logs de qualité: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Statistiques globales

# COMMAND ----------

# Statistiques d'exécution
try:
    stats = log_exec.groupBy("status", "env").count()
    display(stats)
except:
    pass

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")
