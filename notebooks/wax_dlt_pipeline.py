# Databricks notebook source
# MAGIC %md
# MAGIC # WAX Delta Live Tables Pipeline
# MAGIC
# MAGIC Pipeline DLT pour transformation et agrégation des données WAX
# MAGIC
# MAGIC **Architecture**: Delta Live Tables
# MAGIC
# MAGIC ---

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Tables brutes

# COMMAND ----------

# Exemple de table Bronze - à adapter selon vos besoins
@dlt.table(
    name="bronze_wax_raw",
    comment="Données brutes WAX ingérées depuis les fichiers sources"
)
def bronze_wax_raw():
    return (
        spark.read.format("delta")
        .load("/mnt/wax/dev/internal/v1/wax_table_all")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Tables nettoyées et validées

# COMMAND ----------

@dlt.table(
    name="silver_wax_cleaned",
    comment="Données WAX nettoyées et validées",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "valid_date": "FILE_PROCESS_DATE IS NOT NULL",
    "valid_filename": "FILE_NAME_RECEIVED IS NOT NULL"
})
def silver_wax_cleaned():
    return (
        dlt.read("bronze_wax_raw")
        .dropDuplicates()
        .filter(F.col("FILE_PROCESS_DATE").isNotNull())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Tables agrégées pour analytics

# COMMAND ----------

@dlt.table(
    name="gold_wax_daily_summary",
    comment="Résumé quotidien des données WAX",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_wax_daily_summary():
    return (
        dlt.read("silver_wax_cleaned")
        .groupBy("yyyy", "mm", "dd", "FILE_NAME_RECEIVED")
        .agg(
            F.count("*").alias("total_records"),
            F.min("FILE_PROCESS_DATE").alias("first_processed"),
            F.max("FILE_PROCESS_DATE").alias("last_processed")
        )
        .orderBy("yyyy", "mm", "dd", ascending=False)
    )
