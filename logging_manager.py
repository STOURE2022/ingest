"""
logging_manager.py
------------------
Gestion des logs du pipeline WAX :
- Log exécution (pipeline, tables, erreurs)
- Log qualité (erreurs de validation)
Compatible Databricks et mode local.
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


# ==============================================================
# 1️⃣ Log d'exécution
# ==============================================================

def log_execution(
    spark: SparkSession,
    params: dict,
    table_name: str,
    filename: str,
    ingestion_mode: str,
    row_count: int,
    column_count: int,
    masklist_applied: bool,
    error_count: int,
    error_message: str,
    status: str,
    start_time: float
):
    """
    Écrit un log d'exécution complet dans Delta Lake.
    """
    log_exec_path = params.get("log_exec_path", "./logs/exec")

    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("table_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("ingestion_mode", StringType(), True),
        StructField("row_count", IntegerType(), True),
        StructField("column_count", IntegerType(), True),
        StructField("masklist_applied", StringType(), True),
        StructField("error_count", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("status", StringType(), True),
        StructField("duration_sec", StringType(), True),
    ])

    end_time = time.time()
    duration = round(end_time - start_time, 2)

    log_data = [(time.strftime("%Y-%m-%d %H:%M:%S"), table_name, filename, ingestion_mode,
                 row_count, column_count, str(masklist_applied), error_count, error_message,
                 status, str(duration))]

    df_log = spark.createDataFrame(log_data, schema)
    df_log.write.format("delta").mode("append").save(log_exec_path)
    print(f"📝 Log écrit → {log_exec_path} [{status}] ({duration}s)")


# ==============================================================
# 2️⃣ Log Qualité
# ==============================================================

def write_quality_errors(spark, df_errors, params, table_name, filename):
    """
    Sauvegarde les erreurs de qualité dans Delta Lake.
    """
    log_quality_path = params.get("log_quality_path", "./logs/quality")

    if df_errors is None or df_errors.rdd.isEmpty():
        print(f"✅ Aucune erreur qualité détectée pour {table_name}")
        return

    df_final = (
        df_errors.withColumn("table_name", F.lit(table_name))
                 .withColumn("filename", F.lit(filename))
                 .withColumn("timestamp", F.current_timestamp())
    )

    df_final.write.format("delta").mode("append").save(log_quality_path)
    print(f"⚠️ {df_final.count()} erreurs de qualité enregistrées → {log_quality_path}")
