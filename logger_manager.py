# src/logger_manager.py
# --------------------------------------------------------------------------------------
# Gestion des logs d’exécution et de qualité du pipeline WAX.
# Compatible Databricks (Delta / UC) et exécution locale (Parquet ou CSV).
# --------------------------------------------------------------------------------------

import os
import time
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType
)

from environment import is_databricks, ensure_dir


# ======================================================================================
# 1️⃣ - LOG EXÉCUTION
# ======================================================================================
def log_execution(
    spark: SparkSession,
    table_name: str,
    filename: str,
    input_format: str,
    ingestion_mode: str,
    output_zone: str,
    log_path: str,
    env: str,
    row_count: int = 0,
    column_count: int = 0,
    masking_applied: bool = False,
    error_count: int = 0,
    error_msg: str = None,
    status: str = "SUCCESS",
    start_time: float = None
):
    """
    Enregistre un log d’exécution (succès ou échec).
    Écrit en format Delta (Databricks) ou Parquet (local).
    """

    today = datetime.today()
    duration = round(time.time() - start_time, 2) if start_time else None

    schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("input_format", StringType(), True),
        StructField("ingestion_mode", StringType(), True),
        StructField("output_zone", StringType(), True),
        StructField("row_count", IntegerType(), True),
        StructField("column_count", IntegerType(), True),
        StructField("masking_applied", BooleanType(), True),
        StructField("error_count", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("status", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("env", StringType(), True),
        StructField("log_ts", TimestampType(), True),
        StructField("yyyy", IntegerType(), True),
        StructField("mm", IntegerType(), True),
        StructField("dd", IntegerType(), True)
    ])

    row = [(
        str(table_name), str(filename), str(input_format), str(ingestion_mode),
        str(output_zone), int(row_count or 0), int(column_count or 0),
        bool(masking_applied), int(error_count or 0), str(error_msg or ""),
        str(status), float(duration or 0), str(env),
        datetime.now(), today.year, today.month, today.day
    )]

    df_log = spark.createDataFrame(row, schema=schema)

    # Créer le dossier si nécessaire (en local)
    if not is_databricks():
        ensure_dir(log_path)

    # Écriture
    if is_databricks():
        df_log.write.format("delta").mode("append").option("mergeSchema", "true") \
            .partitionBy("yyyy", "mm", "dd").save(log_path)
    else:
        df_log.write.mode("append").parquet(log_path)

    print(f"📝 Log exécution enregistré → {status} ({table_name})")


# ======================================================================================
# 2️⃣ - LOG QUALITÉ
# ======================================================================================
def write_quality_errors(
    spark: SparkSession,
    df_errors: DataFrame,
    table_name: str,
    log_quality_path: str,
    env: str,
    zone: str = "internal"
):
    """
    Enregistre les erreurs de qualité (typage, nullabilité, doublons, etc.)
    """
    if df_errors is None or df_errors.rdd.isEmpty():
        print("✅ Aucun log qualité à écrire.")
        return

    today = datetime.today()

    # Normalisation des colonnes
    for col in ["table_name", "filename", "column_name", "error_message", "raw_value", "error_count"]:
        if col not in df_errors.columns:
            df_errors = df_errors.withColumn(col, F.lit(None).cast(StringType()))

    df_log = (
        df_errors
        .withColumn("table_name", F.coalesce(F.col("table_name"), F.lit(table_name)))
        .withColumn("Zone", F.lit(zone))
        .withColumn("Env", F.lit(env))
        .withColumn("log_ts", F.lit(datetime.now()))
        .withColumn("yyyy", F.lit(today.year))
        .withColumn("mm", F.lit(today.month))
        .withColumn("dd", F.lit(today.day))
    )

    # Créer le dossier si nécessaire (en local)
    if not is_databricks():
        ensure_dir(log_quality_path)

    # Écriture
    if is_databricks():
        try:
            df_log.write.format("delta").mode("append").option("mergeSchema", "true").save(log_quality_path)
        except Exception as e:
            print(f"⚠️ Erreur écriture log qualité : {e}")
    else:
        try:
            df_log.write.mode("append").parquet(log_quality_path)
        except Exception as e:
            print(f"⚠️ Erreur écriture log qualité (local) : {e}")

    print(f"🚦 Log qualité enregistré → {table_name}")


# ======================================================================================
# 3️⃣ - LECTURE DES LOGS (utile pour dashboard ou vérif)
# ======================================================================================
def read_logs(spark: SparkSession, log_path: str) -> DataFrame:
    """
    Lit un log Delta ou Parquet (auto-détection selon environnement).
    """
    if is_databricks():
        return spark.read.format("delta").load(log_path)
    else:
        return spark.read.parquet(log_path)
