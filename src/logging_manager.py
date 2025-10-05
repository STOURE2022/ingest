"""
Module logging_manager.py
-------------------------
Gestion des logs d’exécution et des logs de qualité en Delta Lake.
"""
from datetime import datetime
import time
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               BooleanType, DoubleType, TimestampType)


def log_execution(
        spark,
        table_name: str,
        filename: str,
        input_format: str,
        ingestion_mode: str,
        output_zone: str,
        column_count: int,
        masking_applied: bool,
        error_count: int = 0,
        error_message: str = None,
        status: str = "SUCCESS",
        start_time: float = None,
        env: str = "dev",
        log_path: str = "/mnt/logs/wax_execution_logs_delta",
):
    if spark is None:
        print("⚠️ Avertissement : SparkSession est None dans log_execution()")
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("WAX_Fallback").getOrCreate()

    today = datetime.today()
    duration = round(time.time() - start_time, 2) if start_time else 0.0
    schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("input_format", StringType(), True),
        StructField("ingestion_mode", StringType(), True),
        StructField("output_zone", StringType(), True),
        StructField("column_count", IntegerType(), True),
        StructField("masking_applied", BooleanType(), True),
        StructField("error_count", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("status", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("env", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("yyyy", IntegerType(), True),
        StructField("mm", IntegerType(), True),
        StructField("dd", IntegerType(), True),
    ])
    row = [(str(table_name), str(filename), str(input_format), str(ingestion_mode),
            str(output_zone), int(column_count or 0), bool(masking_applied),
            int(error_count or 0), str(error_message or ""), str(status),
            float(duration or 0), str(env), datetime.now(),
            today.year, today.month, today.day)]
    df_log = spark.createDataFrame(row, schema=schema)
    (df_log.write.format("delta").mode("append").option("mergeSchema", "true")
     .partitionBy("yyyy", "mm", "dd").save(log_path))
    print(f"📝 Log exécution écrit dans {log_path} (status={status})")


def write_quality_errors(
        df_errors,
        table_name: str,
        zone: str = "internal",
        base_path: str = "/mnt/logs/wax_data_quality_errors_delta",
        env: str = "dev",
        spark=None
):
    if df_errors is None or df_errors.rdd.isEmpty():
        return
    if spark is None:
        spark = df_errors.sparkSession
    from datetime import datetime as _dt
    today = _dt.today()
    seen, cols = set(), []
    for c in df_errors.columns:
        c1 = c.lower()
        if c1 not in seen:
            cols.append(c);
            seen.add(c1)
    df_errors = df_errors.select(*cols)
    df_log = (df_errors
              .withColumn("table_name", F.lit(table_name))
              .withColumn("Zone", F.lit(zone))
              .withColumn("Env", F.lit(env))
              .withColumn("ts", F.lit(_dt.now()))
              .withColumn("yyyy", F.lit(today.year))
              .withColumn("mm", F.lit(today.month))
              .withColumn("dd", F.lit(today.day)))
    try:
        spark.read.format("delta").load(base_path)
    except Exception:
        (df_log.write.format("delta").mode("overwrite")
         .partitionBy("yyyy", "mm", "dd").save(base_path))
        print(f"✅ Table Delta créée pour logs qualité : {base_path}")
    (df_log.write.format("delta").mode("append").option("mergeSchema", "true").save(base_path))
    print(f"🚦 Log qualité écrit dans {base_path}")
