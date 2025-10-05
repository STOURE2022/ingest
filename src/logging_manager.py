import time
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, DoubleType, TimestampType
)
import os

def _is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def log_execution(spark: SparkSession, params: dict,
                  table_name: str, filename: str, ingestion_mode: str,
                  column_count: int, anomalies: int, masklist_applied: bool,
                  error_count: int, error_message: str,
                  status: str, duration: float = None, start_time: float = None) -> None:
    env = params.get("env", "local")
    log_path = params["log_exec_path"]
    today = datetime.today()
    if duration is None and start_time is not None:
        duration = round(time.time() - start_time, 2)

    schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("ingestion_mode", StringType(), True),
        StructField("column_count", IntegerType(), True),
        StructField("anomalies", IntegerType(), True),
        StructField("masking_applied", BooleanType(), True),
        StructField("error_count", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("status", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("env", StringType(), True),
        StructField("log_ts", TimestampType(), True),
        StructField("yyyy", IntegerType(), True),
        StructField("mm", IntegerType(), True),
        StructField("dd", IntegerType(), True),
    ])
    row = [(
        str(table_name), str(filename), str(ingestion_mode),
        int(column_count or 0), int(anomalies or 0), bool(masklist_applied),
        int(error_count or 0), str(error_message or ""), str(status or ""),
        float(duration or 0.0), str(env), datetime.now(),
        today.year, today.month, today.day
    )]
    df_log = spark.createDataFrame(row, schema=schema)

    if _is_databricks():
        (df_log.write.format("delta").mode("append").option("mergeSchema", "true")
                  .partitionBy("yyyy","mm","dd").save(log_path))
    else:
        (df_log.write.format("parquet").mode("append")
                  .partitionBy("yyyy","mm","dd").save(log_path))

def write_quality_errors(spark: SparkSession, df_errors: DataFrame,
                         table_name: str, filename: str = None, zone: str = "internal",
                         env: str = "local", base_path: str = None) -> None:
    if df_errors is None or df_errors.rdd.isEmpty():
        return
    base_path = base_path or "/mnt/logs/wax_data_quality_errors_delta"
    from pyspark.sql.functions import col as _col

    # Dédup colonnes (case-insensitive)
    seen, cols = set(), []
    for c in df_errors.columns:
        cl = c.lower()
        if cl not in seen:
            cols.append(c)
            seen.add(cl)
    df_errors = df_errors.select(*cols)

    if "raw_value" not in df_errors.columns:
        df_errors = df_errors.withColumn("raw_value", lit(None).cast("string"))

    today = datetime.today()
    df_log = (
        df_errors
        .withColumn("table_name", lit(table_name))
        .withColumn("filename", lit(filename))
        .withColumn("Zone", lit(zone))
        .withColumn("Env", lit(env))
        .withColumn("log_ts", current_timestamp())
        .withColumn("yyyy", lit(today.year))
        .withColumn("mm", lit(today.month))
        .withColumn("dd", lit(today.day))
    )

    if _is_databricks():
        (df_log.write.format("delta").mode("append").option("mergeSchema", "true").save(base_path))
    else:
        (df_log.write.format("parquet").mode("append").save(base_path))
