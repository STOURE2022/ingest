"""Gestion des logs"""
import os
import time
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import *


def is_databricks() -> bool:
    """Détecte Databricks"""
    return 'DATABRICKS_RUNTIME_VERSION' in os.environ


def log_execution(spark: SparkSession, params: dict, table_name: str, filename: str,
                 ingestion_mode: str, column_count: int, anomalies: int, 
                 masklist_applied: bool, error_count: int, error_message: str, 
                 status: str, start_time: float = None):
    """Log d'exécution"""
    duration = round(time.time() - start_time, 2) if start_time else 0
    today = datetime.today()
    
    schema = StructType([
        StructField("tableName", StringType(), True),
        StructField("fileName", StringType(), True),
        StructField("ingestionMode", StringType(), True),
        StructField("columnCount", IntegerType(), True),
        StructField("anomalies", IntegerType(), True),
        StructField("masklistApplied", BooleanType(), True),
        StructField("errorCount", IntegerType(), True),
        StructField("errorMessage", StringType(), True),
        StructField("status", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("env", StringType(), True),
        StructField("ts", TimestampType(), True),
    ])
    
    row = [(
        str(table_name), str(filename), str(ingestion_mode),
        int(column_count), int(anomalies), bool(masklist_applied),
        int(error_count), str(error_message), str(status),
        float(duration), params.get("env", "local"), datetime.now()
    )]
    
    df = spark.createDataFrame(row, schema)
    
    path = params["log_exec_path"]
    fmt = "delta" if is_databricks() else "parquet"
    df.write.format(fmt).mode("append").save(path)
    print(f"✅ Log sauvegardé: {path}")


def write_quality_errors(spark: SparkSession, df_errors: DataFrame, 
                        table_name: str, filename: str, zone: str, 
                        env: str, base_path: str):
    """Log des erreurs de qualité"""
    if df_errors is None or df_errors.rdd.isEmpty():
        return
    
    df_log = (
        df_errors
        .withColumn("Zone", lit(zone))
        .withColumn("Env", lit(env))
        .withColumn("log_ts", lit(datetime.now()))
    )
    
    fmt = "delta" if is_databricks() else "parquet"
    df_log.write.format(fmt).mode("append").save(base_path)
    print(f"✅ Logs qualité sauvegardés: {base_path}")
