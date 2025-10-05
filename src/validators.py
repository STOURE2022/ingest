"""
Module validators.py
--------------------
Validation : typage Spark, parsing dates, contrôle qualité, validation noms.
"""
import re
from datetime import datetime
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (StringType, IntegerType, LongType, FloatType, DoubleType,
                               BooleanType, DateType, TimestampType, DecimalType,
                               StructType, StructField)
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number, lit


def spark_type_from_config(row):
    t = str(row.get("Field type")).strip().upper()
    if t == "STRING": return StringType()
    if t in ["INT", "INTEGER"]: return IntegerType()
    if t == "LONG": return LongType()
    if t == "FLOAT": return FloatType()
    if t == "DOUBLE": return DoubleType()
    if t == "BOOLEAN": return BooleanType()
    if t == "DATE": return DateType()
    if t == "TIMESTAMP": return TimestampType()
    if t == "DECIMAL":
        prec = int(row.get("Decimal precision", 38) or 38)
        scale = int(row.get("Decimal scale", 18) or 18)
        return DecimalType(prec, scale)
    return StringType()


def parse_date_with_logs(df: DataFrame, cname: str, patterns: list,
                         table_name: str, filename: str,
                         default_date=None):
    raw_col = F.col(cname)
    ts_col = None
    for p in patterns:
        cand = F.expr(f"try_to_timestamp({cname}, '{p}')")
        ts_col = cand if ts_col is None else F.coalesce(ts_col, cand)
    parsed = F.to_date(ts_col)
    parsed_with_default = F.when(parsed.isNull(), F.lit(default_date)).otherwise(parsed)
    df_raw = df.withColumn(cname, parsed_with_default)
    errs = (
        df.withColumn("line_id", F.monotonically_increasing_id() + 1)
        .select(
            F.lit(table_name).alias("table_name"),
            F.lit(filename).alias("filename"),
            F.lit(cname).alias("column_name"),
            F.col("line_id"),
            raw_col.cast("string").alias("raw_value"),
            F.when(parsed.isNull() & (F.trim(raw_col) == ""), F.lit("EMPTY_DATE"))
            .when(parsed.isNull() & (F.trim(raw_col) != ""), F.lit("INVALID_DATE"))
            .otherwise(F.lit(None)).alias("error_type"),
            F.lit(default_date).alias("default_applied")
        ).where(F.col("error_type").isNotNull())
    )
    errs = errs.dropDuplicates(["filename", "table_name", "column_name", "line_id", "raw_value", "error_type"])
    return df_raw, errs


def validate_filename(fname: str, source_table: str, matched_uri: str, log_quality_path: str, spark):
    base = fname.split("/")[-1]
    m = re.search(r"(?P<yyyy>\d{4})(?P<mm>\d{2})(?P<dd>\d{2})?", base)
    error_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("line_id", IntegerType(), True),
        StructField("invalid_value", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("uri", StringType(), True),
    ])
    if not m:
        err_data = [(source_table, base, None, None, None, "Missing/invalid date pattern in filename", matched_uri)]
        spark.createDataFrame(err_data, error_schema).write.format("delta").mode("append").save(log_quality_path)
        return False
    try:
        yyyy, mm, dd = int(m.group("yyyy")), int(m.group("mm")), int(m.group("dd"))
        datetime(yyyy, mm, dd)
        return True
    except ValueError:
        err_data = [(source_table, base, "filename", None, f"{yyyy}-{mm:02d}-{dd:02d}", "INVALID DATE in filename",
                     matched_uri)]
        spark.createDataFrame(err_data, error_schema).write.format("delta").mode("append").save(log_quality_path)
        return False


def check_data_quality(df: DataFrame, table_name: str, merge_keys: list, filename: str = None) -> DataFrame:
    if "line_id" not in df.columns:
        df = df.withColumn("line_id", row_number().over(Window.orderBy(monotonically_increasing_id())))
    schema = "table_name string, filename string, line_id long, column_name string, error_message string, error_count int"
    errors_df = df.sparkSession.createDataFrame([], schema)
    data_columns = [c for c in df.columns if c != "line_id"]
    all_null = all(df.filter(F.col(c).isNotNull()).count() == 0 for c in data_columns)
    if all_null:
        return df.sparkSession.createDataFrame(
            [(table_name, filename, None, "ALL_COLUMNS", "FILE_EMPTY_OR_ALL_NULL", 1)], schema)
    for key in merge_keys or []:
        null_key_count = df.filter(F.col(key).isNull()).count()
        if null_key_count > 0:
            errs = df.sparkSession.createDataFrame([(table_name, filename, None, key, "NULL_KEY", null_key_count)],
                                                   errors_df.schema)
            errors_df = errors_df.unionByName(errs, allowMissingColumns=True)
    if merge_keys:
        dup_df = (df.groupBy(*merge_keys).count().filter(F.col("count") > 1)
                  .select(F.lit(table_name).alias("table_name"),
                          F.lit(filename).alias("filename"),
                          F.lit(None).alias("line_id"),
                          F.lit(",".join(merge_keys)).alias("column_name"),
                          F.lit("DUPLICATE_KEY").alias("error_message"),
                          F.col("count").alias("error_count")))
        errors_df = errors_df.unionByName(dup_df, allowMissingColumns=True)
    return errors_df
