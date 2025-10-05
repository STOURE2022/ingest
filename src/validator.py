import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, DateType, TimestampType,
    BooleanType, DecimalType, LongType, FloatType
)

def spark_type_from_config(row: pd.Series):
    t = str(row.get("Field type", row.get("Field Type", "STRING"))).strip().upper()
    if t == "STRING":    return StringType()
    if t in {"INT","INTEGER"}: return IntegerType()
    if t == "LONG":      return LongType()
    if t == "FLOAT":     return FloatType()
    if t == "DOUBLE":    return DoubleType()
    if t == "BOOLEAN":   return BooleanType()
    if t == "DATE":      return DateType()
    if t == "TIMESTAMP": return TimestampType()
    if t == "DECIMAL":
        prec = int(row.get("Decimal precision", 38) or 38)
        scale = int(row.get("Decimal scale", 18) or 18)
        return DecimalType(prec, scale)
    return StringType()

def parse_dates_with_logs(df: DataFrame, column_name: str, patterns: list,
                          table_name: str, filename: str, default_date=None):
    raw_col = F.col(column_name)
    base_expr = F.when(F.length(F.trim(raw_col)) == 0, F.lit(None)).otherwise(raw_col)

    ts = None
    for p in patterns:
        cand = F.to_timestamp(base_expr, p)
        ts = cand if ts is None else F.coalesce(ts, cand)

    as_date = F.to_date(ts)
    parsed = F.when(as_date.isNull(), F.lit(default_date)).otherwise(as_date)
    df_parsed = df.withColumn(column_name, parsed)

    errs = (
        df.withColumn("line_id", monotonically_increasing_id() + 1)
          .select(
              F.lit(table_name).alias("table_name"),
              F.lit(filename).alias("filename"),
              F.col("line_id"),
              F.lit(column_name).alias("column_name"),
              raw_col.cast("string").alias("raw_value"),
              F.when(F.length(F.trim(raw_col)) == 0, F.lit("EMPTY_DATE"))
               .when(as_date.isNull() & (F.trim(raw_col) != ""), F.lit("INVALID_DATE"))
               .otherwise(F.lit(None)).alias("error_type"),
              F.lit(1).alias("error_count"),
              F.lit(default_date).alias("default_applied")
          ).where(F.col("error_type").isNotNull())
          .dropDuplicates(["filename","table_name","column_name","line_id","raw_value","error_type"])
    )
    return df_parsed, errs

def check_data_quality(df: DataFrame, table_name: str, merge_keys: list,
                       column_defs: pd.DataFrame, filename: str, spark) -> DataFrame:
    if "line_id" not in df.columns:
        df = df.withColumn("line_id", row_number().over(Window.orderBy(monotonically_increasing_id())))

    schema = "table_name string, filename string, line_id long, column_name string, error_message string, error_count int"
    errors_df = spark.createDataFrame([], schema)

    # Fichier vide (hors line_id) ?
    data_cols = [c for c in df.columns if c != "line_id"]
    all_null = all(df.filter(col(c).isNotNull()).limit(1).count() == 0 for c in data_cols)
    if all_null:
        return spark.createDataFrame([(table_name, filename, None, "ALL_COLUMNS", "FILE_EMPTY_OR_ALL_NULL", 1)], schema)

    # Nulls sur clés
    for k in merge_keys or []:
        cnt = df.filter(col(k).isNull()).count()
        if cnt > 0:
            errors_df = errors_df.unionByName(
                spark.createDataFrame([(table_name, filename, None, k, "NULL_KEY", cnt)], schema)
            )

    # Doublons sur clés
    if merge_keys:
        dup = (df.groupBy(*merge_keys).count().filter(col("count") > 1)
               .select(
                   lit(table_name).alias("table_name"),
                   lit(filename).alias("filename"),
                   lit(None).cast("long").alias("line_id"),
                   lit(",".join(merge_keys)).alias("column_name"),
                   lit("DUPLICATE_KEY").alias("error_message"),
                   col("count").alias("error_count")
               ))
        errors_df = errors_df.unionByName(dup, allowMissingColumns=True)

    # Colonnes entièrement NULL si non-nullables
    if {"Column Name","Is Nullable"}.issubset(column_defs.columns):
        for _, r in column_defs.iterrows():
            cname = str(r["Column Name"]).strip()
            is_nullable = str(r["Is Nullable"]).strip().lower() in {"true","1","yes","y","oui"}
            if cname in df.columns and not is_nullable:
                has_non_null = df.filter(col(cname).isNotNull()).limit(1).count() > 0
                if not has_non_null:
                    errors_df = errors_df.unionByName(
                        spark.createDataFrame([(table_name, filename, None, cname, "COLUMN_ALL_NULL", 1)], schema)
                    )

    # Null ligne-à-ligne pour non-nullables
    if {"Column Name","Is Nullable"}.issubset(column_defs.columns):
        for _, r in column_defs.iterrows():
            cname = str(r["Column Name"]).strip()
            is_nullable = str(r["Is Nullable"]).strip().lower() in {"true","1","yes","y","oui"}
            if cname in df.columns and not is_nullable:
                null_rows = df.filter(col(cname).isNull())
                if null_rows.limit(1).count() > 0:
                    if "line_id" not in null_rows.columns:
                        w = Window.orderBy(monotonically_increasing_id())
                        null_rows = null_rows.withColumn("line_id", row_number().over(w))
                    errors_df = errors_df.unionByName(
                        null_rows.select(
                            lit(table_name).alias("table_name"),
                            lit(filename).alias("filename"),
                            col("line_id"),
                            lit(cname).alias("column_name"),
                            lit("NULL_VALUE").alias("error_message"),
                            lit(1).alias("error_count")
                        ), allowMissingColumns=True
                    )
    return errors_df

def print_summary(table_name: str, filename: str, corrupt_rows: int,
                  anomalies_lignes_total: int, cleaned_rows: int, errors_df: DataFrame) -> None:
    print("\n" + "=" * 80)
    print(f"📊 Report Ingestion App | Table={table_name}, File={filename}")
    print(f"Rejected: {corrupt_rows}, anomalies: {anomalies_lignes_total}, cleaned={cleaned_rows}")
    if errors_df is None or errors_df.rdd.isEmpty():
        print("✅ No data quality issues")
    print("=" * 80 + "\n")
