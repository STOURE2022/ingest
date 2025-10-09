# src/validator.py
# --------------------------------------------------------------------------------------
# Validation qualité : typage, nullabilité, doublons, dates invalides.
# Compatible Databricks et local.
# --------------------------------------------------------------------------------------

from datetime import datetime
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from utils import deduplicate_columns, parse_bool
from environment import is_databricks


# ======================================================================================
# 1️⃣ - SCHÉMA D’ERREURS GLOBAL
# ======================================================================================
ERROR_SCHEMA = StructType([
    StructField("table_name", StringType(), True),
    StructField("filename", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("raw_value", StringType(), True),
    StructField("error_count", IntegerType(), True)
])


# ======================================================================================
# 2️⃣ - VÉRIFICATION DES DOUBLONS (MERGE KEYS)
# ======================================================================================
def check_data_quality(
    df: DataFrame,
    table_name: str,
    merge_keys: list[str],
    filename: str = None
) -> DataFrame:
    """
    Vérifie les doublons sur les clés de jointure (merge_keys).
    Retourne un DataFrame d'erreurs (ou vide).
    """

    df = deduplicate_columns(df)

    if not merge_keys:
        return df.sparkSession.createDataFrame([], ERROR_SCHEMA)

    valid_keys = [k for k in merge_keys if k in df.columns]
    if not valid_keys:
        return df.sparkSession.createDataFrame([], ERROR_SCHEMA)

    dup_df = (
        df.groupBy(*valid_keys)
          .count()
          .filter(F.col("count") > 1)
          .select(
              F.lit(table_name).alias("table_name"),
              F.lit(filename or "N/A").alias("filename"),
              F.lit(','.join(valid_keys)).alias("column_name"),
              F.lit("DUPLICATE_KEY").alias("error_message"),
              F.lit(None).cast("string").alias("raw_value"),
              F.col("count").alias("error_count")
          )
    )

    return dup_df


# ======================================================================================
# 3️⃣ - PARSING DE DATES + LOGS D’ERREURS
# ======================================================================================
def parse_date_with_logs(
    df: DataFrame,
    cname: str,
    patterns: list[str],
    table_name: str,
    filename: str,
    default_date=None
) -> tuple[DataFrame, DataFrame]:
    """
    Essaie plusieurs formats de date pour convertir une colonne.
    Retourne (DataFrame converti, DataFrame erreurs).
    """

    raw_col = F.col(cname)
    col_expr = F.when(F.length(F.trim(raw_col)) == 0, F.lit(None)).otherwise(raw_col)

    ts_col = None
    for p in patterns:
        cand = F.expr(f"try_to_timestamp({cname}, '{p}')")
        ts_col = cand if ts_col is None else F.coalesce(ts_col, cand)

    parsed = F.to_date(ts_col)
    parsed_with_default = F.when(parsed.isNull(), F.lit(default_date)).otherwise(parsed)
    df_parsed = df.withColumn(cname, parsed_with_default)

    errs = (
        df.withColumn("line_id", F.monotonically_increasing_id() + 1)
          .select(
              F.lit(table_name).alias("table_name"),
              F.lit(filename).alias("filename"),
              F.lit(cname).alias("column_name"),
              F.when(parsed.isNull() & (F.trim(raw_col) == ""), F.lit("EMPTY_DATE"))
               .when(parsed.isNull() & (F.trim(raw_col) != ""), F.lit("INVALID_DATE"))
               .otherwise(F.lit(None)).alias("error_message"),
              raw_col.cast("string").alias("raw_value"),
              F.lit(1).alias("error_count")
          )
          .where(F.col("error_message").isNotNull())
          .limit(1000)
    )

    return df_parsed, errs


# ======================================================================================
# 4️⃣ - VALIDATION TYPE (COLONNES MÉTIER)
# ======================================================================================
def validate_column_types(
    df: DataFrame,
    table_name: str,
    filename: str,
    column_defs: list[dict],
    type_mapping: dict
) -> tuple[DataFrame, DataFrame]:
    """
    Valide le typage et la nullabilité des colonnes à partir de la config Excel.
    Retourne (DataFrame nettoyé, DataFrame d'erreurs).
    """

    spark = df.sparkSession
    df_errors = spark.createDataFrame([], ERROR_SCHEMA)

    validated_cols = {}
    df = deduplicate_columns(df)

    for col_def in column_defs:
        cname = str(col_def.get("Column Name")).strip()
        expected_type = str(col_def.get("Field type", "STRING")).strip().upper()
        is_nullable = parse_bool(col_def.get("Is Nullable", True), True)

        if cname not in df.columns:
            continue

        if expected_type not in type_mapping:
            continue

        cast_expr = F.expr(f"try_cast({cname} as {expected_type})")

        invalid_cond = cast_expr.isNull() & F.col(cname).isNotNull()
        invalid_count = df.filter(invalid_cond).count()

        # Erreurs de typage
        if invalid_count > 0:
            err = df.filter(invalid_cond).limit(1000).select(
                F.lit(table_name).alias("table_name"),
                F.lit(filename).alias("filename"),
                F.lit(cname).alias("column_name"),
                F.concat(
                    F.lit(f"TYPE MISMATCH: expected {expected_type}, found '"),
                    F.col(cname).cast("string"),
                    F.lit("'")
                ).alias("error_message"),
                F.col(cname).cast("string").alias("raw_value"),
                F.lit(1).alias("error_count")
            )
            df_errors = df_errors.union(err)

        validated_cols[cname] = cast_expr

        # Nullabilité
        if not is_nullable:
            null_count = df.filter(cast_expr.isNull()).count()
            if null_count > 0:
                err = df.filter(cast_expr.isNull()).limit(1000).select(
                    F.lit(table_name).alias("table_name"),
                    F.lit(filename).alias("filename"),
                    F.lit(cname).alias("column_name"),
                    F.lit(f"NULL_VALUE: Column '{cname}' does not allow null").alias("error_message"),
                    F.lit(None).cast("string").alias("raw_value"),
                    F.lit(1).alias("error_count")
                )
                df_errors = df_errors.union(err)

    # Remplacement final des colonnes validées
    for cname, expr in validated_cols.items():
        df = df.withColumn(cname, expr)

    return df, df_errors


# ======================================================================================
# 5️⃣ - RÉSUMÉ QUALITÉ
# ======================================================================================
def print_summary(
    table_name: str,
    filename: str,
    total_rows: int,
    corrupt_rows: int,
    anomalies_total: int,
    cleaned_rows: int
):
    """
    Affiche un résumé synthétique de la qualité des données.
    """
    print("\n" + "=" * 80)
    print(f"📊 Rapport | Table = {table_name}, Fichier = {filename}")
    print(f"Lignes totales : {total_rows}")
    print(f"Lignes corrompues : {corrupt_rows}")
    print(f"Anomalies : {anomalies_total}")
    print(f"Lignes nettoyées : {cleaned_rows}")
    print("=" * 80 + "\n")
