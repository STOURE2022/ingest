"""
validators.py
--------------
Fonctions de validation et contrôle de la qualité des données
avant ingestion dans Delta Lake.

Inspiré du notebook WAX Databricks :
- vérification des doublons
- détection des colonnes nulles
- contrôle de la nullabilité
- vérification des types et formats
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# ==============================================================
# 1️⃣ VALIDATION DU NOM DE FICHIER
# ==============================================================

def validate_filename(fname: str, source_table: str, matched_uri: str, log_quality_path: str):
    """
    Vérifie que le nom du fichier contient une date valide.
    Extrait les parties yyyy, mm, dd à partir du nom.
    Retourne le dict {'yyyy': ..., 'mm': ..., 'dd': ...} si OK, sinon False.
    """
    import os, re
    from datetime import datetime
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    base = os.path.basename(fname)
    print(f"🔍 Analyse du fichier : {base}")

    pattern = r"(?P<yyyy>\d{4})(?P<mm>\d{2})(?P<dd>\d{2})?"
    m = re.search(pattern, base)

    schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("line_id", IntegerType(), True),
        StructField("invalid_value", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("uri", StringType(), True),
    ])

    if not m:
        print(f"❌ Fichier rejeté {base} (pattern date manquant)")
        err_data = [(source_table, base, None, None, None, "Missing or invalid date pattern in filename", matched_uri)]
        err_df = spark.createDataFrame(err_data, schema)
        err_df.write.format("delta").mode("append").save(log_quality_path)
        return False

    try:
        yyyy = int(m.group("yyyy"))
        mm = int(m.group("mm"))
        dd = int(m.group("dd")) if m.group("dd") else 1
        datetime(yyyy, mm, dd)
        print(f"✅ Fichier accepté {base} (date = {yyyy}-{mm:02d}-{dd:02d})")
        return {"yyyy": yyyy, "mm": mm, "dd": dd}
    except Exception:
        print(f"❌ Fichier rejeté {base} (date invalide)")
        err_data = [(source_table, base, "filename", None, base, "Invalid date in filename", matched_uri)]
        err_df = spark.createDataFrame(err_data, schema)
        err_df.write.format("delta").mode("append").save(log_quality_path)
        return False


# ==============================================================
# 2️⃣ VALIDATION QUALITÉ DES DONNÉES
# ==============================================================

def check_data_quality(
    df: DataFrame,
    table_name: str,
    merge_keys: list,
    filename: str = None,
    column_defs=None
) -> DataFrame:
    """
    Vérifie la qualité globale des données :
    - clés nulles
    - doublons
    - colonnes vides ou nullables violées
    - colonnes entièrement nulles
    """

    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    # Structure du DF d’erreurs
    schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("line_id", IntegerType(), True),
        StructField("column_name", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("error_count", IntegerType(), True),
    ])

    errors_df = spark.createDataFrame([], schema)

    if df is None or df.rdd.isEmpty():
        return errors_df

    # Ajout d’un ID de ligne si manquant
    if "line_id" not in df.columns:
        df = df.withColumn("line_id", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))

    # Liste des colonnes à analyser
    data_columns = [c for c in df.columns if c not in ["line_id", "yyyy", "mm", "dd", "FILE_PROCESS_DATE", "FILE_NAME_RECEIVED"]]
    if not data_columns:
        return errors_df

    # Vérifier si le fichier est vide ou toutes colonnes nulles
    all_null = all(df.filter(F.col(c).isNotNull()).count() == 0 for c in data_columns)
    if all_null:
        empty_df = spark.createDataFrame(
            [(table_name, filename, None, "ALL_COLUMNS", "FILE_EMPTY_OR_ALL_NULL", 1)],
            schema
        )
        return errors_df.unionByName(empty_df, allowMissingColumns=True)

    # ==========================================
    # 1. Clés de jointure nulles
    # ==========================================
    for key in merge_keys or []:
        if key in df.columns:
            null_count = df.filter(F.col(key).isNull()).count()
            if null_count > 0:
                err = spark.createDataFrame(
                    [(table_name, filename, None, key, "NULL_KEY", null_count)],
                    schema
                )
                errors_df = errors_df.unionByName(err, allowMissingColumns=True)

    # ==========================================
    # 2. Doublons sur les clés
    # ==========================================
    if merge_keys:
        valid_keys = [k for k in merge_keys if k in df.columns]
        if valid_keys:
            dup_df = (
                df.groupBy(*valid_keys).count()
                .filter(F.col("count") > 1)
                .select(
                    F.lit(table_name).alias("table_name"),
                    F.lit(filename).alias("filename"),
                    F.lit(None).cast("int").alias("line_id"),
                    F.lit(','.join(valid_keys)).alias("column_name"),
                    F.lit("DUPLICATE_KEY").alias("error_message"),
                    F.col("count").alias("error_count")
                )
            )
            errors_df = errors_df.unionByName(dup_df, allowMissingColumns=True)

    # ==========================================
    # 3. Colonnes non nullables
    # ==========================================
    if column_defs is not None:
        subset = column_defs[column_defs["Delta Table Name"] == table_name]
        total_rows = df.count()

        for _, row in subset.iterrows():
            cname = row["Column Name"]
            is_nullable = str(row.get("Is Nullable", "true")).strip().lower() == "true"
            if cname not in df.columns:
                continue

            non_null_count = df.filter(F.col(cname).isNotNull()).count()
            if non_null_count == 0 and not is_nullable:
                err = spark.createDataFrame(
                    [(table_name, filename, None, cname, "COLUMN_ALL_NULL", total_rows)],
                    schema
                )
                errors_df = errors_df.unionByName(err, allowMissingColumns=True)
            elif non_null_count > 0 and not is_nullable:
                null_count = df.filter(F.col(cname).isNull()).count()
                if null_count > 0:
                    null_df = (
                        df.filter(F.col(cname).isNull())
                        .withColumn("error_message", F.lit("NULL_VALUE"))
                        .withColumn("error_count", F.lit(1))
                        .withColumn("table_name", F.lit(table_name))
                        .withColumn("column_name", F.lit(cname))
                        .withColumn("filename", F.lit(filename))
                        .select("table_name", "filename", "line_id", "column_name", "error_message", "error_count")
                    )
                    errors_df = errors_df.unionByName(null_df, allowMissingColumns=True)

    return errors_df


# ==============================================================
# 3️⃣ SYNTHÈSE DES ERREURS
# ==============================================================

def summarize_errors(errors_df: DataFrame, table_name: str):
    """
    Affiche un résumé lisible des erreurs détectées dans les logs.
    """
    if errors_df is None or errors_df.rdd.isEmpty():
        print(f"✅ Aucune erreur détectée pour {table_name}")
        return

    print("\n" + "=" * 80)
    print(f"📊 Résumé qualité - Table : {table_name}")
    print("=" * 80)

    agg = (
        errors_df.groupBy("error_message")
        .agg(F.sum("error_count").alias("nb"))
        .orderBy(F.desc("nb"))
    )

    for row in agg.collect():
        print(f" - {row['error_message']}: {row['nb']} occurrence(s)")

    print("=" * 80 + "\n")
