# =========================================================
# Création des widgets
# =========================================================

# landing = zone de dépôt des fichiers, actuellement dbfs (POC) après (ADLS Azure / data lake storage)
dbutils.widgets.text("zip_path", "dbfs:/FileStore/tables/wax_delta_from_historized.zip", "📦 ZIP Source")
dbutils.widgets.text("excel_path", "dbfs:/FileStore/tables/custom_test2_secret_conf.xlsx", "📑 Excel Config")

# transient/staging = espace de travail temporaire pour le dézippage et lister les fichiers
dbutils.widgets.text("extract_dir", "dbfs:/tmp/unzipped_wax_csvs", "📂 Dossier Extraction ZIP")

# les logs d'exécution
dbutils.widgets.text("log_exec_path", "/mnt/logs/wax_execution_logs_delta", "📝 Logs Exécution (Delta)")
dbutils.widgets.text("log_quality_path", "/mnt/logs/wax_data_quality_errors_delta", "🚦 Log Qualité (Delta)")

# les paramètres de notre pipeline
dbutils.widgets.text("env", "dev", "🌍 Environnement")
dbutils.widgets.text("version", "v1", "⚙️ Version Pipeline")

# Lecture de nos widgets
PARAMS = {k: dbutils.widgets.get(k) for k in [
    "zip_path", "excel_path", "extract_dir",
    "log_exec_path", "log_quality_path", "env", "version"
]}

print("✅ Les paramètres chargés :")
for k, v in PARAMS.items():
    print(f"{k}: {v}")

# =========================================================
# Gestion de nos imports nécessaires
# =========================================================

import os, re, zipfile, subprocess
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql import SparkSession
from functools import reduce

from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, \
    TimestampType
from pyspark.sql.functions import lit, col, count, col, when, monotonically_increasing_id, coalesce, row_number
from pyspark.sql.window import Window

from delta.tables import DeltaTable
from collections import Counter
from py4j.protocol import Py4JJavaError
import pandas as pd
import subprocess
import sys, time
from datetime import datetime
from pyspark.sql import Row

# =========================================================
# Installer openpyxl pour lecture Excel
# =========================================================
try:
    import openpyxl
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
import zipfile


# =========================================================
# Gestion des contraintes métiers
# =========================================================

# Le bool dans le fichier excel
def parse_bool(x, default=False):
    if x is None:
        return default
    s = str(x).strip().lower()
    if s in ["true", "1", "yes", "y", "oui"]:
        return True
    if s in ["false", "0", "no", "n", "non"]:
        return False
    return default


# Le délimiteur dans le fichier excel
def normalize_delimiter(raw) -> str:
    """
    Normalise le délimiteur issu du fichier Excel.
    Doit être un caractère unique (sinon -> erreur).
    """
    if raw is None or str(raw).strip() == "":
        return ","
    s = str(raw).strip().lower()
    if len(s) == 1:
        return s
    raise ValueError(f"Input delimiter '{raw}' non supporté (Spark CSV attend 1 char)")


# Le header dans le fichier excel
def parse_header_mode(x) -> tuple[bool, bool]:
    if x is None:
        return False, False
    s = str(x).strip().upper()
    if s == "HEADER USE":
        return True, True
    if s == "FIRST LINE":
        return True, False
    return False, False


# Gestion des tolérances aux erreurs dans fichier excel
def parse_tolerance(raw, total_rows: int, default=0.0) -> float:
    # Si la valeur est vide, None ou invalide → on utilise la valeur par défaut
    if raw is None or str(raw).strip().lower() in ["", "nan", "n/a", "none"]:
        return default

    s = str(raw).strip().lower().replace(",", ".").replace("%", "").replace(" ", "")

    # Cas pourcentage (ex: "10%", "0.5%")
    m1 = re.search(r"^(\d+(?:\.\d+)?)%$", s)
    if m1:
        return float(m1.group(1)) / 100.0

    # Cas valeur absolue (ex: "5", "3.5")
    m2 = re.search(r"^(\d+(?:\.\d+)?)$", s)
    if m2:
        if total_rows <= 0:
            return 0.0
        return float(m2.group(1)) / total_rows

    # Si rien ne correspond, retourne 0.0
    return 0.0


# =========================================================
# Gestion du type Field type dans le fichier Excel
# =========================================================
def spark_type_from_config(row):
    t = str(row.get("Field type")).strip().upper()
    # print(f"Le type détecté dans la config Excel : {t}")

    if t == "STRING":    return StringType()
    if t in ["INT", "INTEGER"]: return IntegerType()
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


# =========================================================
# Résumé des erreurs qualité
# =========================================================
def print_summary(table_name: str, filename: str, corrupt_rows: int, anomalies_lignes_total: int, cleaned_rows: int,
                  errors_df):
    print("\n" + "=" * 80)
    print(f"📊 Report Ingestion App | Table={table_name}, File={filename}")
    print(
        f"Total rows: {corrupt_rows}, rejected (corrupt_rows): {corrupt_rows}, anomalies possibles: {anomalies_lignes_total}, cleaned={cleaned_rows}")
    print("=" * 80)

    if errors_df is not None and not errors_df.rdd.isEmpty():
        print("⚠️ Data quality issues detected")
        error_counter = Counter()
        null_counter = Counter()

        for r in errors_df.collect():
            rd = r.asDict(recursive=True)
            em = rd.get("error_message") or rd.get("Error") or rd.get("error") or "message_inconnu"
            ec = rd.get("error_count") or rd.get("Count") or 1

            try:
                ec = int(ec)
            except Exception:
                ec = 1

            # Séparation des erreurs nulles et autres
            if "null value" in em or "NULL" in em.lower():
                null_counter[em] += ec
            else:
                error_counter[em] += ec

        # Affichage des erreurs non nulles
        if error_counter:
            print("\nErreurs de typage ou de format :")
            for em, total in error_counter.items():
                print(f" - {em}: {total} erreurs")

        # Affichage des valeurs nulles
        if null_counter:
            print("\nValeurs nulles détectées :")
            for em, total in null_counter.items():
                print(f" - {em}: {total} cas")

    else:
        print("\n✅ No data quality issues")
    print("=" * 80 + "\n")


# =========================================================
# Fonctions de logs d'exécution (Delta + chemin fixe + partitions)
# =========================================================
def log_execution(table_name: str, filename: str, input_format: str, ingestion_mode: str,
                  output_zone: str, column_count: int, masking_applied: bool,
                  error_count: int = 0, error_message: str = None,
                  status: str = "SUCCESS", start_time: float = None,
                  env: str = PARAMS["env"], log_path: str = PARAMS["log_exec_path"]):
    today = datetime.today()
    duration = None
    if start_time:
        duration = round(time.time() - start_time, 2)

    # Schéma explicite pour stabilité
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
        StructField("dd", IntegerType(), True)
    ])

    row = [(str(table_name), str(filename), str(input_format), str(ingestion_mode),
            str(output_zone), int(column_count or 0), bool(masking_applied),
            int(error_count or 0), str(error_message or ""), str(status),
            float(duration or 0), str(env), datetime.now(),
            today.year, today.month, today.day)]

    df_log = spark.createDataFrame(row, schema=schema)

    # Écriture Delta partitionnée par date
    df_log.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("yyyy", "mm", "dd") \
        .save(log_path)

    # print(f"✅ Log exécution écrit dans {log_path}")


# =========================================================
# Log des erreurs de qualité dans Delta (chemin fixe pour le moment)
# =========================================================
def write_quality_errors(df_errors: DataFrame, table_name: str, zone: str = "internal",
                         base_path: str = PARAMS["log_quality_path"],
                         env: str = PARAMS["env"]):
    if df_errors is None or df_errors.rdd.isEmpty():
        # print(f"Aucune erreur de qualité à enregistrer pour {table_name}")
        return

    today = datetime.today()

    # Suppression des doublons de colonnes
    seen = set()
    cols = []
    for c in df_errors.columns:
        c1 = c.lower()
        if c1 not in seen:
            cols.append(c)
            seen.add(c1)

    df_errors = df_errors.select(*cols)

    # Normalisation de la colonne raw_value
    if "raw_value" in df_errors.columns:
        df_errors = df_errors.withColumn("raw_value", F.col("raw_value").cast("string"))
    else:
        df_errors = df_errors.withColumn("raw_value", F.lit(None).cast("string"))

    df_log = (
        df_errors
        .withColumnRenamed("Table", "table_name")
        .withColumnRenamed("Error", "error_message")
        .withColumnRenamed("Count", "error_count")
        .withColumn("Zone", F.lit(zone))
        .withColumn("Env", F.lit(env))
        .withColumn("ts", F.lit(datetime.now()))
        .withColumn("yyyy", F.lit(today.year))
        .withColumn("mm", F.lit(today.month))
        .withColumn("dd", F.lit(today.day))
    )

    # Create the directory if it does not exist
    try:
        dbutils.fs.ls(base_path)
    except Exception:
        dbutils.fs.mkdirs(base_path)

    # Check if the Delta table exists
    try:
        spark.read.format("delta").load(base_path)
    except Exception:
        # If the Delta table does not exist, create it
        df_log.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("yyyy", "mm", "dd") \
            .save(base_path)
        print(f"✅ Delta table created at {base_path}")

    # Append to the existing Delta table
    df_log.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(base_path)

    # print(f"✅ Log qualité écrit dans {base_path}")


# =========================================================
# Mise en place de chemin racine sans partitions
# =========================================================
def build_output_path(env: str, zone: str, table_name: str, version: str, parts: dict):
    return f"/mnt/wax/{env}/{zone}/{version}/{table_name}"


# =========================================================
# Extraction des parties yyyy/mm/dd à partir du nom de fichier
# =========================================================
def extract_parts_from_filename(fname: str) -> dict:
    """
    Essaie d'extraire yyyy/mm/dd à partir du nom de fichier
    Retourne {} si rien trouvé
    """
    base = os.path.basename(fname)
    m = re.search(r"(?P<yyyy>\d{4})(?P<mm>\d{2})(?P<dd>\d{2})?", base)
    if not m:
        return {}

    parts = {}
    if m.group("yyyy"):
        parts["yyyy"] = int(m.group("yyyy"))
    if m.group("mm"):
        parts["mm"] = int(m.group("mm"))
    if m.group("dd"):
        parts["dd"] = int(m.group("dd"))

    return parts


# =========================================================
# Fonction de validation stricte du nom de fichier
# =========================================================
def validate_filename(fname: str, source_table: str, matched_uri: str, log_quality_path: str):
    """
    On vérifie que le nom de fichier contient une date valide.
    Extrait yyyy/mm/dd à partir du nom de fichier.
    Retourne (df_err_global, None) si erreur,
             (df_err_global, True) si OK
    """

    base = os.path.basename(fname)
    print(f"Analyse du fichier : {base}")

    # Schéma explicite pour le DataFrame d'erreur
    error_schema = StructType([
        StructField("table_name", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("line_id", IntegerType(), True),
        StructField("invalid_value", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("uri", StringType(), True),
    ])

    # Expression régulière corrigée
    m = re.search(r"(?P<yyyy>\d{4})(?P<mm>\d{2})(?P<dd>\d{2})?", base)
    if not m:
        print(f"❌ Fichier rejeté {base} (pattern de date manquant)")
        err_data = [(source_table, base, None, None, None,
                     "Missing / invalid date pattern in filename", matched_uri)]
        err_df = spark.createDataFrame(err_data, error_schema)
        err_df.write.format("delta").mode("append").save(log_quality_path)
        return False

    try:
        yyyy, mm, dd = int(m.group("yyyy")), int(m.group("mm")), int(m.group("dd"))
        datetime(yyyy, mm, dd)  # validation date réelle
        print(f"✅ Fichier accepté {base} : date valide {yyyy}-{mm:02d}-{dd:02d}")
        return True
    except ValueError:
        print(f"❌ Fichier rejeté {base} (date invalide)")
        err_data = [(source_table, base, "filename", None, f"{yyyy}-{mm:02d}-{dd:02d}",
                     "INVALID DATE in filename", matched_uri)]
        err_df = spark.createDataFrame(err_data, error_schema)
        err_df.write.format("delta").mode("append").save(log_quality_path)
        return False


# =========================================================
# Écriture Delta à la racine avec partition yyyy/mm/dd
# =========================================================
def save_delta(
        df,
        path: str,
        mode: str = "append",
        add_ts: bool = False,
        parts: dict = None,
        file_name_received: str = None
):
    """
    Sauvegarde un DataFrame en Delta Lake avec partition yyyy/mm/dd.

    Paramètres
    ----------
    df : DataFrame
        DataFrame Spark à écrire.
    path : str
        Chemin Delta Lake cible.
    mode : str, par défaut "append"
        Mode d'écriture (append, overwrite, etc.).
    add_ts : bool, par défaut False
        Si True, ajoute une colonne FILE_PROCESS_DATE (timestamp courant).
    parts : dict, optionnel
        Dictionnaire contenant éventuellement { "yyyy": int, "mm": int, "dd": int }.
        Si non fourni, on utilise la date du jour.
    file_name_received : str, optionnel
        Nom du fichier source (sert à alimenter la colonne FILE_NAME_RECEIVED).
    """

    today = datetime.today()
    y = int((parts or {}).get("yyyy", today.year))
    m = int((parts or {}).get("mm", today.month))
    d = int((parts or {}).get("dd", today.day))

    # Ajout d’un timestamp de traitement
    if add_ts:
        df = df.withColumn("FILE_PROCESS_DATE", F.current_timestamp())

    # Ajout du nom de fichier sans extension
    if file_name_received:
        base_name = os.path.splitext(os.path.basename(file_name_received))[0]
        df = df.withColumn("FILE_NAME_RECEIVED", F.lit(base_name))

    # Forcer l’ordre des colonnes pour avoir FILE_NAME_RECEIVED et FILE_PROCESS_DATE en premier
    ordered_cols = []
    if "FILE_NAME_RECEIVED" in df.columns:
        ordered_cols.append("FILE_NAME_RECEIVED")
    if "FILE_PROCESS_DATE" in df.columns:
        ordered_cols.append("FILE_PROCESS_DATE")
    other_cols = [c for c in df.columns if c not in ordered_cols]
    df = df.select(ordered_cols + other_cols)

    # Supprimer les colonnes dupliquées (case-insensitive)
    seen, cols = set(), []
    for c in df.columns:
        if c.lower() not in seen:
            cols.append(c)
            seen.add(c.lower())
    df = df.select(*cols)

    # Déterminer les types attendus pour les partitions
    if DeltaTable.isDeltaTable(spark, path):
        schema = spark.read.format("delta").load(path).schema
        type_map = {f.name: f.dataType.simpleString() for f in schema.fields}
        yyyy_type = type_map.get("yyyy", "int")
        mm_type = type_map.get("mm", "int")
        dd_type = type_map.get("dd", "int")
    else:
        yyyy_type, mm_type, dd_type = "int", "int", "int"

    # Ajout / cast des colonnes de partition
    df = (
        df.withColumn("yyyy", F.lit(y).cast(yyyy_type))
        .withColumn("mm", F.lit(m).cast(mm_type))
        .withColumn("dd", F.lit(d).cast(dd_type))
    )

    # Écriture finale en Delta Lake
    (
        df.write.format("delta")
        .option("mergeSchema", "true")
        .mode(mode)
        .partitionBy("yyyy", "mm", "dd")
        .save(path)
    )

    print(f"✅ Delta saved at {path} (mode={mode}, partitions={y}-{m:02d}-{d:02d})")


# =========================================================
# Enregistrement de table Delta dans le metastore Hive
# =========================================================
def register_table_in_metastore(spark, table_name: str, path: str, database: str = "wax_obs",
                                if_exists: str = "ignore"):
    """
    Enregistre la table Delta dans le metastore Hive.
    if_exists peut être :
      - "overwrite"   : écrase la table existante
      - "ignore"      : ne fait rien si la table existe déjà
      - "errorexists" : lève une erreur si la table existe déjà
      - "append"      : ajoute les nouvelles données à la table existante
    """

    full_name = f"{database}.{table_name}"
    exists = any(t.name == table_name for t in spark.catalog.listTables(database))

    if exists and if_exists == "ignore":
        print(f"⚠️ Table {full_name} existe déjà -> ignorée")
        return
    elif exists and if_exists == "errorexists":
        raise Exception(f"❌ Table {full_name} existe déjà -> errorexists")
    elif exists and if_exists == "overwrite":
        print(f"♻️ Table {full_name} existe déjà -> supprimée avant recréation")
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
    elif exists and if_exists == "append":
        print(f"➕ Table {full_name} existe déjà -> append (aucun changement metastore)")
        return

    # Création de la table Delta
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name}
        USING DELTA
        LOCATION '{path}'
    """)
    print(f"✅ Table {full_name} enregistrée sur {path}")


def apply_ingestion_mode(df_raw: DataFrame, column_defs: pd.DataFrame,
                         table_name: str, ingestion_mode: str,
                         env: str = PARAMS["env"], zone: str = "internal",
                         version: str = PARAMS["version"], parts=None,
                         FILE_NAME_RECEIVED=None):
    """
    Applique le mode d’ingestion (overwrite, merge, append).
    """
    path_all = build_output_path(env, zone, f"{table_name}_all", version, parts)
    path_last = build_output_path(env, zone, f"{table_name}_last", version, parts)

    specials = column_defs.copy()
    specials["Is Special lower"] = specials["Is Special"].astype(str)
    merge_keys = specials[specials["Is Special lower"] == "isMergeKey"]["Column Name"].tolist()
    update_cols = specials[specials["Is Special lower"] == "isStartValidity"]["Column Name"].tolist()
    update_col = update_cols[0] if update_cols else None

    imode = (ingestion_mode or "").strip().upper()
    print(f"Ingestion mode : {imode}")

    save_delta(df_raw, path_all, mode="append", add_ts=True,
               parts=parts, FILE_NAME_RECEIVED=FILE_NAME_RECEIVED)
    register_table_in_metastore(spark, f"{table_name}_all", path_all, if_exists="ignore")

    if imode == "FULL_SNAPSHOT":
        save_delta(df_raw, path_last, mode="overwrite",
                   parts=parts, FILE_NAME_RECEIVED=FILE_NAME_RECEIVED)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")

    elif imode == "DELTA_FROM_FLOW":
        save_delta(df_raw, path_last, mode="append", add_ts=True,
                   parts=parts, FILE_NAME_RECEIVED=FILE_NAME_RECEIVED)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")

    elif imode == "DELTA_FROM_NON_HISTORIZED":

        specials = column_defs.copy()
        specials["Is Special lower"] = specials["Is Special"].astype(str)
        merge_keys = specials[specials["Is Special lower"] == "isMergeKey"]["Column Name"].tolist()
        update_cols = specials[specials["Is Special lower"] == "isStartValidity"]["Column Name"].tolist()
        update_col = update_col[0] if update_col else None
        print(f"Colonne de comparaison temporelle : {compare_col}")

        if not merge_keys:
            raise Exception("Impossible d’appliquer DELTA_FROM_NON_HISTORIZED sans merge keys")

        fallback_col = "FILE_PROCESS_DATE"
        compare_col = update_col if update_col else fallback_col

        auto_cols = ["FILE_PROCESS_DATE", "yyyy", "mm", "dd"]

        if not merge_keys:
            print("pas de clé de jointure - fallback overwrite")
            save_delta(df_raw, path_all, mode="overwrite", parts=parts, add_ts=True,
                       FILE_NAME_RECEIVED=FILE_NAME_RECEIVED)
            register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")
        else:
            compare_dtype = str(df_raw.schema[compare_col].dataType)
            if compare_dtype == "StringType":
                df_raw = df_raw.withColumn(compare_col, F.to_timestamp(compare_col))

        df_raw = (
            df_raw
            .withColumn("FILE_PROCESS_DATE", F.current_timestamp())
            .withColumn("yyyy", F.lit(parts["yyyy"]).cast("int"))
            .withColumn("mm", F.lit(parts["mm"]).cast("int"))
            .withColumn("dd", F.lit(parts["dd"]).cast("int"))
            .withColumn("id", F.lit(parts["id"]).cast("int"))
        )

        updates = df_raw.alias("updates")

        if DeltaTable.isDeltaTable(spark, path_last):
            target = DeltaTable.forPath(spark, path_last)
            target_cols = [f.name for f in target.toDF().schema.fields]
        else:
            target_cols = df_raw.columns

        # colonnes à mettre à jour (hors merge key et colonnes auto)
        update_cols_clean = [c for c in df_raw.columns if
                             c in target_cols and c not in merge_keys and c not in auto_cols]
        insert_cols_clean = [c for c in df_raw.columns if c in target_cols and c not in auto_cols]

        update_expr = {c: f"updates.{c}" for c in update_cols_clean}
        insert_expr = {c: f"updates.{c}" for c in insert_cols_clean}

        cond = " AND ".join([f"target.{k}=updates.{k}" for k in merge_keys])

        if deltatable.isDeltaTable(spark, path_last):
            (target.alias("target").merge(updates, cond).whenMatchedUpdate(
                condition=f"updates.{update_col} > target.{update_col}", set=update_expr).whenNotMatchedInsert(
                values=insert_expr).execute())
            print(f"✅ Merge avec comparaison sur {compare_col} et clés {merge_keys}")
            register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")
        else:
            print(f"Création table (pas encore Delta). OVERWRITE {compare_col}")
            save_delta(df_raw, path_last, mode="overwrite", add_ts=True, parts=parts,
                       FILE_NAME_RECEIVED=FILE_NAME_RECEIVED)
            register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="overwrite")

    elif imode == "DELTA_FROM_HISTORIZED":
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, FILE_NAME_RECEIVED=FILE_NAME_RECEIVED)
        register_table_in_metastore(spark, f"{table_name}_LAST", path_last, if_exists="append")

    elif imode == "FULL_KEY_REPLACE":
        if not merge_keys:
            raise Exception("Mode FULL KEY REPLACE pour table {table_name} → merge_keys obligatoire.")

        if deltatable.isDeltaTable(spark, path_last):
            target = DeltaTable.forPath(spark, path_last)
            # construire la condition de suppression
            cond = " OR ".join([
                                   f"{k} IN ({','.join([str(x) for x in df_raw.select(k).distinct().rdd.flatMap(lambda x: x).collect()])})"
                                   for k in merge_keys])

            print(f"🔄 Suppression des lignes existantes sur clés = {merge_keys} ; cond = {cond}")
            target.alias("merge").delete(condition=cond)

            print(f"➕ Insertion des nouvelles lignes")
            save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts,
                       FILE_NAME_RECEIVED=FILE_NAME_RECEIVED)
            register_table_in_metastore(spark, f"{table_name}_LAST", path_last, if_exists="append")
        else:
            print(f"⚠️ Delta table {table_name}_LAST inexistante → FULL KEY REPLACE")
            save_delta(df_raw, path_last, mode="overwrite", add_ts=True, parts=parts,
                       FILE_NAME_RECEIVED=FILE_NAME_RECEIVED)
            register_table_in_metastore(spark, f"{table_name}_LAST", path_last, if_exists="overwrite")
    else:
        print(f"❌ Mode ingestion inconnu : {imode}")
        save_delta(df_raw, path_last, mode="append", add_ts=True, parts=parts, FILE_NAME_RECEIVED=FILE_NAME_RECEIVED)
        register_table_in_metastore(spark, f"{table_name}_last", path_last, if_exists="append")


def check_data_quality(df: DataFrame, table_name: str,
                       merge_keys: list, filename: str = None) -> DataFrame:
    """
    Vérifie la qualité des données (nulls, doublons, colonnes vides).
    """
    if "line_id" not in df.columns:
        df = df.withColumn("line_id", row_number().over(Window.orderBy(monotonically_increasing_id())))

    schema = "table_name string, filename string, line_id long, column_name string, error_message string, error_count int"
    errors_df = spark.createDataFrame([], schema)

    data_columns = [c for c in df.columns if c != "line_id"]
    all_null = all(df.filter(col(c).isNotNull()).count() == 0 for c in data_columns)

    if all_null:
        return spark.createDataFrame(
            [(table_name, filename, None, "ALL_COLUMNS", "FILE_EMPTY_OR_ALL_NULL", 1)],
            schema
        )
    # clés nulles
    for key in merge_keys or []:
        null_key_count = df.filter(col(key).isNull()).count()
        if null_key_count > 0:
            errs = spark.createDataFrame([(table_name, filename, None, key, "NULL_KEY", null_key_count)],
                                         errors_df.schema)
            errors_df = errors_df.unionByName(errs, allowMissingColumns=True)

    # doublons sur les clés
    if merge_keys:
        dup_df = (
            df.groupBy(*merge_keys).count().filter(col("count") > 1)
            .select(
                f"'{table_name}' as table_name",
                f"'{filename}' as filename",
                "NULL as line_id",
                f"'{','.join(merge_keys)}'as column_name",
                "'DUPLICATE_KEY' as error_message",
                "count as error_count"
            )
        )
        errors_df = errors_df.unionByName(dup_df, allowMissingColumns=True)

    # Colonnes entièrement nulles
    subset = file_columns_df[file_columns_df["Delta Table Name"] == table_name]
    for row in subset.iterrows():
        cname = crow["Column Name"]
        is_nullable = str(crow.get("Is Nullable", "true")).strip().lower() == "true"

        if cname in df.columns:
            non_null_count = df.filter(col(c_name).isNotNull()).count()
            if non_null_count == 0 and not is_nullable:
                errs = spark.createDataFrame([(table_name, fileName, None, cname, "COLUMN ALL NULL", total_rows)],
                                             errors_df.schema)
                errors_df = errors_df.unionByName(errs, allowMissingColumns=True)

    # Valeurs nulles ligne par ligne (sauf colonnes entièrement nulles)
    for cname in subset.iterrows():
        cname = crow["Column Name"]
        is_nullable = str(crow.get("Is Nullable", "true")).strip().lower() == "true"
        if cname in df.columns and not is_nullable:
            null_df = (
                df.filter(col(c_name).isNull())
                .withColumn("error_message", lit("NULL_VALUE"))
                .withColumn("error_count", lit(1))
                .withColumn("table_name", lit(table_name))
                .withColumn("column_name", lit(cname)).withColumn("filename", lit(filename))
                .select("table_name", "filename", "column_name", "error_message", "error_count",
                        F.monotonically_increasing_id().alias("line_id"))
            )
            errors_df = errors_df.unionByName(null_df, allowMissingColumns=True)

    return errors_df


# =========================================================
# WAX POC – Partie 5 : Préparation du ZIP & Lecture Excel
# =========================================================

print("📦 Extraction ZIP ...")
dbutils.fs.mkdirs(PARAMS["extract_dir"])

# Convertir le chemin dbfs:/ en chemin local pour extraction
extract_dir_local = PARAMS["extract_dir"].replace("dbfs:", "/dbfs")
os.makedirs(extract_dir_local, exist_ok=True)

with zipfile.ZipFile(PARAMS["zip_path"].replace("dbfs:", "/dbfs"), 'r') as zip_ref:
    zip_ref.extractall(extract_dir_local)

# Lecture du fichier Excel (config WAX)
excel_path = dbutils.widgets.get("excel_path")
excel_path_local = excel_path.replace("dbfs:", "/dbfs") if excel_path.startswith("dbfs:") else excel_path

# Chargement des définitions de colonnes et tables
print(f"📑 Lecture Excel : {excel_path}")
file_columns_df = pd.read_excel(excel_path_local, sheet_name="Field-Column")
file_tables_df = pd.read_excel(excel_path_local, sheet_name="File-Table")


# =========================================================
# WAX POC – Partie 6 : Parsing des dates & Mapping des types
# =========================================================

def parse_date_with_logs(df: DataFrame, cname: str, patterns: list,
                         table_name: str, filename: str,
                         default_date=None):
    """
    Parse une colonne de type date/timestamp avec plusieurs patterns.
    Génère des logs détaillés :
    - EMPTY_DATE
    - INVALID_DATE
    """
    raw_col = F.col(cname)

    # Champs vides -> None
    col_expr = F.when(F.length(F.trim(raw_col)) == 0, F.lit(None)).otherwise(raw_col)

    ts_col = None
    for p in patterns:
        cand = F.expr(f"try_to_timestamp({cname}, '{p}')")
        ts_col = cand if ts_col is None else F.coalesce(ts_col, cand)

    parsed = F.to_date(ts_col)
    parsed_with_default = F.when(parsed.isNull(), F.lit(default_date)).otherwise(parsed)

    # Ajout de la colonne parsée
    df_raw = df.withColumn(cname, parsed_with_default)

    # Logs détaillés
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
        )
        .where(F.col("error_type").isNotNull())
    )

    errs = errs.dropDuplicates(
        ["filename", "table_name", "column_name", "line_id", "raw_value", "error_type"]
    )

    return df_raw, errs


# ---------- Type mapping ----------
type_mapping = {
    "STRING": StringType(),
    "INTEGER": IntegerType(),
    "INT": IntegerType(),
    "FLOAT": FloatType(),
    "DOUBLE": DoubleType(),
    "BOOLEAN": BooleanType(),
    "DATE": DateType(),
}

df_raw_list = []
for _, trow in file_tables_df.iterrows():
    start_table_time = time()
    source_table = trow["Data Table Name"]
    filename_pattern = str(trow.get("Filename Pattern", "")).strip()
    input_format = str(trow.get("Input Format", trow.get("Input Format", "csv"))).strip().lower()
    output_zone = str(trow.get("Output Zone", "internal")).strip().lower()
    ingestion_mode = str(trow.get("Ingestion mode", "")).strip()
    print(f"\nTable: {source_table}")

    # Option du File Table
    trim_flag = parse_bool(trow.get("Trim", True), True)
    delimiter_raw = str(trow.get("Input delimiter", ","))

    del_cols_allowed = parse_bool(trow.get("Delete Columns Allowed", False), False)
    ignore_empty = parse_bool(trow.get("Ignore empty Files", True), True)
    merge_files = parse_bool(trow.get("Merge concomitant file", False), False)
    charset = str(trow.get("Input charset", "UTF-8")).strip() if pd.notnull(
        trow.get("Input charset", "UTF-8")) else "UTF-8"
    if not charset.lower() == "nan":
        charset = "UTF-8"

    invalid_gen = parse_bool(trow.get("Invalid Lines Generate", False), False)

    # RegEx motif et capture date/heure
    rx = re.escape(file_name_pattern)
    rx_yes_time = rx.replace("<yyyy>", r"(\d{4})").replace("<mm>", r"(\d{2})").replace("<dd>", r"(\d{2})").replace(
        "<hhmmss>", r"(\d{6})"))

    rx_yes_time = f"^{rx_yes_time}$"
    pattern_no_time = re.replace("<yyyy>", r"(\d{4})").replace("<mm>", r"(\d{2})").replace("<dd>", r"(\d{2})").replace(
        "_<hhmmss>", "")..replace("<hhmmss>", "")
    pattern_no_time = f"^{pattern_no_time}$"

    # collecte toutes les correspondances dans une liste
    matched = [fi for fi in dbutils.fs.ls(
        PARAMS["extract_dir"] if re.match(rx_yes_time, fi.name) or re.match(pattern_no_time, fi.name)]

    if not matched == 0:
        log_execution(table_name=source_table, filename="N/A", input_format=input_format, ingestion_mode=ingestion_mode,
                      output_format=output_format, output_zone=output_zone, row_count=0, column_count=0,
                      masking_applied=(output_zone == "secret"),
                      error_msg=f"No file matching {file_name_pattern}", status="FAILED", start_time=start_time)
        continue

    files_to_read = matched if merge_files else [matched[0]]
    merge_files = str(trow.get("Merge concomitant file", False).strip().lower() == "true"

    # On va produire (uri, parts) où parts = {yyyy, mm, dd}


    def to_tuple(fi):
        return (fi.path, extract_parts_from_filename(fi.name))


    if merge_files:
        files_to_read = [to_tuple(fi) for fi in matched]
    else:
        files_to_read = [to_tuple(matched[0])]

    # Les options d’entrée (issues de file-table)
    delimiter_raw = str(trow.get("Input delimiter", ",") or ",")
    charset = str(trow.get("Input charset", "UTF-8")).strip() if pd.notnull(
        trow.get("Input charset", "UTF-8")) else "UTF-8"
    if not charset or charset.lower() == "nan":
        charset = "UTF-8"
    ignore_empty = str(trow.get("Ignore Empty Files", True)).strip().lower() == "true"
    invalid_gen = str(trow.get("Invalid Lines Generate", False)).strip().lower() == "true"

    # Délimiteur et header
    try:
        sep_char = normalize_delimiter(delimiter_raw)
    except Exception as e:
        log_execution(source_table=source_table, filename="N/A", input_format=input_format,
                      ingestion_mode=ingestion_mode,
                      output_zone=output_zone, row_count=0, colum_count=0, masking_applied=(output_zone == "secret"),
                      error_msg=f"❌ Delimiter error: {e}", status="FAILED", start_time=start_table_time)
        continue
    # badRecordsPath
    bad_records = None
    if invalid_gen:
        bad_records = f"/mnt/logs/badrecords/{PARAMS['env']}/{source_table}"
        dbutils.fs.mkdirs(bad_records)

    header_mode = str(trow.get("Input header", "") or trow.get("Input header", ""))
    # contenu du header mode est : present, none, use
    user_header, first_line_only = parse_header_mode(header_mode)

    # colonnes attendues
    expected_cols = file_columns_df[file_columns_df["Dalta Table Name"] == source_table]["Column Name"].tolist()

    # imposed schema optionnel
    try:
        subset = file_columns_df[file_columns_df["Delta Table Name"] == source_table].copy()
        if not subset.empty and "Field Order" in subset.columns:
            subset = subset.sort_values(by=["Field Order"])
            fields = [StructField(r["Column Name"], spark_type_from_config(r), True) for _, r in subset.iterrows()]
            imposed_schema = StructType(fields)
    except Exception as e:
        # print("imposed schema error : ", e)
        imposed_schema = None

    # dfs = []
    for matched_uri, parts in files_to_read:
        # print(f"Lecture du fichier valide : {matched_uri}, partitions : {parts}")
        reader = (spark.read
                  .option("sep", sep_char)
                  .option("header", user_header)
                  .option("encoding", charset)
                  .option("ignoreEmptyFiles", ignore_empty)
                  .option("badRecordsPath", bad_records)
                  .option("mode", "PERMISSIVE")
                  .option("enforceSchema", False)
                  .option("columnNameOfCorruptRecord", "_corrupt_record"))

        if bad_records:
            reader = reader.option("badRecordsPath", bad_records)

        if input_format in ["csv", "csv_quote", "csv_quote_ml", "csv_deprecated"]:
            if input_format in ["csv_quote", "csv_quote_ml"]:
                reader = reader.option("quote", '\').option("escape", "\\")
                if input_format == "csv_quote_ml":
                    reader = reader.option("multiline", True)

                # gestion de nos headers
                if imposed_schema is not None:
                    df_file = reader.schema(imposed_schema).csv(matched_uri)
                elif user_header and not first_line_only:
                # HEADER_USE
                    df_file = reader.option("header", "true").csv(matched_uri)
                df_file = df_file.withColumn("line_id",
                                             row_number().over(Window.orderBy(monotonically_increasing_id())))
                df_raw_list.append(df_file)
                elif user_header and first_line_only:
                # FIRST_LINE
                tmp_df = reader.option("header", "false").csv(matched_uri)
                tmp_df = tmp_df.withColumn("_rn",
                                           F.row_number().over(Window.orderBy(monotonically_increasing_id()))).filter(
                    F.col("_rn") > 1).drop("_rn")

                expected_cols = file_columns_df[file_columns_df["Delta Table Name"] == source_table][
                    "Column Name"].tolist()
                if len(expected_cols) != len(tmp_df.columns):
                    raise Exception(f"The expected number of columns ({len(expected_cols)}) "
                                    f"does not match the number of columns found ({len(tmp_df.columns)})")

                df_file = tmp_df.toDF(*expected_cols)
                df_file = df_file.withColumn("line_id",
                                             row_number().over(Window.orderBy(monotonically_increasing_id())))
                df_raw_list.append(df_file)
                # print("FIRST_LINE traité : colonnes remappées depuis Excel")

            else:
                # vide
                expected_cols = file_columns_df[file_columns_df["Delta Table Name"] == source_table][
                    "Column Name"].tolist()
                df_file = reader.option("header", "false").csv(matched_uri).toDF(*expected_cols)
                df_file = df_file.withColumn("line_id",
                                             row_number().over(Window.orderBy(monotonically_increasing_id())))
                df_raw_list.append(df_file)
                # print("Colonnes détectées :", df_file.columns)

        elif input_format == "fixed":
            # fixed-width à partir de Column Size + Field Order
            text_df = spark.read.text(matched_uri)
            pos = 1
            exprs = []
            for _, crow in file_columns_df[file_columns_df["Delta Table Name"] == source_table].sort_values(
                    by=["Field Order"]).iterrows():
                cname = crow["Column Name"]
                size = int(crow.get("Column Size", 0) or 0)
                if size <= 0:
                    size = 1
                exprs.append(F.expr(f"substring(value, {pos}, {size})").alias(cname))
                pos += size
            df_raw = text_df.select(*exprs)

        else:
            log_execution(
                table_name=source_table,
                filename="N/A",
                input_format=input_format,
                ingestion_mode=ingestion_mode,
                output_zone=output_zone,
                row_count=0,
                column_count=0,
                masking_applied=(output_zone == "secret"},
                error_msg=f"Unsupported format {input_format}",
            status = "FAILED",
            start_time = start_time
            )
            continue

        df_raw = df_file

        if not del_cols_allowed and expected_cols:
            missing = [c for c in expected_cols if c not in df_raw.columns]
            if missing:
                # Log des colonnes manquantes, détaillé par colonne
                df_missing = spark.createDataFrame(
                    [(os.path.basename(matched_uri), m, "MISSING_COLUMN") for m in missing],
                    "filename STRING, column_name STRING, error_type STRING"
                )
                write_quality_errors(df_missing, source_table, zone=output_zone)
                log_execution(
                    table_name=source_table,
                    filename=os.path.basename(matched_uri),
                    input_format=input_format,
                    ingestion_mode=ingestion_mode,
                    output_zone=output_zone,
                    row_count=0,
                    column_count=len(df_raw.columns),
                    masking_applied=(output_zone == "secret"),
                    error_msg=f"Missing columns {missing} in your file",
                    status="FAILED",
                    start_time=start_time
                )
                continue

        # Ajouter colonnes manquantes si autorisé / ou normaliser
        if expected_cols:
            for c in expected_cols:
                if c not in df_raw.columns:
                    df_raw = df_raw.withColumn(c, lit(None).cast(StringType()))
                else:
                    df_raw = df_raw.withColumn(c, df_raw[c].cast(StringType()))

        # Tolérance lignes corrompues
        total_rows = df_raw.count()
        corrupt_rows = df_raw.filter(
            col("_corrupt_record").isNotNull()).count() if "_corrupt_record" in df_raw.columns else 0

        rej_tol = parse_tolerance(trow.get("Rejected line per file tolerance", "10%"), total_rows)

        if corrupt_rows > rej_tol:
            log_execution(
                table_name=source_table,
                filename=os.path.basename(matched_uri),
                input_format=input_format,
                ingestion_mode=ingestion_mode,
                output_zone=output_zone,
                row_count=0,
                column_count=len(df_raw.columns),
                masking_applied=(output_zone == "secret"),
                error_msg=f"Too many corrupted lines ({corrupt_rows} > {rej_tol})",
                status="FAILED",
                start_time=start_time
            )

        if "_corrupt_record" in df_raw.columns:
            df_raw = df_raw.drop("_corrupt_record")

        # Trim global table
        if trim_flag:
            for c in df_raw.columns:
                df_raw = df_raw.withColumn(c, F.trim(df_raw[c]))

        # Début : Typage + transformation + validation
        total_rows = df_raw.count()
        invalid_flags = []

        for _, crow in file_columns_df[file_columns_df["Delta Table Name"] == source_table].iterrows():
            cname = crow["Column Name"]
            if cname not in df_raw.columns:
                continue

            stype = spark_type_from_config(crow)
            tr_type = str(crow.get("Transformation Type", "")).strip().lower()
            tr_patt = str(crow.get("Transformation pattern", "")).strip()
            regex_repl = str(crow.get("Regex replacement", "")).strip()
            is_nullable = parse_bool(crow.get("Is Nullable", "False"), False)
            err_action = str(crow.get("Error action", "ICI_DRIVEN")).strip().upper()
            if err_action in ["", "NAN", "NONE", "NULL"]:
                err_action = "ICI_DRIVEN"

            enum_list = str(crow.get("Enumeration list", "")).strip()
            ftype = str(crow.get("Field type", "")).strip().upper()
            default_inv = str(crow.get("Default when invalid", "")).strip()

            # Transformations simples
            if tr_type == "uppercase":
                df_raw = df_raw.withColumn(cname, F.upper(col(cname)))
            elif tr_type == "lowercase":
                df_raw = df_raw.withColumn(cname, F.lower(col(cname)))
            elif tr_type == "regex" and tr_patt:
                df_raw = df_raw.withColumn(cname,
                                           F.regexp_replace(F.col(cname), tr_patt, regex_repl if regex_repl else ""))

            # Cast sécurisé
            df_raw = df_raw.withColumn(f"{cname}_cast", F.expr(f"try_cast({cname} as {stype.simpleString()})"))

            # Remplacement par NULL si cast échoué
            df_raw = df_raw.withColumn(
                cname,
                F.when(F.col(f"{cname}_cast").isNotNull(), F.col(f"{cname}_cast")).otherwise(F.lit(None))
            ).drop(f"{cname}_cast")

            # Détection des erreurs de type (valeurs devenues NULL après cast)
            if not is_nullable:
                invalid_cond = F.col(cname).isNull()
                invalid_count = df_raw.filter(invalid_cond).count()

                # Application de l'action d'erreur
                tolerance = parse_tolerance(crow.get("Rejected line per file tolerance", "10%"), total_rows)

                if err_action == "REJECT":
                    if invalid_count > 0:
                        errs = spark.createDataFrame(
                            [(os.path.basename(matched_uri), source_table, cname, invalid_count, "REJECT")],
                            "filename STRING, table_name STRING, column_name STRING, error_count INT, error_type STRING"
                        )
                        write_quality_errors(errs, source_table, zone=output_zone)
                        df_raw = df_raw.filter(~invalid_cond)
                        print(f"{invalid_count} lignes rejetées pour {cname}")
                        display(df_raw)

                elif err_action == "ICT_DRIVEN":
                    # Ajout d’un flag d’erreur pour cette colonne
                    flag_col = f"{cname}_invalid"
                    df_raw = df_raw.withColumn(flag_col, F.when(invalid_cond, F.lit(1)).otherwise(F.lit(0)))
                    invalid_flags.append(flag_col)

                    if total_rows > 0 and (invalid_count / float(total_rows)) > tolerance:
                        print(f"Trop d’erreurs sur {cname} -> abort ICT_DRIVEN")
                        errs_summary = spark.createDataFrame(
                            [(os.path.basename(matched_uri), source_table, cname, invalid_count, "ICT_DRIVEN_ABORT")],
                            "filename STRING, table_name STRING, column_name STRING, error_count INT, error_type STRING"
                        )
                        write_quality_errors(errs_summary, source_table, zone=output_zone)
                        df_raw = spark.createDataFrame([], df_raw.schema)
                        break

                    elif invalid_count > 0:
                        errs_detailed = df_raw.filter(invalid_cond).withColumn("line_id",
                                                                               monotonically_increasing_id()).select(
                            F.lit(os.path.basename(matched_uri)).alias("filename"),
                            F.lit(source_table).alias("table_name"),
                            F.lit(cname).alias("column_name"),
                            F.col("line_id"),
                            F.col(cname).alias("raw_value"),
                            F.lit("ICT_DRIVEN").alias("error_type")
                        )
                        write_quality_errors(errs_detailed, source_table, zone=output_zone)

                elif err_action == "LOG_ONLY":
                    if invalid_count > 0:
                        errs = spark.createDataFrame(
                            [(os.path.basename(matched_uri), source_table, cname, invalid_count, "LOG_ONLY")],
                            "filename STRING, table_name STRING, column_name STRING, error_count INT, error_type STRING"
                        )
                        write_quality_errors(errs, source_table, zone=output_zone)

            # Parsing Date/timestamp
            if isinstance(stype, (DateType, TimestampType)):
                patterns = []
                if tr_patt:
                    patterns.append(tr_patt)
                for p in ["dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy", "yyyy-MM-dd HH:mm:ss", "yyyyMMddHHmmss",
                          "yyyyMMdd", "yyyy-MM-dd'T'HH:mm:ss"]:
                    if p not in patterns:
                        patterns.append(p)

                df_raw, errs = parse_date_with_logs(
                    df_raw, cname, [p for p in patterns if 'T' not in p],
                    source_table, os.path.basename(matched_uri),
                    default_date=default_inv
                )
                # Enregistrer les erreurs si présentes
                write_quality_errors(errs, source_table, zone=output_zone)

            else:
                # Remplacement virgule décimale par point
                df_raw = df_raw.withColumn(cname, F.regexp_replace(F.col(cname), ",", "."))

        # Rejet ligne par ligne pour ICT_DRIVEN (après la boucle)
        if invalid_flags:
            df_raw = df_raw.withColumn("invalid_column_count", sum([F.col(c) for c in invalid_flags]))
            max_invalid_per_line = int(len(invalid_flags) * 0.1)

            df_raw_valid = df_raw.filter(F.col("invalid_column_count") <= max_invalid_per_line)
            df_raw_invalid = df_raw.filter(F.col("invalid_column_count") > max_invalid_per_line)

            df_raw_invalid = df_raw_invalid.withColumn("line_id", monotonically_increasing_id())
            write_quality_errors(df_raw_invalid, source_table, zone=output_zone)

            df_raw = df_raw_valid.drop("invalid_column_count", *invalid_flags)

        # =============================
        # Data quality global (merge key)
        # =============================
        specials = file_columns_df[file_columns_df["Delta Table Name"] == source_table]["Column Name"].copy()

        if "Is Special" in specials.index:
            specials["Is Special lower"] = specials["Is Special"].astype(str).lower()
            merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist()
        else:
            merge_keys = []

        for df_raw in df_raw_list:
            df_err_global = check_data_quality(
                df_raw,
                source_table,
                merge_keys,
                filename=os.path.basename(matched_uri)
            )

            column_errors = {}
            for _, col_def in file_columns_df[file_columns_df["Delta Table Name"] == source_table].iterrows():
                cname = str(col_def.get("Column Name")).strip()
                expected_type = str(col_def.get("Field type")).strip().upper()
                is_nullable = str(col_def.get("Is Nullable", "true")).strip().lower() == "true"

                if cname in df_raw.columns and expected_type in type_mapping:
                    # Cast sécurisé
                    safe_cast = df_raw.withColumn(f"{cname}_cast", expr(f"try_cast({cname} as {expected_type})"))

                    # Détection des erreurs de type
                    invalid_rows = safe_cast.filter(
                        F.col(f"{cname}_cast").isNull() & F.col(cname).isNotNull() & (F.col(cname) != "")))

                    if invalid_rows.count() > 0:
                        if
                    "line_id" not in invalid_rows.columns:
                    window = Window.orderBy(F.monotonically_increasing_id())
                    invalid_rows = invalid_rows.withColumn("line_id", F.row_number().over(window))

                    invalid_samples_df = invalid_rows.select("line_id", cname)
                    invalid_samples = invalid_samples_df.collect()

                    # Ajout au DataFrame global ligne par ligne
                    err = spark.createDataFrame(
                    [(source_table, os.path.basename(matched_uri), r["line_id"], cname,
                      f"TYPE MISMATCH : Expected type {expected_type}, but found : [line {r['line_id']}, value={r[cname]}]"
                      ]
                      for r in invalid_samples],
                    ["table_name", "filename", "line_id", "column_name", "error_message"]
                )
                df_err_global = df_err_global.unionByName(err, allowMissingColumns=True)

                # Remplacement de la colonne originale par la colonne castée
                df_raw = safe_cast.drop(cname).withColumnRenamed(f"{cname}_cast", cname)

                # ============================
                # Contrôle de la nullabilité
                # ============================
                if not is_nullable:
                    null_rows = df_raw.filter(F.col(cname).isNull())
                if null_rows.count() > 0:
                    if
                "line_id" not in null_rows.columns:
                window = Window.orderBy(F.monotonically_increasing_id())
                null_rows = null_rows.withColumn("line_id", F.row_number().over(window))

                null_samples_df = null_rows.select("line_id", cname)
                null_samples = null_samples_df.collect()

                samples_str = [f"[line {r['line_id']}, value={r[cname]}]" for r in null_samples]
                error_message = (
                f"Null value detected [not authorized], ICT count is incremented : "
                + " ".join(samples_str)
            )

            # Ajout au dictionnaire regroupé
            if cname not in column_errors:
                column_errors[cname] = []
            column_errors[cname].append(error_message)

            # Ajout au DataFrame global ligne par ligne
            err = spark.createDataFrame(
            [
                (
                    source_table,
                    os.path.basename(matched_uri),
                    r["line_id"],
                    cname,
                    f"Null value detected [not authorized], ICT count is incremented : [line {r['line_id']}, value={r[cname]}]"
                )
                for r in null_samples
            ],
            ["table_name", "filename", "line_id", "column_name", "error_message"]
        )
        df_err_global = df_err_global.unionByName(err, allowMissingColumns=True)

        print("Record of possible errors detected in the file :")
        display(df_err_global)

        # ============================
        # Écriture des logs qualité
        # ============================
        write_quality_errors(df_err_global, source_table, zone=output_zone)

        # ============================
        # Ingestion (wax all, wax last)
        # ============================
        apply_ingestion_mode(
df_raw,
table_name = source_table,
ingestion_mode = str(ingestion_mode),
column_defs = file_columns_df[file_columns_df["Delta Table Name"] == source_table],
env = PARAMS["env"], zone = output_zone, version = PARAMS["version"],
parts = parts, FILE_NAME_RECEIVED = os.path.basename(matched_uri)
)

# ============================
# Harmonisation des colonnes df_err_global
# ============================
if "error" in df_err_global.columns and "error_message" not in df_err_global.columns:
    df_err_global = df_err_global.withColumnRenamed("error", "error_message")
if "count" in df_err_global.columns and "error_count" not in df_err_global.columns:
    df_err_global = df_err_global.withColumnRenamed("count", "error_count")

# ============================
# Résumé console + log exécution
# ============================
total_rows_after = df_raw.count()

# On filtre uniquement les erreurs Invalid
if "error_message" in df_err_global.columns:
    inv_only = df_err_global.filter(F.col("error_message").contains("Invalid"))
else:
    inv_only = df_err_global.filter(F.col("Error").contains("Invalid"))

if "error_count" in df_err_global.columns:
    corrupt_rows_only = inv_only.agg(F.sum("error_count").alias("s"))
else:
    corrupt_rows_only = inv_only.agg(F.sum("Count").alias("s"))

# Valeur du nombre de lignes corrompues
corrupt_rows_val = corrupt_rows_only.collect()[0]["s"] if corrupt_rows_only.count() > 0 else 0

# Calcul des lignes nettoyées
cleaned_rows = (total_rows or total_rows_after) - (corrupt_rows_val or 0)

# Résumé final + log
print_summary(
table_name = source_table,
filename = os.path.basename(matched_uri),
total_rows = (total_rows, total_rows_after),
corrupt_rows = corrupt_rows_val,
anomalies_total = df_err_global.count(),
cleaned_rows = cleaned_rows,
errors_df = df_err_global
)

# ============================
# Log exécution
# ============================
log_execution(table_name = source_table,
filename = os.path.basename(matched_uri),
input_format = input_format,
ingestion_mode = ingestion_mode,
output_zone = output_zone,
row_count = total_rows,
column_count = len(df_raw.columns),
masking_applied = (output_zone == "secret"),
error_msg = str(df_err_global.count()),
status = "SUCCESS",
error_count = df_err_global.count(),
start_time = start_time
)

# ============================
# Vues sur le dashboard
# Observabilité des logs
# ============================
df_logs = spark.read.format("delta").load(PARAMS["log_exec_path"])
df_logs.createOrReplaceTempView("wax_execution_logs_delta")
print("📊 Full collection of raw logs")
display(spark.sql("""
    SELECT table_name, input_format, filename, row_count, column_count, status,
           error_count, env, duration, log_ts
    FROM wax_execution_logs_delta
    ORDER BY log_ts DESC
    LIMIT 20
"""))

# ============================
# Vérification existence logs qualité
# ============================
try:
    dbutils.fs.ls(PARAMS["log_quality_path"])
    path_exists = True
except Exception:
    path_exists = False

if not path_exists:
    print(f"❌ Le chemin {PARAMS['log_quality_path']} n'existe pas, aucun log qualité à afficher.")
    df_quality = spark.createDataFrame([],
                                       "Table_name string, Error_message string, Error_count int, Zone string, Env string, log_ts timestamp, yyyy int, mm int, dd int"
                                       )
else:
    df_quality = spark.read.format("delta").load(PARAMS["log_quality_path"])
    df_quality.createOrReplaceTempView("wax_data_quality_errors")
    print("📊 File structure quality log")
    display(spark.sql("""
        SELECT table_name, filename, column_name, error_message, error_count, Zone, Env, log_ts
        FROM wax_data_quality_errors
        WHERE error_message IS NOT NULL
    """))

print("✅ WAX Processing Completed")









