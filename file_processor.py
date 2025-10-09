# src/file_processor.py
# --------------------------------------------------------------------------------------
# Gestion de l’extraction ZIP, lecture du fichier Excel de configuration,
# et orchestration du traitement fichier par fichier.
# Compatible Databricks et local (VSCode).
# --------------------------------------------------------------------------------------

import os, re, zipfile, time
import pandas as pd
from datetime import datetime
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructType, StructField

from environment import (
    is_databricks, to_local_path, ensure_dir, resolve_paths
)
from utils import (
    parse_bool, normalize_delimiter, parse_tolerance,
    deduplicate_columns, safe_count, trim_all
)
from validator import (
    check_data_quality, parse_date_with_logs, validate_column_types, print_summary
)
from delta_manager import (
    build_output_path, save_delta_table, register_table_in_metastore
)
from logger_manager import (
    log_execution, write_quality_errors
)
from ingestion import apply_ingestion_mode


# ======================================================================================
# 1️⃣ - EXTRACTION ZIP
# ======================================================================================
def extract_zip_file(zip_path: str, extract_dir: str) -> None:
    """
    Extrait un fichier ZIP vers un répertoire cible.
    """
    zip_local = to_local_path(zip_path)
    extract_local = to_local_path(extract_dir)

    print(f"📦 Extraction ZIP : {zip_local}")
    ensure_dir(extract_local)

    with zipfile.ZipFile(zip_local, 'r') as zip_ref:
        zip_ref.extractall(extract_local)

    print(f"✅ ZIP extrait dans {extract_local}")


# ======================================================================================
# 2️⃣ - LECTURE CONFIGURATION EXCEL
# ======================================================================================
def load_excel_config(excel_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Charge les deux feuilles Excel : Field-Column et File-Table.
    """
    excel_local = to_local_path(excel_path)
    print(f"📑 Lecture du fichier Excel : {excel_local}")

    file_columns_df = pd.read_excel(excel_local, sheet_name="Field-Column")
    file_tables_df = pd.read_excel(excel_local, sheet_name="File-Table")

    print(f"✅ Configuration chargée : {len(file_tables_df)} tables, {len(file_columns_df)} colonnes")
    return file_tables_df, file_columns_df


# ======================================================================================
# 3️⃣ - ORCHESTRATION DU PIPELINE
# ======================================================================================
def process_files(spark: SparkSession, params: dict) -> None:
    """
    Orchestration du pipeline WAX :
      - Extraction ZIP
      - Lecture Excel
      - Lecture et traitement des fichiers
      - Validation / Sauvegarde / Logging
    """

    # Normaliser les chemins
    params = resolve_paths(params)
    env = params["env"]
    version = params["version"]
    wax_base = params["wax_base"]

    # Extraction ZIP
    extract_zip_file(params["zip_path"], params["extract_dir"])

    # Lecture Excel
    file_tables_df, file_columns_df = load_excel_config(params["excel_path"])

    # Boucle principale : traitement table par table
    for _, trow in file_tables_df.iterrows():
        start_time = time.time()

        table_name = str(trow["Delta Table Name"]).strip()
        filename_pattern = str(trow.get("Filename Pattern", "")).strip()
        input_format = str(trow.get("Input Format", "csv")).strip().lower()
        ingestion_mode = str(trow.get("Ingestion mode", "")).strip()
        output_zone = str(trow.get("Output Zone", "internal")).strip().lower()

        print(f"\n{'='*90}\n📋 Table : {table_name}\n{'='*90}")

        # Lecture des options
        delimiter_raw = str(trow.get("Input delimiter", ","))
        ignore_empty = parse_bool(trow.get("Ignore empty Files", True))
        trim_flag = parse_bool(trow.get("Trim", True))

        # Charger les fichiers correspondant au pattern
        rx = re.compile(filename_pattern.replace("<yyyy>", r"\d{4}").replace("<mm>", r"\d{2}").replace("<dd>", r"\d{2}"))
        extract_dir_local = to_local_path(params["extract_dir"])

        files_found = [
            os.path.join(extract_dir_local, f)
            for f in os.listdir(extract_dir_local)
            if rx.match(f)
        ]

        if not files_found:
            print(f"⚠️ Aucun fichier trouvé pour le pattern : {filename_pattern}")
            log_execution(
                spark, table_name, "N/A", input_format, ingestion_mode, output_zone,
                params["log_exec_path"], env,
                error_msg="No matching file", status="FAILED", start_time=start_time
            )
            continue

        print(f"✅ {len(files_found)} fichier(s) trouvé(s).")

        # Lire le premier fichier correspondant
        file_path = files_found[0]
        parts = extract_date_parts(file_path)
        df_raw = read_file_to_df(spark, file_path, input_format, delimiter_raw, ignore_empty)

        if trim_flag:
            df_raw = trim_all(df_raw)

        total_rows = safe_count(df_raw)
        print(f"📄 {total_rows} lignes lues depuis {file_path}")

        # Validation doublons
        merge_keys = get_merge_keys(file_columns_df, table_name)
        df_dup = check_data_quality(df_raw, table_name, merge_keys, filename=os.path.basename(file_path))

        # Validation typage
        column_defs = file_columns_df[file_columns_df["Delta Table Name"] == table_name].to_dict(orient="records")
        from validator import TYPE_MAPPING  # local import to avoid circular
        df_valid, df_type_errors = validate_column_types(df_raw, table_name, os.path.basename(file_path), column_defs, TYPE_MAPPING)

        # Fusion des erreurs
        df_errors = df_dup.union(df_type_errors) if not df_dup.rdd.isEmpty() else df_type_errors

        # Sauvegarde logs qualité
        write_quality_errors(spark, df_errors, table_name, params["log_quality_path"], env)

        # Sauvegarde Delta
        output_path = build_output_path(env, output_zone, table_name, version, wax_base)
        save_delta_table(spark, df_valid, output_path, mode="append", add_ts=True, file_name_received=file_path)

        # Enregistrement métastore / Unity Catalog
        register_table_in_metastore(spark, table_name, output_path)

        # Logging exécution
        log_execution(
            spark,
            table_name,
            os.path.basename(file_path),
            input_format,
            ingestion_mode,
            output_zone,
            params["log_exec_path"],
            env,
            row_count=total_rows,
            column_count=len(df_valid.columns),
            error_count=df_errors.count() if not df_errors.rdd.isEmpty() else 0,
            start_time=start_time
        )

        # Résumé
        print_summary(
            table_name,
            os.path.basename(file_path),
            total_rows,
            0,
            df_errors.count() if not df_errors.rdd.isEmpty() else 0,
            total_rows
        )

        print(f"✅ Table {table_name} traitée avec succès.\n")


# ======================================================================================
# 4️⃣ - OUTILS INTERNES
# ======================================================================================
def extract_date_parts(filename: str) -> dict:
    """
    Extrait les composantes (yyyy, mm, dd) du nom de fichier si présentes.
    """
    m = re.search(r"(?P<yyyy>\d{4})[-_]?(?P<mm>\d{2})[-_]?(?P<dd>\d{2})", os.path.basename(filename))
    if not m:
        return {}
    parts = {}
    if m.group("yyyy"): parts["yyyy"] = int(m.group("yyyy"))
    if m.group("mm"): parts["mm"] = int(m.group("mm"))
    if m.group("dd"): parts["dd"] = int(m.group("dd"))
    return parts


def get_merge_keys(file_columns_df: pd.DataFrame, table_name: str) -> list[str]:
    """
    Récupère les colonnes marquées 'IsMergeKey' dans Field-Column.
    """
    subset = file_columns_df[file_columns_df["Delta Table Name"] == table_name]
    if "Is Special" not in subset.columns:
        return []
    return subset[subset["Is Special"].astype(str).str.lower() == "ismergekey"]["Column Name"].tolist()


def read_file_to_df(
    spark: SparkSession,
    file_path: str,
    input_format: str,
    delimiter_raw: str,
    ignore_empty: bool = True
) -> DataFrame:
    """
    Lecture générique de fichier CSV/FIXED.
    """
    sep_char = normalize_delimiter(delimiter_raw)

    reader = (
        spark.read
        .option("sep", sep_char)
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("ignoreEmptyFiles", str(ignore_empty).lower())
        .option("mode", "PERMISSIVE")
    )

    if input_format in ["csv", "csv_quote", "csv_quote_ml"]:
        if input_format in ["csv_quote", "csv_quote_ml"]:
            reader = reader.option("quote", '"').option("escape", "\\")
        if input_format == "csv_quote_ml":
            reader = reader.option("multiline", "true")
        df = reader.csv(file_path)
    elif input_format == "fixed":
        text_df = spark.read.text(file_path)
        df = text_df.withColumnRenamed("value", "raw_line")
    else:
        raise ValueError(f"Format non supporté : {input_format}")

    df = deduplicate_columns(df)
    return df
