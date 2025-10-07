"""
file_processor.py
-----------------
Gestion du cycle de traitement des fichiers :
1. Extraction du ZIP
2. Lecture du fichier Excel (configuration tables/colonnes)
3. Lecture, typage et nettoyage des CSV
4. Validation et logs
5. Appel de l’ingestion Delta via ingestion.apply_ingestion_mode
"""

import os
import zipfile
import time
import pandas as pd
from functools import reduce
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from ingestion import apply_ingestion_mode
from validators import check_data_quality, validate_filename
from logging_manager import log_execution, write_quality_errors
from utils import parse_bool, normalize_delimiter, parse_header_mode, parse_tolerance, spark_type_from_config


# ==============================================================
# 1. EXTRACTION DU ZIP
# ==============================================================

def extract_zip_file(zip_path: str, extract_dir: str):
    """
    Extrait le fichier ZIP dans un répertoire temporaire.
    """
    print(f"📦 Extraction du ZIP : {zip_path}")
    os.makedirs(extract_dir, exist_ok=True)

    zip_path_local = zip_path.replace("dbfs:", "/dbfs") if zip_path.startswith("dbfs:") else zip_path
    extract_dir_local = extract_dir.replace("dbfs:", "/dbfs") if extract_dir.startswith("dbfs:") else extract_dir

    with zipfile.ZipFile(zip_path_local, 'r') as zip_ref:
        zip_ref.extractall(extract_dir_local)

    print(f"✅ Extraction terminée → {extract_dir_local}")
    return extract_dir_local


# ==============================================================
# 2. LECTURE DU FICHIER EXCEL DE CONFIGURATION
# ==============================================================

def load_excel_config(excel_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Charge les feuilles Excel : Field-Column et File-Table.
    """
    print(f"📑 Lecture Excel : {excel_path}")

    excel_path_local = excel_path.replace("dbfs:", "/dbfs") if excel_path.startswith("dbfs:") else excel_path

    try:
        file_columns_df = pd.read_excel(excel_path_local, sheet_name="Field-Column")
        file_tables_df = pd.read_excel(excel_path_local, sheet_name="File-Table")
        print(f"✅ Configurations chargées : {len(file_tables_df)} tables, {len(file_columns_df)} colonnes")
        return file_tables_df, file_columns_df
    except Exception as e:
        raise RuntimeError(f"❌ Erreur lecture Excel : {e}")


# ==============================================================
# 3. LECTURE & TRAITEMENT DES FICHIERS
# ==============================================================

def process_files(
    spark: SparkSession,
    params: dict,
    file_tables_df: pd.DataFrame,
    file_columns_df: pd.DataFrame
):
    """
    Parcourt toutes les tables définies dans le fichier Excel et applique le pipeline complet.
    """
    extract_dir = params["extract_dir"]
    log_exec_path = params["log_exec_path"]
    log_quality_path = params["log_quality_path"]

    print(f"\n🚀 Début du traitement global : {len(file_tables_df)} tables")

    for _, trow in file_tables_df.iterrows():
        start_time = time.time()
        table_name = trow["Delta Table Name"]
        print(f"\n{'=' * 70}\n📋 Traitement table : {table_name}\n{'=' * 70}")

        # Lecture paramètres File-Table
        ingestion_mode = str(trow.get("Ingestion mode", "FULL_SNAPSHOT")).strip().upper()
        input_format = str(trow.get("Input Format", "csv")).strip().lower()
        delimiter_raw = str(trow.get("Input delimiter", ","))
        ignore_empty = parse_bool(trow.get("Ignore empty Files", True))
        merge_files_flag = parse_bool(trow.get("Merge concomitant file", False))
        filename_pattern = str(trow.get("Filename Pattern", "")).strip()

        # Liste des fichiers
        try:
            all_files = dbutils.fs.ls(extract_dir)
        except Exception as e:
            print(f"❌ Erreur listing {extract_dir}: {e}")
            log_execution(spark, params, table_name, "N/A", ingestion_mode, 0, 0, False, 1, str(e), "FAILED", start_time)
            continue

        matched = [fi for fi in all_files if filename_pattern in fi.name]
        if not matched:
            print(f"⚠️ Aucun fichier trouvé pour le pattern : {filename_pattern}")
            log_execution(spark, params, table_name, "N/A", ingestion_mode, 0, 0, False, 1, "No file match", "FAILED", start_time)
            continue

        # Gestion multi-fichiers
        files_to_read = matched if merge_files_flag else [matched[0]]

        # Lecture CSV / Fixed / autre
        try:
            sep = normalize_delimiter(delimiter_raw)
        except Exception as e:
            print(f"❌ Erreur délimiteur : {e}")
            log_execution(spark, params, table_name, "N/A", ingestion_mode, 0, 0, False, 1, str(e), "FAILED", start_time)
            continue

        df_list = []
        for fi in files_to_read:
            fname = fi.name
            print(f"📄 Lecture fichier : {fname}")

            # Extraction des parties (yyyy, mm, dd)
            parts = validate_filename(fname, table_name, fi.path, log_quality_path)
            if not parts:
                print(f"❌ Fichier {fname} rejeté (nom invalide)")
                continue

            imposed_schema = None
            subset = file_columns_df[file_columns_df["Delta Table Name"] == table_name]
            if not subset.empty and "Field Order" in subset.columns:
                subset = subset.sort_values(by=["Field Order"])
                fields = [StructField(r["Column Name"], spark_type_from_config(r), True) for _, r in subset.iterrows()]
                imposed_schema = StructType(fields)

            reader = (
                spark.read
                .option("sep", sep)
                .option("header", "true")
                .option("ignoreEmptyFiles", str(ignore_empty).lower())
                .option("mode", "PERMISSIVE")
            )

            df_raw = reader.csv(fi.path, schema=imposed_schema)
            df_raw = df_raw.dropna(how="all")

            # Nettoyage colonnes
            expected_cols = subset["Column Name"].tolist()
            for c in expected_cols:
                if c not in df_raw.columns:
                    df_raw = df_raw.withColumn(c, F.lit(None).cast(StringType()))

            # Data Quality
            specials = file_columns_df[file_columns_df["Delta Table Name"] == table_name].copy()
            specials["Is Special lower"] = specials["Is Special"].astype(str).str.lower()
            merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist()

            df_err = check_data_quality(df_raw, table_name, merge_keys, fname, file_columns_df)
            if not df_err.rdd.isEmpty():
                write_quality_errors(spark, df_err, params, table_name, fname)

            df_list.append(df_raw)

        if not df_list:
            print(f"⚠️ Aucun DataFrame traité pour {table_name}")
            continue

        df_merged = reduce(DataFrame.unionByName, df_list)
        total_rows = df_merged.count()

        # Application ingestion
        apply_ingestion_mode(
            spark,
            df_raw=df_merged,
            column_defs=file_columns_df[file_columns_df["Delta Table Name"] == table_name],
            table_name=table_name,
            ingestion_mode=ingestion_mode,
            env=params["env"],
            zone=trow.get("Output Zone", "internal"),
            version=params["version"],
            parts={"yyyy": datetime.today().year, "mm": datetime.today().month, "dd": datetime.today().day},
            FILE_NAME_RECEIVED=fname
        )

        # Log exécution final
        log_execution(
            spark,
            params,
            table_name,
            fname,
            ingestion_mode,
            total_rows,
            len(df_merged.columns),
            False,
            0,
            "SUCCESS",
            "SUCCESS",
            start_time
        )

    print("\n✅ Tous les fichiers traités avec succès.")
