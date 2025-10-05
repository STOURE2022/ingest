import os
import re
import zipfile
from pathlib import Path
from typing import Tuple, Optional, List

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, row_number, monotonically_increasing_id

from validators import spark_type_from_config, parse_dates_with_logs, check_data_quality, print_summary
from utils import parse_bool, normalize_delimiter, parse_header_mode, parse_tolerance, extract_parts_from_filename, ensure_dbfs_local
from logging_manager import log_execution, write_quality_errors
from ingestion import apply_ingestion_mode
from pyspark.sql.types import StructType, StructField

TYPE_MAPPING = {
    "STRING": "string",
    "INTEGER": "int",
    "INT": "int",
    "FLOAT": "float",
    "DOUBLE": "double",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
}

def extract_zip_file(zip_path: str, extract_dir: str, mode: str = "local") -> None:
    print(f"📦 Extraction ZIP : {zip_path} -> {extract_dir}")
    zip_local = ensure_dbfs_local(zip_path) if mode == "databricks" else zip_path
    out_dir = ensure_dbfs_local(extract_dir) if mode == "databricks" else extract_dir
    os.makedirs(out_dir, exist_ok=True)
    with zipfile.ZipFile(zip_local, "r") as z:
        z.extractall(out_dir)
    print("✓ ZIP extrait")

def load_excel_config(excel_path: str, mode: str = "local") -> Tuple[pd.DataFrame, pd.DataFrame]:
    print(f"📑 Lecture Excel : {excel_path}")
    excel_local = ensure_dbfs_local(excel_path) if mode == "databricks" else excel_path
    file_columns_df = pd.read_excel(excel_local, sheet_name="Field-Column")
    file_tables_df  = pd.read_excel(excel_local, sheet_name="File-Table")
    print(f"✓ Config chargée : {len(file_columns_df)} colonnes, {len(file_tables_df)} tables")
    return file_columns_df, file_tables_df

def _drop_first_row(df: DataFrame) -> DataFrame:
    w = Window.orderBy(monotonically_increasing_id())
    return df.withColumn("_rn", row_number().over(w)).filter(F.col("_rn") > 1).drop("_rn")

def read_csv_file(spark: SparkSession, file_path: str, input_format: str,
                  sep_char: str, charset: str, use_header: bool, first_line_only: bool,
                  ignore_empty: bool, imposed_schema: Optional[StructType], bad_records_path: Optional[str],
                  expected_cols: List[str]) -> DataFrame:
    reader = (spark.read.format("csv")
                    .option("sep", sep_char)
                    .option("header", use_header and not first_line_only)
                    .option("encoding", charset)
                    .option("ignoreEmptyFiles", ignore_empty)
                    .option("mode", "PERMISSIVE")
                    .option("enforceSchema", False)
                    .option("columnNameOfCorruptRecord", "_corrupt_record"))
    if bad_records_path:
        reader = reader.option("badRecordsPath", bad_records_path)

    if imposed_schema is not None:
        df_raw = reader.schema(imposed_schema).load(file_path)
    else:
        df_raw = reader.load(file_path)

    if use_header and first_line_only:
        df_raw = _drop_first_row(df_raw)

    # aligner colonnes
    if expected_cols:
        cols_no_line = [c for c in df_raw.columns if c != "line_id"]
        if len(cols_no_line) == len(expected_cols):
            df_raw = df_raw.select(*cols_no_line).toDF(*expected_cols)
        else:
            for c in df_raw.columns:
                df_raw = df_raw.withColumn(c, F.col(c).cast("string"))
            for c in expected_cols:
                if c not in df_raw.columns:
                    df_raw = df_raw.withColumn(c, lit(None).cast("string"))

    # trim global
    for c in df_raw.columns:
        df_raw = df_raw.withColumn(c, F.trim(F.col(c)))

    if "line_id" not in df_raw.columns:
        df_raw = df_raw.withColumn("line_id", F.monotonically_increasing_id())
    return df_raw

def _normalize_error_action(val: str) -> str:
    s = (val or "").strip().upper()
    if s in {"ICT_DRIVEN", "ICI_DRIVEN"}: return "ICT_DRIVEN"
    if s in {"REJECT"}: return "REJECT"
    if s in {"LOG_ONLY","LONG_ONLY"}: return "LOG_ONLY"
    return "ICT_DRIVEN"

def _apply_enum_check(df: DataFrame, cname: str, enum_vals: str) -> DataFrame:
    if not enum_vals: return df
    allowed = [v.strip() for v in enum_vals.split(",") if v.strip()]
    if not allowed: return df
    return df.withColumn(cname, F.when(F.col(cname).isin(allowed), F.col(cname)).otherwise(F.lit(None)))

def apply_column_transformations(df: DataFrame, column_defs: pd.DataFrame, table_name: str,
                                filename: str, zone: str, spark: SparkSession) -> DataFrame:
    total_rows = df.count()
    invalid_flags = []

    for _, crow in column_defs.iterrows():
        cname = str(crow["Column Name"]).strip()
        if cname not in df.columns: continue

        storage_type = str(crow.get("Field type", crow.get("Field Type", "STRING"))).strip().upper()
        tr_type      = str(crow.get("Transformation Type", "")).strip().lower()
        tr_pattern   = str(crow.get("Transformation Pattern", crow.get("Transformation pattern", ""))).strip()
        is_nullable  = str(crow.get("Is Nullable", "true")).strip().lower() in {"true","1","yes","y","oui"}
        enum_vals    = str(crow.get("Enumeration list", crow.get("Enumeration Values",""))).strip()
        default_inv  = str(crow.get("Default when invalid", crow.get("Default Value when Invalid",""))).strip()
        err_action   = _normalize_error_action(str(crow.get("Error action", "ICT_DRIVEN")))

        # transformers
        if tr_type == "uppercase":
            df = df.withColumn(cname, F.upper(F.col(cname)))
        elif tr_type == "lowercase":
            df = df.withColumn(cname, F.lower(F.col(cname)))
        elif tr_type == "regex" and tr_pattern:
            df = df.withColumn(cname, F.regexp_replace(F.col(cname), tr_pattern, ""))

        if enum_vals:
            df = _apply_enum_check(df, cname, enum_vals)

        # Harmoniser décimal avant cast
        if storage_type in {"FLOAT","DOUBLE","DECIMAL"}:
            df = df.withColumn(cname, F.regexp_replace(F.col(cname), ",", "."))

        invalid_cond = F.lit(False)
        if storage_type in {"INTEGER","INT","DOUBLE","FLOAT","LONG","BOOLEAN","STRING"}:
            target = TYPE_MAPPING.get(storage_type, "string")
            df = df.withColumn(cname, F.col(cname).cast(target))
            invalid_cond = F.col(cname).isNull() & F.col(cname).isNotNull()
        elif storage_type in {"DATE","TIMESTAMP"}:
            patterns = [
                "dd/MM/yyyy HH:mm:ss","dd/MM/yyyy",
                "yyyy-MM-dd HH:mm:ss","yyyyMMddHHmmss",
                "yyyyMMdd","yyyy-MM-dd","yyyy/MM/dd"
            ]
            df, errs = parse_dates_with_logs(df, cname, patterns, table_name, filename, default_date=(default_inv or None))
            write_quality_errors(spark, errs, table_name=table_name, filename=filename, zone=zone)

        if not is_nullable:
            invalid_cond = invalid_cond | F.col(cname).isNull()

        invalid_count = df.filter(invalid_cond).count()
        col_tol = parse_tolerance(crow.get("Rejected line per file tolerance", "10%"), total_rows)

        if err_action == "REJECT":
            if invalid_count > 0:
                df_err = spark.createDataFrame(
                    [(filename, table_name, cname, invalid_count, "REJECT")],
                    "filename string, table_name string, column_name string, error_count int, error_type string"
                )
                write_quality_errors(spark, df_err, table_name=table_name, filename=filename, zone=zone)
            df = df.filter(~invalid_cond)

        elif err_action == "LOG_ONLY":
            if invalid_count > 0:
                df_err = spark.createDataFrame(
                    [(filename, table_name, cname, invalid_count, "LOG_ONLY")],
                    "filename string, table_name string, column_name string, error_count int, error_type string"
                )
                write_quality_errors(spark, df_err, table_name=table_name, filename=filename, zone=zone)
            if default_inv != "":
                df = df.withColumn(cname, F.when(invalid_cond, F.lit(default_inv)).otherwise(F.col(cname)))

        elif err_action == "ICT_DRIVEN":
            # Abort si dépasse tolérance
            if total_rows > 0 and (invalid_count / float(total_rows)) > col_tol:
                errs_summary = spark.createDataFrame(
                    [(filename, table_name, cname, invalid_count, "ICT_DRIVEN_ABORT")],
                    "filename string, table_name string, column_name string, error_count int, error_type string"
                )
                write_quality_errors(spark, errs_summary, table_name=table_name, filename=filename, zone=zone)
                return df.limit(0)
            # flag + log détaillé
            flag_col = f"{cname}__invalid"
            df = df.withColumn(flag_col, F.when(invalid_cond, F.lit(1)).otherwise(F.lit(0)))
            invalid_flags.append(flag_col)
            if invalid_count > 0:
                errs_detailed = (
                    df.filter(F.col(flag_col) == 1)
                      .withColumn("line_id", monotonically_increasing_id())
                      .select(
                          F.lit(filename).alias("filename"),
                          F.lit(table_name).alias("table_name"),
                          F.lit(cname).alias("column_name"),
                          F.col("line_id"),
                          F.col(cname).cast("string").alias("raw_value"),
                          F.lit("ICT_DRIVEN").alias("error_type"),
                          F.lit(1).alias("error_count")
                      )
                )
                write_quality_errors(spark, errs_detailed, table_name=table_name, filename=filename, zone=zone)
            if default_inv != "":
                df = df.withColumn(cname, F.when(invalid_cond, F.lit(default_inv)).otherwise(F.col(cname)))

    # Rejet global par ligne (ICT_DRIVEN)
    if invalid_flags:
        df = df.withColumn("invalid_column_count", sum([F.col(c) for c in invalid_flags]))
        max_invalid = max(1, int(len(invalid_flags) * 0.10))
        df_invalid = df.filter(F.col("invalid_column_count") > max_invalid)
        if df_invalid.limit(1).count() > 0:
            df_invalid = df_invalid.withColumn("line_id", monotonically_increasing_id())
            rej = df_invalid.select(
                F.lit(filename).alias("filename"),
                F.lit(table_name).alias("table_name"),
                F.lit("ROW").alias("column_name"),
                F.col("line_id"),
                F.lit("ICT_DRIVEN_ROW_REJECTED").alias("error_type"),
                F.lit(1).alias("error_count"),
                F.lit(None).cast("string").alias("raw_value")
            )
            write_quality_errors(spark, rej, table_name=table_name, filename=filename, zone=zone)
        df = df.filter(F.col("invalid_column_count") <= max_invalid).drop("invalid_column_count", *invalid_flags)

    return df

def validate_missing_columns(df_raw: DataFrame, expected_cols: List[str], source_table: str,
                             matched_uri: str, output_zone: str, params: dict, spark: SparkSession) -> bool:
    missing = [c for c in expected_cols if c not in df_raw.columns]
    if not missing: return True
    df_missing = spark.createDataFrame(
        [(os.path.basename(matched_uri), m, "MISSING_COLUMN") for m in missing],
        "filename STRING, column_name STRING, error_type STRING"
    )
    write_quality_errors(spark, df_missing, table_name=source_table, filename=os.path.basename(matched_uri),
                         zone=output_zone, env=params.get("env","local"), base_path=params.get("log_quality_path"))
    return False

def process_single_file(spark: SparkSession, file_path: str, parts: dict, table_row: pd.Series,
                        column_defs: pd.DataFrame, params: dict, mode: str = "local") -> None:
    import time
    start_time = time.time()

    table_name = str(table_row.get("Delta Table Name", table_row.get("Data Table Name",""))).strip()
    source_table = str(table_row.get("Data Table Name", table_name)).strip()
    input_format = str(table_row.get("Input Format", "csv")).strip().lower()
    output_zone  = str(table_row.get("Output Zone", table_row.get("Input Zone","internal"))).strip().lower()
    ingestion_mode = str(table_row.get("Ingestion mode","")).strip().upper()

    print(f"▶️ Table: {source_table} | File: {os.path.basename(file_path)} | Mode: {ingestion_mode}")

    trim_file     = parse_bool(table_row.get("Trim", True), True)
    ignore_empty  = parse_bool(table_row.get("Ignore empty Files", True), True)
    use_header, first_line_only = parse_header_mode(table_row.get("Input header", table_row.get("Input Header","HEADER USE")))
    charset = str(table_row.get("Input charset","UTF-8") or "UTF-8").strip()
    invalid_gen = parse_bool(table_row.get("Invalid Lines Generate", False), False)
    del_cols_allowed = parse_bool(table_row.get("Delete Columns Allowed", False), False)

    try:
        sep_char = normalize_delimiter(str(table_row.get("Input delimiter", ",")) or ",")
    except Exception as e:
        log_execution(spark, params, source_table, "N/A", ingestion_mode, 0, 0, False, 0, f"Delimiter error: {e}", "FAILED", start_time=start_time)
        return

    if mode == "databricks":
        bad_records_path = f"/mnt/logs/_badrecords/{params['env']}/{source_table}/" if invalid_gen else None
    else:
        bad_records_path = None
        if invalid_gen:
            bad_records_path = os.path.join(params["log_error_path"], source_table.replace(".","_"))
            os.makedirs(bad_records_path, exist_ok=True)

    # expected & schema
    subset = column_defs[column_defs.get("Delta Table Name", column_defs.get("Data Table Name")).eq(table_name)].copy() \
        if "Delta Table Name" in column_defs.columns else column_defs[column_defs["Data Table Name"] == table_name].copy()
    imposed_schema, expected_cols = None, []
    if not subset.empty:
        if "Field Order" in subset.columns:
            subset = subset.sort_values(by=["Field Order"])
        fields = [StructField(str(r["Column Name"]).strip(), spark_type_from_config(r), True) for _, r in subset.iterrows()]
        imposed_schema = StructType(fields)
        expected_cols = [f.name for f in imposed_schema.fields]

    df_raw = read_csv_file(spark, file_path, input_format, sep_char, charset, use_header, first_line_only,
                           ignore_empty, imposed_schema, bad_records_path, expected_cols)

    if not del_cols_allowed and expected_cols:
        if not validate_missing_columns(df_raw, expected_cols, source_table, file_path, output_zone, params, spark):
            log_execution(spark, params, source_table, os.path.basename(file_path), ingestion_mode,
                          len(df_raw.columns), 0, False, 0, f"Missing columns {list(set(expected_cols)-set(df_raw.columns))}",
                          "FAILED", start_time=start_time)
            return

    total_rows = df_raw.count()
    corrupt_rows = df_raw.filter(F.col("_corrupt_record").isNotNull()).count() if "_corrupt_record" in df_raw.columns else 0
    file_rej_tol_ratio = parse_tolerance(table_row.get("Rejected line per file tolerance", "10%"), total_rows)
    # comparer correctement (ratio → nombre max)
    max_corrupt = int(file_rej_tol_ratio * total_rows)
    if corrupt_rows > max_corrupt:
        log_execution(spark, params, source_table, os.path.basename(file_path), ingestion_mode,
                      len(df_raw.columns), 0, False, corrupt_rows, f"Too many corrupted lines: {corrupt_rows}>{max_corrupt}",
                      "FAILED", start_time=start_time)
        return
    if "_corrupt_record" in df_raw.columns:
        df_raw = df_raw.drop("_corrupt_record")

    if trim_file:
        for c in df_raw.columns:
            df_raw = df_raw.withColumn(c, F.trim(F.col(c)))

    # transformations/erreurs par colonne
    df_raw = apply_column_transformations(df_raw, subset, table_name=source_table,
                                          filename=os.path.basename(file_path), zone=output_zone, spark=spark)
    if df_raw.limit(1).count() == 0 and total_rows > 0:
        log_execution(spark, params, source_table, os.path.basename(file_path), ingestion_mode,
                      len(expected_cols), 0, False, total_rows, "ICT_DRIVEN_ABORT", "FAILED", start_time=start_time)
        return

    # DQ globale
    merge_keys = []
    if "isMergeKey" in subset.columns:
        merge_keys = subset[subset["isMergeKey"].astype(str).str.lower().isin(["1","true"])]["Column Name"].tolist()

    df_err_global = check_data_quality(df=df_raw, table_name=source_table, merge_keys=merge_keys,
                                       column_defs=subset, filename=os.path.basename(file_path), spark=spark)
    if df_err_global is not None and not df_err_global.rdd.isEmpty():
        write_quality_errors(spark, df_err_global, table_name=source_table, filename=os.path.basename(file_path),
                             zone=output_zone, env=params.get("env","local"), base_path=params.get("log_quality_path"))

    # Ingestion
    apply_ingestion_mode(spark=spark, df_raw=df_raw, column_defs=subset, table_name=source_table,
                         ingestion_mode=ingestion_mode, params=params, zone=output_zone,
                         version=params.get("version","v1"), parts=parts, file_name_received=os.path.basename(file_path))

    # Résumé + log exécution
    total_rows_after = df_raw.count()
    anomalies_total = 0 if df_err_global is None else df_err_global.count()
    cleaned_rows = total_rows_after
    print_summary(source_table, os.path.basename(file_path), corrupt_rows, anomalies_total, cleaned_rows, df_err_global)

    log_execution(spark, params, source_table, os.path.basename(file_path), ingestion_mode,
                  len(df_raw.columns), anomalies_total, False, anomalies_total, str(anomalies_total), "SUCCESS", start_time=start_time)

def process_files(spark: SparkSession, params: dict, mode: str = "local") -> None:
    extract_zip_file(params["zip_path"], params["extract_dir"], mode=mode)
    cols_df, tables_df = load_excel_config(params["excel_path"], mode=mode)

    extract_dir_local = ensure_dbfs_local(params["extract_dir"]) if mode == "databricks" else params["extract_dir"]
    files = [f for f in os.listdir(extract_dir_local) if os.path.isfile(os.path.join(extract_dir_local, f))]

    for _, trow in tables_df.iterrows():
        pattern = str(trow.get("Filename Pattern", "")).strip()
        candidate_files = files
        if pattern:
            rx = re.escape(pattern)
            rx = (rx.replace(r"\<yyyy\>", r"(\d{4})")
                    .replace(r"\<mm\>", r"(\d{2})")
                    .replace(r"\<dd\>", r"(\d{2})")
                    .replace(r"\<hhmmss\>", r"(\d{6})"))
            rgx = re.compile(f"^{rx}$")
            candidate_files = [f for f in files if rgx.match(f)]

        if not candidate_files:
            print(f"⚠️ Aucun fichier pour la table '{trow.get('Data Table Name', trow.get('Delta Table Name',''))}' (pattern: {pattern or '*'})")
            continue

        merge_files = parse_bool(str(trow.get("Merge concomitant file", False)), False)
        selected = candidate_files if merge_files else [candidate_files[0]]

        for fname in selected:
            file_path = os.path.join(extract_dir_local, fname)
            parts = extract_parts_from_filename(fname)
            process_single_file(spark=spark, file_path=file_path, parts=parts, table_row=trow,
                                column_defs=cols_df, params=params, mode=mode)
