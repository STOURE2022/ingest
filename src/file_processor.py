"""
Module file_processor.py
------------------------
Extraction ZIP, lecture Excel, lecture CSV et orchestration du traitement.
"""
import os, re, time, zipfile, pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, DateType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number, lit

from .utils import normalize_delimiter, parse_bool, parse_header_mode, extract_parts_from_filename
from .validators import spark_type_from_config, parse_date_with_logs, check_data_quality
from .ingestion import apply_ingestion_mode
from .logging_manager import log_execution, write_quality_errors


def extract_zip_file(zip_path: str, extract_dir: str):
    print(f"📦 Extraction ZIP : {zip_path} -> {extract_dir}")
    local_extract_dir = extract_dir.replace("dbfs:", "/dbfs")
    os.makedirs(local_extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path.replace("dbfs:", "/dbfs"), "r") as zip_ref:
        zip_ref.extractall(local_extract_dir)
    return local_extract_dir


def load_excel_config(excel_path: str):
    excel_path_local = excel_path.replace("dbfs:", "/dbfs") if excel_path.startswith("dbfs:") else excel_path
    print(f"📑 Lecture Excel config : {excel_path_local}")
    file_columns_df = pd.read_excel(excel_path_local, sheet_name="Field-Column")
    file_tables_df = pd.read_excel(excel_path_local, sheet_name="File-Table")
    return file_tables_df, file_columns_df


def read_csv_file(spark, matched_uri, sep_char, charset, user_header, first_line_only, imposed_schema, expected_cols):
    reader = (spark.read.option("sep", sep_char).option("encoding", charset)
              .option("mode", "PERMISSIVE").option("enforceSchema", False)
              .option("columnNameOfCorruptRecord", "_corrupt_record"))
    if imposed_schema is not None:
        df_file = reader.schema(imposed_schema).csv(matched_uri)
    elif user_header and not first_line_only:
        df_file = reader.option("header", "true").csv(matched_uri)
    else:
        df_file = reader.option("header", "false").csv(matched_uri)
        if expected_cols:
            df_file = df_file.toDF(*expected_cols)
    return df_file


def apply_column_transformations(df_raw, file_columns_df, source_table, matched_uri, output_zone, spark):
    total_rows = df_raw.count()
    invalid_flags = []
    subset = file_columns_df[file_columns_df["Delta Table Name"] == source_table]
    for _, crow in subset.iterrows():
        cname = crow["Column Name"]
        if cname not in df_raw.columns:
            continue
        stype = spark_type_from_config(crow)
        tr_type = str(crow.get("Transformation Type", "")).strip().lower()
        tr_patt = str(crow.get("Transformation pattern", "")).strip()
        regex_repl = str(crow.get("Regex replacement", "")).strip()
        is_nullable = parse_bool(crow.get("Is Nullable", "True"), True)
        err_action = str(crow.get("Error action", "ICT_DRIVEN")).strip().upper()
        default_inv = str(crow.get("Default when invalid", "")).strip()
        if tr_type == "uppercase":
            df_raw = df_raw.withColumn(cname, F.upper(F.col(cname)))
        elif tr_type == "lowercase":
            df_raw = df_raw.withColumn(cname, F.lower(F.col(cname)))
        elif tr_type == "regex" and tr_patt:
            df_raw = df_raw.withColumn(cname, F.regexp_replace(F.col(cname), tr_patt, regex_repl or ""))
        df_raw = df_raw.withColumn(f"{cname}_cast", F.expr(f"try_cast({cname} as {stype.simpleString()})"))
        df_raw = df_raw.withColumn(cname, F.col(f"{cname}_cast")).drop(f"{cname}_cast")
        if isinstance(stype, (DateType, TimestampType)):
            patterns = [tr_patt] if tr_patt else []
            patterns += ["dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy", "yyyy-MM-dd HH:mm:ss", "yyyyMMdd", "yyyyMMddHHmmss"]
            df_raw, errs = parse_date_with_logs(df_raw, cname, patterns, source_table, os.path.basename(matched_uri),
                                                default_inv)
            write_quality_errors(errs, source_table, zone=output_zone, spark=spark)
        if not is_nullable:
            invalid_cond = F.col(cname).isNull()
            invalid_count = df_raw.filter(invalid_cond).count()
            if invalid_count > 0 and err_action == "REJECT":
                df_raw = df_raw.filter(~invalid_cond)
            elif invalid_count > 0 and err_action == "ICT_DRIVEN":
                flag_col = f"{cname}_invalid"
                df_raw = df_raw.withColumn(flag_col, F.when(invalid_cond, lit(1)).otherwise(lit(0)))
                invalid_flags.append(flag_col)
    if invalid_flags:
        from functools import reduce
        df_raw = df_raw.withColumn("invalid_column_count", sum([F.col(c) for c in invalid_flags]))
        df_raw = df_raw.filter(F.col("invalid_column_count") <= max(1, int(len(invalid_flags) * 0.1)))
        df_raw = df_raw.drop("invalid_column_count", *invalid_flags)
    return df_raw


def process_files(spark, file_tables_df, file_columns_df, params):
    extract_dir = params["extract_dir"]
    for _, trow in file_tables_df.iterrows():
        start_time = time.time()
        source_table = trow["Delta Table Name"]
        filename_pattern = str(trow.get("Filename Pattern", "")).strip()
        input_format = str(trow.get("Input Format", "csv")).strip().lower()
        output_zone = str(trow.get("Output Zone", "internal")).strip().lower()
        ingestion_mode = str(trow.get("Ingestion mode", "")).strip()
        print(f"\n📂 Table : {source_table}")
        rx = (filename_pattern.replace("<yyyy>", r"\\d{4}")
              .replace("<mm>", r"\\d{2}")
              .replace("<dd>", r"\\d{2}"))
        # pour databrick decommenter
        # matched = [fi for fi in dbutils.fs.ls(extract_dir) if re.match(rf"^{rx}$", fi.name)]
        files_in_dir = []
        extract_dir_local = extract_dir.replace("dbfs:", "/dbfs") if extract_dir.startswith("dbfs:") else extract_dir
        for root, _, files in os.walk(extract_dir_local):
            for name in files:
                if re.match(rf"^{rx}$", name):
                    full_path = os.path.join(root, name)
                    files_in_dir.append({"path": full_path, "name": name})

        matched = files_in_dir
        if not matched:
            log_execution(spark, source_table, "N/A", input_format, ingestion_mode, output_zone,
                          0, False, 0, f"No file matched {filename_pattern}", "FAILED", start_time,
                          env=params["env"], log_path=params["log_exec_path"])
            continue
        files_to_read = [(fi.path, extract_parts_from_filename(fi.name)) for fi in matched]
        sep_char = normalize_delimiter(trow.get("Input delimiter", ","))
        header_mode = str(trow.get("Input header", ""))
        user_header, first_line_only = parse_header_mode(header_mode)
        expected_cols = file_columns_df[file_columns_df["Delta Table Name"] == source_table]["Column Name"].tolist()
        df_raw_list = []
        for matched_uri, parts in files_to_read:
            df_file = read_csv_file(spark, matched_uri, sep_char, "UTF-8", user_header, first_line_only, None,
                                    expected_cols)
            df_file = df_file.withColumn("line_id", row_number().over(Window.orderBy(monotonically_increasing_id())))
            df_raw_list.append((df_file, matched_uri, parts))
        for df_raw, matched_uri, parts in df_raw_list:
            if "_corrupt_record" in df_raw.columns:
                df_raw = df_raw.drop("_corrupt_record")
            df_raw = apply_column_transformations(df_raw, file_columns_df, source_table, matched_uri, output_zone,
                                                  spark)
            specials = file_columns_df[file_columns_df["Delta Table Name"] == source_table]
            merge_keys = specials[specials["Is Special"].astype(str).str.lower() == "ismergekey"][
                "Column Name"].tolist()
            df_err_global = check_data_quality(df_raw, source_table, merge_keys, os.path.basename(matched_uri))
            write_quality_errors(df_err_global, source_table, zone=output_zone, spark=spark)
            apply_ingestion_mode(df_raw, specials, source_table, ingestion_mode,
                                 env=params["env"], zone=output_zone, version=params["version"],
                                 parts=parts, file_name_received=os.path.basename(matched_uri), spark=spark)
            log_execution(spark, source_table, os.path.basename(matched_uri), input_format, ingestion_mode,
                          output_zone, len(df_raw.columns), False, df_err_global.count(),
                          str(df_err_global.count()), "SUCCESS", start_time,
                          env=params["env"], log_path=params["log_exec_path"])
    print("✅ Processing terminé")
