"""Traitement des fichiers"""
import os
import zipfile
import pandas as pd
from pyspark.sql import SparkSession
from utils import parse_bool, normalize_delimiter, parse_header_mode, extract_parts_from_filename
from validators import check_data_quality, print_summary
from logging_manager import log_execution, write_quality_errors
from ingestion import apply_ingestion_mode


def extract_zip_file(zip_path: str, extract_dir: str):
    """Extrait le ZIP"""
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_dir)
    print(f"✅ ZIP extrait: {extract_dir}")


def load_excel_config(excel_path: str):
    """Charge la config Excel"""
    columns_df = pd.read_excel(excel_path, sheet_name="Field-Column")
    tables_df = pd.read_excel(excel_path, sheet_name="File-Table")
    print(f"✅ Config chargée: {len(columns_df)} colonnes, {len(tables_df)} tables")
    return columns_df, tables_df


def process_files(spark: SparkSession, params: dict, mode: str = "local"):
    """Traite tous les fichiers"""
    print("\n" + "="*70)
    print("🚀 TRAITEMENT DES FICHIERS")
    print("="*70 + "\n")
    
    # Extraire le ZIP
    extract_zip_file(params["zip_path"], params["extract_dir"])
    
    # Charger la config
    columns_df, tables_df = load_excel_config(params["excel_path"])
    
    # Traiter chaque table
    for _, row in tables_df.iterrows():
        table_name = str(row.get("Delta Table Name", "")).strip()
        print(f"\n📋 Table: {table_name}")
        
        # Simulation simple pour l'exemple
        print(f"⚠️  Traitement simplifié (à compléter selon vos besoins)")
    
    print("\n✅ Traitement terminé")
