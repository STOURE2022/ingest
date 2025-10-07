"""Validation des données"""
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *


EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"


def spark_type_from_config(row: pd.Series):
    """Détermine le type Spark depuis la config"""
    t = str(row.get("Field Type", "STRING")).strip().upper()
    
    types = {
        "STRING": StringType(), "INT": IntegerType(), "INTEGER": IntegerType(),
        "LONG": LongType(), "FLOAT": DoubleType(), "DOUBLE": DoubleType(),
        "BOOLEAN": BooleanType(), "DATE": DateType(), "TIMESTAMP": TimestampType()
    }
    
    return types.get(t, StringType())


def check_data_quality(df: DataFrame, table: str, merge_keys: list, 
                       column_defs: pd.DataFrame, filename: str, spark) -> DataFrame:
    """Vérifie la qualité des données"""
    schema = "table_name string, filename string, line_id long, column_name string, error_message string, error_count int"
    errors = spark.createDataFrame([], schema)
    
    # Vérifier clés nulles
    for key in merge_keys or []:
        if key in df.columns:
            null_count = df.filter(F.col(key).isNull()).count()
            if null_count > 0:
                err = spark.createDataFrame(
                    [(table, filename, None, key, "NULL_KEY", null_count)], schema
                )
                errors = errors.unionByName(err, allowMissingColumns=True)
    
    return errors


def print_summary(table: str, filename: str, total: int, anomalies: int, cleaned: int, errors_df: DataFrame):
    """Affiche le résumé"""
    print(f"\n{'='*70}")
    print(f"📊 Résumé | Table={table}, File={filename}")
    print(f"Total={total}, Anomalies={anomalies}, Cleaned={cleaned}")
    
    if errors_df and not errors_df.rdd.isEmpty():
        print("⚠️  Erreurs détectées:")
        for row in errors_df.collect()[:5]:
            print(f"  - {row.error_message}: {row.error_count}")
    else:
        print("✅ Aucune erreur")
    print("="*70)
