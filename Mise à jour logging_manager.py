"""
Gestion des logs d'exécution et qualité - Version unifiée Local/Databricks
"""

import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

class LoggerManager:
    """Gestionnaire de logs"""
    
    def __init__(self, spark, config, env, dbutils):
        self.spark = spark
        self.config = config
        self.env = env
        self.dbutils = dbutils
        self.format = "delta" if env.is_databricks else "parquet"
    
    def log_execution(self, table_name: str, filename: str, input_format: str,
                      ingestion_mode: str, output_zone: str, row_count: int = 0,
                      column_count: int = 0, masking_applied: bool = False,
                      error_count: int = 0, error_msg: str = None,
                      status: str = "SUCCESS", start_time: float = None):
        """Log d'exécution"""
        
        # Normaliser le chemin
        log_path = self.env.normalize_path(self.config.log_exec_path)
        
        # Créer le répertoire si nécessaire
        try:
            self.dbutils.fs.mkdirs(self.config.log_exec_path)
        except Exception as e:
            if not self.env.is_databricks:
                pass
            else:
                print(f"⚠️ Erreur création répertoire logs : {e}")
        
        today = datetime.today()
        duration = round(time.time() - start_time, 2) if start_time else None
        
        schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("filename", StringType(), True),
            StructField("input_format", StringType(), True),
            StructField("ingestion_mode", StringType(), True),
            StructField("output_zone", StringType(), True),
            StructField("row_count", IntegerType(), True),
            StructField("column_count", IntegerType(), True),
            StructField("masking_applied", BooleanType(), True),
            StructField("error_count", IntegerType(), True),
            StructField("error_message", StringType(), True),
            StructField("status", StringType(), True),
            StructField("duration", DoubleType(), True),
            StructField("env", StringType(), True),
            StructField("log_ts", TimestampType(), True),
            StructField("yyyy", IntegerType(), True),
            StructField("mm", IntegerType(), True),
            StructField("dd", IntegerType(), True)
        ])
        
        row = [(
            str(table_name), str(filename), str(input_format), str(ingestion_mode),
            str(output_zone), int(row_count or 0), int(column_count or 0), bool(masking_applied),
            int(error_count or 0), str(error_msg or ""), str(status),
            float(duration or 0), str(self.config.env), datetime.now(),
            today.year, today.month, today.day
        )]
        
        df_log = self.spark.createDataFrame(row, schema=schema)
        
        # Sauvegarder
        try:
            df_log.write.format(self.format).mode("append") \
                .option("mergeSchema", "true").partitionBy("yyyy", "mm", "dd").save(log_path)
        except Exception as e:
            print(f"⚠️ Erreur sauvegarde log exécution : {e}")
    
    def write_quality_errors(self, df_errors: DataFrame, table_name: str, zone: str = "internal"):
        """Log erreurs qualité"""
        from utils import deduplicate_columns
        
        if df_errors is None or df_errors.rdd.isEmpty():
            return
        
        # Normaliser le chemin
        log_path = self.env.normalize_path(self.config.log_quality_path)
        
        # Créer le répertoire si nécessaire
        try:
            self.dbutils.fs.mkdirs(self.config.log_quality_path)
        except Exception as e:
            if not self.env.is_databricks:
                pass
            else:
                print(f"⚠️ Erreur création répertoire logs qualité : {e}")
        
        today = datetime.today()
        df_errors = deduplicate_columns(df_errors)
        
        # Supprimer line_id si présent
        if "line_id" in df_errors.columns:
            df_errors = df_errors.drop("line_id")
        
        # Assurer raw_value est string
        if "raw_value" in df_errors.columns:
            df_errors = df_errors.withColumn("raw_value", F.col("raw_value").cast("string"))
        else:
            df_errors = df_errors.withColumn("raw_value", F.lit(None).cast("string"))
        
        # Ajouter métadonnées
        df_log = (
            df_errors
            .withColumn("table_name", F.coalesce(F.col("table_name"), F.lit(table_name)))
            .withColumn("Zone", F.lit(zone))
            .withColumn("Env", F.lit(self.config.env))
            .withColumn("log_ts", F.lit(datetime.now()))
            .withColumn("yyyy", F.lit(today.year))
            .withColumn("mm", F.lit(today.month))
            .withColumn("dd", F.lit(today.day))
        )
        
        df_log = deduplicate_columns(df_log)
        
        # Vérifier si la table existe
        try:
            self.spark.read.format(self.format).load(log_path)
            table_exists = True
        except Exception:
            table_exists = False
        
        # Sauvegarder
        try:
            if not table_exists:
                df_log.write.format(self.format).mode("overwrite") \
                    .partitionBy("yyyy", "mm", "dd").save(log_path)
                print(f"✅ Table {self.format.upper()} créée : {log_path}")
            else:
                df_log.write.format(self.format).mode("append") \
                    .option("mergeSchema", "false").save(log_path)
        except Exception as e:
            print(f"⚠️ Erreur sauvegarde logs qualité : {e}")
    
    def print_summary(self, table_name: str, filename: str, total_rows, corrupt_rows: int,
                      anomalies_total: int, cleaned_rows: int, errors_df: DataFrame):
        """Résumé console"""
        print("\n" + "=" * 80)
        print(f"📊 Rapport | Table={table_name}, File={filename}")
        
        if isinstance(total_rows, tuple):
            print(f"Lignes: {total_rows[0]} → {total_rows[1]}, rejetées: {corrupt_rows}, "
                  f"anomalies: {anomalies_total}, nettoyées: {cleaned_rows}")
        else:
            print(f"Lignes: {total_rows}, rejetées: {corrupt_rows}, "
                  f"anomalies: {anomalies_total}, nettoyées: {cleaned_rows}")
        
        print("=" * 80)
        
        if errors_df is not None and not errors_df.rdd.isEmpty():
            print("⚠️ Problèmes détectés")
            
            error_summary = (
                errors_df
                .groupBy("error_message")
                .agg(F.sum("error_count").alias("total_count"))
                .orderBy(F.desc("total_count"))
                .limit(50)
                .collect()
            )
            
            null_counter = {}
            error_counter = {}
            
            for row in error_summary:
                em = row["error_message"]
                ec = row["total_count"]
                
                if "null" in str(em).lower():
                    null_counter[em] = ec
                else:
                    error_counter[em] = ec
            
            if error_counter:
                print("\n🔴 Erreurs typage/format :")
                for em, total in sorted(error_counter.items(), key=lambda x: x[1], reverse=True):
                    print(f"  - {em}: {total}")
            
            if null_counter:
                print("\n⚪ Valeurs nulles :")
                for em, total in sorted(null_counter.items(), key=lambda x: x[1], reverse=True):
                    print(f"  - {em}: {total}")
        else:
            print("\n✅ Aucun problème")
        
        print("=" * 80 + "\n")
