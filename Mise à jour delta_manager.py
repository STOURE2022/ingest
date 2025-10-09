"""
Gestion des tables Delta Lake - Version unifiée Local/Databricks
"""

from datetime import datetime
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DeltaManager:
    """Gestionnaire Delta Lake (ou Parquet en local)"""
    
    def __init__(self, spark, config, env):
        self.spark = spark
        self.config = config
        self.env = env
        self.format = "delta" if env.is_databricks else "parquet"
        
        if not env.is_databricks:
            print("ℹ️ Mode LOCAL : utilisation de PARQUET au lieu de Delta Lake")
    
    def save_delta(self, df: DataFrame, path: str, mode: str = "append",
                   add_ts: bool = False, parts: dict = None, file_name_received: str = None):
        """Sauvegarde DataFrame en Delta/Parquet"""
        from utils import deduplicate_columns
        import os
        
        # Normaliser le chemin
        delta_path = self.env.normalize_path(path)
        
        # Extraire dates
        today = datetime.today()
        y = int((parts or {}).get("yyyy", today.year))
        m = int((parts or {}).get("mm", today.month))
        d = int((parts or {}).get("dd", today.day))
        
        # Ajouter métadonnées
        if add_ts:
            df = df.withColumn("FILE_PROCESS_DATE", F.current_timestamp())
        
        if file_name_received:
            base_name = os.path.splitext(os.path.basename(file_name_received))[0]
            df = df.withColumn("FILE_NAME_RECEIVED", F.lit(base_name))
        
        # Réorganiser colonnes (métadonnées en premier)
        ordered_cols = []
        for meta_col in ["FILE_NAME_RECEIVED", "FILE_PROCESS_DATE"]:
            if meta_col in df.columns:
                ordered_cols.append(meta_col)
        other_cols = [c for c in df.columns if c not in ordered_cols]
        df = df.select(ordered_cols + other_cols)
        
        # Dédupliquer colonnes
        df = deduplicate_columns(df)
        
        # Ajouter colonnes de partitionnement
        df = (df.withColumn("yyyy", F.lit(y).cast("int"))
               .withColumn("mm", F.lit(m).cast("int"))
               .withColumn("dd", F.lit(d).cast("int")))
        
        # Optimisation partitionnement
        row_count = df.count()
        if row_count > 1_000_000:
            num_partitions = max(1, row_count // 1_000_000)
            df = df.repartition(num_partitions, "yyyy", "mm", "dd")
        
        # Sauvegarder selon le format
        if self.format == "delta":
            # Delta Lake (Databricks)
            df.write.format("delta").option("mergeSchema", "true").mode(mode) \
                .partitionBy("yyyy", "mm", "dd").save(delta_path)
        else:
            # Parquet (Local)
            df.write.format("parquet").mode(mode) \
                .partitionBy("yyyy", "mm", "dd").save(delta_path)
        
        print(f"✅ {self.format.upper()} sauvegardé : {delta_path}")
        print(f"   Mode: {mode}, Date: {y}-{m:02d}-{d:02d}, Lignes: {row_count}")
    
    def register_table_in_metastore(self, table_name: str, path: str,
                                    database: str = "wax_obs", if_exists: str = "ignore"):
        """Enregistre table dans metastore"""
        
        # En local, skip le metastore
        if not self.env.is_databricks:
            print(f"ℹ️ Local : metastore ignoré pour {table_name}")
            print(f"   Accès direct via : spark.read.parquet('{path}')")
            return
        
        # Databricks : enregistrer dans le metastore
        full_name = f"{database}.{table_name}"
        
        try:
            tables = self.spark.catalog.listTables(database)
            exists = any(t.name == table_name for t in tables)
        except Exception:
            try:
                self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
                exists = False
            except Exception as e:
                print(f"⚠️ Metastore non disponible : {e}")
                return
        
        if exists and if_exists == "ignore":
            print(f"⚠️ Table {full_name} existe déjà")
            return
        elif exists and if_exists == "errorexists":
            raise Exception(f"❌ Table {full_name} existe déjà")
        elif exists and if_exists == "overwrite":
            print(f"♻️ DROP + CREATE {full_name}")
            self.spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        elif exists and if_exists == "append":
            print(f"➕ Append {full_name}")
            return
        
        normalized_path = self.env.normalize_path(path)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_name}
            USING DELTA
            LOCATION '{normalized_path}'
        """)
        print(f"✅ Table {full_name} enregistrée")
    
    def table_exists(self, path: str) -> bool:
        """Vérifie si une table existe"""
        normalized_path = self.env.normalize_path(path)
        
        if self.format == "delta":
            try:
                from delta.tables import DeltaTable
                return DeltaTable.isDeltaTable(self.spark, normalized_path)
            except:
                return False
        else:
            # Parquet : vérifier si le répertoire existe
            return Path(normalized_path).exists()
    
    def read_table(self, path: str) -> DataFrame:
        """Lit une table Delta/Parquet"""
        normalized_path = self.env.normalize_path(path)
        return self.spark.read.format(self.format).load(normalized_path)
