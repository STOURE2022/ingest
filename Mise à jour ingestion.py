"""
Gestion des modes d'ingestion et orchestration du pipeline
"""

from datetime import datetime
from pyspark.sql import functions as F

class IngestionManager:
    """Gestionnaire des modes d'ingestion"""
    
    def __init__(self, spark, config, delta_manager):
        self.spark = spark
        self.config = config
        self.delta_manager = delta_manager
    
    def apply_ingestion_mode(self, df_raw, column_defs, table_name: str,
                             ingestion_mode: str, zone: str = "internal",
                             parts: dict = None, file_name_received: str = None):
        """Applique mode ingestion"""
        
        path_all = self.config.build_output_path(zone, f"{table_name}_all", parts)
        path_last = self.config.build_output_path(zone, f"{table_name}_last", parts)
        
        specials = column_defs.copy()
        specials["Is Special lower"] = specials["Is Special"].astype(str).str.lower()
        merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist()
        update_cols = specials[specials["Is Special lower"] == "isstartvalidity"]["Column Name"].tolist()
        update_col = update_cols[0] if update_cols else None
        
        imode = (ingestion_mode or "").strip().upper()
        print(f"🔄 Mode : {imode}")
        
        # Sauvegarde _all (toujours en append)
        self.delta_manager.save_delta(df_raw, path_all, mode="append", add_ts=True,
                                     parts=parts, file_name_received=file_name_received)
        self.delta_manager.register_table_in_metastore(f"{table_name}_all", path_all, if_exists="ignore")
        
        # Traitement selon le mode
        if imode == "FULL_SNAPSHOT":
            self.delta_manager.save_delta(df_raw, path_last, mode="overwrite",
                                         parts=parts, file_name_received=file_name_received)
            self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="overwrite")
        
        elif imode == "DELTA_FROM_FLOW":
            self.delta_manager.save_delta(df_raw, path_last, mode="append", add_ts=True,
                                         parts=parts, file_name_received=file_name_received)
            self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="append")
        
        elif imode == "DELTA_FROM_NON_HISTORIZED":
            if not merge_keys:
                raise ValueError(f"No merge keys for {table_name}")
            
            # EN LOCAL : Mode simplifié (pas de merge, juste overwrite)
            if not self.delta_manager.env.is_databricks:
                print("ℹ️ Mode local : MERGE non supporté, utilisation de OVERWRITE")
                self.delta_manager.save_delta(df_raw, path_last, mode="overwrite", add_ts=True,
                                             parts=parts, file_name_received=file_name_received)
                self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="overwrite")
            else:
                # DATABRICKS : Merge complet avec Delta
                from delta.tables import DeltaTable
                
                fallback_col = "FILE_PROCESS_DATE"
                compare_col = update_col if update_col else fallback_col
                auto_cols = ["FILE_PROCESS_DATE", "yyyy", "mm", "dd"]
                
                if compare_col in df_raw.columns:
                    compare_dtype = str(df_raw.schema[compare_col].dataType)
                    if compare_dtype == "StringType":
                        df_raw = df_raw.withColumn(compare_col, F.to_timestamp(compare_col))
                
                df_raw = (
                    df_raw
                    .withColumn("FILE_PROCESS_DATE", F.current_timestamp())
                    .withColumn("yyyy", F.lit(parts.get("yyyy", datetime.today().year)).cast("int"))
                    .withColumn("mm", F.lit(parts.get("mm", datetime.today().month)).cast("int"))
                    .withColumn("dd", F.lit(parts.get("dd", datetime.today().day)).cast("int"))
                )
                
                updates = df_raw.alias("updates")
                
                if DeltaTable.isDeltaTable(self.spark, path_last):
                    target = DeltaTable.forPath(self.spark, path_last)
                    target_cols = [f.name for f in target.toDF().schema.fields]
                else:
                    target_cols = df_raw.columns
                
                update_cols_clean = [c for c in df_raw.columns 
                                    if c in target_cols and c not in merge_keys and c not in auto_cols]
                insert_cols_clean = [c for c in df_raw.columns 
                                    if c in target_cols and c not in auto_cols]
                
                update_expr = {c: f"updates.{c}" for c in update_cols_clean}
                insert_expr = {c: f"updates.{c}" for c in insert_cols_clean}
                cond = " AND ".join([f"target.{k}=updates.{k}" for k in merge_keys])
                
                if DeltaTable.isDeltaTable(self.spark, path_last):
                    (target.alias("target")
                     .merge(updates, cond)
                     .whenMatchedUpdate(condition=f"updates.{compare_col} > target.{compare_col}", set=update_expr)
                     .whenNotMatchedInsert(values=insert_expr)
                     .execute())
                    print(f"✅ Merge sur {compare_col}")
                    self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="append")
                else:
                    self.delta_manager.save_delta(df_raw, path_last, mode="overwrite", add_ts=True,
                                                 parts=parts, file_name_received=file_name_received)
                    self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="overwrite")
        
        elif imode == "DELTA_FROM_HISTORIZED":
            self.delta_manager.save_delta(df_raw, path_last, mode="append", add_ts=True,
                                         parts=parts, file_name_received=file_name_received)
            self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="append")
        
        elif imode == "FULL_KEY_REPLACE":
            if not merge_keys:
                raise ValueError(f"No merge keys for {table_name}")
            
            # EN LOCAL : Mode simplifié
            if not self.delta_manager.env.is_databricks:
                print("ℹ️ Mode local : KEY_REPLACE non supporté, utilisation de OVERWRITE")
                self.delta_manager.save_delta(df_raw, path_last, mode="overwrite", add_ts=True,
                                             parts=parts, file_name_received=file_name_received)
                self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="overwrite")
            else:
                # DATABRICKS : Delete + Insert avec Delta
                from delta.tables import DeltaTable
                
                if DeltaTable.isDeltaTable(self.spark, path_last):
                    target = DeltaTable.forPath(self.spark, path_last)
                    conditions = []
                    for k in merge_keys:
                        values = df_raw.select(k).distinct().rdd.flatMap(lambda x: x).collect()
                        values_str = ','.join([f"'{str(x)}'" for x in values])
                        conditions.append(f"{k} IN ({values_str})")
                    cond = " OR ".join(conditions)
                    
                    target.delete(condition=cond)
                    self.delta_manager.save_delta(df_raw, path_last, mode="append", add_ts=True,
                                                 parts=parts, file_name_received=file_name_received)
                    self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="append")
                else:
                    self.delta_manager.save_delta(df_raw, path_last, mode="overwrite", add_ts=True,
                                                 parts=parts, file_name_received=file_name_received)
                    self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="overwrite")
        else:
            # Mode par défaut
            self.delta_manager.save_delta(df_raw, path_last, mode="append", add_ts=True,
                                         parts=parts, file_name_received=file_name_received)
            self.delta_manager.register_table_in_metastore(f"{table_name}_last", path_last, if_exists="append")
