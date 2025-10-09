"""
Détection et gestion de l'environnement (Local vs Databricks)
"""

import os
import sys
from pathlib import Path
from typing import Optional

class Environment:
    """Gestionnaire d'environnement unifié"""
    
    def __init__(self):
        self.is_databricks = self._detect_databricks()
        self.dbutils = None
        
        if self.is_databricks:
            try:
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                self.dbutils = DBUtils(spark)
            except ImportError:
                self.is_databricks = False
        
        print(f"🌍 Environnement détecté : {'Databricks' if self.is_databricks else 'Local'}")
    
    def _detect_databricks(self) -> bool:
        """Détecte si on est sur Databricks"""
        return (
            'DATABRICKS_RUNTIME_VERSION' in os.environ or
            'DB_HOME' in os.environ or
            '/databricks/' in sys.executable
        )
    
    def normalize_path(self, path: str) -> str:
        """Normalise les chemins selon l'environnement"""
        if not path:
            return path
        
        if self.is_databricks:
            # Databricks : garder les chemins DBFS
            if path.startswith('/dbfs/'):
                return f"dbfs:{path[5:]}"
            elif path.startswith('dbfs:/'):
                return path
            elif path.startswith('/mnt/'):
                return path
            return path
        else:
            # Local : convertir DBFS vers chemins locaux
            if path.startswith('dbfs:'):
                path = path.replace('dbfs:', './data')
            elif path.startswith('/mnt/'):
                path = path.replace('/mnt/', './data/mnt/')
            elif path.startswith('/FileStore/'):
                path = path.replace('/FileStore/', './data/FileStore/')
            
            # Créer les répertoires si nécessaire
            path_obj = Path(path)
            if not path_obj.suffix:  # C'est un répertoire
                path_obj.mkdir(parents=True, exist_ok=True)
            else:  # C'est un fichier
                path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            return str(path_obj)
    
    def get_spark(self):
        """Obtient la session Spark"""
        from pyspark.sql import SparkSession
        
        if self.is_databricks:
            return SparkSession.builder.getOrCreate()
        else:
            # Configuration locale SIMPLE (sans Delta Lake)
            print("ℹ️ Configuration Spark locale (sans Delta Lake)")
            print("   → Utilisation de Parquet pour le stockage")
            
            spark = (SparkSession.builder
                    .appName("WAX-Local")
                    .master("local[*]")
                    .config("spark.driver.memory", "4g")
                    .config("spark.executor.memory", "4g")
                    .config("spark.sql.warehouse.dir", "./spark-warehouse")
                    .getOrCreate())
            
            print("✅ Spark démarré avec succès")
            return spark


class FileSystemAdapter:
    """Adaptateur pour accès fichiers (compatible dbutils)"""
    
    def __init__(self, env: Environment):
        self.env = env
    
    def ls(self, path: str):
        """Liste les fichiers"""
        normalized_path = self.env.normalize_path(path)
        
        if self.env.is_databricks and self.env.dbutils:
            return self.env.dbutils.fs.ls(path)
        else:
            # Local
            path_obj = Path(normalized_path)
            if not path_obj.exists():
                return []
            
            files = []
            for item in path_obj.iterdir():
                files.append(type('FileInfo', (), {
                    'path': str(item),
                    'name': item.name,
                    'size': item.stat().st_size if item.is_file() else 0
                })())
            return files
    
    def mkdirs(self, path: str):
        """Crée les répertoires"""
        normalized_path = self.env.normalize_path(path)
        
        if self.env.is_databricks and self.env.dbutils:
            return self.env.dbutils.fs.mkdirs(path)
        else:
            Path(normalized_path).mkdir(parents=True, exist_ok=True)
    
    def rm(self, path: str, recurse: bool = False):
        """Supprime fichiers/répertoires"""
        normalized_path = self.env.normalize_path(path)
        
        if self.env.is_databricks and self.env.dbutils:
            return self.env.dbutils.fs.rm(path, recurse)
        else:
            import shutil
            path_obj = Path(normalized_path)
            if path_obj.is_file():
                path_obj.unlink()
            elif path_obj.is_dir() and recurse:
                shutil.rmtree(path_obj)


class WidgetsAdapter:
    """Adaptateur pour widgets Databricks"""
    
    def __init__(self, env: Environment):
        self.env = env
        self._widgets = {}
    
    def text(self, name: str, default_value: str, label: str = None):
        """Crée un widget texte"""
        if self.env.is_databricks and self.env.dbutils:
            self.env.dbutils.widgets.text(name, default_value, label or name)
        else:
            # En local, utiliser les variables d'env ou valeurs par défaut
            self._widgets[name] = os.environ.get(f"WAX_{name.upper()}", default_value)
            print(f"📝 Widget '{name}': {self._widgets[name]}")
    
    def get(self, name: str) -> str:
        """Récupère la valeur d'un widget"""
        if self.env.is_databricks and self.env.dbutils:
            return self.env.dbutils.widgets.get(name)
        else:
            return self._widgets.get(name, "")


class DBUtilsAdapter:
    """Adaptateur complet pour dbutils"""
    
    def __init__(self, env: Environment):
        self.env = env
        self.fs = FileSystemAdapter(env)
        self.widgets = WidgetsAdapter(env)
    
    @property
    def notebook(self):
        """Adaptateur notebook (non implémenté en local)"""
        if self.env.is_databricks and self.env.dbutils:
            return self.env.dbutils.notebook
        return None


# Instance globale
_env = None
_dbutils = None

def get_environment() -> Environment:
    """Obtient l'environnement global"""
    global _env
    if _env is None:
        _env = Environment()
    return _env

def get_dbutils() -> DBUtilsAdapter:
    """Obtient l'adaptateur dbutils"""
    global _dbutils
    if _dbutils is None:
        _dbutils = DBUtilsAdapter(get_environment())
    return _dbutils
