# src/__init__.py
# --------------------------------------------------------------------------------------
# Package d'initialisation pour le pipeline WAX
# Gère l'import des sous-modules et assure compatibilité Databricks / Local.
# --------------------------------------------------------------------------------------

__version__ = "1.0.0"
__author__ = "Soumailou TOURE"
__project__ = "WAX Data Ingestion – Databricks + Local compatible"

# Imports principaux du package
from . import (
    config,
    environment,
    utils,
    logger_manager,
    delta_manager,
    validator,
    file_processor,
    ingestion,
    main,
)

# Réexportations simplifiées (optionnel)
from .main import main
from .file_processor import process_files
from .delta_manager import save_delta_table, register_table_in_metastore
from .logger_manager import log_execution, write_quality_errors

# Raccourcis de compatibilité Databricks
def is_databricks_env() -> bool:
    """Retourne True si le code s'exécute dans Databricks."""
    try:
        from .environment import is_databricks
        return is_databricks()
    except Exception:
        return False


print(f"📦 Package WAX initialisé (v{__version__}) – Environnement : {'Databricks' if is_databricks_env() else 'Local'}")
