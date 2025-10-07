"""
utils.py
---------
Fonctions utilitaires réutilisables dans le framework WAX :
- Parsing des booléens, délimiteurs, headers et tolérances
- Extraction d’informations depuis les noms de fichiers
"""

import re
import os
from typing import Any, Tuple


# ==============================================================
# BOOLÉENS
# ==============================================================

def parse_bool(value: Any, default: bool = False) -> bool:
    """
    Convertit une valeur en booléen.
    Accepte: true/false, 1/0, yes/no, y/n, oui/non.

    Args:
        value: Valeur à convertir
        default: Valeur par défaut si la conversion échoue

    Returns:
        bool
    """
    if value is None:
        return default

    s = str(value).strip().lower()
    if s in ["true", "1", "yes", "y", "oui"]:
        return True
    if s in ["false", "0", "no", "n", "non"]:
        return False
    return default


# ==============================================================
# DÉLIMITEURS CSV
# ==============================================================

def normalize_delimiter(raw_delimiter: Any) -> str:
    """
    Normalise le délimiteur issu du fichier Excel.
    Spark CSV nécessite un caractère unique comme délimiteur.

    Args:
        raw_delimiter: valeur brute (souvent ';' ou ',')

    Returns:
        Caractère unique (ex: ',')

    Raises:
        ValueError si plus d’un caractère.
    """
    if raw_delimiter is None or str(raw_delimiter).strip() == "":
        return ","

    s = str(raw_delimiter).strip()
    if len(s) == 1:
        return s

    raise ValueError(f"Délimiteur '{raw_delimiter}' non supporté (1 seul caractère attendu).")


# ==============================================================
# HEADER CSV
# ==============================================================

def parse_header_mode(header_mode: str) -> Tuple[bool, bool]:
    """
    Détermine comment interpréter le header dans les fichiers CSV.

    Modes supportés :
    - HEADER USE → Utilise la première ligne comme nom de colonnes
    - FIRST LINE → Ignore la première ligne
    - Sinon → Aucun header

    Returns:
        (use_header, first_line_only)
    """
    if header_mode is None:
        return False, False

    mode = str(header_mode).strip().upper()

    if mode == "HEADER USE":
        return True, True
    if mode == "FIRST LINE":
        return True, False
    return False, False


# ==============================================================
# TOLÉRANCE AUX ERREURS
# ==============================================================

def parse_tolerance(raw_value: Any, total_rows: int, default: str = "10%") -> float:
    """
    Calcule la tolérance aux erreurs dans un fichier.

    Accepte :
    - "10%" → 0.1
    - "5"   → 5 / total_rows

    Args:
        raw_value: Valeur brute (str ou float)
        total_rows: Nombre total de lignes
        default: Valeur par défaut si invalide

    Returns:
        float (entre 0 et 1)
    """
    if raw_value is None or str(raw_value).strip().lower() in ["", "nan", "n/a", "none"]:
        raw_value = default

    s = str(raw_value).strip().lower().replace("%", "").replace(",", ".")

    # Cas 1 : Pourcentage
    match_percent = re.search(r"(\d+(\.\d+)?)%", str(raw_value))
    if match_percent:
        return float(match_percent.group(1)) / 100

    # Cas 2 : Valeur absolue
    match_absolute = re.search(r"^(\d+(\.\d+)?)$", s)
    if match_absolute and total_rows > 0:
        return float(match_absolute.group(1)) / total_rows

    return 0.0


# ==============================================================
# EXTRACTION DE DATE DEPUIS LE NOM DE FICHIER
# ==============================================================

def extract_parts_from_filename(filename: str) -> dict:
    """
    Extrait les parties yyyy, mm, dd à partir du nom du fichier.

    Exemples :
        site_20251201_120001.csv → {'yyyy': 2025, 'mm': 12, 'dd': 1}
        file_2024-07-15.csv      → {'yyyy': 2024, 'mm': 7, 'dd': 15}

    Args:
        filename: Nom du fichier

    Returns:
        dict { 'yyyy': int, 'mm': int, 'dd': int }
    """
    base = os.path.basename(filename)
    match = re.search(r"(?P<yyyy>\d{4})[-_](?P<mm>\d{2})[-_]?(?P<dd>\d{2})?", base)

    if not match:
        return {}

    parts = {}
    try:
        if match.group("yyyy"):
            parts["yyyy"] = int(match.group("yyyy"))
        if match.group("mm"):
            parts["mm"] = int(match.group("mm"))
        if match.group("dd"):
            parts["dd"] = int(match.group("dd"))
    except Exception:
        pass

    return parts
