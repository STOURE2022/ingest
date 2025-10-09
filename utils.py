# src/utils.py
# --------------------------------------------------------------------------------------
# Fonctions utilitaires génériques (parsing, tolérance, nettoyage, comptage).
# Issues du notebook WAX Data Ingestion Pipeline.
# --------------------------------------------------------------------------------------

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# =========================
# 1) Booléens depuis Excel ou config
# =========================
def parse_bool(x, default: bool = False) -> bool:
    """
    Convertit une valeur texte/numérique en booléen.
    """
    if x is None:
        return default
    s = str(x).strip().lower()
    if s in ["true", "1", "yes", "y", "oui"]:
        return True
    if s in ["false", "0", "no", "n", "non"]:
        return False
    return default


# =========================
# 2) Délimiteur CSV
# =========================
def normalize_delimiter(raw) -> str:
    """
    Normalise le délimiteur CSV.
    - Si vide → ','
    - Si une seule lettre → OK
    - Sinon → ValueError
    """
    if raw is None or str(raw).strip() == "":
        return ","
    s = str(raw).strip()
    if len(s) == 1:
        return s
    raise ValueError(f"Délimiteur '{raw}' invalide")


# =========================
# 3) Mode header (Databricks Excel)
# =========================
def parse_header_mode(x) -> tuple[bool, bool]:
    """
    Détermine si le fichier a un header exploitable et s’il faut ignorer la première ligne.
    Retourne (use_header, first_line_only)
    """
    if x is None:
        return False, False
    s = str(x).strip().upper()
    if s == "HEADER USE":
        return True, True
    if s == "FIRST LINE":
        return True, False
    return False, False


# =========================
# 4) Tolérance (rejected lines)
# =========================
def parse_tolerance(raw, total_rows: int, default: float = 0.0) -> float:
    """
    Calcule la tolérance à partir d'une chaîne contenant % ou valeur absolue.
    Exemples :
        "10%"  → 0.10
        "5"    → 5 / total_rows
    """
    if raw is None or str(raw).strip().lower() in ["", "nan", "n/a", "none"]:
        return default

    s = str(raw).strip().lower().replace(",", ".").replace("%", "").replace(" ", "")
    m = re.search(r"^(\d+(?:\.\d+)?)%?$", s)
    if not m:
        return default

    val = float(m.group(1))
    if "%" in str(raw):
        return val / 100.0
    if total_rows <= 0:
        return 0.0
    return val / total_rows


# =========================
# 5) Dé-duplication de colonnes
# =========================
def deduplicate_columns(df: DataFrame) -> DataFrame:
    """
    Supprime les colonnes dupliquées (insensibles à la casse).
    """
    seen, cols = set(), []
    for c in df.columns:
        c_lower = c.lower()
        if c_lower not in seen:
            cols.append(c)
            seen.add(c_lower)
    return df.select(*cols)


# =========================
# 6) Count sécurisé
# =========================
def safe_count(df: DataFrame) -> int:
    """
    Retourne le nombre de lignes sans lever d’erreur si le DataFrame est vide ou invalide.
    """
    try:
        return df.count()
    except Exception:
        return 0


# =========================
# 7) Trim global d’un DataFrame
# =========================
def trim_all(df: DataFrame) -> DataFrame:
    """
    Supprime les espaces autour de toutes les colonnes string.
    """
    for c, t in df.dtypes:
        if t in ["string", "str"]:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df


# =========================
# 8) Validation d’une valeur numérique
# =========================
def is_numeric(value) -> bool:
    """
    Vérifie si une valeur est numérique (int/float/str numérique).
    """
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False
