"""Fonctions utilitaires"""
import re
import os
from typing import Any, Tuple


def parse_bool(value: Any, default: bool = False) -> bool:
    """Convertit en booléen"""
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in ["true", "1", "yes", "y", "oui"]:
        return True
    if s in ["false", "0", "no", "n", "non"]:
        return False
    return default


def normalize_delimiter(raw: Any) -> str:
    """Normalise le délimiteur"""
    if raw is None or str(raw).strip() == "":
        return ","
    s = str(raw).strip()
    if len(s) == 1:
        return s
    raise ValueError(f"Délimiteur invalide: {raw}")


def parse_header_mode(mode: str) -> Tuple[bool, bool]:
    """Parse le mode header"""
    if not mode:
        return False, False
    m = str(mode).strip().upper()
    if m == "HEADER_USE":
        return True, True
    if m == "FIRST_LINE":
        return True, False
    return False, False


def parse_tolerance(raw: Any, total: int, default: str = "10%") -> float:
    """Parse la tolérance aux erreurs"""
    if not raw or str(raw).strip().lower() in ["", "nan", "none"]:
        raw = default
    
    s = str(raw).strip().replace("%", "").replace(",", ".")
    
    if "%" in str(raw):
        return float(s) / 100.0
    
    m = re.search(r"^(\d+(?:\.\d+)?)$", s)
    if m and total > 0:
        return float(m.group(1)) / total
    
    return 0.0


def extract_parts_from_filename(filename: str) -> dict:
    """Extrait yyyy/mm/dd du nom de fichier"""
    base = os.path.basename(filename)
    m = re.search(r"(\d{4})[_-](\d{2})[_-](\d{2})", base)
    if m:
        return {"yyyy": int(m.group(1)), "mm": int(m.group(2)), "dd": int(m.group(3))}
    return {}


def build_output_path(env: str, zone: str, table: str, version: str, parts: dict = None) -> str:
    """Construit le chemin de sortie"""
    return f"/mnt/wax/{env}/{zone}/{version}/{table}"
