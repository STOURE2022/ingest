import os
import re
from typing import Any, Tuple, Dict

def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in {"true","1","yes","y","oui"}: return True
    if s in {"false","0","no","n","non"}: return False
    return default

def normalize_delimiter(raw_delimiter: Any) -> str:
    if raw_delimiter is None or str(raw_delimiter).strip() == "":
        return ","
    s = str(raw_delimiter).strip()
    if len(s) == 1:
        return s
    raise ValueError("Délimiteur non supporté : doit être 1 caractère.")

def parse_header_mode(header_mode: Any) -> Tuple[bool, bool]:
    """
    Retourne (use_header, first_line_only)
    Accepte: "HEADER USE" / "HEADER_USE" et "FIRST LINE" / "FIRST_LINE"
    """
    if header_mode is None:
        return False, False
    s = str(header_mode).strip().upper().replace(" ", "_")
    if s == "HEADER_USE": return True, True
    if s == "FIRST_LINE": return True, False
    return False, False

def parse_tolerance(raw_value: Any, total_rows: int, default: str = "10%") -> float:
    """
    Retourne un ratio (0..1). Gère '10%' ou '5' (absolu converti via total_rows).
    """
    if raw_value is None or str(raw_value).strip().lower() in {"", "nan", "n/a", "none"}:
        raw_value = default
    s = str(raw_value).strip().lower().replace(",", ".").replace(" ", "")
    m_pct = re.fullmatch(r"(\d+(?:\.\d+)?)\%?", s)
    if m_pct and "%" in s:
        return float(m_pct.group(1)) / 100.0
    m_abs = re.fullmatch(r"(\d+(?:\.\d+)?)", s)
    if m_abs:
        if total_rows <= 0: return 0.0
        return float(m_abs.group(1)) / float(total_rows)
    return 0.0

def extract_parts_from_filename(filename: str) -> Dict[str, int]:
    base = os.path.basename(filename)
    m = re.search(r"(?P<yyyy>\d{4})[-_]?((?P<mm>\d{2})[-_]?((?P<dd>\d{2}))?)", base)
    if not m: return {}
    parts = {}
    if m.group("yyyy"): parts["yyyy"] = int(m.group("yyyy"))
    if m.group("mm"):   parts["mm"]   = int(m.group("mm"))
    if m.group("dd"):   parts["dd"]   = int(m.group("dd"))
    return parts

def build_output_path(env: str, zone: str, table_name: str, version: str, parts: dict) -> str:
    return f"/mnt/wax/{env}/{zone}/{version}/{table_name}"

def ensure_dbfs_local(path: str) -> str:
    return path.replace("dbfs:", "/dbfs") if path.startswith("dbfs:") else path
