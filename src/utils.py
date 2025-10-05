"""
Module utils.py
---------------
Fonctions utilitaires génériques.
"""
import re


def parse_bool(x, default=False) -> bool:
    if x is None:
        return default
    s = str(x).strip().lower()
    if s in ["true", "1", "yes", "y", "oui"]:
        return True
    if s in ["false", "0", "no", "n", "non"]:
        return False
    return default


def normalize_delimiter(raw) -> str:
    if raw is None or str(raw).strip() == "":
        return ","
    s = str(raw).strip()
    if len(s) == 1:
        return s
    raise ValueError(f"❌ Delimiter '{raw}' non supporté (1 seul caractère attendu)")


def parse_header_mode(x) -> tuple[bool, bool]:
    if x is None:
        return False, False
    s = str(x).strip().upper()
    if s == "HEADER USE":
        return True, True
    if s == "FIRST LINE":
        return True, False
    return False, False


def parse_tolerance(raw, total_rows: int, default=0.0) -> float:
    if raw is None or str(raw).strip().lower() in ["", "nan", "n/a", "none"]:
        return default
    s = str(raw).strip().lower().replace(",", ".").replace("%", "").replace(" ", "")
    m2 = re.search(r"^(\d+(?:\.\d+)?)$", s)
    if m2:
        return float(m2.group(1)) / 100.0 if "%" in str(raw) else (
            float(m2.group(1)) / total_rows if total_rows > 0 else 0.0)
    return 0.0


def extract_parts_from_filename(fname: str) -> dict:
    base = fname.split("/")[-1]
    m = re.search(r"(?P<yyyy>\d{4})(?P<mm>\d{2})(?P<dd>\d{2})?", base)
    if not m:
        return {}
    parts = {}
    if m.group("yyyy"): parts["yyyy"] = int(m.group("yyyy"))
    if m.group("mm"): parts["mm"] = int(m.group("mm"))
    if m.group("dd"): parts["dd"] = int(m.group("dd"))
    return parts


def build_output_path(env: str, zone: str, table_name: str, version: str, parts: dict) -> str:
    return f"/mnt/wax/{env}/{zone}/{version}/{table_name}"
