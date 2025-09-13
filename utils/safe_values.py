from utils.logger import logger

def _safe_int(v, default=None):
    try:
        # handle bools, numeric strings, numbers
        if v is None:
            return default
        return int(v)
    except (TypeError, ValueError):
        return default

def _norm_str(v, default=""):
    if v is None:
        return default
    s = str(v).strip()
    try:
        s = s.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    return s

def _norm_name(v: object) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    # prevent delimiter breakage and overlong strings
    s = s.replace("|", "/")
    try:
        s = s.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    return s[:255]  # table limit

def _to_int(v, default=0):
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return int(v)
    except (TypeError, ValueError):
        return default

def _to_float(v, default=None):
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return float(v)
    except (TypeError, ValueError):
        return default
def _form_str(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    try:
        s = s.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    return s[:15]  # enforce column length

def _username_str(v: object) -> str:
    # keep up to 255 chars; allow utf8 (table is utf8mb4)
    s = "" if v is None else str(v).strip()
    return s[:255]
