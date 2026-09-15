from utils.logger import logger

_UINT64_MASK = (1 << 64) - 1
_MAX_EXACT_FLOAT_INT = 1 << 53   # float64 represents every integer up to here exactly
_MAX_ENCOUNTER_ID_LEN = 32       # bounds what can end up as a Redis member


def normalize_encounter_id(eid) -> str | None:
    """
    Normalize an encounter_id to a canonical unsigned int64 string.

    Golbat's encounter_id is a uint64 and nearly half of them are above 2^63,
    so anything in the chain that round-trips them through a SIGNED int64
    turns them negative. Masking maps both spellings of the same encounter to
    one value, so "-6076667798472980710" and "12370076275236570906" cannot be
    treated as two different Pokémon.

    Returns None for missing/zero/empty values.
    """
    if eid is None:
        return None
    if isinstance(eid, float):
        # Below 2^53, float64 represents every integer exactly - safe to
        # convert. Beyond it (where real 64-bit encounter_ids live), whatever
        # produced this float has already lost precision before we saw it;
        # converting anyway risks silently matching the WRONG encounter, since
        # many distinct real ids round to the same float at that magnitude.
        if eid.is_integer() and abs(eid) <= _MAX_EXACT_FLOAT_INT:
            eid = int(eid)
        else:
            logger.warning(
                f"⚠️ encounter_id arrived as an imprecise float ({eid!r}) - "
                f"treating as missing rather than risk matching the wrong encounter"
            )
            return None
    try:
        val = int(eid) & _UINT64_MASK
        return str(val) if val != 0 else None
    except (TypeError, ValueError):
        # Not numeric at all: keep it, but bounded - this becomes a Redis member.
        s = str(eid).strip().lower()[:_MAX_ENCOUNTER_ID_LEN]
        return s if s and s != "0" else None


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

def _valid_coords(lat: float | None, lon: float | None) -> bool:
    if lat is None or lon is None:
        return False
    # Treat 0/0 as invalid for your use-case
    if lat == 0.0 or lon == 0.0:
        return False
    # Basic range checks
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return False
    return True
