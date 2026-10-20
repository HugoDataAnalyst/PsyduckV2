from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from typing import Optional
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import re
import time
import pytz
from server_fastapi import global_state

redis_manager = RedisManager()

_HGETALL_CHUNK = 500


async def aggregate_keys(keys: list, mode: str) -> dict:
    """
    Aggregates hash data from a list of keys.
    For "sum" mode, sums all field values (assuming integer values).
    For "grouped" mode, returns a dictionary mapping each key to its hash data.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    aggregated = {}
    # Pipelined: one round trip per chunk instead of one per key
    for i in range(0, len(keys), _HGETALL_CHUNK):
        chunk = keys[i:i + _HGETALL_CHUNK]
        async with client.pipeline(transaction=False) as pipe:
            for key in chunk:
                pipe.hgetall(key)
            results = await pipe.execute()
        for key, data in zip(chunk, results):
            if mode == "sum":
                for field, value in data.items():
                    try:
                        value = int(value)
                    except Exception:
                        value = 0
                    aggregated[field] = aggregated.get(field, 0) + value
            elif mode in ["grouped", "surged"]:
                aggregated[key] = {k: int(v) for k, v in data.items()}
    logger.debug(f"✅ Mode:{mode} Aggregation complete over {len(keys)} keys")
    return aggregated


# ── Key discovery without KEYS ────────────────────────────────────────────────
#
# Counter keys end in a time component ("...:{area}:{YYYYMMDDHH}" etc.), so for
# a bounded window the candidate names can be generated and checked with one
# pipelined EXISTS - instead of KEYS, which walks the whole keyspace (hundreds of
# thousands of ts:/counter: keys) and blocks Redis while it does. Windows too
# wide to enumerate fall back to SCAN, which costs as much but never blocks.

MAX_GENERATED_KEYS = 20000
_KNOWN_AREAS_TTL = 300
_known_areas_cache: tuple[float, list[str]] = (0.0, [])

_SLOT_STEPS = {
    "%Y%m%d%H": ("hour", lambda dt: dt.replace(minute=0, second=0, microsecond=0)),
    "%Y%m%d":   ("day",  lambda dt: dt.replace(hour=0, minute=0, second=0, microsecond=0)),
    "%Y%m":     ("month", lambda dt: dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)),
}


def _time_slots(time_format: str, start: datetime, end: datetime, limit: int) -> list[str] | None:
    """Every key time string with start <= slot < end (filter_keys_by_time's rule), or None if more than limit."""
    unit, floor = _SLOT_STEPS[time_format]
    step = {"hour": timedelta(hours=1), "day": timedelta(days=1), "month": relativedelta(months=1)}[unit]
    slots = []
    dt = floor(start)
    while dt < end:
        if dt >= start:
            slots.append(dt.strftime(time_format))
            if len(slots) > limit:
                return None
        dt += step
    return slots


async def known_areas() -> list[str]:
    """
    Area names a "global" query has to cover: the current geofences plus every
    area ever registered in area_names (removed areas still have history).
    """
    global _known_areas_cache
    now = time.monotonic()
    if now - _known_areas_cache[0] < _KNOWN_AREAS_TTL and _known_areas_cache[1]:
        return _known_areas_cache[1]

    names: set[str] = set()
    try:
        from utils.global_state_manager import GlobalStateManager
        geofences = await GlobalStateManager.get_geofences()
    except Exception:
        geofences = None
    for g in geofences or global_state.geofences or []:
        if g.get("name"):
            names.add(g["name"])
    try:
        from sql.connect_db import fetch_all
        for row in await fetch_all("SELECT name FROM area_names", ()):
            if row.get("name"):
                names.add(row["name"])
    except Exception as e:
        logger.debug(f"known_areas: area_names unavailable ({e}); using geofences only")

    result = sorted(names)
    if result:
        _known_areas_cache = (now, result)
    return result


async def find_time_keys(
    client,
    pattern: str,
    time_format: str,
    start: datetime,
    end: datetime,
    suffixes: tuple[str, ...] | None = None,
) -> list[str]:
    """
    Existing keys matching `pattern` whose time component lies in [start, end).

    pattern is what used to go to KEYS: "counter:<family>:*" for all areas or
    "counter:<family>:<area>:*" for one. The time component is the last part
    of the key, or the one before a fixed suffix (e.g. the 0/1 weather flag),
    in which case every possible suffix is passed in `suffixes`.
    """
    if not (pattern.startswith("counter:") and pattern.endswith(":*")) or time_format not in _SLOT_STEPS:
        raise ValueError(f"find_time_keys: unsupported pattern/format {pattern!r} {time_format!r}")
    # "counter:<family>[:<area>]:*"
    family, _, area = pattern[len("counter:"):-2].partition(":")
    family = f"counter:{family}"
    areas = [area] if area else await known_areas()
    tails = suffixes or (None,)

    budget = MAX_GENERATED_KEYS // max(1, len(areas) * len(tails))
    slots = _time_slots(time_format, start, end, budget) if areas else None

    if slots is None:
        # Too wide to enumerate (or no area list): non-blocking SCAN
        component_index = -2 if suffixes else -1
        keys = [k async for k in client.scan_iter(match=pattern, count=1000)]
        return filter_keys_by_time(keys, time_format, start, end, component_index=component_index)

    names = [
        f"{family}:{a}:{slot}" + (f":{tail}" if tail is not None else "")
        for a in areas for slot in slots for tail in tails
    ]
    if not names:
        return []
    found = []
    for i in range(0, len(names), 5000):
        chunk = names[i:i + 5000]
        async with client.pipeline(transaction=False) as pipe:
            for name in chunk:
                pipe.exists(name)
            exists = await pipe.execute()
        found.extend(n for n, e in zip(chunk, exists) if e)
    return found


def filter_keys_by_time(keys: list, time_format: str, start: datetime, end: datetime, component_index: int = -1) -> list:
    """
    Filters keys by extracting the time component from the specified component index (default is the last component)
    after splitting the key by a colon, parsing it according to time_format, and returning only those keys
    whose datetime falls between start and end.
    """
    filtered = []
    for key in keys:
        parts = key.split(":")
        if len(parts) < abs(component_index):
            continue
        try:
            timestr = parts[component_index]
            dt = datetime.strptime(timestr, time_format)
            logger.debug(f"Key: {key} parsed time: {dt}")
            if start <= dt < end:
                logger.debug(f"Key: {key} is in range.")
                filtered.append(key)
            else:
                logger.debug(f"Key: {key} with time {dt} is out of range.")
        except Exception as e:
            logger.warning(f"❌ Could not parse time from key {key}: {e}")
    logger.debug(f"✅ Filtered down to {len(filtered)} keys")
    return filtered

def parse_time_input(time_str: str, area_offset: int = 0) -> datetime:
    """
    Parses time input with precise handling for Redis queries:
    - ISO timestamps: treated as exact local_area_utc (what's stored in Redis)
    - Relative times: calculated in area's local time then converted to local_area_utc
    - "now": current time in area's local time converted to local_area_utc

    Args:
        time_str: Time string to parse
        area_offset: Timezone offset in hours for the area

    Returns:
        datetime object matching Redis' local_area_utc storage
    """
    time_str = time_str.strip().lower()

    # 1. Handle ISO format - treat as exact local_area_utc (already adjusted)
    try:
        dt = datetime.fromisoformat(time_str)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        pass

    # Get current UTC time
    now_utc = datetime.utcnow()

    # 2. Handle "now" - current local time in area converted to local_area_utc
    if time_str == "now":
        # Equivalent to: (now_utc + area_offset) - 0
        return now_utc + timedelta(hours=area_offset)

    # 3. Handle relative times
    pattern = re.compile(r"(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)")
    match = pattern.fullmatch(time_str)
    if match:
        value = int(match.group(1))
        unit = match.group(2).rstrip('s')  # Remove plural

        # Calculate reference time (current local time in area)
        local_now = now_utc + timedelta(hours=area_offset)

        # Calculate the past time in local timezone
        if unit == "second":
            past_local = local_now - timedelta(seconds=value)
        elif unit == "minute":
            past_local = local_now - timedelta(minutes=value)
        elif unit == "hour":
            past_local = local_now - timedelta(hours=value)
        elif unit == "day":
            past_local = local_now - timedelta(days=value)
        elif unit == "week":
            past_local = local_now - timedelta(weeks=value)
        elif unit == "month":
            past_local = local_now - relativedelta(months=value)
        elif unit == "year":
            past_local = local_now - relativedelta(years=value)

        # Convert to local_area_utc
        return past_local

    raise ValueError(f"Unrecognized time format: {time_str}")

def get_area_offset(area: str, geofences: list) -> int:
    """
    Get timezone offset for a given area.
    Defaults to 0 (UTC) if area not found or is global.
    """
    if area.lower() in ["global", "all"]:
        return {geofence["name"]: geofence.get("offset", 0) for geofence in geofences}

    for geofence in geofences:
        if geofence["name"].lower() == area.lower():
            return geofence.get("offset", 0)

    return 0

def get_area_offsets_for_list(areas: list[str], geofences: list) -> dict[str, int]:
    """
    Map a list of area names to offsets using geofences.
    Case-insensitive match; returns dict[name] = offset.
    """
    index = {g["name"].lower(): g for g in geofences}
    out = {}
    for raw in areas:
        name = raw.strip()
        if not name:
            continue
        g = index.get(name.lower())
        if g:
            out[g["name"]] = g.get("offset", 0)
    return out
