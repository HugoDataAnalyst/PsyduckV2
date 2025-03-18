from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import re

redis_manager = RedisManager()

async def aggregate_keys(keys: list, mode: str) -> dict:
    """
    Aggregates hash data from a list of keys.
    For "sum" mode, sums all field values (assuming integer values).
    For "grouped" mode, returns a dictionary mapping each key to its hash data.
    """
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    aggregated = {}
    for key in keys:
        data = await client.hgetall(key)
        logger.info(f"🔑 Aggregating key: {key} with data: {data}")
        if mode == "sum":
            for field, value in data.items():
                try:
                    value = int(value)
                except Exception:
                    value = 0
                aggregated[field] = aggregated.get(field, 0) + value
        elif mode == "grouped":
            aggregated[key] = {k: int(v) for k, v in data.items()}
    logger.info(f"✅ Aggregation complete. Aggregated data: {aggregated}")
    return aggregated

def filter_keys_by_time(keys: list, time_format: str, start: datetime, end: datetime) -> list:
    """
    Filters keys by extracting the time component at the end of the key,
    parsing it according to time_format, and returning only those keys
    whose datetime falls within [start, end].
    """
    filtered = []
    # Build a regex pattern that captures the time part at the end of the key.
    pattern_str = r".*:([\d]{%d})$" % len(datetime.now().strftime(time_format))
    pattern = re.compile(pattern_str)
    logger.info(f"▶️ Using regex pattern: {pattern_str} for time_format: {time_format}")
    logger.info(f"🔍 Filtering {len(keys)} 🔑 keys between {start} and {end}")

    for key in keys:
        m = pattern.match(key)
        if m:
            timestr = m.group(1)
            try:
                dt = datetime.strptime(timestr, time_format)
                logger.info(f"🔑 Key: {key} parsed time: {dt}")
                if start <= dt <= end:
                    logger.info(f"🔑 Key: {key} ✅ is in range.")
                    filtered.append(key)
                else:
                    logger.info(f"🔑 Key: {key} with time {dt} ❌ is out of range.")
            except Exception as e:
                logger.warning(f"❌ Could not parse time from 🔑 key {key}: {e}")
        else:
            logger.info(f"🔑 Key: {key} ❌ does not match pattern")
    logger.info(f"✅ Filtered down to {len(filtered)} 🔑 keys")
    return filtered

def parse_time_input(time_str: str, reference: datetime = None) -> datetime:
    """
    Parses a time input string.
    If time_str can be parsed as ISO, returns that datetime.
    Otherwise, if time_str is in a relative format like "1 month", "10 days", "1 day", "3 months", "1 year", etc.,
    subtracts that duration from the reference (or now if reference is None) and returns that datetime.
    If time_str is "now", returns datetime.now().
    """
    if reference is None:
        reference = datetime.now()
    time_str = time_str.strip().lower()
    if time_str == "now":
        return datetime.now()
    try:
        # Try to interpret as an ISO datetime
        return datetime.fromisoformat(time_str)
    except Exception:
        pass
    # Match a relative time pattern, e.g., "10 days", "1 month", "3 years"
    pattern = re.compile(r"(\d+)\s*(day|days|month|months|year|years)")
    match = pattern.fullmatch(time_str)
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        if unit in ("day", "days"):
            return reference - timedelta(days=value)
        elif unit in ("month", "months"):
            try:
                from dateutil.relativedelta import relativedelta
                return reference - relativedelta(months=value)
            except ImportError:
                # Approximate a month as 30 days if dateutil is not available
                return reference - timedelta(days=30 * value)
        elif unit in ("year", "years"):
            try:
                from dateutil.relativedelta import relativedelta
                return reference - relativedelta(years=value)
            except ImportError:
                # Approximate a year as 365 days
                return reference - timedelta(days=365 * value)
    raise ValueError(f"Unrecognized time format: {time_str}")
