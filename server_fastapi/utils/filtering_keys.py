from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import re

redis_manager = RedisManager()

async def aggregate_keys(keys: list, mode: str, pool: str = None) -> dict:
    """
    Aggregates hash data from a list of keys.
    For "sum" mode, sums all field values (assuming integer values).
    For "grouped" mode, returns a dictionary mapping each key to its hash data.
    Uses the specified Redis pool if provided.
    """
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    aggregated = {}
    for key in keys:
        data = await client.hgetall(key, pool=pool)  # Expected to return a dict {field: value}
        if mode == "sum":
            for field, value in data.items():
                try:
                    value = int(value)
                except Exception:
                    value = 0
                aggregated[field] = aggregated.get(field, 0) + value
        elif mode == "grouped":
            aggregated[key] = {k: int(v) for k, v in data.items()}
    return aggregated

def filter_keys_by_time(keys: list, time_format: str, start: datetime, end: datetime) -> list:
    """
    Filters keys by extracting the time component at the end of the key,
    parsing it according to time_format, and returning only those keys
    whose datetime falls within [start, end].
    """
    filtered = []
    # Build a regex pattern that captures the time part at the end of the key.
    pattern = re.compile(r".*:([\d]{%d})$" % len(datetime.now().strftime(time_format)))
    for key in keys:
        m = pattern.match(key)
        if m:
            timestr = m.group(1)
            try:
                dt = datetime.strptime(timestr, time_format)
                if start <= dt <= end:
                    filtered.append(key)
            except Exception as e:
                logger.warning(f"Could not parse time from key {key}: {e}")
    return filtered
