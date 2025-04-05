import dis
import config as AppConfig
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

# Define the TTH buckets (in minutes).
TTH_BUCKETS = [
    (0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
    (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
    (50, 55), (55, 60)
]

def get_tth_bucket(despawn_timer_sec):
    """
    Convert despawn time (in seconds) to minutes and assign it to the correct TTH bucket.
    Returns a bucket string like "0_5", "5_10", etc.
    """
    logger.debug(f"▶️ Despawn timer: {despawn_timer_sec}s")
    despawn_timer_min = despawn_timer_sec // 60  # convert seconds to minutes
    for min_tth, max_tth in TTH_BUCKETS:
        if min_tth <= despawn_timer_min < max_tth:
            return f"{min_tth}_{max_tth}"
    return None  # Out of range

def build_tth_key(area: str, tth_bucket: str) -> str:
    """
    Build a plain text key for TTH timeseries.
    Example: ts:tth_pokemon:Saarlouis:10_15
    """
    return f"ts:tth_pokemon:{area}:{tth_bucket}"

def get_time_bucket(first_seen: int) -> str:
    """
    Round the timestamp to the nearest minute and return it as a string.
    """
    bucket = (first_seen // 60) * 60
    return str(bucket)

async def add_tth_timeseries_pokemon_event(data, pipe=None):
    """
    Add a Pokémon TTH event using plain text hash keys.

    The key is built as:
        ts:tth_pokemon:{area_name}:{tth_bucket}

    The hash field is the time bucket (first_seen rounded to the minute, in seconds as a string)
    and its value is incremented by 1.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot add Pokémon TTH event to timeseries.")
        return "ERROR"

    # ✅ Check that 'disappear_time_verified' is present and True.
    if not data.get("disappear_time_verified", False):
        status_verified = data.get("disappear_time_verified")
        logger.debug(f"⚠️ Skipping Pokémon data because disappear_time_verified is not True: {status_verified}")
        return "IGNORED"

    # Retrieve despawn timer and determine bucket.
    despawn_timer = data.get("despawn_timer", 0)
    if despawn_timer <= 0:
        logger.warning(f"⚠️ Ignoring Pokémon with invalid despawn timer: {despawn_timer}s")
        return "IGNORED"

    tth_bucket = get_tth_bucket(despawn_timer)
    if not tth_bucket:
        logger.warning(f"❌ Ignoring Pokémon with out-of-range despawn timer: {despawn_timer}s")
        return "IGNORED"

    # Retrieve first_seen timestamp and round it (as seconds).
    first_seen = data["first_seen"]
    bucket_field = get_time_bucket(first_seen)

    area = data["area_name"]
    key = build_tth_key(area, tth_bucket)
    logger.debug(f"🔑 Constructed TTH timeseries key: {key}")

    updated_fields = {}
    if pipe:
        pipe.hincrby(key, bucket_field, 1)
        updated_fields[tth_bucket] = "OK"
    else:
        await client.hincrby(key, bucket_field, 1)
        updated_fields[tth_bucket] = "OK"

    logger.debug(f"✅ Added Pokémon TTH event to hash: {key}")
    return updated_fields
