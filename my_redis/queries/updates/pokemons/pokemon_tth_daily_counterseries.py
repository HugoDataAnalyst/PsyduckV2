from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

TTH_BUCKETS = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
               (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
               (50, 55), (55, 60)]

def get_tth_bucket(despawn_timer_sec):
    """Converts despawn time from seconds to minutes and assigns it to the correct TTH bucket."""
    despawn_timer_min = despawn_timer_sec // 60
    for min_tth, max_tth in TTH_BUCKETS:
        if min_tth <= despawn_timer_min < max_tth:
            return f"{min_tth}_{max_tth}"
    return None

async def update_tth_pokemon_daily_counter(data, pipe=None):
    """
    Increment Pokémon TTH counters for the given area & time-to-hatch range per calendar day.
    Supports optional Redis pipeline for batch processing.

    Key: counter:tth_pokemon_daily:{area}:{YYYYMMDD}  (actual calendar date, not week-aligned)
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot update Pokémon TTH daily counter.")
        return "ERROR"

    if not data.get("disappear_time_verified", False):
        status_verified = data.get("disappear_time_verified")
        logger.debug(f"⚠️ Skipping Pokémon data because disappear_time_verified is not True: {status_verified}")
        return "IGNORED"

    ts = data["first_seen"]
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")

    area = data["area_name"]
    despawn_timer = data.get("despawn_timer", 0)
    if despawn_timer <= 0:
        logger.warning(f"⚠️ Ignoring Pokémon with invalid despawn timer: {despawn_timer}s")
        return "IGNORED"

    tth_bucket = get_tth_bucket(despawn_timer)
    if not tth_bucket:
        logger.warning(f"❌ Ignoring Pokémon with out-of-range despawn timer: {despawn_timer}s")
        return "IGNORED"

    hash_key = f"counter:tth_pokemon_daily:{area}:{date_str}"
    field_name = f"{tth_bucket}"

    logger.debug(f"🔑 Hash Key: {hash_key}, Field: {field_name}")

    updated_fields = {}

    if pipe:
        pipe.hincrby(hash_key, field_name, 1)
        updated_fields[tth_bucket] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(hash_key, field_name, 1)
            await pipe.execute()
        updated_fields[tth_bucket] = "OK"

    logger.debug(f"✅ Incremented TTH daily counter {field_name} for area {area}.")
    return updated_fields
