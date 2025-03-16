import config as AppConfig
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from utils.redis_key_checker import ensure_timeseries_key

redis_manager = RedisManager()

TTH_BUCKETS = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
               (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
               (50, 55), (55, 60)]

def get_tth_bucket(despawn_timer_sec):
    """Converts despawn time from seconds to minutes and assigns it to the correct TTH bucket."""
    despawn_timer_min = despawn_timer_sec // 60  # Convert seconds to minutes

    for min_tth, max_tth in TTH_BUCKETS:
        if min_tth <= despawn_timer_min < max_tth:
            return f"{min_tth}_{max_tth}"

    return None  # Out of range

async def add_tth_timeseries_pokemon_event(data, pipe=None):
    """
    Add a Pokémon event into Redis TimeSeries for TTH-based tracking.
    Supports an optional Redis pipeline for batch processing.
    """
    redis_status = await redis_manager.check_redis_connection("pokemon_pool")
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot add Pokémon TTH event to time series.")
        return "ERROR"

    # Retrieve despawn timer and determine bucket
    despawn_timer = data.get("despawn_timer", 0)

    if despawn_timer <= 0:
        logger.warning(f"⚠️ Ignoring Pokémon with invalid despawn timer: {despawn_timer}s")
        return "IGNORED"

    tth_bucket = get_tth_bucket(despawn_timer)
    if not tth_bucket:
        logger.warning(f"❌ Ignoring Pokémon with out-of-range despawn timer: {despawn_timer}s")
        return "IGNORED"

    # Retrieve timestamp (first_seen, rounded to minute)
    first_seen = data["first_seen"]
    ts = int((first_seen // 60) * 60 * 1000)  # Convert to ms, rounding to minute

    area = data["area_name"]
    key = f"ts:tth_pokemon:{area}:{tth_bucket}"

    logger.debug(f"🔑 Constructed TimeSeries Key: {key}")

    client = redis_manager.redis_client
    updated_fields = {}

    # Ensure the time series key exists
    retention_ms = AppConfig.tth_timeseries_retention_ms
    logger.debug(f"🚨 Set PokémonTTH retention timer: {AppConfig.tth_timeseries_retention_ms}")
    await ensure_timeseries_key(client, key, "tth", area, tth_bucket, "", retention_ms, pipe)

    if pipe:
        pipe.execute_command("TS.ADD", key, ts, 1, "DUPLICATE_POLICY", "SUM")  # Add to pipeline
        updated_fields[tth_bucket] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.execute_command("TS.ADD", key, ts, 1, "DUPLICATE_POLICY", "SUM")
            await pipe.execute()  # Execute pipeline transaction

        updated_fields[tth_bucket] = "OK"

    logger.info(f"✅ Added Pokémon TTH event to TimeSeries: {key}")
    return updated_fields
