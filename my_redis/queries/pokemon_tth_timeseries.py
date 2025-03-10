from my_redis.connect_redis import RedisManager
from utils.logger import logger
from utils.redis_key_checker import ensure_timeseries_key

redis_manager = RedisManager()

TTH_BUCKETS = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
               (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
               (50, 55), (55, 60)]

def get_tth_bucket(despawn_timer):
    """Assigns a Pokémon to the correct TTH bucket."""
    for min_tth, max_tth in TTH_BUCKETS:
        if min_tth <= despawn_timer < max_tth:
            return f"{min_tth}_{max_tth}"
    return None

async def add_tth_timeseries_pokemon_event(data):
    """
    Add a Pokémon event into Redis TimeSeries for TTH-based tracking.
    """
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot add Pokémon TTH event to time series.")
        return

    # Retrieve despawn timer and determine bucket
    despawn_timer = data.get("despawn_timer", 0)
    tth_bucket = get_tth_bucket(despawn_timer)
    if not tth_bucket:
        logger.warning(f"❌ Ignoring Pokémon with out-of-range despawn timer: {despawn_timer}s")
        return

    # Retrieve timestamp (first_seen, rounded to minute)
    first_seen = data["first_seen"]
    ts = int((first_seen // 60) * 60 * 1000)  # Convert to ms, rounding to minute

    area = data["area"]
    key = f"ts:tth_pokemon:{area}:{tth_bucket}"

    logger.debug(f"🔑 Constructed TimeSeries Key: {key}")

    client = redis_manager.redis_client

    # Ensure the time series key exists
    await ensure_timeseries_key(client, key, "tth", area, tth_bucket, "", "2592000000")  # 30-day retention

    # Add event to the time series
    await client.execute_command("TS.ADD", key, ts, 1, "DUPLICATE_POLICY", "SUM")

    logger.info(f"✅ Added Pokémon TTH event to TimeSeries: {key}")
