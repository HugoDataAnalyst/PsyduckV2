from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

# Define TTH Ranges
TTH_BUCKETS = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
               (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
               (50, 55), (55, 60)]

def get_tth_bucket(despawn_timer):
    """Assigns a Pokémon to the correct TTH bucket."""
    for min_tth, max_tth in TTH_BUCKETS:
        if min_tth <= despawn_timer < max_tth:
            return f"{min_tth}_{max_tth}"
    return None

async def update_tth_pokemon_counter(data):
    """
    Increment Pokémon TTH counters for the given area & time-to-hatch range.
    - Uses Redis hash to store per-day per-area TTH counters.
    """
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot update Pokémon TTH counter.")
        return

    # Convert first_seen timestamp to YYYYMMDD format
    ts = data["first_seen"]
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")

    area = data["area"]
    despawn_timer = data.get("despawn_timer", 0)

    # Determine TTH bucket
    tth_bucket = get_tth_bucket(despawn_timer)
    if not tth_bucket:
        logger.warning(f"❌ Ignoring Pokémon with out-of-range despawn timer: {despawn_timer}s")
        return

    # Construct Redis key for the area and date
    hash_key = f"counter:tth_pokemon:{area}:{date_str}"
    field_name = f"{tth_bucket}"

    logger.debug(f"🔑 Hash Key: {hash_key}, Field: {field_name}")

    client = redis_manager.redis_client
    await client.hincrby(hash_key, field_name, 1)  # Increment counter

    logger.debug(f"✅ Incremented TTH counter {field_name} for area {area}.")
