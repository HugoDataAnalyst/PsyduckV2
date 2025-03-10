from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_total_pokemon_counter(data):
    """
    Update daily counters for a Pokémon event using a Redis hash.

    The key is structured as:
      counter:pokemon_total:{area}:{pokemon_id}:{form}:{YYYYMMDD}

    The hash contains fields for each metric:
      - "total"
      - "iv100"
      - "iv0"
      - "pvp_little"
      - "pvp_great"
      - "pvp_ultra"

    Each event increments the corresponding field by 1 if the condition is met.
    """
    # Ensure Redis connection is active
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot update Pokémon counter.")
        return

    # Convert the first_seen timestamp (in seconds) into a date string (YYYYMMDD)
    ts = data["first_seen"]
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")
    logger.debug(f"✅ Converted first_seen timestamp {ts} to date string {date_str}")

    area = data["area"]
    pokemon_id = data["pokemon_id"]
    form = data.get("form", 0)

    # Construct the hash key
    hash_key = f"counter:pokemon_total:{area}:{pokemon_id}:{form}:{date_str}"
    logger.debug(f"🔑 Constructed hash key: {hash_key}")

    # Determine the metric increments for this event
    inc_total      = 1
    inc_iv100      = 1 if data.get("iv") == 100 else 0
    inc_iv0        = 1 if data.get("iv") == 0 else 0
    inc_pvp_little = 1 if data.get("pvp_little_rank") and 1 in data.get("pvp_little_rank") else 0
    inc_pvp_great  = 1 if data.get("pvp_great_rank") and 1 in data.get("pvp_great_rank") else 0
    inc_pvp_ultra  = 1 if data.get("pvp_ultra_rank") and 1 in data.get("pvp_ultra_rank") else 0

    logger.debug(f"🎚️ Counter increments: total={inc_total}, iv100={inc_iv100}, iv0={inc_iv0}, "
                 f"pvp_little={inc_pvp_little}, pvp_great={inc_pvp_great}, pvp_ultra={inc_pvp_ultra}")

    client = redis_manager.redis_client

    # Create a pipeline to batch HINCRBY commands on the hash
    pipe = client.pipeline()
    pipe.hincrby(hash_key, "total", inc_total)
    if inc_iv100:
        pipe.hincrby(hash_key, "iv100", inc_iv100)
    if inc_iv0:
        pipe.hincrby(hash_key, "iv0", inc_iv0)
    if inc_pvp_little:
        pipe.hincrby(hash_key, "pvp_little", inc_pvp_little)
    if inc_pvp_great:
        pipe.hincrby(hash_key, "pvp_great", inc_pvp_great)
    if inc_pvp_ultra:
        pipe.hincrby(hash_key, "pvp_ultra", inc_pvp_ultra)

    results = await pipe.execute()
    logger.debug(f"✅ Bulk updated Pokémon counters for key {hash_key} with results: {results}")
    return {
        "hash_key": hash_key,
        "date": date_str,
        "results": results
    }
