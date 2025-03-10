from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_total_pokemon_counter(data):
    """
    Update daily counters for a Pokémon event using a single Redis hash per area per day.

    The key is structured as:
      counter:pokemon_total:{area}:{YYYYMMDD}

    Each field in the hash is structured as:
      {pokemon_id}:{form}:{metric}

    Metrics include:
      - total
      - iv100
      - iv0
      - pvp_little
      - pvp_great
      - pvp_ultra
    """
    # Ensure Redis connection is active
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot update Pokémon counter.")
        return

    # Convert first_seen timestamp (in seconds) to a date string (YYYYMMDD)
    ts = data["first_seen"]
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")
    logger.debug(f"✅ Converted first_seen timestamp {ts} to date string {date_str}")

    area = data["area"]
    pokemon_id = data["pokemon_id"]
    form = data.get("form", 0)

    # Construct the hash key for the area and date
    hash_key = f"counter:pokemon_total:{area}:{date_str}"
    logger.debug(f"🔑 Constructed hash key: {hash_key}")

    # Construct field names for each metric using the composite key {pokemon_id}:{form}:<metric>
    field_total      = f"{pokemon_id}:{form}:total"
    field_iv100      = f"{pokemon_id}:{form}:iv100"
    field_iv0        = f"{pokemon_id}:{form}:iv0"
    field_pvp_little = f"{pokemon_id}:{form}:pvp_little"
    field_pvp_great  = f"{pokemon_id}:{form}:pvp_great"
    field_pvp_ultra  = f"{pokemon_id}:{form}:pvp_ultra"

    logger.debug(f"🔑 Constructed fields: total={field_total}, iv100={field_iv100}, iv0={field_iv0}, "
                 f"pvp_little={field_pvp_little}, pvp_great={field_pvp_great}, pvp_ultra={field_pvp_ultra}")

    # Determine metric increments for this event
    inc_total      = 1
    inc_iv100      = 1 if data.get("iv") == 100 else 0
    inc_iv0        = 1 if data.get("iv") == 0 else 0
    inc_pvp_little = 1 if data.get("pvp_little_rank") and 1 in data.get("pvp_little_rank") else 0
    inc_pvp_great  = 1 if data.get("pvp_great_rank") and 1 in data.get("pvp_great_rank") else 0
    inc_pvp_ultra  = 1 if data.get("pvp_ultra_rank") and 1 in data.get("pvp_ultra_rank") else 0

    logger.debug(f"🎚️ Counter increments: total={inc_total}, iv100={inc_iv100}, iv0={inc_iv0}, "
                 f"pvp_little={inc_pvp_little}, pvp_great={inc_pvp_great}, pvp_ultra={inc_pvp_ultra}")

    client = redis_manager.redis_client

    # Use a pipeline to batch the HINCRBY commands
    pipe = client.pipeline()
    pipe.hincrby(hash_key, field_total, inc_total)
    if inc_iv100:
        pipe.hincrby(hash_key, field_iv100, inc_iv100)
    if inc_iv0:
        pipe.hincrby(hash_key, field_iv0, inc_iv0)
    if inc_pvp_little:
        pipe.hincrby(hash_key, field_pvp_little, inc_pvp_little)
    if inc_pvp_great:
        pipe.hincrby(hash_key, field_pvp_great, inc_pvp_great)
    if inc_pvp_ultra:
        pipe.hincrby(hash_key, field_pvp_ultra, inc_pvp_ultra)

    results = await pipe.execute()
    logger.debug(f"✅ Bulk updated Pokémon counters in hash {hash_key} with results: {results}")
    return {
        "hash_key": hash_key,
        "fields": {
            "total": field_total,
            "iv100": field_iv100,
            "iv0": field_iv0,
            "pvp_little": field_pvp_little,
            "pvp_great": field_pvp_great,
            "pvp_ultra": field_pvp_ultra,
        },
        "date": date_str,
        "results": results
    }
