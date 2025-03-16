from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_pokemon_hourly_counter(data, pipe=None):
    """
    Update hourly counters for Pokémon events using a single Redis hash per area per hour.

    The hash key is constructed as:
      "counter:pokemon_hourly:{area}:{date_hour}"
    where date_hour is formatted as YYYYMMDDHH.

    This function only increments counters for:
      - iv100 (if iv == 100)
      - iv0 (if iv == 0)
      - pvp_little (if pvp_little_rank qualifies)
      - pvp_great (if pvp_great_rank qualifies)
      - pvp_ultra (if pvp_ultra_rank qualifies)
      - shiny (if shiny is truthy)
    """
    redis_status = await redis_manager.check_redis_connection("pokemon_pool")
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot update Pokémon counter.")
        return "ERROR"

    # Convert first_seen timestamp (in seconds) to a date string (YYYYMMDD)
    ts = data["first_seen"]
    date_hour = datetime.fromtimestamp(ts).strftime("%Y%m%d%H")

    area = data["area_name"]
    pokemon_id = data["pokemon_id"]
    form = data.get("form", 0)

    # Construct the hash key for the area and date
    hash_key = f"counter:pokemon_hourly:{area}:{date_hour}"

    # Construct field names for each metric
    field_iv100      = f"{pokemon_id}:{form}:iv100"
    field_iv0        = f"{pokemon_id}:{form}:iv0"
    field_pvp_little = f"{pokemon_id}:{form}:pvp_little"
    field_pvp_great  = f"{pokemon_id}:{form}:pvp_great"
    field_pvp_ultra  = f"{pokemon_id}:{form}:pvp_ultra"
    field_shiny      = f"{pokemon_id}:{form}:shiny"

    # Determine metric increments
    inc_iv100      = 1 if data.get("iv") == 100 else 0
    inc_iv0        = 1 if data.get("iv") == 0 else 0
    inc_pvp_little = 1 if data.get("pvp_little_rank") and 1 in data.get("pvp_little_rank") else 0
    inc_pvp_great  = 1 if data.get("pvp_great_rank") and 1 in data.get("pvp_great_rank") else 0
    inc_pvp_ultra  = 1 if data.get("pvp_ultra_rank") and 1 in data.get("pvp_ultra_rank") else 0
    inc_shiny      = 1 if data.get("shiny") else 0

    client = redis_manager.redis_client
    updated_fields = {}

    if pipe:
        # Add commands to the provided pipeline
        if inc_iv100:
            pipe.hincrby(hash_key, field_iv100, inc_iv100)
            updated_fields["iv100"] = "OK"
        if inc_iv0:
            pipe.hincrby(hash_key, field_iv0, inc_iv0)
            updated_fields["iv0"] = "OK"
        if inc_pvp_little:
            pipe.hincrby(hash_key, field_pvp_little, inc_pvp_little)
            updated_fields["pvp_little"] = "OK"
        if inc_pvp_great:
            pipe.hincrby(hash_key, field_pvp_great, inc_pvp_great)
            updated_fields["pvp_great"] = "OK"
        if inc_pvp_ultra:
            pipe.hincrby(hash_key, field_pvp_ultra, inc_pvp_ultra)
            updated_fields["pvp_ultra"] = "OK"
        if inc_shiny:
            pipe.hincrby(hash_key, field_shiny, inc_shiny)
            updated_fields["shiny"] = "OK"
    else:
        # Execute pipeline transaction if no external pipeline is passed
        async with client.pipeline() as pipe:
            if inc_iv100:
                pipe.hincrby(hash_key, field_iv100, inc_iv100)
                updated_fields["iv100"] = "OK"
            if inc_iv0:
                pipe.hincrby(hash_key, field_iv0, inc_iv0)
                updated_fields["iv0"] = "OK"
            if inc_pvp_little:
                pipe.hincrby(hash_key, field_pvp_little, inc_pvp_little)
                updated_fields["pvp_little"] = "OK"
            if inc_pvp_great:
                pipe.hincrby(hash_key, field_pvp_great, inc_pvp_great)
                updated_fields["pvp_great"] = "OK"
            if inc_pvp_ultra:
                pipe.hincrby(hash_key, field_pvp_ultra, inc_pvp_ultra)
                updated_fields["pvp_ultra"] = "OK"
            if inc_shiny:
                pipe.hincrby(hash_key, field_shiny, inc_shiny)
                updated_fields["shiny"] = "OK"
            await pipe.execute()  # Execute pipeline

    logger.info(f"✅ Bulk updated Pokémon Hourly counters in hash {hash_key} with fields: {updated_fields}")
    return updated_fields
