from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_total_pokemon_counter(data, pipe=None):
    """
    Update daily counters for a Pokémon event using a single Redis hash per area per day.
    Supports Redis pipelines for batch processing.
    """
    client = await redis_manager.check_redis_connection("pokemon_pool")
    if not client:
        logger.error("❌ Redis is not connected. Cannot update Pokémon counter.")
        return "ERROR"

    ts = data["first_seen"]
    dt = datetime.fromtimestamp(ts)
    # dt.weekday() returns 0 for Monday, 6 for Sunday.
    monday_dt = dt - timedelta(days=dt.weekday(),
                                hours=dt.hour,
                                minutes=dt.minute,
                                seconds=dt.second,
                                microseconds=dt.microsecond)
    date_str = monday_dt.strftime("%Y%m%d")

    area = data["area_name"]
    pokemon_id = data["pokemon_id"]
    form = data.get("form", 0)

    # Construct the hash key for the area and date
    hash_key = f"counter:pokemon_total:{area}:{date_str}"

    # Construct field names for each metric
    field_total      = f"{pokemon_id}:{form}:total"
    field_iv100      = f"{pokemon_id}:{form}:iv100"
    field_iv0        = f"{pokemon_id}:{form}:iv0"
    field_pvp_little = f"{pokemon_id}:{form}:pvp_little"
    field_pvp_great  = f"{pokemon_id}:{form}:pvp_great"
    field_pvp_ultra  = f"{pokemon_id}:{form}:pvp_ultra"
    field_shiny      = f"{pokemon_id}:{form}:shiny"

    # Determine metric increments
    inc_total      = 1
    inc_iv100      = 1 if data.get("iv") == 100 else 0
    inc_iv0        = 1 if data.get("iv") == 0 else 0
    inc_pvp_little = 1 if data.get("pvp_little_rank") and 1 in data.get("pvp_little_rank") else 0
    inc_pvp_great  = 1 if data.get("pvp_great_rank") and 1 in data.get("pvp_great_rank") else 0
    inc_pvp_ultra  = 1 if data.get("pvp_ultra_rank") and 1 in data.get("pvp_ultra_rank") else 0
    inc_shiny      = 1 if data.get("shiny") else 0

    updated_fields = {}

    if pipe:
        # Add commands to the provided pipeline
        pipe.hincrby(hash_key, field_total, inc_total)
        updated_fields["total"] = "OK"
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
            pipe.hincrby(hash_key, field_total, inc_total)
            updated_fields["total"] = "OK"
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

    logger.info(f"✅ Bulk updated Pokémon counters in hash {hash_key}")
    return updated_fields
