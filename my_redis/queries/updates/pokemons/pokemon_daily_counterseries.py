from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_daily_pokemon_counter(data, pipe=None):
    """
    Update daily counters for a Pokémon event using a single Redis hash per area per calendar day.
    Supports Redis pipelines for batch processing.

    Key: counter:pokemon_daily:{area}:{YYYYMMDD}  (actual calendar date, not week-aligned)
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot update Pokémon daily counter.")
        return "ERROR"

    ts = data["first_seen"]
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")

    area = data["area_name"]
    pokemon_id = data["pokemon_id"]
    form = data.get("form", 0)

    hash_key = f"counter:pokemon_daily:{area}:{date_str}"

    field_total      = f"{pokemon_id}:{form}:total"
    field_iv100      = f"{pokemon_id}:{form}:iv100"
    field_iv0        = f"{pokemon_id}:{form}:iv0"
    field_pvp_little = f"{pokemon_id}:{form}:pvp_little"
    field_pvp_great  = f"{pokemon_id}:{form}:pvp_great"
    field_pvp_ultra  = f"{pokemon_id}:{form}:pvp_ultra"
    field_shiny      = f"{pokemon_id}:{form}:shiny"

    inc_iv100      = 1 if data.get("iv") == 100 else 0
    inc_iv0        = 1 if data.get("iv") == 0 else 0
    inc_pvp_little = 1 if data.get("pvp_little_rank") and 1 in data.get("pvp_little_rank") else 0
    inc_pvp_great  = 1 if data.get("pvp_great_rank") and 1 in data.get("pvp_great_rank") else 0
    inc_pvp_ultra  = 1 if data.get("pvp_ultra_rank") and 1 in data.get("pvp_ultra_rank") else 0
    inc_shiny      = 1 if data.get("shiny") else 0

    updated_fields = {}

    if pipe:
        pipe.hincrby(hash_key, field_total, 1)
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
        async with client.pipeline() as pipe:
            pipe.hincrby(hash_key, field_total, 1)
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
            await pipe.execute()

    logger.debug(f"✅ Bulk updated Pokémon daily counters in hash {hash_key}")
    return updated_fields
