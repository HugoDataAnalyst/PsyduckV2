import config as AppConfig
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils.redis_key_checker import ensure_timeseries_key

redis_manager = RedisManager()

async def add_timeseries_total_pokemon_event(data, pipe=None):
    """
    Add a Pokémon event into Redis TimeSeries.
    Supports Redis pipelines for batch processing.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot add Pokémon event to time series.")
        return "ERROR"

    # Retrieve and round the first_seen timestamp to the nearest minute
    first_seen = data["first_seen"]
    rounded_timestamp = (first_seen // 60) * 60
    ts = int(rounded_timestamp * 1000)  # Convert to milliseconds

    area = data["area_name"]
    pokemon_id = data["pokemon_id"]
    form = data.get("form", 0)

    # Define keys for each metric
    key_total      = f"ts:pokemon_totals:total:{area}:{pokemon_id}:{form}"
    key_iv100      = f"ts:pokemon_totals:iv100:{area}:{pokemon_id}:{form}"
    key_iv0        = f"ts:pokemon_totals:iv0:{area}:{pokemon_id}:{form}"
    key_pvp_little = f"ts:pokemon_totals:pvp_little:{area}:{pokemon_id}:{form}"
    key_pvp_great  = f"ts:pokemon_totals:pvp_great:{area}:{pokemon_id}:{form}"
    key_pvp_ultra  = f"ts:pokemon_totals:pvp_ultra:{area}:{pokemon_id}:{form}"
    key_shiny      = f"ts:pokemon_totals:shiny:{area}:{pokemon_id}:{form}"

    updated_fields = {}

    # Ensure keys exist
    retention_ms = AppConfig.timeseries_pokemon_retention_ms
    logger.debug(f"🚨 Set PokémonTotal retention timer: {AppConfig.timeseries_pokemon_retention_ms}")
    await ensure_timeseries_key(client, key_total, "pokemon_total", area, pokemon_id, form, retention_ms)
    await ensure_timeseries_key(client, key_iv100, "pokemon_iv100", area, pokemon_id, form, retention_ms)
    await ensure_timeseries_key(client, key_iv0, "pokemon_iv0", area, pokemon_id, form, retention_ms)
    await ensure_timeseries_key(client, key_pvp_little, "pokemon_pvp_little", area, pokemon_id, form, retention_ms)
    await ensure_timeseries_key(client, key_pvp_great, "pokemon_pvp_great", area, pokemon_id, form, retention_ms)
    await ensure_timeseries_key(client, key_pvp_ultra, "pokemon_pvp_ultra", area, pokemon_id, form, retention_ms)
    await ensure_timeseries_key(client, key_shiny, "pokemon_shiny", area, pokemon_id, form, retention_ms)

    # Determine metric increments
    inc_total      = 1  # Always add 1 for total
    inc_iv100      = 1 if data.get("iv") == 100 else 0
    inc_iv0        = 1 if data.get("iv") == 0 else 0
    inc_pvp_little = 1 if data.get("pvp_little_rank") and 1 in data.get("pvp_little_rank") else 0
    inc_pvp_great  = 1 if data.get("pvp_great_rank") and 1 in data.get("pvp_great_rank") else 0
    inc_pvp_ultra  = 1 if data.get("pvp_ultra_rank") and 1 in data.get("pvp_ultra_rank") else 0
    inc_shiny      = 1 if data.get("shiny") else 0

    if pipe:
        # Add to Redis pipeline
        pipe.execute_command("TS.ADD", key_total, ts, inc_total, "DUPLICATE_POLICY", "SUM")
        updated_fields["total"] = "OK"
        if inc_iv100:
            pipe.execute_command("TS.ADD", key_iv100, ts, inc_iv100, "DUPLICATE_POLICY", "SUM")
            updated_fields["iv100"] = "OK"
        if inc_iv0:
            pipe.execute_command("TS.ADD", key_iv0, ts, inc_iv0, "DUPLICATE_POLICY", "SUM")
            updated_fields["iv0"] = "OK"
        if inc_pvp_little:
            pipe.execute_command("TS.ADD", key_pvp_little, ts, inc_pvp_little, "DUPLICATE_POLICY", "SUM")
            updated_fields["pvp_little"] = "OK"
        if inc_pvp_great:
            pipe.execute_command("TS.ADD", key_pvp_great, ts, inc_pvp_great, "DUPLICATE_POLICY", "SUM")
            updated_fields["pvp_great"] = "OK"
        if inc_pvp_ultra:
            pipe.execute_command("TS.ADD", key_pvp_ultra, ts, inc_pvp_ultra, "DUPLICATE_POLICY", "SUM")
            updated_fields["pvp_ultra"] = "OK"
        if inc_shiny:
            pipe.execute_command("TS.ADD", key_shiny, ts, inc_shiny, "DUPLICATE_POLICY", "SUM")
            updated_fields["shiny"] = "OK"
    else:
        # Execute in a single Redis transaction
        async with client.pipeline() as pipe:
            pipe.execute_command("TS.ADD", key_total, ts, inc_total, "DUPLICATE_POLICY", "SUM")
            updated_fields["total"] = "OK"
            if inc_iv100:
                pipe.execute_command("TS.ADD", key_iv100, ts, inc_iv100, "DUPLICATE_POLICY", "SUM")
                updated_fields["iv100"] = "OK"
            if inc_iv0:
                pipe.execute_command("TS.ADD", key_iv0, ts, inc_iv0, "DUPLICATE_POLICY", "SUM")
                updated_fields["iv0"] = "OK"
            if inc_pvp_little:
                pipe.execute_command("TS.ADD", key_pvp_little, ts, inc_pvp_little, "DUPLICATE_POLICY", "SUM")
                updated_fields["pvp_little"] = "OK"
            if inc_pvp_great:
                pipe.execute_command("TS.ADD", key_pvp_great, ts, inc_pvp_great, "DUPLICATE_POLICY", "SUM")
                updated_fields["pvp_great"] = "OK"
            if inc_pvp_ultra:
                pipe.execute_command("TS.ADD", key_pvp_ultra, ts, inc_pvp_ultra, "DUPLICATE_POLICY", "SUM")
                updated_fields["pvp_ultra"] = "OK"
            if inc_shiny:
                pipe.execute_command("TS.ADD", key_shiny, ts, inc_shiny, "DUPLICATE_POLICY", "SUM")
                updated_fields["shiny"] = "OK"
            await pipe.execute()

    logger.debug(f"✅ Added Pokémon event to time series for Pokémon ID {pokemon_id} in area {area}")
    return updated_fields
