from my_redis.connect_redis import RedisManager
from utils.logger import logger
from utils.redis_key_checker import ensure_timeseries_key

redis_manager = RedisManager()

async def add_timeseries_total_pokemon_event(data, pipe=None):
    """
    Add a Pokémon event into Redis TimeSeries.
    Supports Redis pipelines for batch processing.
    """
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot add Pokémon event to time series.")
        return

    # Retrieve and round the first_seen timestamp to the nearest minute
    first_seen = data["first_seen"]
    rounded_timestamp = (first_seen // 60) * 60
    ts = int(rounded_timestamp * 1000)  # Convert to milliseconds

    area = data["area"]
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

    client = redis_manager.redis_client

    # Ensure keys exist
    retention_ms = "2592000000"  # 30-day retention
    await ensure_timeseries_key(client, key_total, "total", area, pokemon_id, form, retention_ms, pipe)
    await ensure_timeseries_key(client, key_iv100, "iv100", area, pokemon_id, form, retention_ms, pipe)
    await ensure_timeseries_key(client, key_iv0, "iv0", area, pokemon_id, form, retention_ms, pipe)
    await ensure_timeseries_key(client, key_pvp_little, "pvp_little", area, pokemon_id, form, retention_ms, pipe)
    await ensure_timeseries_key(client, key_pvp_great, "pvp_great", area, pokemon_id, form, retention_ms, pipe)
    await ensure_timeseries_key(client, key_pvp_ultra, "pvp_ultra", area, pokemon_id, form, retention_ms, pipe)
    await ensure_timeseries_key(client, key_shiny, "shiny", area, pokemon_id, form, retention_ms, pipe)

    if pipe:
        # Add to Redis pipeline
        pipe.execute_command("TS.ADD", key_total, ts, 1, "DUPLICATE_POLICY", "SUM")
        pipe.execute_command("TS.ADD", key_iv100, ts, 1, "DUPLICATE_POLICY", "SUM")
        pipe.execute_command("TS.ADD", key_iv0, ts, 1, "DUPLICATE_POLICY", "SUM")
        pipe.execute_command("TS.ADD", key_pvp_little, ts, 1, "DUPLICATE_POLICY", "SUM")
        pipe.execute_command("TS.ADD", key_pvp_great, ts, 1, "DUPLICATE_POLICY", "SUM")
        pipe.execute_command("TS.ADD", key_pvp_ultra, ts, 1, "DUPLICATE_POLICY", "SUM")
        pipe.execute_command("TS.ADD", key_shiny, ts, 1, "DUPLICATE_POLICY", "SUM")
    else:
        # Execute in a single Redis transaction
        async with client.pipeline() as pipe:
            pipe.execute_command("TS.ADD", key_total, ts, 1, "DUPLICATE_POLICY", "SUM")
            pipe.execute_command("TS.ADD", key_iv100, ts, 1, "DUPLICATE_POLICY", "SUM")
            pipe.execute_command("TS.ADD", key_iv0, ts, 1, "DUPLICATE_POLICY", "SUM")
            pipe.execute_command("TS.ADD", key_pvp_little, ts, 1, "DUPLICATE_POLICY", "SUM")
            pipe.execute_command("TS.ADD", key_pvp_great, ts, 1, "DUPLICATE_POLICY", "SUM")
            pipe.execute_command("TS.ADD", key_pvp_ultra, ts, 1, "DUPLICATE_POLICY", "SUM")
            pipe.execute_command("TS.ADD", key_shiny, ts, 1, "DUPLICATE_POLICY", "SUM")
            await pipe.execute()

    logger.info(f"✅ Added Pokémon event to time series for Pokémon ID {pokemon_id} in area {area}")
