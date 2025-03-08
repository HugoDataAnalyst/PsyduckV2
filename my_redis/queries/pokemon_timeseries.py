from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def ensure_timeseries_key(client, key, metric, area, pokemon_id, form, retention_ms):
    """
    Ensure that the time series for the given key exists.
    If not, create it with TS.CREATE including retention, duplicate policy, and labels.
    """
    exists = await client.exists(key)
    if not exists:
        logger.debug(f"🔑 Time series key {key} does not exist. Creating it with TS.CREATE.")
        # TS.CREATE <key> RETENTION <retention_ms> DUPLICATE_POLICY SUM LABELS metric <metric> area <area> pokemon_id <pokemon_id> form <form>
        result = await client.execute_command(
            "TS.CREATE",
            key,
            "RETENTION", retention_ms,
            "DUPLICATE_POLICY", "SUM",
            "LABELS",
            "metric", metric,
            "area", str(area),
            "pokemon_id", int(pokemon_id),
            "form", str(form)
        )
        if result:
            logger.debug(f"✅ Time series key {key} created.")
    else:
        logger.debug(f"❌ Time series key {key} already exists.")

async def add_timeseries_total_pokemon_event(data):
    """
    Add a Pokémon event into Redis TimeSeries.

    Expected data keys (from filter_data.py):
      - pokemon_id, form, area, iv, pvp_little_rank, pvp_great_rank, pvp_ultra_rank, first_seen

    The first_seen timestamp (in seconds) is rounded to the nearest minute.
    We then convert it to milliseconds (required for TS.ADD).

    A retention period of one month (30 days, 2592000000 ms) is set when creating
    the time series, and DUPLICATE_POLICY SUM is used so that events in the same minute
    aggregate into a single data point.

    Keys are prefixed with "ts:pokemon_totals" to ensure uniqueness.
    """
    # Ensure Redis connection is active
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot add Pokémon event to time series.")
        return

    # Retrieve and round the first_seen timestamp to the nearest minute
    first_seen = data["first_seen"]  # in seconds
    rounded_timestamp = (first_seen // 60) * 60  # rounds down to the minute
    ts = int(rounded_timestamp * 1000)  # convert to milliseconds
    logger.debug(f"✅ Rounded first_seen from {first_seen} to {rounded_timestamp} seconds, converted to {ts} ms")

    area = data["area"]
    pokemon_id = data["pokemon_id"]
    form = data.get("form", 0)

    # Define keys for each metric with unique prefix "ts:pokemon_totals"
    key_total      = f"ts:pokemon_totals:total:{area}:{pokemon_id}:{form}"
    key_iv100      = f"ts:pokemon_totals:iv100:{area}:{pokemon_id}:{form}"
    key_iv0        = f"ts:pokemon_totals:iv0:{area}:{pokemon_id}:{form}"
    key_pvp_little = f"ts:pokemon_totals:pvp_little:{area}:{pokemon_id}:{form}"
    key_pvp_great  = f"ts:pokemon_totals:pvp_great:{area}:{pokemon_id}:{form}"
    key_pvp_ultra  = f"ts:pokemon_totals:pvp_ultra:{area}:{pokemon_id}:{form}"

    logger.debug(f"🔑 Constructed keys: total={key_total}, iv100={key_iv100}, iv0={key_iv0}, "
                 f"pvp_little={key_pvp_little}, pvp_great={key_pvp_great}, pvp_ultra={key_pvp_ultra}")

    # Determine metric values based on the event
    total      = 1
    iv100      = 1 if data.get("iv") == 100 else 0
    iv0        = 1 if data.get("iv") == 0 else 0
    pvp_little = 1 if data.get("pvp_little_rank") and 1 in data.get("pvp_little_rank") else 0
    pvp_great  = 1 if data.get("pvp_great_rank") and 1 in data.get("pvp_great_rank") else 0
    pvp_ultra  = 1 if data.get("pvp_ultra_rank") and 1 in data.get("pvp_ultra_rank") else 0

    logger.debug(f"🎚️ Metric values: total={total}, iv100={iv100}, iv0={iv0}, "
                 f"pvp_little={pvp_little}, pvp_great={pvp_great}, pvp_ultra={pvp_ultra}")

    client = redis_manager.redis_client

    # Hardcoded retention period for a month (30 days) in milliseconds
    retention_ms = "2592000000"

    # Ensure each time series key exists; if not, create it with labels
    if total:
        await ensure_timeseries_key(client, key_total, "total", area, pokemon_id, form, retention_ms)
    if iv100:
        await ensure_timeseries_key(client, key_iv100, "iv100", area, pokemon_id, form, retention_ms)
    if iv0:
        await ensure_timeseries_key(client, key_iv0, "iv0", area, pokemon_id, form, retention_ms)
    if pvp_little:
        await ensure_timeseries_key(client, key_pvp_little, "pvp_little", area, pokemon_id, form, retention_ms)
    if pvp_great:
        await ensure_timeseries_key(client, key_pvp_great, "pvp_great", area, pokemon_id, form, retention_ms)
    if pvp_ultra:
        await ensure_timeseries_key(client, key_pvp_ultra, "pvp_ultra", area, pokemon_id, form, retention_ms)

    # Use TS.ADD commands with DUPLICATE_POLICY SUM to add data points (aggregating if duplicate)
    if total:
        await client.execute_command("TS.ADD", key_total, ts, total, "DUPLICATE_POLICY", "SUM")
    if iv100:
        await client.execute_command("TS.ADD", key_iv100, ts, iv100, "DUPLICATE_POLICY", "SUM")
    if iv0:
        await client.execute_command("TS.ADD", key_iv0, ts, iv0, "DUPLICATE_POLICY", "SUM")
    if pvp_little:
        await client.execute_command("TS.ADD", key_pvp_little, ts, pvp_little, "DUPLICATE_POLICY", "SUM")
    if pvp_great:
        await client.execute_command("TS.ADD", key_pvp_great, ts, pvp_great, "DUPLICATE_POLICY", "SUM")
    if pvp_ultra:
        await client.execute_command("TS.ADD", key_pvp_ultra, ts, pvp_ultra, "DUPLICATE_POLICY", "SUM")

    logger.info(f"✅ Added Pokémon event to time series for Pokémon ID {pokemon_id} in area {area}")
    return {
        "total_key": key_total,
        "iv100_key": key_iv100,
        "iv0_key": key_iv0,
        "pvp_little_key": key_pvp_little,
        "pvp_great_key": key_pvp_great,
        "pvp_ultra_key": key_pvp_ultra,
        "timestamp": ts
    }
