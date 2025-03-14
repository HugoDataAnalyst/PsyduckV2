import config as AppConfig
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from utils.redis_key_checker import ensure_timeseries_key

redis_manager = RedisManager()

async def add_raid_timeseries_event(data, pipe=None):
    """
    Add a Raid event into Redis TimeSeries for tracking.
    Supports an optional Redis pipeline for batch processing.

    Expected keys in `data`:
      - "raid_first_seen": raid spawn UTC timestamp (in seconds)
      - "area_name": area name (string)
      - "raid_pokemon": raid boss Pokémon ID (integer)
      - "raid_level": raid level (integer)
      - "raid_form": raid boss form (integer, default 0)
      - "raid_costume": raid boss costume (integer)
      - "raid_is_exclusive": whether the raid is exclusive (boolean/int; converted to 0 or 1)
      - "raid_ex_raid_eligible": whether the raid is eligible for EX (boolean/int; converted to 0 or 1)
    """
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot add Raid event to time series.")
        return "ERROR"

    # Retrieve and round the first_seen timestamp to the nearest minute
    raid_first_seen = data["raid_first_seen"]
    rounded_timestamp = (raid_first_seen // 60) * 60
    ts = int(rounded_timestamp * 1000)  # Convert to milliseconds

    area = data["area_name"]
    raid_pokemon = data["raid_pokemon"]
    raid_level = data["raid_level"]
    raid_form = data["raid_form"]
    raid_costume = data["raid_costume"]
    raid_is_exclusive = data["raid_is_exclusive"]
    raid_ex_raid_eligible = data["raid_ex_raid_eligible"]

    # Construct the timeseries key from the raid data
    key = f"ts:raid_totals:{area}:{raid_pokemon}:{raid_level}:{raid_form}:{raid_costume}:{raid_is_exclusive}:{raid_ex_raid_eligible}"
    logger.debug(f"🔑 Constructed Raid Timeseries Key: {key}")

    client = redis_manager.redis_client
    updated_fields = {}

    # Get retention from config.
    retention_ms = AppConfig.raid_timeseries_retention_ms
    logger.debug(f"🚨 Set Raid TimeSeries retention timer: {retention_ms}")

    # Ensure the timeseries key exists.
    await ensure_timeseries_key(redis_manager.redis_client, key, "raid", area, f"{raid_pokemon}:{raid_level}:{raid_form}", "", retention_ms, pipe)

    client = redis_manager.redis_client
    updated_fields = {}
    if pipe:
        pipe.execute_command("TS.ADD", key, ts, 1, "DUPLICATE_POLICY", "SUM")
        updated_fields["total"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.execute_command("TS.ADD", key, ts, 1, "DUPLICATE_POLICY", "SUM")
            await pipe.execute()
        updated_fields["total"] = "OK"

    logger.info(f"✅ Added Raid event to Timeseries: {key}")
    return updated_fields
