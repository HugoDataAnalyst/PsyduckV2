import config as AppConfig
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from utils.redis_key_checker import ensure_timeseries_key

redis_manager = RedisManager()

async def add_timeseries_invasion_event(data, pipe=None):
    """
    Add a Invasion event to a Redis TimeSeries for detailed tracking.

    Expected keys in `data`:
      - "invasion_first_seen": UTC timestamp (in seconds) for the invasion start
      - "area_name": area name (string)
      - "invasion_type": invasion display type (int)
      - "invasion_character": invasion character (int)
      - "invasion_grunt_type": invasion grunt (int)
      - "invasion_confirmed": invasion confirmed flag (int or boolean)
    """
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot add Pokémon TTH event to time series.")
        return "ERROR"

    firt_seen = data["invasion_first_seen"]
    ts = int(firt_seen // 60) * 60 * 1000  # Convert to milliseconds, rounding to minute

    area = data["area_name"]

    display_type = data["invasion_type"]
    character = data["invasion_character"]
    grunt = data["invasion_grunt_type"]
    confirmed = int(bool(data["invasion_confirmed"]))

    # Construct a timeseries key combining these values.
    key = f"ts:invasion:{area}:{display_type}:{character}:{grunt}:{confirmed}"

    logger.debug(f"🔑 Constructed Invasion TimeSeries Key: {key}")

    client = redis_manager.redis_client
    updated_fields = {}

    # Ensure the time series key exists
    retention_ms = AppConfig.invasion_timeseries_retention_ms
    logger.debug(f"🚨 Set Invasion retention timer: {retention_ms}")
    await ensure_timeseries_key(client, key, "invasion", area, "", "", retention_ms, pipe)

    if pipe:
        pipe.execute_command("TS.ADD", key, ts, 1, "DUPLICATE_POLICY", "SUM")  # Add to pipeline
        updated_fields["total"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.execute_command("TS.ADD", key, ts, 1, "DUPLICATE_POLICY", "SUM")
            await pipe.execute()  # Execute pipeline transaction

        updated_fields["total"] = "OK"

    logger.info(f"✅ Added Invasion event to TimeSeries: {key}")
    return updated_fields
