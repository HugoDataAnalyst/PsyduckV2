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
    grunt = data["invasion_grunt_type"]
    confirmed = int(bool(data["invasion_confirmed"]))

    # Construct a timeseries key combining these values.
    key_total = f"ts:invasion:total:{area}:{display_type}:{grunt}"
    key_confirmed = f"ts:invasion:confirmed:{area}:{display_type}:{grunt}"

    logger.debug(f"🔑 Constructed Invasion TimeSeries Key: {key_total}")


    client = redis_manager.redis_client
    updated_fields = {}

    # Ensure the time series key exists
    retention_ms = AppConfig.invasion_timeseries_retention_ms
    logger.debug(f"🚨 Set Invasion retention timer: {retention_ms}")
    await ensure_timeseries_key(client, key_total, "invasion_total", area, display_type, grunt, retention_ms, pipe)
    await ensure_timeseries_key(client, key_confirmed, "invasion_confirmed", area, display_type, grunt, retention_ms, pipe)

    # Determine metric increments
    inc_total      = 1  # Always add 1 for total
    inc_confirmed  = 1 if confirmed and 1 in confirmed else 0

    if pipe:
        pipe.execute_command("TS.ADD", key_total, ts, inc_total, "DUPLICATE_POLICY", "SUM")  # Add to pipeline
        updated_fields["total"] = "OK"
        if inc_confirmed:
            pipe.execute_command("TS.ADD", key_confirmed, ts, inc_confirmed, "DUPLICATE_POLICY", "SUM")
            updated_fields["confirmed"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.execute_command("TS.ADD", key_total, ts, inc_total, "DUPLICATE_POLICY", "SUM")
            updated_fields["total"] = "OK"
            if inc_confirmed:
                pipe.execute_command("TS.ADD", key_confirmed, ts, inc_confirmed, "DUPLICATE_POLICY", "SUM")
                updated_fields["confirmed"] = "OK"
            await pipe.execute()  # Execute pipeline transaction


    logger.info(f"✅ Added Invasion event to TimeSeries for display {display_type} with grunt: {grunt} in the Area: {area}")
    return updated_fields
