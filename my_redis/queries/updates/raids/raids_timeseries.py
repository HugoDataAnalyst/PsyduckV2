from ast import Await
import config as AppConfig
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils.redis_key_checker import ensure_timeseries_key

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
    client = await redis_manager.check_redis_connection()
    if not client:
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
    key_total = f"ts:raids_total:total:{area}:{raid_pokemon}:{raid_level}:{raid_form}"
    key_costume = f"ts:raids_total:costume:{area}:{raid_pokemon}:{raid_level}:{raid_form}"
    key_exclusive = f"ts:raids_total:exclusive:{area}:{raid_pokemon}:{raid_level}:{raid_form}"
    key_ex_raid_eligible = f"ts:raids_total:ex_raid_eligible:{area}:{raid_pokemon}:{raid_level}:{raid_form}"

    logger.debug(f"🔑 Constructed Raid Timeseries Key: {key_total}")

    # Get retention from config.
    retention_ms = AppConfig.raid_timeseries_retention_ms
    logger.debug(f"🚨 Set Raid TimeSeries retention timer: {retention_ms}")

    # Ensure the timeseries key exists.
    await ensure_timeseries_key(client, key_total, "raid_total", area, raid_pokemon, raid_form, retention_ms)
    await ensure_timeseries_key(client, key_costume, "raid_costume", area, raid_pokemon, raid_form, retention_ms)
    await ensure_timeseries_key(client, key_exclusive, "raid_exclusive", area, raid_pokemon, raid_form, retention_ms)
    await ensure_timeseries_key(client, key_ex_raid_eligible, "raid_ex_raid_eligible", area, raid_pokemon, raid_form, retention_ms)


    # Determine metric increments
    inc_total      = 1  # Always add 1 for total
    inc_costume = int(bool(raid_costume) and str(raid_costume) != "0")
    inc_exclusive  = 1 if raid_is_exclusive == 1 else 0
    inc_ex_raid_eligible = 1 if raid_ex_raid_eligible == 1 else 0

    updated_fields = {}
    if pipe:
        pipe.execute_command("TS.ADD", key_total, ts, inc_total, "DUPLICATE_POLICY", "SUM")
        updated_fields["total"] = "OK"
        if inc_costume:
            pipe.execute_command("TS.ADD", key_costume, ts, inc_costume, "DUPLICATE_POLICY", "SUM")
            updated_fields["costume"] = "OK"
        if inc_exclusive:
            pipe.execute_command("TS.ADD", key_exclusive, ts, inc_exclusive, "DUPLICATE_POLICY", "SUM")
            updated_fields["exclusive"] = "OK"
        if inc_ex_raid_eligible:
            pipe.execute_command("TS.ADD", key_ex_raid_eligible, ts, inc_ex_raid_eligible, "DUPLICATE_POLICY", "SUM")
            updated_fields["ex_raid_eligible"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.execute_command("TS.ADD", key_total, ts, inc_total, "DUPLICATE_POLICY", "SUM")
            updated_fields["total"] = "OK"
            if inc_costume:
                pipe.execute_command("TS.ADD", key_costume, ts, inc_costume, "DUPLICATE_POLICY", "SUM")
                updated_fields["costume"] = "OK"
            if inc_exclusive:
                pipe.execute_command("TS.ADD", key_exclusive, ts, inc_exclusive, "DUPLICATE_POLICY", "SUM")
                updated_fields["exclusive"] = "OK"
            if inc_ex_raid_eligible:
                pipe.execute_command("TS.ADD", key_ex_raid_eligible, ts, inc_ex_raid_eligible, "DUPLICATE_POLICY", "SUM")
                updated_fields["ex_raid_eligible"] = "OK"
            await pipe.execute()


    logger.debug(f"✅ Added Raid event to Timeseries for Pokémon ID: {raid_pokemon} in area {area}")
    return updated_fields
