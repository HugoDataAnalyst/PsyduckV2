from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_raid_daily_counter(raid_data, pipe=None):
    """
    Update daily counters for Raid events using a single Redis hash per area per calendar day.
    Supports Redis pipelines for batch processing.

    Key: counter:raid_daily:{area}:{YYYYMMDD}  (actual calendar date, not week-aligned)
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot update Raid daily counters.")
        return None

    ts = raid_data["raid_start"]
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")

    area = raid_data["area_name"]
    raid_pokemon      = raid_data["raid_pokemon"]
    raid_level        = raid_data["raid_level"]
    raid_form         = raid_data["raid_form"]
    raid_costume      = raid_data["raid_costume"]
    raid_is_exclusive = raid_data["raid_is_exclusive"]
    raid_ex_eligible  = raid_data["raid_ex_raid_eligible"]

    hash_key   = f"counter:raid_daily:{area}:{date_str}"
    field_name = f"{raid_pokemon}:{raid_level}:{raid_form}:{raid_costume}:{raid_is_exclusive}:{raid_ex_eligible}:total"

    updated_fields = {}

    if pipe:
        pipe.hincrby(hash_key, field_name, 1)
        updated_fields[field_name] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(hash_key, field_name, 1)
            await pipe.execute()
        updated_fields[field_name] = "OK"

    logger.debug(f"✅ Updated daily Raid counter in hash '{hash_key}' with fields: {updated_fields}")
    return updated_fields
