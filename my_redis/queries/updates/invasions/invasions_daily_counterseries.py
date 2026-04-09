from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_invasion_daily_counter(data, pipe=None):
    """
    Update daily counters for Invasion events using a single Redis hash per area per calendar day.
    Supports Redis pipelines for batch processing.

    Key: counter:invasion_daily:{area}:{YYYYMMDD}  (actual calendar date, not week-aligned)
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot update Invasion daily counter.")
        return "ERROR"

    ts = data["invasion_first_seen"]
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")

    area         = data["area_name"]
    display_type = data["invasion_type"]
    character    = data["invasion_character"]
    grunt        = data["invasion_grunt_type"]
    confirmed    = int(bool(data["invasion_confirmed"]))

    hash_key   = f"counter:invasion_daily:{area}:{date_str}"
    field_name = f"{display_type}:{character}:{grunt}:{confirmed}:total"

    updated_fields = {}

    if pipe:
        pipe.hincrby(hash_key, field_name, 1)
        updated_fields[field_name] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(hash_key, field_name, 1)
            await pipe.execute()
        updated_fields[field_name] = "OK"

    logger.debug(f"✅ Updated Invasion daily counter in hash '{hash_key}' for field '{field_name}'")
    return updated_fields
