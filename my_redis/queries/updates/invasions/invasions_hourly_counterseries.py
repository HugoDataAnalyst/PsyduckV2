from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_invasion_hourly_counter(data, pipe=None):
    """
    Update hourly counters for Invasion events using a single Redis hash per area per hour.

    Expected keys in `data`:
      - "invasion_first_seen": UTC timestamp (in seconds)
      - "area_name": area name
      - "display_type", "character", "grunt", "confirmed": invasion attributes
    """
    client = await redis_manager.check_redis_connection("invasion_pool")
    if not client:
        logger.error("❌ Redis is not connected. Cannot update Invasion hourly counter.")
        return "ERROR"

    ts = data["invasion_first_seen"]
    date_hour = datetime.fromtimestamp(ts).strftime("%Y%m%d%H")

    area = data["area_name"]
    display_type = data["invasion_type"]
    character = data["invasion_character"]
    grunt = data["invasion_grunt_type"]
    confirmed = int(bool(data["invasion_confirmed"]))

    # Construct the hash key for the area and hour
    hash_key = f"counter:invasion_hourly:{area}:{date_hour}"

    # Construct field name for each metric
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

    logger.debug(f"✅ Updated Invasion hourly counter in hash '{hash_key}' for field '{field_name}'")
    return updated_fields
