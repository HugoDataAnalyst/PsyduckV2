from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_invasion_counter(data, pipe=None):
    """
    Update daily counters for Invasion events using a single Redis hash per area per day.

    Expected keys in `data`:
      - "invasion_first_seen": UTC timestamp (in seconds)
      - "area_name": area name
      - "pokestop": pokestop ID
      - "display_type", "character", "grunt", "confirmed": invasion attributes
    """
    client = await redis_manager.check_redis_connection("invasion_pool")
    if not client:
        logger.error("❌ Redis is not connected. Cannot update Invasion counter.")
        return "ERROR"

    ts = data["invasion_first_seen"]
    dt = datetime.fromtimestamp(ts)
    # dt.weekday() returns 0 for Monday, 6 for Sunday.
    monday_dt = dt - timedelta(days=dt.weekday(),
                                hours=dt.hour,
                                minutes=dt.minute,
                                seconds=dt.second,
                                microseconds=dt.microsecond)
    date_str = monday_dt.strftime("%Y%m%d")

    area = data["area_name"]
    display_type = data["invasion_type"]
    character = data["invasion_character"]
    grunt = data["invasion_grunt_type"]
    confirmed = int(bool(data["invasion_confirmed"]))

    # Construct the hash key for the area and date
    hash_key = f"counter:invasion:{area}:{date_str}"

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

    logger.debug(f"✅ Updated Invasion daily counter in hash '{hash_key}' for field '{field_name}'")
    return updated_fields
