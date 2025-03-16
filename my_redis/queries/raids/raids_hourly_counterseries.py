from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_raid_hourly_counter(raid_data, pipe=None):
    """
    Update hourly counters for Raid events using a single Redis hash per area per hour.

    The hash key is:
      "counter:raid_hourly:{area}:{date_hour}"
    where date_hour is formatted as YYYYMMDDHH.

    The field name is constructed as:
      "{raid_pokemon}:{raid_level}:{raid_form}:{raid_costume}:{raid_is_exclusive}:{raid_ex_raid_eligible}:total"
    """
    redis_status = await redis_manager.check_redis_connection("raid_pool")
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot update Raid hourly counter.")
        return None

    ts = raid_data["raid_first_seen"]
    date_hour = datetime.fromtimestamp(ts).strftime("%Y%m%d%H")

    area = raid_data.get("area_name")

    raid_pokemon = raid_data["raid_pokemon"]
    raid_level = raid_data["raid_level"]
    raid_form = raid_data["raid_form"]
    raid_costume = raid_data["raid_costume"]
    raid_is_exclusive = raid_data["raid_is_exclusive"]
    raid_ex_eligible = raid_data["raid_ex_raid_eligible"]

    hash_key = f"counter:raid_hourly:{area}:{date_hour}"
    field_name = f"{raid_pokemon}:{raid_level}:{raid_form}:{raid_costume}:{raid_is_exclusive}:{raid_ex_eligible}:total"

    client = redis_manager.redis_client
    updated_fields = {}
    if pipe:
        pipe.hincrby(hash_key, field_name, 1)
        updated_fields[field_name] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(hash_key, field_name, 1)
            await pipe.execute()
        updated_fields[field_name] = "OK"

    logger.info(f"✅ Updated Raid hourly counter in hash '{hash_key}' for field '{field_name}'")
    return updated_fields
