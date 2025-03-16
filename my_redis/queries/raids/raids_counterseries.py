from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_raid_counter(raid_data, pipe=None):
    """
    Update daily counters for Raids events using a single Redis hash per area per day.
    Supports Redis pipelines for batch processing.
    """
    redis_status = await redis_manager.check_redis_connection("raid_pool")
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot update Raid counters.")
        return None

    ts = raid_data["raid_first_seen"]
    dt = datetime.fromtimestamp(ts)
    # dt.weekday() returns 0 for Monday, 6 for Sunday.
    monday_dt = dt - timedelta(days=dt.weekday(),
                                hours=dt.hour,
                                minutes=dt.minute,
                                seconds=dt.second,
                                microseconds=dt.microsecond)
    date_str = monday_dt.strftime("%Y%m%d")

    area = raid_data["area_name"]
    raid_pokemon = raid_data["raid_pokemon"]
    raid_level = raid_data["raid_level"]
    raid_form = raid_data["raid_form"]
    raid_costume = raid_data["raid_costume"]
    raid_is_exclusive = raid_data["raid_is_exclusive"]
    raid_ex_eligible = raid_data["raid_ex_raid_eligible"]

    # Construct the hash key for the area and date
    hash_key = f"counter:raid_total:{area}:{date_str}"

    # Construct field names for each metric
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

    logger.info(f"✅ Updated Daily Raid counter in hash '{hash_key}' with fields: {updated_fields}")
    return updated_fields
