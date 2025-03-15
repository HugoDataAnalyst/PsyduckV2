from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_quest_counter(data, pipe=None):
    """
    Update daily counters for Quest events using a single Redis hash per area per day.

    Expected keys in `data`:
      - "first_seen": UTC timestamp (in seconds)
      - "area_name": area name
      - "pokestop": pokestop ID
      - "quest_type": a field indicating the quest type (e.g., "ar" or "normal")
    """
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot update Quest counter.")
        return "ERROR"

    # Convert first_seen timestamp (in seconds) to a date string (YYYYMMDD)
    ts = data["first_seen"]
    dt = datetime.fromtimestamp(ts)
    # dt.weekday() returns 0 for Monday, 6 for Sunday.
    monday_dt = dt - timedelta(days=dt.weekday(),
                                hours=dt.hour,
                                minutes=dt.minute,
                                seconds=dt.second,
                                microseconds=dt.microsecond)
    date_str = monday_dt.strftime("%Y%m%d")

    area = data["area_name"]
    pokestop = data["pokestop_id"]
    # Determine quest type from the two possible keys.
    with_ar = data.get("with_ar", False)

    if with_ar:
        mode = "ar"
        ar_type = data.get("ar_type", "")
        reward_ar_type = data.get("reward_ar_type", "")
        reward_ar_item_id = data.get("reward_ar_item_id", "")
        reward_ar_item_amount = data.get("reward_ar_item_amount", "")
        reward_ar_poke_id = data.get("reward_ar_poke_id", "")
        reward_ar_poke_form = data.get("reward_ar_poke_form", "")
        field_details = f"{ar_type}:{reward_ar_type}:{reward_ar_item_id}:{reward_ar_item_amount}:{reward_ar_poke_id}:{reward_ar_poke_form}"
    else:
        mode = "normal"
        normal_type = data.get("normal_type", "")
        reward_normal_type = data.get("reward_normal_type", "")
        reward_normal_item_id = data.get("reward_normal_item_id", "")
        reward_normal_item_amount = data.get("reward_normal_item_amount", "")
        reward_normal_poke_id = data.get("reward_normal_poke_id", "")
        reward_normal_poke_form = data.get("reward_normal_poke_form", "")
        field_details = f"{normal_type}:{reward_normal_type}:{reward_normal_item_id}:{reward_normal_item_amount}:{reward_normal_poke_id}:{reward_normal_poke_form}"

    hash_key = f"counter:quest:{area}:{date_str}"
    field_name = f"{pokestop}:{mode}:{field_details}:total"

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

    logger.info(f"✅ Updated Quest daily counter in hash '{hash_key}' for field '{field_name}'")
    return updated_fields
