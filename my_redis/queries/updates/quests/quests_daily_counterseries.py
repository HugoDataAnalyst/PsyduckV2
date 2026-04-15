from datetime import datetime
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def update_quest_daily_counter(data, pipe=None):
    """
    Update daily counters for Quest events using a single Redis hash per area per calendar day.
    Supports Redis pipelines for batch processing.

    Key: counter:quest_daily:{area}:{YYYYMMDD}  (actual calendar date, not week-aligned)
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot update Quest daily counter.")
        return "ERROR"

    ts = data["first_seen"]
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")

    area    = data["area_name"]
    with_ar = data.get("ar_type") is not None

    if with_ar:
        mode = "ar"
        ar_type               = data.get("ar_type", "")
        reward_ar_type        = data.get("reward_ar_type", "")
        reward_ar_item_id     = data.get("reward_ar_item_id", "")
        reward_ar_item_amount = data.get("reward_ar_item_amount", "")
        reward_ar_poke_id     = data.get("reward_ar_poke_id", "")
        reward_ar_poke_form   = data.get("reward_ar_poke_form", "")
        field_details = f"{ar_type}:{reward_ar_type}:{reward_ar_item_id}:{reward_ar_item_amount}:{reward_ar_poke_id}:{reward_ar_poke_form}"
    else:
        mode = "normal"
        normal_type               = data.get("normal_type", "")
        reward_normal_type        = data.get("reward_normal_type", "")
        reward_normal_item_id     = data.get("reward_normal_item_id", "")
        reward_normal_item_amount = data.get("reward_normal_item_amount", "")
        reward_normal_poke_id     = data.get("reward_normal_poke_id", "")
        reward_normal_poke_form   = data.get("reward_normal_poke_form", "")
        field_details = f"{normal_type}:{reward_normal_type}:{reward_normal_item_id}:{reward_normal_item_amount}:{reward_normal_poke_id}:{reward_normal_poke_form}"

    hash_key   = f"counter:quest_daily:{area}:{date_str}"
    field_name = f"{mode}:{field_details}:total"

    updated_fields = {}

    if pipe:
        pipe.hincrby(hash_key, field_name, 1)
        updated_fields[field_name] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(hash_key, field_name, 1)
            await pipe.execute()
        updated_fields[field_name] = "OK"

    logger.debug(f"✅ Updated Quest daily counter in hash '{hash_key}' for field '{field_name}'")
    return updated_fields
