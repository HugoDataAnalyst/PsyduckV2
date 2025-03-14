import config as AppConfig
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from utils.redis_key_checker import ensure_timeseries_key

redis_manager = RedisManager()

async def add_timeseries_quest_event(data, pipe=None):
    """
    Add a Quest event to Redis TimeSeries for detailed tracking.

    Expected keys in `data`:
      - "first_seen": UTC timestamp (in seconds) for when the quest is seen
      - "area_name": area name
      - "pokestop": pokestop ID
      - "quest_type": a field indicating the quest type (e.g., "ar" or "normal")
    """
    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot add Quest event to timeseries.")
        return "ERROR"

    # Retrieve timestamp (first_seen, rounded to minute)
    first_seen = data["first_seen"]
    ts = int((first_seen // 60) * 60 * 1000)  # Convert to ms, rounding to minute

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


    # Construct the timeseries key from the quest data
    key = f"ts:quests:{area}:{pokestop}:{mode}"
    field_name = f"{field_details}:total"

    logger.debug(f"🔑 Quest TS Key: {key}, Field: {field_name}")

    # Ensure the time series key exists
    retention_ms = AppConfig.quests_timeseries_retention_ms
    logger.debug(f"🚨 Set Quest retention timer: {retention_ms}")
    await ensure_timeseries_key(redis_manager.redis_client, key, "quest", area, f"{pokestop}:{mode}", "", retention_ms, pipe)

    client = redis_manager.redis_client
    updated_fields = {}
    if pipe:
        pipe.execute_command("TS.ADD", key, ts, 1, "DUPLICATE_POLICY", "SUM")
        updated_fields["total"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.execute_command("TS.ADD", key, ts, 1, "DUPLICATE_POLICY", "SUM")
            await pipe.execute()
        updated_fields["total"] = "OK"

    logger.info(f"✅ Added Quest event to timeseries: {key} at timestamp {ts}")
    return updated_fields
