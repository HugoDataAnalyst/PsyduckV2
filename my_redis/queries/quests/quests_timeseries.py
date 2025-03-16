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
    # Log the raw input data for debugging
    logger.debug(f"🆕 Raw quest data: {data}")
    # Define defaults so both variables are always available
    ar_type = ""
    normal_type = ""
    # Determine quest type from the two possible keys.
    with_ar = data.get("ar_type") is not None

    # Build field details based on the mode.
    if with_ar:
        mode = "ar"
        ar_type = data.get("ar_type", "")
        reward_ar_type = data.get("reward_ar_type", "")
        reward_ar_item_id = data.get("reward_ar_item_id", "")
        reward_ar_item_amount = data.get("reward_ar_item_amount", "")
        reward_ar_poke_id = data.get("reward_ar_poke_id", "")
        reward_ar_poke_form = data.get("reward_ar_poke_form", "")
        ar_field_details = f"{ar_type}:{reward_ar_type}:{reward_ar_item_id}:{reward_ar_item_amount}:{reward_ar_poke_id}:{reward_ar_poke_form}"
    else:
        mode = "normal"
        normal_type = data.get("normal_type", "")
        reward_normal_type = data.get("reward_normal_type", "")
        reward_normal_item_id = data.get("reward_normal_item_id", "")
        reward_normal_item_amount = data.get("reward_normal_item_amount", "")
        reward_normal_poke_id = data.get("reward_normal_poke_id", "")
        reward_normal_poke_form = data.get("reward_normal_poke_form", "")
        normal_field_details = f"{normal_type}:{reward_normal_type}:{reward_normal_item_id}:{reward_normal_item_amount}:{reward_normal_poke_id}:{reward_normal_poke_form}"

    # Define keys for each series.
    key_overall = f"ts:quests_total:total:{area}:{mode}"
    if with_ar:
        key_ar_detailed = f"ts:quests_total:total_ar_detailed:{area}:{mode}:{ar_field_details}"
    else:
        key_normal_detailed = f"ts:quests_total:total_normal_detailed:{area}:{mode}:{normal_field_details}"

    retention_ms = AppConfig.quests_timeseries_retention_ms
    logger.debug(f"🚨 Set Quest retention timer: {retention_ms}")

    client = redis_manager.redis_client
    updated_fields = {}

    # Ensure overall series key exists.
    await ensure_timeseries_key(client, key_overall, "quest_total", area, mode, "", retention_ms, pipe)
    # Ensure mode-specific series key exists.
    if with_ar:
        await ensure_timeseries_key(client, key_ar_detailed, "quest_ar_detailed", area, mode, reward_ar_type, retention_ms, pipe)
    else:
        await ensure_timeseries_key(client, key_normal_detailed, "quest_normal_detailed", area, mode, reward_normal_type, retention_ms, pipe)


    # Determine metric increments
    inc_total      = 1  # Always add 1 for total

    # Add the data point (value=1) to the overall series.
    if pipe:
        pipe.execute_command("TS.ADD", key_overall, ts, inc_total, "DUPLICATE_POLICY", "SUM")
        updated_fields["total"] = "OK"
        if with_ar:
            pipe.execute_command("TS.ADD", key_ar_detailed, ts, 1, "DUPLICATE_POLICY", "SUM")
            updated_fields["ar_detailed"] = "OK"
        else:
            pipe.execute_command("TS.ADD", key_normal_detailed, ts, 1, "DUPLICATE_POLICY", "SUM")
            updated_fields["normal_detailed"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.execute_command("TS.ADD", key_overall, ts, inc_total, "DUPLICATE_POLICY", "SUM")
            updated_fields["total"] = "OK"
            if with_ar:
                pipe.execute_command("TS.ADD", key_ar_detailed, ts, 1, "DUPLICATE_POLICY", "SUM")
                updated_fields["ar_detailed"] = "OK"
            else:
                pipe.execute_command("TS.ADD", key_normal_detailed, ts, 1, "DUPLICATE_POLICY", "SUM")
                updated_fields["normal_detailed"] = "OK"
            await pipe.execute()
        updated_fields["total"] = "OK"


    logger.info(f"✅ Added Quest event to timeseries: overall key {key_overall} at timestamp {ts}")
    if with_ar:
        logger.info(f"✅ Added Quest event to AR key with reward: {reward_ar_type} at timestamp {ts}")
    else:
        logger.info(f"✅ Added Quest event to Normal key with reward: {reward_normal_type} at timestamp {ts}")

    return updated_fields
