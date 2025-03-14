from datetime import datetime
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
    date_str = datetime.fromtimestamp(ts).strftime("%Y%m%d")

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
