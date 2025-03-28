import os
import asyncio
from datetime import datetime
from typing import Dict, Any, Union, Literal

from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils.filtering_keys import parse_time_input
import config as AppConfig

redis_manager = RedisManager()

def get_time_bucket(first_seen: int) -> str:
    """
    Round the timestamp to the nearest minute (in seconds) and return as a string.
    """
    bucket = (first_seen // 60) * 60
    return str(bucket)

async def add_timeseries_quest_event(data: Dict[str, Any], pipe=None) -> Dict[str, Any]:
    """
    Add a Quest event using plain text hash keys.

    Expected keys in `data`:
      - "first_seen": UTC timestamp (in seconds) for when the quest is seen.
      - "area_name": area name.
      - Additionally, quest type details to determine if it's AR or normal.
        * For AR quests, expect keys like "ar_type", "reward_ar_type", "reward_ar_item_id",
          "reward_ar_item_amount", "reward_ar_poke_id", "reward_ar_poke_form".
        * For normal quests, expect keys like "normal_type", "reward_normal_type",
          "reward_normal_item_id", "reward_normal_item_amount", "reward_normal_poke_id",
          "reward_normal_poke_form".

    The overall quest key is built as:
      ts:quests_total:total:{area_name}:{mode}
    And the detailed key is built as:
      ts:quests_total:total_ar_detailed:{area_name}:{mode}:{ar_field_details}  (for AR quests)
      ts:quests_total:total_normal_detailed:{area_name}:{mode}:{normal_field_details}  (for normal quests)

    The hash field is the time bucket (first_seen rounded to the minute), and its value is incremented by 1.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot add Quest event to timeseries.")
        return "ERROR"

    # Retrieve and round the first_seen timestamp.
    first_seen = data["first_seen"]
    bucket = get_time_bucket(first_seen)

    area = data["area_name"]

    # Determine quest mode.
    with_ar = data.get("ar_type") is not None
    if with_ar:
        mode = "ar"
        ar_type = data.get("ar_type", "")
        reward_ar_type = data.get("reward_ar_type", "")
        reward_ar_item_id = data.get("reward_ar_item_id", "")
        reward_ar_item_amount = data.get("reward_ar_item_amount", "")
        reward_ar_poke_id = data.get("reward_ar_poke_id", "")
        reward_ar_poke_form = data.get("reward_ar_poke_form", "")
        # Concatenate field details.
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

    # Define keys.
    key_overall = f"ts:quests_total:total:{area}:{mode}"
    if with_ar:
        key_detailed = f"ts:quests_total:total_ar_detailed:{area}:{mode}:{field_details}"
    else:
        key_detailed = f"ts:quests_total:total_normal_detailed:{area}:{mode}:{field_details}"

    # Increment values (always 1).
    inc = 1
    updated_fields = {}
    if pipe:
        pipe.hincrby(key_overall, bucket, inc)
        updated_fields["total"] = "OK"
        pipe.hincrby(key_detailed, bucket, inc)
        updated_fields["detailed"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(key_overall, bucket, inc)
            updated_fields["total"] = "OK"
            pipe.hincrby(key_detailed, bucket, inc)
            updated_fields["detailed"] = "OK"
            await pipe.execute()

    logger.debug(f"✅ Added Quest event to hash: overall key {key_overall} at bucket {bucket}")
    if with_ar:
        logger.debug(f"✅ Added Quest event to AR detailed key with reward {reward_ar_type} at bucket {bucket}")
    else:
        logger.debug(f"✅ Added Quest event to Normal detailed key with reward {reward_normal_type} at bucket {bucket}")

    return updated_fields
