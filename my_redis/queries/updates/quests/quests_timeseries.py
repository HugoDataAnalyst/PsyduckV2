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
    Add a Quest event using plain text hash keys with the new key format.

    Expected keys in `data`:
      - "first_seen": UTC timestamp (in seconds) for when the quest is seen.
      - "area_name": area name.
      - For AR quests, expected keys include:
          "ar_type", "reward_ar_type", "reward_ar_item_id",
          "reward_ar_item_amount", "reward_ar_poke_id", "reward_ar_poke_form"
      - For normal quests, expected keys include:
          "normal_type", "reward_normal_type", "reward_normal_item_id",
          "reward_normal_item_amount", "reward_normal_poke_id", "reward_normal_poke_form"

    The Redis key is built as:
      ts:quests_total:{mode}:{area_name}:{field_details}
    where {mode} is either "ar" or "normal" and {field_details} is a colon‑separated string
    of the respective fields.

    The hash field is the time bucket (first_seen rounded to the minute), and its value is incremented by 1.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis is not connected. Cannot add Quest event to timeseries.")
        return {"status": "ERROR", "message": "Redis not connected"}

    # Retrieve and round the first_seen timestamp.
    first_seen = data["first_seen"]
    bucket = get_time_bucket(first_seen)
    area = data["area_name"]

    # Determine quest mode and field details.
    if data.get("ar_type") is not None:
        mode = "ar"
        ar_type = data.get("ar_type", "")
        reward_ar_type = data.get("reward_ar_type", "")
        reward_ar_item_id = data.get("reward_ar_item_id", "")
        reward_ar_item_amount = data.get("reward_ar_item_amount", "")
        reward_ar_poke_id = data.get("reward_ar_poke_id", "")
        reward_ar_poke_form = data.get("reward_ar_poke_form", "")
        # Concatenate field details for AR quests.
        field_details = f"{ar_type}:{reward_ar_type}:{reward_ar_item_id}:{reward_ar_item_amount}:{reward_ar_poke_id}:{reward_ar_poke_form}"
    else:
        mode = "normal"
        normal_type = data.get("normal_type", "")
        reward_normal_type = data.get("reward_normal_type", "")
        reward_normal_item_id = data.get("reward_normal_item_id", "")
        reward_normal_item_amount = data.get("reward_normal_item_amount", "")
        reward_normal_poke_id = data.get("reward_normal_poke_id", "")
        reward_normal_poke_form = data.get("reward_normal_poke_form", "")
        # Concatenate field details for normal quests.
        field_details = f"{normal_type}:{reward_normal_type}:{reward_normal_item_id}:{reward_normal_item_amount}:{reward_normal_poke_id}:{reward_normal_poke_form}"

    # Build the new key using the new format.
    key = f"ts:quests_total:{mode}:{area}:{field_details}"
    logger.debug(f"Built Quest key: {key}")

    # Increment the value in the hash for the given time bucket.
    inc = 1
    updated_fields = {}
    if pipe:
        pipe.hincrby(key, bucket, inc)
        updated_fields["status"] = "OK"
    else:
        async with client.pipeline() as pipe:
            pipe.hincrby(key, bucket, inc)
            updated_fields["status"] = "OK"
            await pipe.execute()

    logger.debug(f"✅ Added Quest event to key {key} at bucket {bucket}")
    return updated_fields
