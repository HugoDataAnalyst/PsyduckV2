import os
import asyncio
from datetime import datetime
from typing import Dict, Any, Union, Literal

from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils.filtering_keys import parse_time_input
import config as AppConfig

redis_manager = RedisManager()


def build_hash_key(data_type: str, metric: str, area: str, entity: str, form: Union[str, int], date_hour: str = None) -> str:
    """
    Build a plain text hash key with optional hourly partition.

    New format (hourly-partitioned):
      ts:pokemon:total:Matosinhos:422:0:2025-11-20-18

    Old format (for backward compatibility):
      ts:pokemon:total:Matosinhos:422:0
    """
    if date_hour:
        return f"ts:{data_type}:{metric}:{area}:{entity}:{form}:{date_hour}"
    return f"ts:{data_type}:{metric}:{area}:{entity}:{form}"


def get_time_bucket(first_seen: int) -> str:
    """
    Round the timestamp to the nearest minute (or desired bucket) and return as a string.
    """
    bucket = (first_seen // 60) * 60
    return str(bucket)


async def add_pokemon_timeseries_event(data: Dict[str, Any], pipe=None) -> Dict[str, Any]:
    """
    Add a Pokémon event using plain text hash keys.

    Key format:
      ts:pokemon:{metric}:{area_name}:{pokemon_id}:{form}

    The hash field is the time bucket (rounded timestamp) and its value is the count.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis connection failed")
        return {"status": "ERROR", "message": "Redis not connected"}

    # Validate required fields.
    required_fields = ["pokemon_id", "area_name", "first_seen"]
    if any(field not in data for field in required_fields):
        logger.error(f"Missing required fields in data: {data}")
        return {"status": "ERROR", "message": "Missing required fields"}

    # Set fixed data type.
    data_type = "pokemon"
    # Use area_name and pokemon_id for key construction.
    area = str(data["area_name"])
    entity = str(data["pokemon_id"])
    form = data.get("form", 0)  # Default form to 0 if not provided.
    first_seen = data["first_seen"]
    bucket = get_time_bucket(first_seen)

    # Generate hourly partition key: YYYY-MM-DD-HH
    date_hour = datetime.fromtimestamp(first_seen).strftime('%Y-%m-%d-%H')

    # Determine metric increments.
    inc_total      = 1  # Always increment total.
    inc_iv100      = 1 if data.get("iv") == 100 else 0
    inc_iv0        = 1 if data.get("iv") == 0 else 0
    inc_shiny      = 1 if data.get("shiny") else 0
    inc_pvp_little = 1 if data.get("pvp_little_rank") and 1 in data.get("pvp_little_rank") else 0
    inc_pvp_great  = 1 if data.get("pvp_great_rank") and 1 in data.get("pvp_great_rank") else 0
    inc_pvp_ultra  = 1 if data.get("pvp_ultra_rank") and 1 in data.get("pvp_ultra_rank") else 0

    metrics = {
        "total": inc_total,
        "iv100": inc_iv100,
        "iv0": inc_iv0,
        "shiny": inc_shiny,
        "pvp_little": inc_pvp_little,
        "pvp_great": inc_pvp_great,
        "pvp_ultra": inc_pvp_ultra
    }

    updated_fields = {}
    if pipe:
        for metric, inc in metrics.items():
            if inc:
                key = build_hash_key(data_type, metric, area, entity, form, date_hour)
                pipe.hincrby(key, bucket, inc)
                updated_fields[metric] = "OK"
    else:
        async with client.pipeline() as pipe:
            for metric, inc in metrics.items():
                if inc:
                    key = build_hash_key(data_type, metric, area, entity, form, date_hour)
                    pipe.hincrby(key, bucket, inc)
                    updated_fields[metric] = "OK"
            await pipe.execute()

    logger.debug(f"✅ Added event for {data_type} {entity} in {area} (form: {form}) at {date_hour} bucket {bucket}")
    return updated_fields

