import os
import asyncio
from datetime import datetime
from typing import Dict, Any, Union, Literal

from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils.filtering_keys import parse_time_input
import config as AppConfig

redis_manager = RedisManager()


def build_hash_key(data_type: str, metric: str, area: str, entity: str, form: Union[str, int]) -> str:
    """
    Build a plain text hash key.
    Example: ts:pokemon:total:Matosinhos:422:0
    """
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
                key = build_hash_key(data_type, metric, area, entity, form)
                pipe.hincrby(key, bucket, inc)
                updated_fields[metric] = "OK"
    else:
        async with client.pipeline() as pipe:
            for metric, inc in metrics.items():
                if inc:
                    key = build_hash_key(data_type, metric, area, entity, form)
                    pipe.hincrby(key, bucket, inc)
                    updated_fields[metric] = "OK"
            await pipe.execute()

    logger.debug(f"✅ Added event for {data_type} {entity} in {area} (form: {form}) at bucket {bucket}")
    return updated_fields


async def get_stats(
    data_type: str,
    area: Union[str, Literal["all", "global"]],
    entity: Union[str, Literal["all", "global"]],
    form: Union[str, int, Literal["all", "global"]] = "all",
    start_ts: Union[int, str, None] = None,
    end_ts: Union[int, str, None] = None,
    reference_time: datetime = None
) -> Dict[str, int]:
    """
    Retrieve aggregated statistics from plain text hash keys.

    Keys are in the format:
      ts:pokemon:{metric}:{area}:{pokemon_id}:{form}

    Supports wildcards for area, entity, or form by building a key pattern.
    The hash fields (time buckets) are summed if they fall within the provided time range.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Redis connection failed")
        return {}

    # Convert time strings to timestamps.
    processed_start = None
    processed_end = None
    if isinstance(start_ts, str):
        try:
            dt = parse_time_input(start_ts, reference_time)
            processed_start = int(dt.timestamp())
        except Exception as e:
            logger.error(f"Invalid start time: {e}")
    elif start_ts is not None:
        processed_start = start_ts

    if isinstance(end_ts, str):
        try:
            dt = parse_time_input(end_ts, reference_time)
            processed_end = int(dt.timestamp())
        except Exception as e:
            logger.error(f"Invalid end time: {e}")
    elif end_ts is not None:
        processed_end = end_ts

    # Build key patterns for wildcards.
    area_pattern = "*" if area in ("all", "global") else area
    entity_pattern = "*" if entity in ("all", "global") else entity
    form_pattern = "*" if form in ("all", "global") else str(form)

    # Pattern: ts:pokemon:*:{area_pattern}:{entity_pattern}:{form_pattern}
    pattern = f"ts:{data_type}:*:{area_pattern}:{entity_pattern}:{form_pattern}"
    keys = await client.keys(pattern)
    logger.debug(f"Scanning keys with pattern: {pattern} -> Found {len(keys)} keys.")

    # Initialize result counts.
    results = {"total": 0}
    metrics_list = ["total", "iv100", "iv0", "shiny", "pvp_little", "pvp_great", "pvp_ultra"]
    for m in metrics_list:
        results[m] = 0

    for key in keys:
        # Expected key format: ts:pokemon:{metric}:{area}:{pokemon_id}:{form}
        parts = key.split(":")
        if len(parts) != 6:
            continue
        _, key_data_type, metric, key_area, key_entity, key_form = parts
        if key_data_type != data_type:
            continue
        if area not in ("all", "global") and key_area != area:
            continue
        if entity not in ("all", "global") and key_entity != entity:
            continue
        if form not in ("all", "global") and str(key_form) != str(form):
            continue

        hash_fields = await client.hgetall(key)
        for field, count in hash_fields.items():
            try:
                ts = int(field)  # time bucket as integer timestamp (seconds)
                if processed_start is not None and ts < processed_start:
                    continue
                if processed_end is not None and ts > processed_end:
                    continue
                results[metric] += int(count)
                results["total"] += int(count)
            except Exception as e:
                logger.warning(f"Error processing field {field} in key {key}: {e}")
                continue

    return results
