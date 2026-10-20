from __future__ import annotations
from redis.asyncio.client import Redis
from datetime import datetime
from utils.logger import logger
from utils.safe_values import _safe_int, _valid_coords, _to_float, _norm_name
from sql.tasks.raids_processor import RaidSQLProcessor
from my_redis.queries.buffer import flush_guard
import config as AppConfig
import asyncio


async def _get_client_with_retry(redis_client: Redis | None, max_attempts: int = 3, delay: float = 0.3) -> Redis | None:
    """
    Try to get a working Redis client with retry logic.
    First tries the provided client, then falls back to getting a fresh connection.
    """
    from my_redis.connect_redis import RedisManager
    redis_manager = RedisManager()

    for attempt in range(1, max_attempts + 1):
        # Try provided client first
        if redis_client:
            try:
                if await redis_client.ping():
                    return redis_client
            except Exception:
                pass

        # Fall back to getting a fresh connection
        try:
            client = await redis_manager.get_connection_with_retry(max_attempts=2, delay=0.2)
            if client:
                return client
        except Exception:
            pass

        if attempt < max_attempts:
            await asyncio.sleep(delay)
            logger.debug(f"⏳ Raids buffer retry attempt {attempt}/{max_attempts}")

    return None


class RaidsRedisBuffer:
    """
    Buffers raw raid events as newline-free composite strings (list), then flushes in batches.

    Composite key (pipe-delimited):
      gym|raid_pokemon|raid_form|raid_level|raid_team|raid_costume|raid_is_exclusive|raid_ex_raid_eligible|area_id|first_seen
    """
    redis_key = "buffer:raid_events"
    aggregation_threshold = int(AppConfig.raid_max_threshold)

    @classmethod
    async def increment_event(cls, redis_client: Redis, event_data: dict):
        try:
            gym               = event_data.get("raid_gym_id")
            gym_name          = _norm_name(event_data.get("raid_gym_name"))
            lat               = _to_float(event_data.get("raid_latitude"))
            lon               = _to_float(event_data.get("raid_longitude"))

            raid_pokemon      = _safe_int(event_data.get("raid_pokemon"), 0)
            raid_form         = str(event_data.get("raid_form", "0"))
            raid_level        = _safe_int(event_data.get("raid_level"), 0)
            raid_team         = _safe_int(event_data.get("raid_team_id"), 0)
            raid_costume      = str(event_data.get("raid_costume", "0"))
            raid_is_exclusive = _safe_int(event_data.get("raid_is_exclusive"), 0)
            raid_ex_eligible  = _safe_int(event_data.get("raid_ex_raid_eligible"), 0)
            area_id           = _safe_int(event_data.get("area_id"), None)
            first_seen        = _safe_int(event_data.get("raid_first_seen"), None)

            if not gym or area_id is None or first_seen is None:
                logger.warning("❌ Raid event missing required fields (gym/area_id/first_seen). Skipping.")
                return

            # Skip if coords are missing/zero/invalid
            if not _valid_coords(lat, lon):
                logger.debug(f"↩️  Raid: dropped event for `{gym}` due to invalid coords lat={lat} lon={lon}")
                return
            # gym|gym_name|lat|lon|...|area_id|first_seen
            unique_key = (
                f"{gym}|{gym_name}|{lat}|{lon}|{raid_pokemon}|{raid_form}|{raid_level}|{raid_team}|"
                f"{raid_costume}|{raid_is_exclusive}|{raid_ex_eligible}|{area_id}|{first_seen}"
            )

            # Get a working client with retry
            client = await _get_client_with_retry(redis_client)
            if not client:
                logger.error("❌ Raids buffer: No Redis connection available after retries. Event lost.")
                return

            # Append with retry
            new_len = None
            for attempt in range(3):
                try:
                    new_len = await client.rpush(cls.redis_key, unique_key)
                    break
                except Exception as e:
                    if attempt < 2:
                        client = await _get_client_with_retry(None)
                        if not client:
                            logger.error(f"❌ Raids buffer: rpush failed, no client available: {e}")
                            return
                    else:
                        logger.error(f"❌ Raids buffer: rpush failed after retries: {e}")
                        return

            logger.debug(f"➕ Appended raid event. List length now {new_len}.")

            queued = await client.llen(cls.redis_key)
            logger.debug(f"📊 🏰 Current queued raid events: {queued}")

            if queued >= cls.aggregation_threshold:
                logger.warning(f"📊 🏰 Raid buffer threshold reached: {queued}. Flushing…")
                await cls.flush_if_ready(client)

        except Exception as e:
            logger.error(f"❌ Error incrementing raid event: {e}")

    @classmethod
    async def _consume_list_to_batch(cls, redis_client: Redis, temp_key: str) -> list[dict]:
        lines = await redis_client.lrange(temp_key, 0, -1)
        if not lines:
            return []

        data_batch: list[dict] = []
        malformed = 0

        for b in lines:
            try:
                line = b.decode() if isinstance(b, (bytes, bytearray)) else b
                # gym|gym_name|lat|lon|raid_pokemon|raid_form|raid_level|raid_team|raid_costume|raid_is_exclusive|raid_ex_raid_eligible|area_id|first_seen
                (
                    gym, gym_name, lat_s, lon_s, raid_pokemon_s, raid_form_s, raid_level_s, raid_team_s,
                    raid_costume_s, raid_is_exclusive_s, raid_ex_eligible_s, area_id_s, first_seen_s
                ) = line.split("|", 12)

                data_batch.append({
                    "gym": gym,
                    "gym_name": gym_name,
                    "latitude": float(lat_s),
                    "longitude": float(lon_s),
                    "raid_pokemon": int(raid_pokemon_s),
                    "raid_form": raid_form_s,
                    "raid_level": int(raid_level_s),
                    "raid_team": int(raid_team_s),
                    "raid_costume": raid_costume_s,
                    "raid_is_exclusive": int(raid_is_exclusive_s),
                    "raid_ex_raid_eligible": int(raid_ex_eligible_s),
                    "area_id": int(area_id_s),
                    "first_seen": int(first_seen_s),
                })
            except Exception:
                malformed += 1

        if malformed:
            logger.warning(f"⚠️ Raids buffer: skipped {malformed} malformed line(s)")
        return data_batch

    @classmethod
    async def _flush(cls, redis_client: Redis, tag: str, label: str) -> int:
        """
        Move the buffer aside, write it to MySQL, and delete it only on success.
        A failed write puts the batch back into the live buffer for the next flush.
        """
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error(f"❌ {label}: No Redis connection available after retries.")
            return 0
        inserted = await flush_guard.flush_list_buffer(
            client, cls.redis_key, tag, cls._consume_list_to_batch,
            RaidSQLProcessor.bulk_insert_raid_daily_events, label,
        )
        if inserted:
            logger.success(f"📬 {label} inserted +{inserted} rows into MySQL.")
        return inserted

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        """Flush buffered raid events (if any). Returns rows inserted into MySQL."""
        return await cls._flush(redis_client, "flushing", "Raids daily-events")

    @classmethod
    async def force_flush(cls, redis_client: Redis) -> int:
        """Force flush regardless of threshold."""
        return await cls._flush(redis_client, "force_flushing", "FORCE raids daily-events")
