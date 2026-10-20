from __future__ import annotations
from redis.asyncio.client import Redis
from utils.logger import logger
from utils.safe_values import _safe_int, _norm_str, _norm_name, _valid_coords, _to_float
from sql.tasks.invasions_processor import InvasionSQLProcessor
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
            logger.debug(f"⏳ Invasions buffer retry attempt {attempt}/{max_attempts}")

    return None


class InvasionsRedisBuffer:
    """
    Buffers invasion events as pipe-delimited lines in a Redis LIST, then flushes in batches.

    Composite line (10 fields):
      pokestop|pokestop_name|latitude|longitude|display_type|character|grunt|confirmed|area_id|first_seen_epoch
    """
    redis_key = "buffer:invasion_events"
    aggregation_threshold = int(AppConfig.invasion_max_threshold)

    @classmethod
    def entry(cls, event_data: dict) -> str | None:
        """Validate one event and build its buffer line; None (already logged) to skip it."""
        pokestop      = _norm_str(event_data.get("invasion_pokestop_id"))
        pokestop_name = _norm_name(event_data.get("invasion_pokestop_name"))
        lat           = _to_float(event_data.get("invasion_latitude"))
        lon           = _to_float(event_data.get("invasion_longitude"))

        display_type  = _safe_int(event_data.get("invasion_type"), 0)
        character     = _safe_int(event_data.get("invasion_character"), 0)
        grunt         = _safe_int(event_data.get("invasion_grunt_type"), 0)
        confirmed     = _safe_int(event_data.get("invasion_confirmed"), 0)
        area_id       = _safe_int(event_data.get("area_id"), None)
        first_seen    = _safe_int(event_data.get("invasion_first_seen"), None)

        if not pokestop or area_id is None or first_seen is None:
            missing = [name for name, bad in (("invasion_pokestop_id", not pokestop), ("area_id", area_id is None),
                                              ("invasion_first_seen", first_seen is None)) if bad]
            logger.debug(f"❌ invasions: missing required fields {missing} (pokestop={pokestop!r})")
            return None

        # Skip if coords are missing/zero/invalid
        if not _valid_coords(lat, lon):
            logger.debug(f"↩️  invasions: dropped event for `{pokestop}` due to invalid coords lat={lat} lon={lon}")
            return None

        line = (
            f"{pokestop}|{pokestop_name}|{lat}|{lon}|"
            f"{display_type}|{character}|{grunt}|{confirmed}|{area_id}|{first_seen}"
        )
        return line

    @classmethod
    def stage(cls, pipe, event_data: dict) -> bool:
        """
        Queue this event's buffer append on the caller's pipeline - no round
        trip of its own. Flushing is the leader's flusher's job, every
        flush_interval seconds; webhook requests never flush inline.
        """
        try:
            line = cls.entry(event_data)
        except Exception as e:
            logger.error(f"❌ Invasions buffer: could not build entry: {e}")
            return False
        if line is None:
            return False
        pipe.rpush(cls.redis_key, line)
        return True

    @classmethod
    async def _consume_list_to_batch(cls, redis_client: Redis, temp_key: str) -> list[dict]:
        rows = await redis_client.lrange(temp_key, 0, -1)
        if not rows:
            return []

        batch: list[dict] = []
        malformed = 0
        for b in rows:
            try:
                line = b.decode() if isinstance(b, (bytes, bytearray)) else b
                # pokestop|pokestop_name|latitude|longitude|display_type|character|grunt|confirmed|area_id|first_seen
                (
                    pokestop, pokestop_name, lat_s, lon_s,
                    dt_s, ch_s, gr_s, conf_s, area_s, fs_s
                ) = line.split("|", 9)

                batch.append({
                    "pokestop": pokestop,
                    "pokestop_name": pokestop_name,
                    "latitude": float(lat_s),
                    "longitude": float(lon_s),
                    "display_type": int(dt_s),
                    "character": int(ch_s),
                    "grunt": int(gr_s),
                    "confirmed": int(conf_s),
                    "area_id": int(area_s),
                    "first_seen": int(fs_s),
                })
            except Exception:
                malformed += 1

        if malformed:
            logger.warning(f"⚠️ invasions buffer: skipped {malformed} malformed line(s)")
        return batch

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
            InvasionSQLProcessor.bulk_insert_invasions_daily_events, label,
        )
        if inserted:
            logger.success(f"📬 {label} inserted +{inserted} rows.")
        return inserted

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        return await cls._flush(redis_client, "flushing", "Invasions daily-events")

    @classmethod
    async def force_flush(cls, redis_client: Redis) -> int:
        return await cls._flush(redis_client, "force_flushing", "FORCE invasions daily-events")
