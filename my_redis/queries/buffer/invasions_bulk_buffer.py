from __future__ import annotations
from redis.asyncio.client import Redis
from utils.logger import logger
from utils.safe_values import _safe_int, _norm_str, _norm_name, _valid_coords, _to_float
from sql.tasks.invasions_processor import InvasionSQLProcessor
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
    async def increment_event(cls, redis_client: Redis, event_data: dict):
        """
        Accepts filtered invasion payload:
          invasion_pokestop_id, invasion_pokestop_name, invasion_latitude, invasion_longitude,
          invasion_type, invasion_character, invasion_grunt_type,
          invasion_confirmed, area_id, invasion_first_seen
        """
        try:
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
                logger.debug("❌ invasions: missing pokestop/area_id/first_seen")
                return

            # Skip if coords are missing/zero/invalid
            if not _valid_coords(lat, lon):
                logger.debug(f"↩️  invasions: dropped event for `{pokestop}` due to invalid coords lat={lat} lon={lon}")
                return

            line = (
                f"{pokestop}|{pokestop_name}|{lat}|{lon}|"
                f"{display_type}|{character}|{grunt}|{confirmed}|{area_id}|{first_seen}"
            )

            # Get a working client with retry
            client = await _get_client_with_retry(redis_client)
            if not client:
                logger.error("❌ Invasions buffer: No Redis connection available after retries. Event lost.")
                return

            # Append with retry
            new_len = None
            for attempt in range(3):
                try:
                    new_len = await client.rpush(cls.redis_key, line)
                    break
                except Exception as e:
                    if attempt < 2:
                        client = await _get_client_with_retry(None)
                        if not client:
                            logger.error(f"❌ Invasions buffer: rpush failed, no client available: {e}")
                            return
                    else:
                        logger.error(f"❌ Invasions buffer: rpush failed after retries: {e}")
                        return

            logger.debug(f"➕ Appended invasion event. len={new_len}")

            queued = await client.llen(cls.redis_key)
            if queued >= cls.aggregation_threshold:
                logger.warning(f"📊 🚀 Invasion buffer threshold {queued} reached. Flushing…")
                await cls.flush_if_ready(client)

        except Exception as e:
            logger.error(f"❌ invasions increment_event error: {e}", exc_info=True)

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
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        # Get a working client with retry
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error("❌ Invasions flush: No Redis connection available after retries.")
            return 0

        temp_key = None
        try:
            if not await client.exists(cls.redis_key):
                return 0

            temp_key = cls.redis_key + ":flushing"
            try:
                await client.rename(cls.redis_key, temp_key)
            except Exception as e:
                if "no such key" in str(e).lower():
                    return 0
                raise

            data_batch = await cls._consume_list_to_batch(client, temp_key)
            if not data_batch:
                return 0

            inserted = await InvasionSQLProcessor.bulk_insert_invasions_daily_events(data_batch)
            logger.success(f"📬 Invasions daily-events inserted +{inserted} rows.")
            return inserted

        except Exception as e:
            logger.error(f"❌ invasions flush error: {e}", exc_info=True)
            return 0
        finally:
            try:
                if temp_key and client:
                    await client.delete(temp_key)
            except Exception:
                pass

    @classmethod
    async def force_flush(cls, redis_client: Redis) -> int:
        # Get a working client with retry
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error("❌ Invasions force-flush: No Redis connection available after retries.")
            return 0

        temp_key = None
        try:
            if not await client.exists(cls.redis_key):
                return 0

            temp_key = cls.redis_key + ":force_flushing"
            try:
                await client.rename(cls.redis_key, temp_key)
            except Exception as e:
                if "no such key" in str(e).lower():
                    return 0
                raise

            data_batch = await cls._consume_list_to_batch(client, temp_key)
            if not data_batch:
                return 0

            inserted = await InvasionSQLProcessor.bulk_insert_invasions_daily_events(data_batch)
            logger.success(f"📬 FORCE invasions daily-events inserted +{inserted} rows.")
            return inserted

        except Exception as e:
            logger.error(f"❌ invasions force-flush error: {e}", exc_info=True)
            return 0
        finally:
            try:
                if temp_key and client:
                    await client.delete(temp_key)
            except Exception:
                pass
