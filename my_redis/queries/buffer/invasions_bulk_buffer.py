from __future__ import annotations
from redis.asyncio.client import Redis
from utils.logger import logger
from utils.safe_values import _safe_int, _norm_str, _norm_name, _valid_coords, _to_float
from sql.tasks.invasions_processor import InvasionSQLProcessor
import config as AppConfig


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

            new_len = await redis_client.rpush(cls.redis_key, line)
            logger.debug(f"➕ Appended invasion event. len={new_len}")

            queued = await redis_client.llen(cls.redis_key)
            if queued >= cls.aggregation_threshold:
                logger.warning(f"📊 🚀 Invasion buffer threshold {queued} reached. Flushing…")
                await cls.flush_if_ready(redis_client)

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
        temp_key = None
        try:
            if not await redis_client.exists(cls.redis_key):
                return 0

            temp_key = cls.redis_key + ":flushing"
            try:
                await redis_client.rename(cls.redis_key, temp_key)
            except Exception as e:
                if "no such key" in str(e).lower():
                    return 0
                raise

            data_batch = await cls._consume_list_to_batch(redis_client, temp_key)
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
                if temp_key:
                    await redis_client.delete(temp_key)
            except Exception:
                pass

    @classmethod
    async def force_flush(cls, redis_client: Redis) -> int:
        temp_key = None
        try:
            if not await redis_client.exists(cls.redis_key):
                return 0

            temp_key = cls.redis_key + ":force_flushing"
            try:
                await redis_client.rename(cls.redis_key, temp_key)
            except Exception as e:
                if "no such key" in str(e).lower():
                    return 0
                raise

            data_batch = await cls._consume_list_to_batch(redis_client, temp_key)
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
                if temp_key:
                    await redis_client.delete(temp_key)
            except Exception:
                pass
