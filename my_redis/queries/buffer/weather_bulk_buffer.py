from __future__ import annotations
from redis.asyncio.client import Redis
from utils.logger import logger
from utils.safe_values import _safe_int, _valid_coords, _to_float
from sql.tasks.weather_processor import WeatherSQLProcessor
from my_redis.queries.buffer import flush_guard

# Order of the integer fields in a buffer line, after s2_cell_id
_LEVELS = (
    "gameplay_condition", "warn_weather", "severity", "wind_direction", "cloud_level",
    "rain_level", "wind_level", "snow_level", "fog_level", "special_effect_level",
)


class WeatherRedisBuffer:
    """
    Buffers weather change events as pipe-delimited lines in a Redis LIST, then flushes in batches.

    Composite line (15 fields):
      s2_cell_id|gameplay_condition|warn_weather|severity|wind_direction|cloud_level|rain_level|
      wind_level|snow_level|fog_level|special_effect_level|latitude|longitude|area_id|updated_epoch
    """
    redis_key = "buffer:weather_events"

    @classmethod
    def entry(cls, event_data: dict, area: dict) -> str | None:
        """Validate one event for one of its areas and build the buffer line; None (already logged) to skip it."""
        s2_cell_id = _safe_int(event_data.get("weather_s2_cell_id"), None)
        area_id    = _safe_int(area.get("area_id"), None)
        updated    = _safe_int(area.get("updated"), None)
        lat        = _to_float(event_data.get("weather_latitude"))
        lon        = _to_float(event_data.get("weather_longitude"))

        if s2_cell_id is None or area_id is None or updated is None:
            missing = [name for name, bad in (("s2_cell_id", s2_cell_id is None), ("area_id", area_id is None),
                                              ("updated", updated is None)) if bad]
            logger.debug(f"❌ weather: missing required fields {missing} (s2_cell_id={s2_cell_id!r})")
            return None

        if not _valid_coords(lat, lon):
            logger.debug(f"↩️  weather: dropped event for cell {s2_cell_id} due to invalid coords lat={lat} lon={lon}")
            return None

        levels = "|".join(str(_safe_int(event_data.get(f"weather_{k}"), 0)) for k in _LEVELS)
        return f"{s2_cell_id}|{levels}|{lat}|{lon}|{area_id}|{updated}"

    @classmethod
    def stage(cls, pipe, event_data: dict) -> bool:
        """Queue one buffer line per overlapping area on the caller's pipeline (no own round trip)."""
        try:
            lines = [cls.entry(event_data, area) for area in event_data.get("areas", [])]
        except Exception as e:
            logger.error(f"❌ Weather buffer: could not build entry: {e}")
            return False
        lines = [line for line in lines if line is not None]
        if not lines:
            return False
        pipe.rpush(cls.redis_key, *lines)
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
                parts = line.split("|")
                cell, levels, (lat_s, lon_s, area_s, upd_s) = parts[0], parts[1:11], parts[11:15]
                row = {"s2_cell_id": int(cell), "latitude": float(lat_s), "longitude": float(lon_s),
                       "area_id": int(area_s), "updated": int(upd_s)}
                row.update({k: int(v) for k, v in zip(_LEVELS, levels, strict=True)})
                batch.append(row)
            except Exception:
                malformed += 1

        if malformed:
            logger.warning(f"⚠️ weather buffer: skipped {malformed} malformed line(s)")
        return batch

    @classmethod
    async def _flush(cls, redis_client: Redis, tag: str, label: str) -> int:
        """
        Move the buffer aside, write it to MySQL, and delete it only on success.
        A failed write puts the batch back into the live buffer for the next flush.
        """
        inserted = await flush_guard.flush_list_buffer(
            redis_client, cls.redis_key, tag, cls._consume_list_to_batch,
            WeatherSQLProcessor.bulk_insert_weather_daily_events, label,
        )
        if inserted:
            logger.success(f"📬 {label} inserted +{inserted} rows.")
        return inserted

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        return await cls._flush(redis_client, "flushing", "Weather daily-events")

    @classmethod
    async def force_flush(cls, redis_client: Redis) -> int:
        return await cls._flush(redis_client, "force_flushing", "FORCE weather daily-events")
