"""
In-memory buffer for Invasion events.

Each worker process accumulates events independently in Python memory.
The flusher (running in all workers) periodically swaps the buffer and
inserts the snapshot into MySQL via the SQL processor.

No Redis I/O occurs here — eliminates all buffer:invasion_events key writes.
"""
from __future__ import annotations
import asyncio
from utils.logger import logger
from utils.safe_values import _safe_int, _norm_str, _norm_name, _valid_coords, _to_float
import config as AppConfig


class InvasionsBuffer:
    """
    In-memory buffer for invasion daily events.

    Events are stored as pipe-delimited strings (10 fields):
      pokestop|pokestop_name|latitude|longitude|display_type|character|grunt|confirmed|area_id|first_seen
    """
    _events: list[str] = []
    _lock: asyncio.Lock | None = None
    aggregation_threshold: int = int(AppConfig.invasion_max_threshold)

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    def add_event(cls, event_data: dict) -> None:
        """Append one event to the buffer. Synchronous — no Redis, no await."""
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
                logger.debug("❌ Invasions buffer: missing pokestop/area_id/first_seen. Skipping.")
                return

            if not _valid_coords(lat, lon):
                logger.debug(f"↩️  Invasions buffer: dropped event for `{pokestop}` due to invalid coords lat={lat} lon={lon}")
                return

            cls._events.append(
                f"{pokestop}|{pokestop_name}|{lat}|{lon}|"
                f"{display_type}|{character}|{grunt}|{confirmed}|{area_id}|{first_seen}"
            )

        except Exception as e:
            logger.error(f"❌ Invasions buffer add_event error: {e}")

    @classmethod
    def size(cls) -> int:
        return len(cls._events)

    @classmethod
    async def flush(cls) -> list[str] | None:
        """
        Atomically swap the buffer. Returns events list or None if empty.
        Caller parses and inserts into MySQL.
        """
        async with cls._get_lock():
            if not cls._events:
                return None
            events = cls._events
            cls._events = []
        return events

    @staticmethod
    def build_batch(events: list[str]) -> tuple[list[dict], int]:
        """
        Parse raw event strings into a data_batch for the SQL processor.
        Returns (data_batch, malformed_count).
        """
        data_batch: list[dict] = []
        malformed = 0

        for line in events:
            try:
                (
                    pokestop, pokestop_name, lat_s, lon_s,
                    dt_s, ch_s, gr_s, conf_s, area_s, fs_s
                ) = line.split("|", 9)

                data_batch.append({
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

        return data_batch, malformed
