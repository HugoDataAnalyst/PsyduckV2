"""
In-memory buffer for Raid events.

Each worker process accumulates events independently in Python memory.
The flusher (running in all workers) periodically swaps the buffer and
inserts the snapshot into MySQL via the SQL processor.

No Redis I/O occurs here — eliminates all buffer:raid_events key writes.
"""
from __future__ import annotations
import asyncio
from utils.logger import logger
from utils.safe_values import _safe_int, _valid_coords, _to_float, _norm_name
import config as AppConfig


class RaidsBuffer:
    """
    In-memory buffer for raid daily events.

    Events are stored as pipe-delimited strings (13 fields):
      gym|gym_name|lat|lon|raid_pokemon|raid_form|raid_level|raid_team|
      raid_costume|raid_is_exclusive|raid_ex_raid_eligible|area_id|first_seen
    """
    _events: list[str] = []
    _lock: asyncio.Lock | None = None
    aggregation_threshold: int = int(AppConfig.raid_max_threshold)

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    def add_event(cls, event_data: dict) -> None:
        """Append one event to the buffer. Synchronous — no Redis, no await."""
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
                logger.warning("❌ Raids buffer: event missing required fields (gym/area_id/first_seen). Skipping.")
                return

            if not _valid_coords(lat, lon):
                logger.debug(f"↩️  Raids buffer: dropped event for `{gym}` due to invalid coords lat={lat} lon={lon}")
                return

            cls._events.append(
                f"{gym}|{gym_name}|{lat}|{lon}|{raid_pokemon}|{raid_form}|{raid_level}|{raid_team}|"
                f"{raid_costume}|{raid_is_exclusive}|{raid_ex_eligible}|{area_id}|{first_seen}"
            )

        except Exception as e:
            logger.error(f"❌ Raids buffer add_event error: {e}")

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

        return data_batch, malformed
