"""
In-memory buffers for Pokemon IV events and Shiny rate events.

Each worker process accumulates events independently in Python memory.
The flusher (running in all workers) periodically swaps the buffer and
inserts the snapshot into MySQL via the SQL processors.

No Redis I/O occurs here — eliminates all buffer:pokemon_iv_* and
buffer:agg_shiny_rates_hash key writes.
"""
from __future__ import annotations
import asyncio
from datetime import datetime
from utils.logger import logger
from utils.safe_values import _safe_int, _to_float
from sql.tasks.pokemon_processor import PokemonSQLProcessor
import config as AppConfig


class PokemonIVBuffer:
    """
    In-memory buffer for Pokemon IV daily events.

    Events are stored as pipe-delimited strings:
      spawnpoint|pokemon_id|form|iv|level|area_id|first_seen

    Coordinates are stored separately keyed by spawnpoint
    (first-seen wins, matching the old hsetnx behaviour).
    """
    _events: list[str] = []
    _coords: dict[str, str] = {}    # spawnpoint → "lat,lon"
    _lock: asyncio.Lock | None = None
    aggregation_threshold: int = int(AppConfig.pokemon_max_threshold)

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    def add_event(cls, event_data: dict) -> None:
        """Append one event to the buffer. Synchronous — no Redis, no await."""
        try:
            spawnpoint = event_data.get("spawnpoint")
            pokemon_id = event_data.get("pokemon_id")
            form       = str(event_data.get("form", 0))
            iv_raw     = event_data.get("iv")
            level      = event_data.get("level")
            area_id    = event_data.get("area_id")
            first_seen = event_data.get("first_seen")
            latitude   = _to_float(event_data.get("latitude"))
            longitude  = _to_float(event_data.get("longitude"))

            if None in [spawnpoint, pokemon_id, iv_raw, area_id, first_seen]:
                logger.warning("❌ Pokemon IV buffer: event missing required fields. Skipping.")
                return

            round_iv = int(round(float(iv_raw)))
            level    = int(level) if level is not None else 0

            cls._events.append(
                f"{spawnpoint}|{int(pokemon_id)}|{form}|{round_iv}|{level}|{int(area_id)}|{int(first_seen)}"
            )
            if latitude is not None and longitude is not None:
                cls._coords.setdefault(str(spawnpoint), f"{latitude},{longitude}")

        except Exception as e:
            logger.error(f"❌ Pokemon IV buffer add_event error: {e}")

    @classmethod
    def size(cls) -> int:
        return len(cls._events)

    @classmethod
    async def flush(cls) -> tuple[list[str], dict[str, str]] | None:
        """
        Atomically swap the buffer. Returns (events, coords) or None if empty.
        Caller parses and inserts into MySQL.
        """
        async with cls._get_lock():
            if not cls._events:
                return None
            events, coords = cls._events, cls._coords
            cls._events, cls._coords = [], {}
        return events, coords

    @staticmethod
    def build_batch(
        events: list[str],
        coords: dict[str, str],
    ) -> tuple[list[dict], int, int, int]:
        """
        Parse raw event strings + coords map into a data_batch for the SQL processor.
        Returns (data_batch, used_coords, missing_coords, malformed).
        """
        data_batch: list[dict] = []
        malformed = 0
        used_coords = 0
        missing_coords = 0

        for line in events:
            try:
                sp_hex, pid_s, form_s, iv_s, level_s, area_s, fs_s = line.split("|")
                coord_str = coords.get(sp_hex)
                if coord_str:
                    lat_s, lon_s = coord_str.split(",", 1)
                    lat, lon = float(lat_s), float(lon_s)
                    used_coords += 1
                else:
                    lat, lon = None, None
                    missing_coords += 1

                data_batch.append({
                    "spawnpoint": sp_hex,
                    "pokemon_id": int(pid_s),
                    "form": form_s,
                    "iv": int(iv_s),
                    "level": int(level_s),
                    "area_id": int(area_s),
                    "first_seen": int(fs_s),
                    "latitude": lat,
                    "longitude": lon,
                })
            except Exception:
                malformed += 1

        return data_batch, used_coords, missing_coords, malformed


class ShinyRateBuffer:
    """
    In-memory buffer for shiny rate aggregation.

    Counts are accumulated per composite key:
      username|pokemon_id|form|shiny|area_id|YYMM

    Multiple events for the same key are summed (matching old hincrby behaviour).
    Each worker accumulates its own partial counts; MySQL ON DUPLICATE KEY UPDATE
    merges them correctly across workers.
    """
    _counts: dict[str, int] = {}
    _lock: asyncio.Lock | None = None
    aggregation_threshold: int = int(AppConfig.shiny_max_threshold)

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    def add_event(cls, event_data: dict) -> None:
        """Increment the shiny count for this event's composite key. Synchronous — no Redis."""
        try:
            username   = event_data.get("username")
            pokemon_id = event_data.get("pokemon_id")
            form       = str(event_data.get("form", 0))
            shiny_raw  = event_data.get("shiny", 0)
            area_id    = event_data.get("area_id")
            first_seen = event_data.get("first_seen")

            if None in [username, pokemon_id, area_id, first_seen]:
                logger.warning("❌ Shiny buffer: event missing required fields. Skipping.")
                return

            if isinstance(shiny_raw, bool):
                shiny = 1 if shiny_raw else 0
            else:
                shiny = 1 if _safe_int(shiny_raw, 0) else 0

            month_year = datetime.fromtimestamp(int(first_seen)).strftime("%y%m")
            key = f"{username}|{int(pokemon_id)}|{form}|{shiny}|{int(area_id)}|{month_year}"
            cls._counts[key] = cls._counts.get(key, 0) + 1

        except Exception as e:
            logger.error(f"❌ Shiny buffer add_event error: {e}")

    @classmethod
    def size(cls) -> int:
        return len(cls._counts)

    @classmethod
    async def flush(cls) -> dict[str, int] | None:
        """Atomically swap the counts dict. Returns counts or None if empty."""
        async with cls._get_lock():
            if not cls._counts:
                return None
            counts = cls._counts
            cls._counts = {}
        return counts

    @staticmethod
    def build_batch(counts: dict[str, int]) -> tuple[list[dict], int]:
        """
        Parse raw counts dict into a data_batch for the SQL processor.
        Returns (data_batch, malformed_count).
        """
        data_batch: list[dict] = []
        malformed = 0
        for composite_key, count in counts.items():
            try:
                parts = composite_key.split("|")
                if len(parts) != 6:
                    malformed += 1
                    continue
                username, pid_s, form_s, shiny_s, area_s, yymm_s = parts
                data_batch.append({
                    "username": username,
                    "pokemon_id": int(pid_s),
                    "form": form_s,
                    "shiny": int(shiny_s),
                    "area_id": int(area_s),
                    "first_seen": int(datetime.strptime(yymm_s, "%y%m").timestamp()),
                    "increment": int(count),
                })
            except Exception:
                malformed += 1
        return data_batch, malformed
