"""
In-memory buffer for Quest events.

Each worker process accumulates events independently in Python memory.
The flusher (running in all workers) periodically swaps the buffer and
inserts the snapshot into MySQL via the SQL processor.

No Redis I/O occurs here — eliminates all buffer:quest_events key writes.
"""
from __future__ import annotations
import asyncio
from utils.logger import logger
from utils.safe_values import _safe_int, _norm_str, _norm_name, _valid_coords, _to_float
import config as AppConfig


class QuestsBuffer:
    """
    In-memory buffer for quest daily events.

    Events are stored as pipe-delimited strings (13 fields):
      pokestop|pokestop_name|lat|lon|mode|task_type|area_id|first_seen|kind|item_id|item_amount|poke_id|poke_form

    Where:
      - mode: 0=normal, 1=ar
      - kind: 0=item, 1=pokemon
      - item_id/item_amount used when kind=0; poke_id/poke_form when kind=1 (others set to 0/"")
    """
    _events: list[str] = []
    _lock: asyncio.Lock | None = None
    aggregation_threshold: int = int(AppConfig.quest_max_threshold)

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    def add_event(cls, event_data: dict) -> None:
        """Append one event to the buffer. Synchronous — no Redis, no await."""
        try:
            pokestop      = _norm_str(event_data.get("pokestop_id"))
            pokestop_name = _norm_name(event_data.get("pokestop_name"))
            lat           = _to_float(event_data.get("latitude"))
            lon           = _to_float(event_data.get("longitude"))

            area_id    = _safe_int(event_data.get("area_id"))
            first_seen = _safe_int(event_data.get("first_seen"))
            mode       = 1 if _safe_int(event_data.get("ar_type"), 0) > 0 else 0
            task_type  = _safe_int(event_data.get("ar_type") if mode else event_data.get("normal_type"), 0)

            if not pokestop or area_id is None or first_seen is None or task_type == 0:
                logger.debug("❌ Quests buffer: missing pokestop/area_id/first_seen/task_type. Skipping.")
                return

            if not _valid_coords(lat, lon):
                logger.debug(f"↩️  Quests buffer: dropped event for `{pokestop}` due to invalid coords lat={lat} lon={lon}")
                return

            # Reward resolution
            poke_id   = _safe_int(event_data.get("reward_ar_poke_id" if mode else "reward_normal_poke_id"), 0)
            poke_form = _norm_str(event_data.get("reward_ar_poke_form" if mode else "reward_normal_poke_form"), "")
            item_id   = _safe_int(event_data.get("reward_ar_item_id" if mode else "reward_normal_item_id"), 0)
            item_amt  = _safe_int(event_data.get("reward_ar_item_amount" if mode else "reward_normal_item_amount"), 0)

            if poke_id and poke_id > 0:
                kind = 1
                if not poke_form:
                    poke_form = "0"
                item_id = 0
                item_amt = 0
            elif item_id and item_id > 0:
                kind = 0
                if item_amt is None or item_amt <= 0:
                    item_amt = 1
                poke_id = 0
                poke_form = ""
            else:
                logger.debug("❌ Quests buffer: no usable reward (item or pokemon). Skipping.")
                return

            cls._events.append(
                f"{pokestop}|{pokestop_name}|{lat}|{lon}|{mode}|{task_type}|{area_id}|{first_seen}|{kind}|"
                f"{item_id}|{item_amt}|{poke_id}|{poke_form}"
            )

        except Exception as e:
            logger.error(f"❌ Quests buffer add_event error: {e}")

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
                    pokestop, name, lat_s, lon_s, mode_s, task_type_s, area_id_s, first_seen_s, kind_s,
                    item_id_s, item_amount_s, poke_id_s, poke_form
                ) = line.split("|", 12)

                data_batch.append({
                    "pokestop": pokestop,
                    "pokestop_name": name,
                    "latitude": float(lat_s),
                    "longitude": float(lon_s),
                    "mode": int(mode_s),
                    "task_type": int(task_type_s),
                    "area_id": int(area_id_s),
                    "first_seen": int(first_seen_s),
                    "kind": int(kind_s),
                    "item_id": int(item_id_s),
                    "item_amount": int(item_amount_s),
                    "poke_id": int(poke_id_s),
                    "poke_form": poke_form,
                })
            except Exception:
                malformed += 1

        return data_batch, malformed
