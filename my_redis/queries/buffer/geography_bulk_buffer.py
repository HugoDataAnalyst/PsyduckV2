from __future__ import annotations
import time
from redis.asyncio.client import Redis
from utils.logger import logger
from utils.s2cell import spawnpoint_l16
from sql.tasks.geography_processor import GEOGRAPHY_FIELDS, GeographySQLProcessor
from my_redis.queries.buffer import flush_guard


class GeographyRedisBuffer:
    """
    Buffers each S2 level-16 cell's latest game geography in a Redis HASH (latest wins, one field per cell:
    every spawnpoint of a cell carries the same geography), then flushes it in batches.

    Field: l16 cell id (decimal). Value (8 fields, empty = NULL):
      landform|moisture|landcover|temperature|features|park_status|settlement|utc_epoch
    """
    redis_key = "buffer:cell_geography"

    @classmethod
    def entry(cls, event_data: dict) -> tuple[str, str] | None:
        """(l16, packed value) for one pokemon event with geography; None to skip it."""
        geo = event_data.get("geography")
        spawnpoint = event_data.get("spawnpoint")
        if not geo or not spawnpoint:
            return None
        sp = int(spawnpoint, 16)
        if sp >> 44 or not sp & 1:  # a spawnpoint is an S2 level-20 cell id >> 20: 44 bits, lsb set
            logger.debug(f"↩️  geography: dropped spawnpoint {spawnpoint}, not an S2 level-20 cell")
            return None
        values = "|".join("" if geo.get(k) is None else str(geo[k]) for k in GEOGRAPHY_FIELDS)
        return str(spawnpoint_l16(sp)), f"{values}|{int(time.time())}"

    @classmethod
    def stage(cls, pipe, event_data: dict) -> bool:
        """Queue this cell's geography on the caller's pipeline (no own round trip)."""
        try:
            entry = cls.entry(event_data)
        except Exception as e:
            logger.error(f"❌ Geography buffer: could not build entry: {e}")
            return False
        if entry is None:
            return False
        pipe.hset(cls.redis_key, *entry)
        return True

    @classmethod
    async def _consume_hash_to_batch(cls, redis_client: Redis, temp_key: str) -> list[dict]:
        raw = await redis_client.hgetall(temp_key)
        batch: list[dict] = []
        malformed = 0
        for k, v in (raw or {}).items():
            try:
                l16 = k.decode() if isinstance(k, (bytes, bytearray)) else k
                line = v.decode() if isinstance(v, (bytes, bytearray)) else v
                parts = line.split("|")
                values, epoch_s = parts[:7], parts[7]
                row = {"l16": int(l16), "updated": int(epoch_s)}
                row.update({name: int(x) if x else None for name, x in zip(GEOGRAPHY_FIELDS, values, strict=True)})
                batch.append(row)
            except Exception:
                malformed += 1

        if malformed:
            logger.warning(f"⚠️ geography buffer: skipped {malformed} malformed entr(ies)")
        return batch

    @classmethod
    async def _flush(cls, redis_client: Redis, tag: str, label: str) -> int:
        """
        Move the buffer aside, write it to MySQL, and delete it only on success.
        A failed write merges the batch back into the live buffer (newer entries win) for the next flush.
        """
        written = await flush_guard.flush_list_buffer(
            redis_client, cls.redis_key, tag, cls._consume_hash_to_batch,
            GeographySQLProcessor.upsert_cell_geography, label,
        )
        if written:
            logger.success(f"📬 {label} wrote {written} cells.")
        return written

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        return await cls._flush(redis_client, "flushing", "Cell geography")

    @classmethod
    async def force_flush(cls, redis_client: Redis) -> int:
        return await cls._flush(redis_client, "force_flushing", "FORCE cell geography")
