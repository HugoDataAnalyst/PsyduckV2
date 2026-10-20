"""
Crash- and failure-safe hand-off of Redis buffers to the SQL flushers.

A flush moves the live buffer to a temporary key, writes it to MySQL, and only
then deletes it. If the write fails, the batch is merged back into the live
buffer so the next flush retries it; if the process dies mid-flush, the
temporary key survives and `recover_orphans` merges it back later.

Temporary keys are unique per flush, so two flushes running at once (the
background flusher and an inline threshold flush) can never overwrite each
other's batch, which a fixed ":flushing" name allowed.
"""
from __future__ import annotations

import asyncio
import os
import re
import time
import uuid

from utils.logger import logger

FLUSH_TAGS = ("flushing", "force_flushing")

# A new-style temporary key older than this is treated as abandoned. A healthy
# flush finishes in seconds; the IV processor's worst retry path is ~2 minutes.
ORPHAN_MIN_AGE_S = 900

# Buffers whose hash values are counts (merged by adding). Other hashes hold
# first-seen data such as spawnpoint coordinates (merged keeping existing values).
COUNTER_HASHES = {"buffer:agg_shiny_rates_hash"}

# buffer:<name>:<tag>[:<epoch>:<pid>:<rand>]
_TEMP_RE = re.compile(r"^(?P<live>buffer:[^:]+):(?P<tag>force_flushing|flushing)(?::(?P<epoch>\d+):\d+:[0-9a-f]+)?$")

_MERGE_LIST = """
local src, dst = KEYS[1], KEYS[2]
if redis.call('EXISTS', dst) == 0 then
  if redis.call('EXISTS', src) == 1 then redis.call('RENAME', src, dst) end
  return 1
end
local n = redis.call('LLEN', src)
local i = 0
while i < n do
  local chunk = redis.call('LRANGE', src, i, i + 4999)
  if #chunk == 0 then break end
  redis.call('RPUSH', dst, unpack(chunk))
  i = i + #chunk
end
redis.call('DEL', src)
return 1
"""

_MERGE_HASH_INCR = """
local src, dst = KEYS[1], KEYS[2]
if redis.call('EXISTS', dst) == 0 then
  if redis.call('EXISTS', src) == 1 then redis.call('RENAME', src, dst) end
  return 1
end
local cursor = '0'
repeat
  local res = redis.call('HSCAN', src, cursor, 'COUNT', 1000)
  cursor = res[1]
  local kv = res[2]
  for i = 1, #kv, 2 do redis.call('HINCRBY', dst, kv[i], kv[i + 1]) end
until cursor == '0'
redis.call('DEL', src)
return 1
"""

_MERGE_HASH_SETNX = """
local src, dst = KEYS[1], KEYS[2]
if redis.call('EXISTS', dst) == 0 then
  if redis.call('EXISTS', src) == 1 then redis.call('RENAME', src, dst) end
  return 1
end
local cursor = '0'
repeat
  local res = redis.call('HSCAN', src, cursor, 'COUNT', 1000)
  cursor = res[1]
  local kv = res[2]
  for i = 1, #kv, 2 do redis.call('HSETNX', dst, kv[i], kv[i + 1]) end
until cursor == '0'
redis.call('DEL', src)
return 1
"""


def new_temp_key(live_key: str, tag: str = "flushing") -> str:
    return f"{live_key}:{tag}:{int(time.time())}:{os.getpid()}:{uuid.uuid4().hex[:6]}"


async def claim(client, live_key: str, tag: str = "flushing") -> str | None:
    """
    Atomically move the live buffer to a fresh temporary key.
    Returns the temporary key, or None when there is nothing to flush.
    """
    temp_key = new_temp_key(live_key, tag)
    try:
        await client.rename(live_key, temp_key)
    except Exception as e:
        if "no such key" in str(e).lower():
            return None
        raise
    return temp_key


async def requeue_list(client, temp_key: str, live_key: str) -> None:
    await client.eval(_MERGE_LIST, 2, temp_key, live_key)


async def requeue_hash(client, temp_key: str, live_key: str) -> None:
    script = _MERGE_HASH_INCR if live_key in COUNTER_HASHES else _MERGE_HASH_SETNX
    await client.eval(script, 2, temp_key, live_key)


async def requeue(client, temp_key: str | None, live_key: str) -> None:
    """
    Merge a temporary batch back into its live buffer. Never raises: if Redis is
    unreachable the temporary key simply stays put and is recovered later as an
    orphan, so nothing is lost either way.
    """
    if not temp_key:
        return
    try:
        kind = await client.type(temp_key)
        if kind == "list":
            await requeue_list(client, temp_key, live_key)
        elif kind == "hash":
            await requeue_hash(client, temp_key, live_key)
        elif kind == "none":
            return
        else:
            logger.error(f"❌ flush_guard: unexpected type {kind!r} for {temp_key}; left in place")
            return
        logger.warning(f"↩️ Re-queued failed batch {temp_key} -> {live_key}")
    except Exception as e:
        logger.error(f"❌ flush_guard: could not re-queue {temp_key} ({e}); it will be recovered as an orphan")


async def requeue_counts(client, live_key: str, counts: dict[str, int]) -> None:
    """Add counts back to a counter buffer (used when only part of a batch was written)."""
    if not counts:
        return
    async with client.pipeline(transaction=False) as pipe:
        for field, n in counts.items():
            pipe.hincrby(live_key, field, int(n))
        await pipe.execute()
    logger.warning(f"↩️ Re-queued {len(counts)} unwritten entries -> {live_key}")


async def discard(client, *temp_keys: str | None) -> None:
    keys = [k for k in temp_keys if k]
    if keys:
        await client.delete(*keys)


async def flush_list_buffer(client, live_key: str, tag: str, parse, write, label: str) -> int:
    """
    Generic claim -> parse -> write -> delete for list buffers whose SQL write is
    idempotent (INSERT IGNORE / overwrite upserts), so retrying a whole batch is safe.

    parse(client, temp_key) -> list[dict]   rows to write (malformed lines dropped)
    write(rows) -> int                      raises on failure (FlushError)
    """
    temp_key = await claim(client, live_key, tag)
    if temp_key is None:
        logger.debug(f"📭 No {label} data to flush.")
        return 0
    try:
        rows = await parse(client, temp_key)
        if not rows:
            await discard(client, temp_key)   # nothing usable: every line was malformed
            return 0
        inserted = await write(rows)
    except Exception as e:
        logger.error(f"❌ {label} flush failed, keeping the batch for retry: {e}")
        await requeue(client, temp_key, live_key)
        return 0
    await discard(client, temp_key)
    return inserted


async def recover_orphans(client, min_age_s: int = ORPHAN_MIN_AGE_S, include_legacy: bool = False) -> int:
    """
    Merge abandoned temporary keys back into their live buffers.

    include_legacy also recovers keys with the old fixed names
    (buffer:<name>:flushing). Only pass it at leader startup, before any
    flusher runs: older code may still be using those names elsewhere.
    Returns the number of temporary keys recovered.
    """
    recovered = 0
    now = int(time.time())
    async for key in client.scan_iter(match="buffer:*flushing*", count=1000):
        m = _TEMP_RE.match(key)
        if not m:
            continue
        epoch = m.group("epoch")
        if epoch is None:
            if not include_legacy:
                continue
        elif now - int(epoch) < min_age_s:
            continue
        size = await _size(client, key)
        await requeue(client, key, m.group("live"))
        recovered += 1
        logger.warning(f"🛟 Recovered orphaned flush batch {key} ({size} entries) -> {m.group('live')}")
    return recovered


async def _size(client, key: str):
    kind = await client.type(key)
    if kind == "list":
        return await client.llen(key)
    if kind == "hash":
        return await client.hlen(key)
    return 0


def startup_orphan_age_s(workers: int) -> int:
    """
    With a single worker nothing else can be mid-flush when the leader starts,
    so every temporary key is an orphan. With several workers a follower may be
    running an inline threshold flush, so only clearly abandoned keys qualify.
    """
    return 0 if workers <= 1 else ORPHAN_MIN_AGE_S


class OrphanSweeper:
    """Leader-only service: periodically recovers batches left by failed flushes."""

    def __init__(self, redis_manager, interval: int = 300, min_age_s: int = ORPHAN_MIN_AGE_S):
        self._redis_manager = redis_manager
        self.interval = interval
        self.min_age_s = min_age_s
        self._task = None
        self._running = False

    async def _loop(self):
        while self._running:
            await asyncio.sleep(self.interval)
            try:
                client = await self._redis_manager.check_redis_connection()
                if client:
                    n = await recover_orphans(client, min_age_s=self.min_age_s, include_legacy=True)
                    if n:
                        logger.warning(f"🛟 Orphan sweep recovered {n} abandoned flush batch(es)")
            except Exception as e:
                logger.error(f"❌ Orphan sweep failed: {e}")

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
