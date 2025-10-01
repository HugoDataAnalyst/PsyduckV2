from __future__ import annotations
import asyncio
import time
from typing import Dict, Optional, Tuple
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import config as AppConfig

redis_manager = RedisManager()

# Config knobs for batching / budgets
SCAN_COUNT_DEFAULT       = 500        # keys per scan step
HSCAN_COUNT_DEFAULT      = 500        # fields per hscan step inside script
HDEL_BATCH_DEFAULT       = 1000       # max fields HDEL per call
PER_KEY_STEPS_DEFAULT    = 10         # how many HSCAN-steps per key per eval (keeps script short)
PER_PATTERN_TIME_BUDGET  = 1.5        # seconds to spend per pattern before yielding
GLOBAL_TIME_BUDGET       = 8.0        # seconds to spend per cleanup cycle (soft target)
SLEEP_BETWEEN_KEYS       = 0          # 0 or small sleep to yield
LOCK_KEY                 = "ts:cleanup:lock"
LOCK_TTL_SEC             = 60         # prevents overlapping runs

# We keep the per-key cleaner as a small script (fast, bounded)
_PER_KEY_CLEAN_SHA: Optional[str] = None

PER_KEY_CLEAN_SCRIPT = r"""
-- Args:
-- KEYS: none
-- ARGV[1]=key
-- ARGV[2]=cutoff (epoch seconds)
-- ARGV[3]=hscan_count
-- ARGV[4]=hdel_batch
-- ARGV[5]=max_steps (how many HSCAN loops to run this call)

local key          = ARGV[1]
local cutoff       = tonumber(ARGV[2])
local hscan_count  = tonumber(ARGV[3] or 500)
local hdel_batch   = tonumber(ARGV[4] or 1000)
local max_steps    = tonumber(ARGV[5] or 10)

local removed = 0
local hcursor = "0"
local steps = 0

repeat
    local hreply = redis.call("HSCAN", key, hcursor, "COUNT", hscan_count)
    hcursor = hreply[1]
    local flat = hreply[2]

    local pending = {}
    for i = 1, #flat, 2 do
        local field = flat[i]
        local ts = tonumber(field)
        if ts and ts < cutoff then
            table.insert(pending, field)
            if #pending >= hdel_batch then
                redis.call("HDEL", key, unpack(pending))
                removed = removed + #pending
                pending = {}
            end
        end
    end
    if #pending > 0 then
        redis.call("HDEL", key, unpack(pending))
        removed = removed + #pending
    end

    steps = steps + 1
    if steps >= max_steps then
        break
    end
until hcursor == "0"

local emptied = 0
if redis.call("TYPE", key).ok == "hash" then
    if redis.call("HLEN", key) == 0 then
        redis.call("DEL", key)
        emptied = 1
    end
end

-- return: removed, emptied, next_cursor (for continuation)
return { removed, emptied, hcursor }
"""

def get_retention_mapping() -> Dict[str, int]:
    """Timeseries retention (in seconds)."""
    return {
        "ts:pokemon:*":      AppConfig.timeseries_pokemon_retention_ms // 1000,
        "ts:tth_pokemon:*":  AppConfig.tth_timeseries_retention_ms // 1000,
        "ts:raids_total:*":  AppConfig.raid_timeseries_retention_ms // 1000,
        "ts:invasion:*":     AppConfig.invasion_timeseries_retention_ms // 1000,
        "ts:quests_total:*": AppConfig.quests_timeseries_retention_ms // 1000,
    }

async def _client():
    if not await redis_manager.check_redis_connection():
        return None
    return redis_manager.redis_client

async def _ensure_per_key_script(client):
    global _PER_KEY_CLEAN_SHA
    if not _PER_KEY_CLEAN_SHA:
        _PER_KEY_CLEAN_SHA = await client.script_load(PER_KEY_CLEAN_SCRIPT)

async def _try_acquire_lock(client) -> bool:
    # SET lock NX EX to avoid overlapping runs across processes
    try:
        ok = await client.set(LOCK_KEY, str(time.time()), nx=True, ex=LOCK_TTL_SEC)
        return bool(ok)
    except Exception as e:
        logger.warning(f"Lock acquire failed: {e}")
        return False

async def _release_lock(client):
    try:
        await client.delete(LOCK_KEY)
    except Exception:
        pass

async def _clean_hash_key(client, key: str, cutoff: int) -> Tuple[int, int, str]:
    """Run small-batch cleanup on a single hash key. Returns (removed, emptied, next_cursor)."""
    await _ensure_per_key_script(client)
    try:
        removed, emptied, next_cursor = await client.evalsha(
            _PER_KEY_CLEAN_SHA,
            0,
            key,
            str(cutoff),
            str(HSCAN_COUNT_DEFAULT),
            str(HDEL_BATCH_DEFAULT),
            str(PER_KEY_STEPS_DEFAULT),
        )
        # Redis may return integers as ints already depending on client
        return int(removed or 0), int(emptied or 0), str(next_cursor or "0")
    except Exception as e:
        # handle NOSCRIPT race
        if "NOSCRIPT" in str(e).upper():
            await _ensure_per_key_script(client)
            removed, emptied, next_cursor = await client.evalsha(
                _PER_KEY_CLEAN_SHA,
                0,
                key,
                str(cutoff),
                str(HSCAN_COUNT_DEFAULT),
                str(HDEL_BATCH_DEFAULT),
                str(PER_KEY_STEPS_DEFAULT),
            )
            return int(removed or 0), int(emptied or 0), str(next_cursor or "0")
        raise

async def cleanup_timeseries_for_pattern(pattern: str, retention_sec: int) -> None:
    client = await _client()
    if not client:
        logger.error(f"❌ Redis connection failed for pattern {pattern}")
        return

    cutoff = int(time.time()) - retention_sec
    logger.debug(f"▶️ Cleanup start for pattern={pattern}, cutoff={cutoff}")

    total_removed = 0
    total_emptied = 0
    started = time.time()

    # Iterate keys non-blocking using scan_iter (client-side SCAN cursor)
    async for key in client.scan_iter(match=pattern, count=SCAN_COUNT_DEFAULT):
        # Per key, call the small Lua cleaner; it may return a non-zero next_cursor -> continue calls
        next_cursor = "0"
        try:
            # loop until the hash has been fully scanned (HSCAN cursor returns "0"),
            # but keep each eval small; yield to loop between calls
            while True:
                removed, emptied, next_cursor = await _clean_hash_key(client, key, cutoff)
                total_removed += removed
                total_emptied += emptied

                # optional cooperative yield
                if SLEEP_BETWEEN_KEYS:
                    await asyncio.sleep(SLEEP_BETWEEN_KEYS)
                else:
                    await asyncio.sleep(0)

                if next_cursor == "0":
                    break

                # obey per-pattern time budget; let other work run
                if (time.time() - started) >= PER_PATTERN_TIME_BUDGET:
                    # soft break to keep the server responsive; caller may revisit later runs
                    break

        except Exception as e:
            # If *this* key fails, log and continue with others
            if "BUSY" in str(e).upper():
                # Should be rare now; our scripts are tiny.
                logger.warning(f"⚠️ Redis BUSY while cleaning key={key} (will retry next cycle).")
            else:
                logger.error(f"❌ Error cleaning key={key}: {e}")

        # check per-pattern time budget
        if (time.time() - started) >= PER_PATTERN_TIME_BUDGET:
            # We've spent enough on this pattern now; continue next pattern
            break

    duration = time.time() - started
    logger.success(
        f"Lua ♻️ cleanup ✅ completed for {pattern}\n"
        f"• Fields 🗑️ removed: {total_removed}\n"
        f"• Empty keys 🔥 deleted: {total_emptied}\n"
        f"• Duration ⏱️: {duration:.2f}s"
    )

async def cleanup_timeseries() -> None:
    """Run cleanup for all patterns with progress tracking (non-blocking, incremental)."""
    client = await _client()
    if not client:
        logger.error("❌ Redis not connected; skip cleanup run.")
        return

    # prevent overlapping runs
    if not await _try_acquire_lock(client):
        logger.info("🔒 Another cleanup is in progress; skipping this cycle.")
        return

    try:
        total_start = time.time()
        for pattern, retention in get_retention_mapping().items():
            logger.debug(f"▶️ Pattern: {pattern}")
            await cleanup_timeseries_for_pattern(pattern, retention)
            # small yield between patterns
            await asyncio.sleep(0)

            # optional global budget
            if (time.time() - total_start) >= GLOBAL_TIME_BUDGET:
                logger.info("⏱️ Global cleanup time budget reached; deferring remainder to next cycle.")
                break

        total_duration = time.time() - total_start
        logger.debug(f"✅ Cleanup pass finished in ⏱️ {total_duration:.2f}s")

    finally:
        await _release_lock(client)

async def periodic_cleanup() -> None:
    """Run cleanup periodically (hourly), cooperatively."""
    try:
        while True:
            logger.info("▶️ Starting periodic cleanup cycle")
            start_time = time.time()

            try:
                await cleanup_timeseries()
            except Exception as e:
                logger.error(f"❌ Cleanup failed: {e}")
                # on hard failure, wait a bit before retry to avoid tight loops
                await asyncio.sleep(60)

            duration = time.time() - start_time
            # sleep the remainder of an hour
            sleep_time = max(AppConfig.cleanup_interval_seconds - duration, 5)
            logger.success(f"✅ Cleanup completed. 💤 Sleeping for ⏱️ {sleep_time:.1f}s")
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("🛑 Cleanup task cancelled")
        raise
