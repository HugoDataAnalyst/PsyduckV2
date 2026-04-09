from __future__ import annotations
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import config as AppConfig

redis_manager = RedisManager()

# Config knobs for chunked cleanup
SCAN_COUNT_DEFAULT       = 1000       # keys per scan step
HSCAN_COUNT_DEFAULT      = 500        # fields per hscan step inside script
HDEL_BATCH_DEFAULT       = 1000       # max fields HDEL per call
CHUNK_SIZE_DEFAULT       = 100        # keys per Lua chunk
CHUNK_SLEEP_DEFAULT      = 0.15       # sleep between chunks (seconds)
LOCK_KEY                 = "ts:cleanup:lock"
LOCK_TTL_SEC             = 300        # prevents overlapping runs (5 minutes)

# Chunked cleanup script - processes multiple keys per call
_CLEANUP_CHUNK_SHA: Optional[str] = None

CLEANUP_CHUNK_SCRIPT = r"""
-- KEYS: array of hash keys to clean
-- ARGV[1]: cutoff timestamp (seconds)
-- ARGV[2]: hscan_count
-- ARGV[3]: hdel_batch

local cutoff = tonumber(ARGV[1])
local hscan_count = tonumber(ARGV[2] or 500)
local hdel_batch = tonumber(ARGV[3] or 1000)

local total_removed = 0
local total_emptied = 0

-- Process each key in the chunk
for _, key in ipairs(KEYS) do
    local hcursor = "0"
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
                    total_removed = total_removed + #pending
                    pending = {}
                end
            end
        end
        if #pending > 0 then
            redis.call("HDEL", key, unpack(pending))
            total_removed = total_removed + #pending
        end
    until hcursor == "0"

    -- Check if key is now empty and delete it
    if redis.call("TYPE", key).ok == "hash" then
        if redis.call("HLEN", key) == 0 then
            redis.call("DEL", key)
            total_emptied = total_emptied + 1
        end
    end
end

return { total_removed, total_emptied }
"""

def is_noscript_error(e: Exception) -> bool:
    s = str(e)
    needles = (
        "NOSCRIPT",
        "No matching script",
        "Please use EVAL",
    )
    s_up = s.upper()
    return any(n.upper() in s_up for n in needles)

def get_counter_hourly_retention_mapping() -> Dict[str, int]:
    """Counter hourly retention in hours. 0 = cleanup disabled for that type."""
    return {
        "counter:pokemon_hourly:*":     AppConfig.counter_pokemon_hourly_retention_hours,
        "counter:tth_pokemon_hourly:*": AppConfig.counter_tth_pokemon_hourly_retention_hours,
        "counter:raid_hourly:*":        AppConfig.counter_raid_hourly_retention_hours,
        "counter:invasion_hourly:*":    AppConfig.counter_invasion_hourly_retention_hours,
        "counter:quest_hourly:*":       AppConfig.counter_quest_hourly_retention_hours,
    }

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

async def _ensure_cleanup_script(client):
    """Load chunked cleanup script into Redis if not already cached"""
    global _CLEANUP_CHUNK_SHA
    try:
        if not _CLEANUP_CHUNK_SHA:
            _CLEANUP_CHUNK_SHA = await client.script_load(CLEANUP_CHUNK_SCRIPT)
            logger.debug(f"♻️ Cleanup script loaded with SHA: {_CLEANUP_CHUNK_SHA}")
    except Exception as e:
        _CLEANUP_CHUNK_SHA = None
        raise

async def _try_acquire_lock(client) -> bool:
    """Acquire cleanup lock to prevent overlapping runs"""
    try:
        ok = await client.set(LOCK_KEY, str(time.time()), nx=True, ex=LOCK_TTL_SEC)
        return bool(ok)
    except Exception as e:
        logger.warning(f"Lock acquire failed: {e}")
        return False

async def _release_lock(client):
    """Release cleanup lock"""
    try:
        await client.delete(LOCK_KEY)
    except Exception:
        pass

async def _scan_keys_by_pattern(client, pattern: str) -> list[str]:
    """SCAN for all matching keys (non-blocking, fast)"""
    scan_start = time.monotonic()
    all_keys = []

    cursor = 0
    while True:
        cursor, keys = await client.scan(cursor, match=pattern, count=SCAN_COUNT_DEFAULT)
        all_keys.extend(k.decode() if isinstance(k, bytes) else k for k in keys)
        if cursor == 0:
            break

    scan_elapsed = time.monotonic() - scan_start
    logger.debug(f"♻️ SCAN collected {len(all_keys)} keys for pattern '{pattern}' in {scan_elapsed:.3f}s")
    return all_keys

async def _clean_keys_chunk(client, keys: list[str], cutoff: int) -> Tuple[int, int]:
    """Clean a chunk of keys using Lua script. Returns (removed_fields, emptied_keys)."""
    global _CLEANUP_CHUNK_SHA
    await _ensure_cleanup_script(client)

    # Convert bytes keys to strings
    str_keys = [k.decode("utf-8") if isinstance(k, bytes) else k for k in keys]

    try:
        removed, emptied = await client.evalsha(
            _CLEANUP_CHUNK_SHA,
            len(str_keys),
            *str_keys,
            str(cutoff),
            str(HSCAN_COUNT_DEFAULT),
            str(HDEL_BATCH_DEFAULT),
        )
        return int(removed or 0), int(emptied or 0)
    except Exception as e:
        if is_noscript_error(e):
            logger.warning("📜 Cleanup script missing. Reloading and retrying…")
            _CLEANUP_CHUNK_SHA = None
            await _ensure_cleanup_script(client)
            removed, emptied = await client.evalsha(
                _CLEANUP_CHUNK_SHA,
                len(str_keys),
                *str_keys,
                str(cutoff),
                str(HSCAN_COUNT_DEFAULT),
                str(HDEL_BATCH_DEFAULT),
            )
            return int(removed or 0), int(emptied or 0)
        raise

async def cleanup_counter_hourly_for_pattern(client, pattern: str, retention_hours: int) -> int:
    """
    SCAN counter:*_hourly:* keys, parse the YYYYMMDDHH suffix from each key name,
    and pipeline-DEL keys older than retention_hours.

    Unlike the timeseries Lua cleanup (which removes old fields within a hash),
    this deletes entire keys — the date is in the key name, not the field names.

    Returns the number of keys deleted. 0 retention_hours = disabled.
    """
    if retention_hours == 0:
        logger.debug(f"♻️ Counter hourly cleanup disabled for {pattern} (retention=0)")
        return 0

    cutoff  = datetime.utcnow() - timedelta(hours=retention_hours)
    deleted = 0
    cursor  = 0

    while True:
        cursor, keys = await client.scan(cursor, match=pattern, count=SCAN_COUNT_DEFAULT)

        to_delete = []
        for key in keys:
            key_str  = key.decode() if isinstance(key, bytes) else key
            date_str = key_str.rsplit(":", 1)[-1]
            if len(date_str) == 10 and date_str.isdigit():
                try:
                    if datetime.strptime(date_str, "%Y%m%d%H") < cutoff:
                        to_delete.append(key)
                except ValueError:
                    pass

        if to_delete:
            for i in range(0, len(to_delete), HDEL_BATCH_DEFAULT):
                batch = to_delete[i:i + HDEL_BATCH_DEFAULT]
                await client.delete(*batch)
                deleted += len(batch)

        await asyncio.sleep(0)   # yield between scan pages

        if cursor == 0:
            break

    logger.info(f"♻️ Counter hourly cleanup {pattern}: {deleted} keys deleted")
    return deleted


async def cleanup_all_counter_hourly(client) -> int:
    """Iterate the counter hourly retention mapping and clean all enabled patterns."""
    total = 0
    for pattern, retention_hours in get_counter_hourly_retention_mapping().items():
        total += await cleanup_counter_hourly_for_pattern(client, pattern, retention_hours)
        await asyncio.sleep(0.1)   # small yield between patterns
    return total


async def cleanup_timeseries_for_pattern(pattern: str, retention_sec: int) -> None:
    """Cleanup timeseries for a pattern using chunked Lua approach (non-blocking)"""
    client = await _client()
    if not client:
        logger.error(f"❌ Redis connection failed for pattern {pattern}")
        return

    cutoff = int(time.time()) - retention_sec
    logger.info(f"▶️ Cleanup start for pattern={pattern}, retention={retention_sec}s, cutoff={cutoff}")

    started = time.time()

    # Step 1: SCAN for all keys (non-blocking, ~0.1-0.3s)
    all_keys = await _scan_keys_by_pattern(client, pattern)

    if not all_keys:
        logger.debug(f"♻️ No keys found for pattern '{pattern}'")
        return

    # Step 2: Split into chunks
    chunks = [all_keys[i:i+CHUNK_SIZE_DEFAULT] for i in range(0, len(all_keys), CHUNK_SIZE_DEFAULT)]
    logger.info(f"♻️ Processing {len(all_keys)} keys in {len(chunks)} chunks of ~{CHUNK_SIZE_DEFAULT} keys")

    total_removed = 0
    total_emptied = 0

    # Step 3: Process chunks with sleep intervals
    for i, chunk in enumerate(chunks):
        chunk_start = time.monotonic()

        try:
            removed, emptied = await _clean_keys_chunk(client, chunk, cutoff)
            total_removed += removed
            total_emptied += emptied

            chunk_elapsed = time.monotonic() - chunk_start
            logger.debug(f"♻️ Chunk {i+1}/{len(chunks)} cleaned {len(chunk)} keys: {removed} fields removed, {emptied} keys deleted in {chunk_elapsed:.3f}s")

        except Exception as e:
            if "BUSY" in str(e).upper():
                logger.warning(f"⚠️ Redis BUSY while cleaning chunk {i+1}/{len(chunks)} (will retry next cycle)")
            else:
                logger.error(f"❌ Error cleaning chunk {i+1}/{len(chunks)}: {e}")

        # Sleep between chunks to allow writes (except last chunk)
        if i < len(chunks) - 1:
            await asyncio.sleep(CHUNK_SLEEP_DEFAULT)

    duration = time.time() - started
    logger.success(
        f"♻️ Cleanup ✅ completed for {pattern}\n"
        f"• Keys processed: {len(all_keys)}\n"
        f"• Fields 🗑️ removed: {total_removed}\n"
        f"• Empty keys 🔥 deleted: {total_emptied}\n"
        f"• Duration ⏱️: {duration:.2f}s"
    )

async def cleanup_timeseries() -> None:
    """Run cleanup for all patterns (non-blocking, processes all keys)"""
    client = await _client()
    if not client:
        logger.error("❌ Redis not connected; skip cleanup run.")
        return

    # Prevent overlapping runs
    if not await _try_acquire_lock(client):
        logger.info("🔒 Another cleanup is in progress; skipping this cycle.")
        return

    try:
        total_start = time.time()

        # ── Timeseries cleanup (field-level Lua script) ───────────────────────
        for pattern, retention in get_retention_mapping().items():
            logger.info(f"▶️ Pattern: {pattern}, retention: {retention}s ({retention/3600:.1f}h)")
            await cleanup_timeseries_for_pattern(pattern, retention)
            await asyncio.sleep(0.1)

        # ── Counter hourly cleanup (whole-key DEL by key-name date suffix) ────
        total_counter_deleted = await cleanup_all_counter_hourly(client)
        logger.info(f"♻️ Counter hourly cleanup total: {total_counter_deleted} keys deleted")

        total_duration = time.time() - total_start
        logger.success(f"✅ Cleanup pass finished in ⏱️ {total_duration:.2f}s")

    finally:
        await _release_lock(client)

async def periodic_cleanup() -> None:
    """Run cleanup periodically (based on config interval)"""
    try:
        while True:
            logger.info("▶️ Starting periodic cleanup cycle")
            start_time = time.time()

            try:
                await cleanup_timeseries()
            except Exception as e:
                logger.error(f"❌ Cleanup failed: {e}")
                # On hard failure, wait a bit before retry
                await asyncio.sleep(60)

            duration = time.time() - start_time
            # Sleep the remainder of the interval
            sleep_time = max(AppConfig.cleanup_interval_seconds - duration, 5)
            logger.success(f"✅ Cleanup completed. 💤 Sleeping for ⏱️ {sleep_time:.1f}s")
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("🛑 Cleanup task cancelled")
        raise
