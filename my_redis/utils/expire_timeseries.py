import asyncio
import time
from typing import Dict
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import config as AppConfig

redis_manager = RedisManager()

# Cache the SHA at module level so it survives different client instances
_CLEANUP_SHA = None

def get_retention_mapping() -> Dict[str, int]:
    """Build retention mapping with compiled patterns for faster matching."""

    return {
        "ts:pokemon:*":      AppConfig.timeseries_pokemon_retention_ms // 1000,
        "ts:tth_pokemon:*":  AppConfig.tth_timeseries_retention_ms // 1000,
        "ts:raids_total:*":  AppConfig.raid_timeseries_retention_ms // 1000,
        "ts:invasion:*":     AppConfig.invasion_timeseries_retention_ms // 1000,
        "ts:quests_total:*": AppConfig.quests_timeseries_retention_ms // 1000,
    }

async def _ensure_script_loaded(client):
    global _CLEANUP_SHA
    if not _CLEANUP_SHA:
        _CLEANUP_SHA = await client.script_load(CLEANUP_SCRIPT)

async def cleanup_timeseries_for_pattern(pattern: str, retention_sec: int) -> None:
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error(f"❌ Redis connection failed for pattern {pattern}")
        return

    cutoff = int(time.time()) - retention_sec
    logger.debug(f"▶️ Starting Lua cleanup for {pattern} (cutoff: {cutoff})")

    try:
        await _ensure_script_loaded(client)

        start_time = time.time()
        try:
            # ARGV: pattern, cutoff, key-scan, field-scan, hdel-batch
            fields_removed, empty_keys_deleted = await client.evalsha(
                _CLEANUP_SHA, 0, pattern, str(cutoff), "1000", "500", "1000"
            )
        except Exception as e:
            # Redis may report NOSCRIPT or similar after restart
            if "NOSCRIPT" in str(e).upper():
                logger.warning("📜 Cleanup script missing in Redis cache. Reloading…")
                await _ensure_script_loaded(client)
                fields_removed, empty_keys_deleted = await client.evalsha(
                    _CLEANUP_SHA, 0, pattern, str(cutoff), "1000", "500", "1000"
                )
            else:
                raise

        duration = time.time() - start_time
        logger.success(
            f"Lua ♻️ cleanup ✅ completed for {pattern}\n"
            f"• Fields 🗑️ removed: {fields_removed}\n"
            f"• Empty keys 🔥 deleted: {empty_keys_deleted}\n"
            f"• Duration ⏱️: {duration:.2f}s"
        )

    except Exception as e:
        logger.error(f"❌ Lua script failed: {e}")

async def cleanup_timeseries() -> None:
    """Run cleanup for all patterns with progress tracking."""
    retention_mapping = get_retention_mapping()
    total_start = time.time()

    for pattern, retention in retention_mapping.items():
        logger.debug(f"▶️ Starting pattern: {pattern}")
        await cleanup_timeseries_for_pattern(pattern, retention)

    total_duration = time.time() - total_start
    logger.debug(f"✅ All patterns completed in ⏱️ {total_duration:.2f} seconds")

async def periodic_cleanup() -> None:
    """Run cleanup with staggered timing to avoid spikes."""
    try:
        while True:
            logger.info("▶️ Starting periodic cleanup cycle")
            start_time = time.time()

            try:
                await cleanup_timeseries()
            except Exception as e:
                logger.error(f"❌ Cleanup failed: {e}")
                await asyncio.sleep(60)
                continue

            duration = time.time() - start_time
            sleep_time = max(3600 - duration, 0)
            logger.success(f"✅ Cleanup completed. 💤 Sleeping for ⏱️ {sleep_time:.1f} seconds")
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("🛑 Cleanup task cancelled")
        raise

CLEANUP_SCRIPT = r"""
local pattern     = ARGV[1]
local cutoff      = tonumber(ARGV[2])
local scan_count  = tonumber(ARGV[3] or 1000)   -- SCAN COUNT for keys
local hscan_count = tonumber(ARGV[4] or 500)    -- HSCAN COUNT for fields
local hdel_batch  = tonumber(ARGV[5] or 1000)   -- max fields per HDEL call

local fields_removed = 0
local empty_keys_deleted = 0

local cursor = "0"
repeat
    local reply = redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", scan_count)
    cursor = reply[1]
    local keys = reply[2]

    for _, key in ipairs(keys) do
        local hcursor = "0"
        local pending = {}

        repeat
            local hreply = redis.call("HSCAN", key, hcursor, "COUNT", hscan_count)
            hcursor = hreply[1]
            local flat = hreply[2]  -- [field1, value1, field2, value2, ...]

            for i = 1, #flat, 2 do
                local field = flat[i]
                local ts = tonumber(field)
                if ts and ts < cutoff then
                    table.insert(pending, field)
                    if #pending >= hdel_batch then
                        redis.call("HDEL", key, unpack(pending))
                        fields_removed = fields_removed + #pending
                        pending = {}
                    end
                end
            end
        until hcursor == "0"

        if #pending > 0 then
            redis.call("HDEL", key, unpack(pending))
            fields_removed = fields_removed + #pending
            pending = {}
        end

        if redis.call("HLEN", key) == 0 and redis.call("EXISTS", key) == 1 then
            redis.call("DEL", key)
            empty_keys_deleted = empty_keys_deleted + 1
        end
    end
until cursor == "0"

return {fields_removed, empty_keys_deleted}
"""
