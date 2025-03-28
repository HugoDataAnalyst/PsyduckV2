import asyncio
import time
from typing import List, Dict
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import config as AppConfig

redis_manager = RedisManager()

def get_retention_mapping() -> Dict[str, int]:
    """Build retention mapping with compiled patterns for faster matching."""
    return {
        "ts:pokemon:*": AppConfig.timeseries_pokemon_retention_ms // 1000,
        "ts:tth_pokemon:*": AppConfig.tth_timeseries_retention_ms // 1000,
        "ts:raids_total:*": AppConfig.raid_timeseries_retention_ms // 1000,
        "ts:invasion:*": AppConfig.invasion_timeseries_retention_ms // 1000,
        "ts:quests_total:*": AppConfig.quests_timeseries_retention_ms // 1000,
    }

async def cleanup_timeseries_for_pattern(pattern: str, retention_sec: int) -> None:
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error(f"❌ Redis connection failed for pattern {pattern}")
        return

    cutoff = int(time.time()) - retention_sec
    logger.info(f"▶️ Starting Lua cleanup for {pattern} (cutoff: {cutoff})")

    try:
        # Register script if not already cached
        if not hasattr(client, "cleanup_script_sha"):
            client.cleanup_script_sha = await client.script_load(CLEANUP_SCRIPT)

        start_time = time.time()
        fields_removed = await client.evalsha(
            client.cleanup_script_sha,
            0,  # No keys used, only ARGV
            pattern,
            str(cutoff),
            "1000"  # SCAN batch size
        )

        duration = time.time() - start_time
        logger.success(
            f"Lua ♻️ cleanup ✅ completed for {pattern}\n"
            f"• Fields 🗑️ removed: {fields_removed}\n"
            f"• Duration ⏱️: {duration:.2f}s"
        )

    except Exception as e:
        logger.error(f"❌ Lua script failed: {e}")
    finally:
        await client.aclose()

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
                await asyncio.sleep(60)  # Wait before retry
                continue

            duration = time.time() - start_time
            sleep_time = max(3600 - duration, 0)  # Ensure full hour between runs
            logger.success(f"✅ Cleanup completed. 💤 Sleeping for ⏱️ {sleep_time:.1f} seconds")
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("🛑 Cleanup task cancelled")
        raise

CLEANUP_SCRIPT = """
local pattern = ARGV[1]
local cutoff = tonumber(ARGV[2])
local batch_size = tonumber(ARGV[3] or 1000)
local fields_removed = 0

local cursor = "0"
repeat
    local reply = redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", batch_size)
    cursor = reply[1]
    local keys = reply[2]

    for _, key in ipairs(keys) do
        local fields = redis.call("HKEYS", key)
        local to_delete = {}

        for _, field in ipairs(fields) do
            local timestamp = tonumber(field)
            if timestamp and timestamp < cutoff then
                table.insert(to_delete, field)
            end
        end

        if #to_delete > 0 then
            redis.call("HDEL", key, unpack(to_delete))
            fields_removed = fields_removed + #to_delete
        end
    end
until cursor == "0"

return fields_removed
"""


if __name__ == "__main__":
    asyncio.run(cleanup_timeseries())
