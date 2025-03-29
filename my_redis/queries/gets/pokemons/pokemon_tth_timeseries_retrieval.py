import asyncio
from datetime import datetime
from typing import Dict
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

# Lua script as defined above.
TTH_TIMESERIES_SCRIPT = """
local pattern = ARGV[1]
local start_ts = tonumber(ARGV[2])
local end_ts = tonumber(ARGV[3])
local mode = ARGV[4]
local batch_size = 1000

local sum_results = {}
local grouped_results = {}
local surged_results = {}

local cursor = '0'
repeat
    local reply = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', batch_size)
    cursor = reply[1]
    local keys = reply[2]
    for _, key in ipairs(keys) do
        local hash_data = redis.call('HGETALL', key)
        -- Extract key parts: ts, tth_pokemon, area, tth_bucket
        local key_parts = {}
        for part in string.gmatch(key, '([^:]+)') do
            table.insert(key_parts, part)
        end
        for i = 1, #hash_data, 2 do
            local ts = tonumber(hash_data[i])
            local count = tonumber(hash_data[i+1])
            if ts and count and ts >= start_ts and ts <= end_ts then
                local bucket = key_parts[4] or "unknown"
                -- Sum mode aggregation
                sum_results[bucket] = (sum_results[bucket] or 0) + count
                -- Grouped mode aggregation: group by time bucket (minute-rounded timestamp)
                if not grouped_results[bucket] then
                    grouped_results[bucket] = {}
                end
                local time_bucket = tostring(math.floor(ts / 60) * 60)
                grouped_results[bucket][time_bucket] = (grouped_results[bucket][time_bucket] or 0) + count
                -- Surged mode aggregation: group by hour of day.
                if not surged_results[bucket] then
                    surged_results[bucket] = {}
                end
                local hour = tostring(math.floor((ts % 86400) / 3600))
                surged_results[bucket][hour] = (surged_results[bucket][hour] or 0) + count
            end
        end
    end
until cursor == '0'

if mode == 'sum' then
    local arr = {}
    for k, v in pairs(sum_results) do
        table.insert(arr, k)
        table.insert(arr, v)
    end
    return arr
elseif mode == 'grouped' then
    local arr = {}
    for bucket, groups in pairs(grouped_results) do
        local inner = {}
        for time_bucket, count in pairs(groups) do
            table.insert(inner, time_bucket)
            table.insert(inner, count)
        end
        table.insert(arr, bucket)
        table.insert(arr, inner)
    end
    return arr
elseif mode == 'surged' then
    local arr = {}
    for bucket, hours in pairs(surged_results) do
        local inner = {}
        for hour, count in pairs(hours) do
            table.insert(inner, hour)
            table.insert(inner, count)
        end
        table.insert(arr, bucket)
        table.insert(arr, inner)
    end
    return arr
else
    return {}
end
"""

class PokemonTTHTimeSeries:
    def __init__(self, area: str, start: datetime, end: datetime, tth_bucket: str = "all", mode: str = "sum"):
        """
        Parameters:
          - area: The area filter. Use "all" or "global" to match every area.
          - tth_bucket: The despawn timer bucket filter (e.g., "10_15"). Use "all" to match any bucket.
          - mode: Aggregation mode ("sum", "grouped", or "surged").
        """
        self.area = area
        self.start = start
        self.end = end
        self.tth_bucket = tth_bucket
        self.mode = mode.lower()
        self.script_sha = None

        logger.info(
            f"Initialized PokemonTTHTimeSeries with area: {self.area}, "
            f"tth_bucket: {self.tth_bucket}, start: {self.start}, end: {self.end}, mode: {self.mode}"
        )

    async def _load_script(self, client):
        if not self.script_sha:
            logger.info("Loading TTH Lua script into Redis...")
            self.script_sha = await client.script_load(TTH_TIMESERIES_SCRIPT)
            logger.info(f"TTH Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.info("TTH Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_pattern(self) -> str:
        # Replace "all" or "global" with wildcard for both area and tth_bucket.
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        bucket = "*" if self.tth_bucket.lower() in ["all"] else self.tth_bucket
        pattern = f"ts:tth_pokemon:{area}:{bucket}"
        logger.info(f"Built TTH key pattern: {pattern}")
        return pattern

    async def retrieve_timeseries(self) -> Dict:
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("Redis connection failed")
            return {"mode": self.mode, "data": {}}

        def convert_redis_result(res):
            if isinstance(res, list):
                if len(res) % 2 == 0:
                    return {res[i]: convert_redis_result(res[i + 1]) for i in range(0, len(res), 2)}
                else:
                    return [convert_redis_result(item) for item in res]
            return res

        try:
            pattern = self._build_key_pattern()
            start_ts = int(self.start.timestamp())
            end_ts = int(self.end.timestamp())
            logger.info(f"TTH Time range for query: start_ts={start_ts}, end_ts={end_ts}")

            script_sha = await self._load_script(client)
            logger.info("Executing TTH Lua script with evalsha...")
            raw_data = await client.evalsha(
                script_sha,
                0,  # No keys, only ARGV
                pattern,
                str(start_ts),
                str(end_ts),
                self.mode
            )
            logger.info(f"Raw TTH data from Lua script (pre-conversion): {raw_data}")
            raw_data = convert_redis_result(raw_data)
            logger.info(f"Converted TTH raw data: {raw_data}")

            formatted_data = {}
            if self.mode == "sum":
                # raw_data is a dict mapping tth_bucket -> count.
                # Order by the numeric value of the lower bound of the bucket.
                formatted_data = dict(
                    sorted(raw_data.items(), key=lambda item: int(item[0].split('_')[0]))
                )
                logger.info(f"Formatted TTH 'sum' data: {formatted_data}")
            elif self.mode == "grouped":
                formatted_data = {}
                for bucket, groups in raw_data.items():
                    if isinstance(groups, list):
                        groups = {groups[i]: groups[i + 1] for i in range(0, len(groups), 2)}
                    # Order inner dictionary by timestamp (converted to int)
                    ordered_groups = dict(sorted(groups.items(), key=lambda x: int(x[0])))
                    formatted_data[bucket] = ordered_groups
                # Order the outer dictionary by the lower bound of each bucket.
                formatted_data = dict(
                    sorted(formatted_data.items(), key=lambda item: int(item[0].split('_')[0]))
                )
                logger.info(f"Formatted TTH 'grouped' data: {formatted_data}")
            elif self.mode == "surged":
                # For surged mode, assume your existing logic works (grouping by hour, etc.).
                formatted_data = {}
                for bucket, inner in raw_data.items():
                    if isinstance(inner, list):
                        hours = {inner[i]: inner[i + 1] for i in range(0, len(inner), 2)}
                    else:
                        hours = inner
                    formatted_data[bucket] = dict(
                        sorted(
                            {f"hour {int(h)}": v for h, v in hours.items()}.items(),
                            key=lambda x: int(x[0].split()[1])
                        )
                    )
                # Optionally, order the outer dictionary by bucket as well.
                formatted_data = dict(
                    sorted(formatted_data.items(), key=lambda item: int(item[0].split('_')[0]))
                )
                logger.info(f"Formatted TTH 'surged' data: {formatted_data}")

            logger.info(f"Finished processing TTH timeseries data for pattern: {pattern}")
            return {"mode": self.mode, "data": formatted_data}

        except Exception as e:
            logger.error(f"TTH Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}
