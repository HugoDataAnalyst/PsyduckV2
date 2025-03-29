import asyncio
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Union
from my_redis.connect_redis import RedisManager
from utils.logger import logger
try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    relativedelta = None

redis_manager = RedisManager()

# Optimized Lua script for timeseries retrieval
TIMESERIES_SCRIPT = """
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

        -- Extract key parts (ts:pokemon:metric:area:pokemon_id:form)
        local key_parts = {}
        for part in string.gmatch(key, '([^:]+)') do
            table.insert(key_parts, part)
        end

        for i = 1, #hash_data, 2 do
            local ts = tonumber(hash_data[i])
            local count = tonumber(hash_data[i+1])
            if ts and count and ts >= start_ts and ts <= end_ts then
                local metric = key_parts[3] or 'unknown'
                local pokemon_id = key_parts[5] or 'all'
                local form = key_parts[6] or '0'

                -- Sum mode aggregation
                sum_results[metric] = (sum_results[metric] or 0) + count

                -- Grouped mode aggregation
                local group_key = pokemon_id .. ':' .. form
                if not grouped_results[metric] then
                    grouped_results[metric] = {}
                end
                grouped_results[metric][group_key] = (grouped_results[metric][group_key] or 0) + count

                -- Surged mode aggregation
                local hour = tostring(math.floor((ts % 86400) / 3600))
                if not surged_results[metric] then
                    surged_results[metric] = {}
                end
                surged_results[metric][hour] = (surged_results[metric][hour] or 0) + count
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
    for metric, groups in pairs(grouped_results) do
        local inner = {}
        for group_key, count in pairs(groups) do
            table.insert(inner, group_key)
            table.insert(inner, count)
        end
        table.insert(arr, metric)
        table.insert(arr, inner)
    end
    return arr
elseif mode == 'surged' then
    local arr = {}
    for metric, hours in pairs(surged_results) do
        local inner = {}
        for hour, count in pairs(hours) do
            table.insert(inner, hour)
            table.insert(inner, count)
        end
        table.insert(arr, metric)
        table.insert(arr, inner)
    end
    return arr
else
    return {}
end
"""

class PokemonTimeSeries:
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 pokemon_id: str = "all", form: str = "all"):
        # Ensure that the provided area exactly matches the stored key convention.
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.pokemon_id = pokemon_id
        self.form = form
        self.script_sha = None

        logger.info(f"Initialized PokemonTimeSeries with area: {self.area}, "
                     f"start: {self.start}, end: {self.end}, mode: {self.mode}, "
                     f"pokemon_id: {self.pokemon_id}, form: {self.form}")

    async def _load_script(self, client):
        """Load Lua script into Redis if not already cached"""
        if not self.script_sha:
            logger.info("Loading Lua script into Redis...")
            self.script_sha = await client.script_load(TIMESERIES_SCRIPT)
            logger.info(f"Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.info("Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    async def retrieve_timeseries(self) -> Dict:
        """Retrieve timeseries data using optimized Lua script"""
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("Redis connection failed")
            return {"mode": self.mode, "data": {}}

        # Helper: convert a Redis Lua table (list) into a dictionary.
        def convert_redis_result(res):
            if isinstance(res, list):
                if len(res) % 2 == 0:
                    return {res[i]: convert_redis_result(res[i + 1]) for i in range(0, len(res), 2)}
                else:
                    return [convert_redis_result(item) for item in res]
            return res

        try:
            # Build the key pattern based on filters.
            pattern = self._build_key_pattern()
            logger.info(f"Key pattern built: {pattern}")

            # Convert datetime objects to Unix timestamps.
            start_ts = int(self.start.timestamp())
            end_ts = int(self.end.timestamp())
            logger.info(f"Time range for query: start_ts={start_ts}, end_ts={end_ts}")

            # Load and execute Lua script.
            script_sha = await self._load_script(client)
            logger.info("Executing Lua script with evalsha...")
            raw_data = await client.evalsha(
                script_sha,
                0,  # No keys, only ARGV
                pattern,
                str(start_ts),
                str(end_ts),
                self.mode
            )
            logger.info(f"Raw data from Lua script (pre-conversion): {raw_data}")

            raw_data = convert_redis_result(raw_data)
            logger.info(f"Converted raw data: {raw_data}")

            # Format results based on mode.
            formatted_data = {}
            if self.mode == "sum":
                formatted_data = {k: v for k, v in raw_data.items()}
                logger.info(f"Formatted 'sum' data: {formatted_data}")
            elif self.mode == "grouped":
                for metric, groups in raw_data.items():
                    formatted_data[metric] = dict(
                        sorted(
                            groups.items(),
                            key=lambda x: (int(x[0].split(':')[0]), int(x[0].split(':')[1]))
                        )
                    )
                logger.info(f"Formatted 'grouped' data: {formatted_data}")
            elif self.mode == "surged":
                formatted_data = {}
                # raw_data is expected to be a dict mapping metric -> flat list [hour, count, hour, count, ...]
                for metric, inner in raw_data.items():
                    if isinstance(inner, list):
                        # Convert the flat list into a dictionary: { hour: count, ... }
                        hours = {inner[i]: inner[i+1] for i in range(0, len(inner), 2)}
                    else:
                        hours = inner
                    # Re-label each hour (if desired) and sort by hour (numeric order)
                    formatted_data[metric] = dict(
                        sorted(
                            {f"hour {int(h)}": v for h, v in hours.items()}.items(),
                            key=lambda x: int(x[0].split()[1])
                        )
                    )
                logger.info(f"Formatted 'surged' data: {formatted_data}")

            logger.info(f"Finished processing timeseries data for pattern: {pattern}")
            return {"mode": self.mode, "data": formatted_data}

        except Exception as e:
            logger.error(f"Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}

    def _build_key_pattern(self) -> str:
        """
        Build Redis key pattern based on filters.
        When area is not "all", the pattern is built from area, pokemon_id, and form.
        """
        # For the metric we always want to match all, so it's always "*"
        metric = "*"
        # For each filter, if the user set it to "all" or "global", substitute with the wildcard "*"
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        pokemon_id = "*" if self.pokemon_id.lower() in ["all"] else self.pokemon_id
        form = "*" if self.form.lower() in ["all"] else self.form
        pattern = f"ts:pokemon:{metric}:{area}:{pokemon_id}:{form}"
        logger.info(f"Built key pattern: {pattern}")
        return pattern
