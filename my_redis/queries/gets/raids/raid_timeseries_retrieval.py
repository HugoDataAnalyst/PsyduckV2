import asyncio
import time
from datetime import datetime
from typing import Dict, Any
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

RAID_TIMESERIES_SCRIPT = """
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
    -- Extract key parts: ts, raids_total, raid_type, area, raid_pokemon, raid_level, raid_form
    local key_parts = {}
    for part in string.gmatch(key, "([^:]+)") do
      table.insert(key_parts, part)
    end
    local raid_type = key_parts[3] or "unknown"
    for i = 1, #hash_data, 2 do
      local ts = tonumber(hash_data[i])
      local count = tonumber(hash_data[i+1])
      if ts and count and ts >= start_ts and ts <= end_ts then
         -- Sum mode: aggregate counts per raid_type.
         sum_results[raid_type] = (sum_results[raid_type] or 0) + count
         -- Grouped mode: group by raid_type then by the bucket (timestamp).
         if not grouped_results[raid_type] then
           grouped_results[raid_type] = {}
         end
         local bucket_str = tostring(ts)
         grouped_results[raid_type][bucket_str] = (grouped_results[raid_type][bucket_str] or 0) + count
         -- Surged mode: group by raid_type then by the hour (extracted from the timestamp).
         if not surged_results[raid_type] then
           surged_results[raid_type] = {}
         end
         local hour = tostring(math.floor((ts % 86400) / 3600))
         surged_results[raid_type][hour] = (surged_results[raid_type][hour] or 0) + count
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
  for rt, groups in pairs(grouped_results) do
    local inner = {}
    for bucket, count in pairs(groups) do
      table.insert(inner, bucket)
      table.insert(inner, count)
    end
    table.insert(arr, rt)
    table.insert(arr, inner)
  end
  return arr
elseif mode == 'surged' then
  local arr = {}
  for rt, hours in pairs(surged_results) do
    local inner = {}
    for hour, count in pairs(hours) do
      table.insert(inner, hour)
      table.insert(inner, count)
    end
    table.insert(arr, rt)
    table.insert(arr, inner)
  end
  return arr
else
  return {}
end
"""

class RaidTimeSeries:
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 raid_type: str = "all", raid_pokemon: str = "all", raid_level: str = "all", raid_form: str = "all"):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.raid_type = raid_type
        self.raid_pokemon = raid_pokemon
        self.raid_level = raid_level
        self.raid_form = raid_form
        self.script_sha = None

        logger.info(
            f"Initialized RaidTimeSeries with area: {self.area}, mode: {self.mode}, "
            f"raid_type: {self.raid_type}, raid_pokemon: {self.raid_pokemon}, "
            f"raid_level: {self.raid_level}, raid_form: {self.raid_form}, "
            f"start: {self.start}, end: {self.end}"
        )

    async def _load_script(self, client):
        if not self.script_sha:
            logger.debug("Loading Raid Lua script into Redis...")
            self.script_sha = await client.script_load(RAID_TIMESERIES_SCRIPT)
            logger.debug(f"Raid Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.debug("Raid Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_pattern(self) -> str:
        # Replace any filter set to "all" (or "global" for area) with a wildcard "*"
        raid_type = "*" if self.raid_type.lower() in ["all"] else self.raid_type
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        raid_pokemon = "*" if self.raid_pokemon.lower() in ["all"] else self.raid_pokemon
        raid_level = "*" if self.raid_level.lower() in ["all"] else self.raid_level
        raid_form = "*" if self.raid_form.lower() in ["all"] else self.raid_form
        pattern = f"ts:raids_total:{raid_type}:{area}:{raid_pokemon}:{raid_level}:{raid_form}"
        logger.debug(f"Built Raid key pattern: {pattern}")
        return pattern

    async def raid_retrieve_timeseries(self) -> Dict[str, Any]:
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("Redis connection failed")
            return {"mode": self.mode, "data": {}}

        def convert_redis_result(res):
            if isinstance(res, list):
                if len(res) % 2 == 0:
                    return {res[i]: convert_redis_result(res[i+1]) for i in range(0, len(res), 2)}
                else:
                    return [convert_redis_result(item) for item in res]
            return res

        try:
            pattern = self._build_key_pattern()
            start_ts = int(self.start.timestamp())
            end_ts = int(self.end.timestamp())
            logger.debug(f"Raid Time range for query: start_ts={start_ts}, end_ts={end_ts}")

            # Start timing right before loading/executing the script
            request_start = time.monotonic()

            script_sha = await self._load_script(client)
            logger.debug("Executing Raid Lua script with evalsha...")
            raw_data = await client.evalsha(
                script_sha,
                0,  # No keys; only ARGV
                pattern,
                str(start_ts),
                str(end_ts),
                self.mode
            )
            logger.debug(f"Raw Raid data from Lua script (pre-conversion): {raw_data}")
            raw_data = convert_redis_result(raw_data)
            logger.debug(f"Converted Raid raw data: {raw_data}")

            formatted_data = {}
            if self.mode == "sum":
                # raw_data is a dict mapping raid_type -> count.
                # Order by raid_type (alphabetically, or adjust if numeric ordering is desired).
                formatted_data = dict(sorted(raw_data.items(), key=lambda item: item[0]))
                logger.debug(f"Formatted Raid 'sum' data: {formatted_data}")
            elif self.mode == "grouped":
                formatted_data = {}
                for rt, groups in raw_data.items():
                    if isinstance(groups, list):
                        groups = {groups[i]: groups[i+1] for i in range(0, len(groups), 2)}
                    # Order inner dictionary by timestamp (converted to int)
                    ordered_groups = dict(sorted(groups.items(), key=lambda x: int(x[0])))
                    formatted_data[rt] = ordered_groups
                # Order the outer dictionary by raid_type.
                formatted_data = dict(sorted(formatted_data.items(), key=lambda item: item[0]))
                logger.debug(f"Formatted Raid 'grouped' data: {formatted_data}")
            elif self.mode == "surged":
                formatted_data = {}
                for rt, inner in raw_data.items():
                    if isinstance(inner, list):
                        hours = {inner[i]: inner[i+1] for i in range(0, len(inner), 2)}
                    else:
                        hours = inner
                    formatted_data[rt] = dict(
                        sorted({f"hour {int(h)}": v for h, v in hours.items()}.items(), key=lambda x: int(x[0].split()[1]))
                    )
                formatted_data = dict(sorted(formatted_data.items(), key=lambda item: item[0]))
                logger.debug(f"Formatted Raid 'surged' data: {formatted_data}")

            # End timing after script execution
            request_end = time.monotonic()
            elapsed_time = request_end - request_start
            logger.info(f"Raid retrieval execution took {elapsed_time:.3f} seconds")

            return {"mode": self.mode, "data": formatted_data}
        except Exception as e:
            logger.error(f"Raid Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}
