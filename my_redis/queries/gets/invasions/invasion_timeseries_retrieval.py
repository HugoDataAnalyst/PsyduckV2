import asyncio
import time
from datetime import datetime
from typing import Dict, Any
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

INVASION_TIMESERIES_SCRIPT = """
local pattern = ARGV[1]
local start_ts = tonumber(ARGV[2])
local end_ts = tonumber(ARGV[3])
local mode = ARGV[4]
local batch_size = 1000

local total = 0
local sum_results = {}
local grouped_results = {}
local surged_results = {}
local confirmed_results = {}

local cursor = '0'
repeat
  local reply = redis.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', batch_size)
  cursor = reply[1]
  local keys = reply[2]
  for _, key in ipairs(keys) do
    local hash_data = redis.call('HGETALL', key)
    -- Key format: ts:invasion:total:{area}:{display_type}:{grunt}:{confirmed}
    local key_parts = {}
    for part in string.gmatch(key, "([^:]+)") do
      table.insert(key_parts, part)
    end
    local group_key = key_parts[5] .. ":" .. key_parts[6] .. ":" .. key_parts[7]
    for i = 1, #hash_data, 2 do
      local ts = tonumber(hash_data[i])
      local count = tonumber(hash_data[i+1])
      if ts and count and ts >= start_ts and ts < end_ts then
         total = total + count
         sum_results[group_key] = (sum_results[group_key] or 0) + count

         if not grouped_results[group_key] then
           grouped_results[group_key] = {}
         end
         local bucket_str = tostring(ts)
         grouped_results[group_key][bucket_str] = (grouped_results[group_key][bucket_str] or 0) + count

         if not surged_results[group_key] then
           surged_results[group_key] = {}
         end
         local hour = tostring(math.floor((ts % 86400) / 3600))
         surged_results[group_key][hour] = (surged_results[group_key][hour] or 0) + count

         -- Accumulate counts per confirmed flag.
         local confirmed_key = key_parts[7]
         confirmed_results[confirmed_key] = (confirmed_results[confirmed_key] or 0) + count
      end
    end
  end
until cursor == '0'

if mode == 'sum' then
  -- Convert confirmed_results into an array of key/value pairs.
  local confirmed_arr = {}
  for k, v in pairs(confirmed_results) do
    table.insert(confirmed_arr, k)
    table.insert(confirmed_arr, v)
  end
  local result = {"total", total, "confirmed", confirmed_arr}
  return result
elseif mode == 'grouped' then
  local arr = {}
  for group_key, groups in pairs(grouped_results) do
    local inner = {}
    for bucket, count in pairs(groups) do
      table.insert(inner, bucket)
      table.insert(inner, count)
    end
    table.insert(arr, group_key)
    table.insert(arr, inner)
  end
  return arr
elseif mode == 'surged' then
  local arr = {}
  for group_key, hours in pairs(surged_results) do
    local inner = {}
    for hour, count in pairs(hours) do
      table.insert(inner, hour)
      table.insert(inner, count)
    end
    table.insert(arr, group_key)
    table.insert(arr, inner)
  end
  return arr
else
  return {}
end
"""

class InvasionTimeSeries:
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 display_type: str = "all", grunt: str = "all", confirmed: str = "all"):
        """
        Parameters:
          - area: Area name filter. Use "all" or "global" to match every area.
          - display_type: Invasion display type (e.g., a numeric value or "all").
          - grunt: Invasion grunt filter (numeric or "all").
          - confirmed: Invasion confirmed flag filter (numeric or "all").
          - mode: Aggregation mode: "sum", "grouped", or "surged".
        """
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.display_type = display_type
        self.grunt = grunt
        self.confirmed = confirmed
        self.script_sha = None

        logger.info(
            f"▶️ Initialized InvasionTimeSeries with area: {self.area}, mode: {self.mode}, "
            f"🕴️ Display type: {self.display_type}, grunt: {self.grunt}, confirmed: {self.confirmed}, "
            f"▶️ Start: {self.start}, ⏸️ End: {self.end}"
        )

    async def _load_script(self, client):
        if not self.script_sha:
            logger.debug("🔄 Loading 🕴️ Invasion Lua script into Redis...")
            self.script_sha = await client.script_load(INVASION_TIMESERIES_SCRIPT)
            logger.debug(f"🕴️ Invasion Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.debug("🕴️ Invasion Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_pattern(self) -> str:
        # Key format: ts:invasion:total:{area}:{display_type}:{grunt}:{confirmed}
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        display_type = "*" if self.display_type.lower() in ["all"] else self.display_type
        grunt = "*" if self.grunt.lower() in ["all"] else self.grunt
        confirmed = "*" if self.confirmed.lower() in ["all"] else self.confirmed
        pattern = f"ts:invasion:total:{area}:{display_type}:{grunt}:{confirmed}"
        logger.debug(f"Built 🕴️ Invasion 🔑 key pattern: {pattern}")
        return pattern

    async def invasion_retrieve_timeseries(self) -> Dict[str, Any]:
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
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
            logger.debug(f"🕴️ Invasion ⏱️ Time range for query: start_ts={start_ts}, end_ts={end_ts}")

            # Start timing right before loading/executing the script
            request_start = time.monotonic()

            script_sha = await self._load_script(client)
            logger.debug("Executing 🕴️ Invasion Lua script with evalsha...")
            raw_data = await client.evalsha(
                script_sha,
                0,  # No keys, only ARGV
                pattern,
                str(start_ts),
                str(end_ts),
                self.mode
            )

            logger.debug(f"Raw 🕴️ Invasion data from Lua script (pre-conversion): {raw_data}")
            raw_data = convert_redis_result(raw_data)
            logger.debug(f"Converted 🕴️ Invasion raw data: {raw_data}")

            formatted_data = {}
            if self.mode == "sum":
                # For mode "sum", raw_data is now a dictionary with keys "total" and "confirmed".
                formatted_data = raw_data
                logger.debug(f"Formatted 🕴️ Invasion 'sum' data: {formatted_data}")
            elif self.mode == "grouped":
                formatted_data = {}
                for group_key, groups in raw_data.items():
                    if isinstance(groups, list):
                        groups = {groups[i]: groups[i+1] for i in range(0, len(groups), 2)}
                    # Order inner dictionary by timestamp (as integer)
                    ordered_groups = dict(sorted(groups.items(), key=lambda x: int(x[0])))
                    formatted_data[group_key] = ordered_groups
                formatted_data = dict(
                    sorted(formatted_data.items(), key=lambda item: tuple(int(x) if x.isdigit() else x for x in item[0].split(":")))
                )
                logger.debug(f"Formatted 🕴️ Invasion 'grouped' data: {formatted_data}")
            elif self.mode == "surged":
                formatted_data = {}
                for group_key, inner in raw_data.items():
                    if isinstance(inner, list):
                        hours = {inner[i]: inner[i+1] for i in range(0, len(inner), 2)}
                    else:
                        hours = inner
                    formatted_data[group_key] = dict(
                        sorted({f"hour {int(h)}": v for h, v in hours.items()}.items(), key=lambda x: int(x[0].split()[1]))
                    )
                formatted_data = dict(
                    sorted(formatted_data.items(), key=lambda item: tuple(int(x) if x.isdigit() else x for x in item[0].split(":")))
                )
                logger.debug(f"Formatted 🕴️ Invasion 'surged' data: {formatted_data}")

            # End timing after script execution
            request_end = time.monotonic()
            elapsed_time = request_end - request_start
            logger.info(f"🕴️ Invasion retrieval execution took ⏱️ {elapsed_time:.3f} seconds")

            return {"mode": self.mode, "data": formatted_data}
        except Exception as e:
            logger.error(f"❌ 🕴️ Invasion Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}
