import asyncio
import time
from datetime import datetime
from typing import Dict, Any
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

QUEST_TIMESERIES_SCRIPT = """
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
    for i = 1, #hash_data, 2 do
      local ts = tonumber(hash_data[i])
      local count = tonumber(hash_data[i+1])
      if ts and count and ts >= start_ts and ts <= end_ts then
         if mode == 'sum' then
           sum_results[key] = (sum_results[key] or 0) + count
         elseif mode == 'grouped' then
           if not grouped_results[key] then grouped_results[key] = {} end
           local bucket_str = tostring(ts)
           grouped_results[key][bucket_str] = (grouped_results[key][bucket_str] or 0) + count
         elseif mode == 'surged' then
           if not surged_results[key] then surged_results[key] = {} end
           local hour = tostring(math.floor((ts % 86400) / 3600))
           surged_results[key][hour] = (surged_results[key][hour] or 0) + count
         end
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
  for k, groups in pairs(grouped_results) do
    local inner = {}
    for bucket, count in pairs(groups) do
      table.insert(inner, bucket)
      table.insert(inner, count)
    end
    table.insert(arr, k)
    table.insert(arr, inner)
  end
  return arr
elseif mode == 'surged' then
  local arr = {}
  for k, hours in pairs(surged_results) do
    local inner = {}
    for hour, count in pairs(hours) do
      table.insert(inner, hour)
      table.insert(inner, count)
    end
    table.insert(arr, k)
    table.insert(arr, inner)
  end
  return arr
else
  return {}
end
"""

class QuestTimeSeries:
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 quest_variant: str = "overall", quest_mode: str = "normal", field_details: str = "all"):
        """
        Parameters:
          - area: Area name filter. Use "all" or "global" to match every area.
          - start, end: Datetime objects for the time range.
          - mode: Aggregation mode: "sum", "grouped", or "surged".
          - quest_variant: Either "overall" (for overall quest counts) or "detailed" (for detailed AR/normal quests).
          - quest_mode: For both overall and detailed, indicates quest mode ("ar" or "normal"). If set to "all", wildcard is used.
          - field_details: Only applicable when quest_variant is "detailed". If "all", a wildcard is used; otherwise, provide the colon‑separated field details.
        """
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.quest_variant = quest_variant.lower()
        self.quest_mode = quest_mode.lower()
        self.field_details = field_details
        self.script_sha = None

        logger.info(
            f"Initialized QuestTimeSeries with area: {self.area}, mode: {self.mode}, "
            f"quest_variant: {self.quest_variant}, quest_mode: {self.quest_mode}, "
            f"field_details: {self.field_details}, start: {self.start}, end: {self.end}"
        )

    async def _load_script(self, client):
        if not self.script_sha:
            logger.info("Loading Quest Lua script into Redis...")
            self.script_sha = await client.script_load(QUEST_TIMESERIES_SCRIPT)
            logger.info(f"Quest Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.info("Quest Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_pattern(self) -> str:
        # Substitute any filter value equal to "all" or "global" with a wildcard "*"
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        quest_mode = "*" if self.quest_mode.lower() == "all" else self.quest_mode

        if self.quest_variant == "overall":
            # Overall keys: ts:quests_total:total:{area}:{quest_mode}
            pattern = f"ts:quests_total:total:{area}:{quest_mode}"
        else:
            # Detailed keys depend on quest_mode
            # For AR: ts:quests_total:total_ar_detailed:{area}:{quest_mode}:{field_details}
            # For normal: ts:quests_total:total_normal_detailed:{area}:{quest_mode}:{field_details}
            # Use wildcard for field_details if "all"
            field_details = "*" if self.field_details.lower() == "all" else self.field_details
            if quest_mode == "ar":
                pattern = f"ts:quests_total:total_ar_detailed:{area}:{quest_mode}:{field_details}"
            else:
                pattern = f"ts:quests_total:total_normal_detailed:{area}:{quest_mode}:{field_details}"
        logger.info(f"Built Quest key pattern: {pattern}")
        return pattern

    async def quest_retrieve_timeseries(self) -> Dict[str, Any]:
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
            logger.info(f"Quest Time range for query: start_ts={start_ts}, end_ts={end_ts}")

            # Start timing right before loading/executing the script
            request_start = time.monotonic()

            script_sha = await self._load_script(client)
            logger.info("Executing Quest Lua script with evalsha...")
            raw_data = await client.evalsha(
                script_sha,
                0,  # No keys; only ARGV
                pattern,
                str(start_ts),
                str(end_ts),
                self.mode
            )

            logger.info(f"Raw Quest data from Lua script (pre-conversion): {raw_data}")
            raw_data = convert_redis_result(raw_data)
            logger.info(f"Converted Quest raw data: {raw_data}")

            formatted_data = {}
            if self.mode == "sum":
                # raw_data is a dict mapping each key to its aggregate count.
                # Order by key (you can further parse the composite key if needed).
                formatted_data = dict(sorted(raw_data.items(), key=lambda item: item[0]))
                logger.info(f"Formatted Quest 'sum' data: {formatted_data}")
            elif self.mode == "grouped":
                formatted_data = {}
                for k, groups in raw_data.items():
                    if isinstance(groups, list):
                        groups = {groups[i]: groups[i+1] for i in range(0, len(groups), 2)}
                    # Order inner dictionary by timestamp (converted to int)
                    ordered_groups = dict(sorted(groups.items(), key=lambda x: int(x[0])))
                    formatted_data[k] = ordered_groups
                # Order outer dictionary by key.
                formatted_data = dict(sorted(formatted_data.items(), key=lambda item: item[0]))
                logger.info(f"Formatted Quest 'grouped' data: {formatted_data}")
            elif self.mode == "surged":
                formatted_data = {}
                for k, inner in raw_data.items():
                    if isinstance(inner, list):
                        hours = {inner[i]: inner[i+1] for i in range(0, len(inner), 2)}
                    else:
                        hours = inner
                    formatted_data[k] = dict(sorted({f"hour {int(h)}": v for h, v in hours.items()}.items(), key=lambda x: int(x[0].split()[1])))
                formatted_data = dict(sorted(formatted_data.items(), key=lambda item: item[0]))
                logger.info(f"Formatted Quest 'surged' data: {formatted_data}")

            # End timing after script execution
            request_end = time.monotonic()
            elapsed_time = request_end - request_start
            logger.info(f"Quest retrieval execution took {elapsed_time:.3f} seconds")

            return {"mode": self.mode, "data": formatted_data}
        except Exception as e:
            logger.error(f"Quest Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}
