import asyncio
import time
from datetime import datetime
from typing import Dict, Any
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from server_fastapi import global_state

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
                 quest_mode: str = "normal", field_details: str = "all"):
        """
        Parameters:
          - area: Area name filter. Use "all" or "global" to match every area.
          - start, end: Datetime objects for the time range.
          - mode: Aggregation mode: "sum", "grouped", or "surged".
          - quest_mode: Quest mode—either "ar" or "normal". If set to "all", a wildcard is used.
          - field_details: The quest type to filter for (i.e. the first field in field_details).
                         If "all", no filtering on quest type is done.

        The keys are stored in the new format:
            ts:quests_total:{quest_mode}:{area}:{field_details}
        For retrieval, if field_details is not "all", we build a pattern as:
            {field_details}:*:*:*:*:*
        so that only keys with that quest type (first field) are returned.
        """
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.quest_mode = quest_mode.lower()
        # Here, self.field_details represents the quest_type filter.
        self.field_details = field_details
        self.script_sha = None

        logger.info(
            f"▶️ Initialized 🔎 QuestTimeSeries with area: {self.area}, mode: {self.mode}, "
            f"🔎 Quest Mode: {self.quest_mode}, field_details: {self.field_details}, "
            f"▶️ Time Range: {self.start} ⏸️ to {self.end}"
        )

    async def _load_script(self, client):
        if not self.script_sha:
            logger.debug("🔄 Loading 🔎 Quest Lua script into Redis...")
            self.script_sha = await client.script_load(QUEST_TIMESERIES_SCRIPT)
            logger.debug(f"🔎 Quest Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.debug("🔎 Quest Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_pattern(self) -> str:
        # Replace any filter equal to "all" or "global" with wildcard "*"
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        quest_mode = "*" if self.quest_mode.lower() == "all" else self.quest_mode

        # For field_details, which in this case is used for filtering the quest type (the first field),
        # if it is "all" we match any field_details in the 6-field string.
        # Otherwise, we require the key to start with the given quest type, followed by five colon-separated wildcards.
        if self.field_details.lower() == "all":
            field_details_pattern = "*:*:*:*:*:*"
        else:
            field_details_pattern = f"{self.field_details}:*:*:*:*:*"

        # New key format: ts:quests_total:{quest_mode}:{area}:{field_details_pattern}
        pattern = f"ts:quests_total:{quest_mode}:{area}:{field_details_pattern}"
        logger.debug(f"Built 🔎 Quest key pattern: {pattern}")
        return pattern

    async def quest_retrieve_timeseries(self) -> Dict[str, Any]:
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
            logger.debug(f"🔎 Quest ⏱️ Time range for query: start_ts={start_ts}, end_ts={end_ts}")

            request_start = time.monotonic()
            script_sha = await self._load_script(client)
            logger.debug("Executing 🔎 Quest Lua script with evalsha...")
            raw_data = await client.evalsha(
                script_sha,
                0,
                pattern,
                str(start_ts),
                str(end_ts),
                self.mode
            )

            logger.debug(f"Raw 🔎 Quest data from Lua script (pre-conversion): {raw_data}")
            raw_data = convert_redis_result(raw_data)
            logger.debug(f"Converted 🔎 Quest raw data: {raw_data}")

            formatted_data = {}
            if self.mode == "sum":
                # If the area filter is "all" or "global", group quest totals per area
                if self.area.lower() in ["all", "global"]:
                    area_totals = {}
                    quest_grand_total = 0
                    for key, v in raw_data.items():
                        try:
                            # Our key format: ts:quests_total:{quest_mode}:{area}:{field_details}
                            parts = key.split(":")
                            # The area is at index 3
                            key_area = parts[3] if len(parts) > 3 else "unknown"
                        except Exception:
                            key_area = "unknown"
                        count = int(v)
                        area_totals[key_area] = area_totals.get(key_area, 0) + count
                        quest_grand_total += count
                    # Retrieve cached pokestops from global_state (which is periodically updated)
                    pokestops_data = global_state.cached_pokestops or {"areas": {}, "pokestop_grand_total": 0}
                    formatted_data = {
                        "areas": area_totals,
                        "quest_grand_total": quest_grand_total,
                        "total pokestops": pokestops_data
                    }
                    logger.debug(f"Formatted 🔎 Quest 'sum' data (by area): {formatted_data}")
                else:
                    # Otherwise, compute a single total sum.
                    total_sum = sum(int(v) for v in raw_data.values())
                    formatted_data = total_sum
                    logger.debug(f"Formatted 🔎 Quest 'sum' data (total): {formatted_data}")
            elif self.mode == "grouped":
                # Detailed breakdown per key remains unchanged.
                formatted_data = {}
                for k, groups in raw_data.items():
                    if isinstance(groups, list):
                        groups = {groups[i]: groups[i+1] for i in range(0, len(groups), 2)}
                    ordered_groups = dict(sorted(groups.items(), key=lambda x: int(x[0])))
                    formatted_data[k] = ordered_groups
                formatted_data = dict(sorted(formatted_data.items(), key=lambda item: item[0]))
                logger.debug(f"Formatted 🔎 Quest 'grouped' data: {formatted_data}")
            elif self.mode == "surged":
                formatted_data = {}
                for k, inner in raw_data.items():
                    if isinstance(inner, list):
                        hours = {inner[i]: inner[i+1] for i in range(0, len(inner), 2)}
                    else:
                        hours = inner
                    formatted_data[k] = dict(sorted({f"hour {int(h)}": v for h, v in hours.items()}.items(), key=lambda x: int(x[0].split()[1])))
                formatted_data = dict(sorted(formatted_data.items(), key=lambda item: item[0]))
                logger.debug(f"Formatted 🔎 Quest 'surged' data: {formatted_data}")

            request_end = time.monotonic()
            elapsed_time = request_end - request_start
            logger.info(f"🔎 Quest retrieval execution took ⏱️ {elapsed_time:.3f} seconds")

            return {"mode": self.mode, "data": formatted_data}
        except Exception as e:
            logger.error(f"❌ 🔎 Quest Lua script execution failed: {e}", exc_info=True)
            return {"mode": self.mode, "data": {}}
