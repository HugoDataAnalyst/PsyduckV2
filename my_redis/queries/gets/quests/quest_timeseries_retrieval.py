import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Iterable, Union
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
      if ts and count and ts >= start_ts and ts < end_ts then
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
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        quest_mode: str = "all",
        field_details: Union[str, Iterable[str], None] = "all",
    ):
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
        self.script_sha = None

        def _norm(x):
            if x is None or (isinstance(x, str) and x.lower() == "all"):
                return None
            if isinstance(x, str):
                return {x}
            return set(map(str, x))

        # multi-select quest types (we only constrain f1 in the 6-field segment)
        self.quest_types = _norm(field_details)

        logger.info(
            f"▶️ Initialized 🔎 QuestTimeSeries area={self.area}, mode={self.mode}, "
            f"quest_mode={self.quest_mode}, quest_types={self.quest_types or 'ALL'}, "
            f"range={self.start}..{self.end}"
        )
    async def _load_script(self, client):
        if not self.script_sha:
            logger.debug("🔄 Loading 🔎 Quest Lua script into Redis...")
            self.script_sha = await client.script_load(QUEST_TIMESERIES_SCRIPT)
            logger.debug(f"🔎 Quest Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.debug("🔎 Quest Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_patterns(self) -> list[str]:
        # Replace "all"/"global" with wildcard for area; wildcard quest_mode if "all"
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        quest_mode = "*" if self.quest_mode == "all" else self.quest_mode

        # field_details pattern: if quest_types is None -> "*:*:*:*:*:*"
        # else -> for each quest_type -> "{quest_type}:*:*:*:*:*"
        if self.quest_types is None:
            fd_patterns = ["*:*:*:*:*:*"]
        else:
            fd_patterns = [f"{qt}:*:*:*:*:*" for qt in self.quest_types]

        patterns = [f"ts:quests_total:{quest_mode}:{area}:{fd}" for fd in fd_patterns]
        logger.debug(f"Built 🔎 Quest {len(patterns)} key pattern(s): {patterns[:5]}{'...' if len(patterns)>5 else ''}")
        return patterns

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

        # accumulators to merge across multiple patterns
        acc_sum: Dict[str, int] = {}
        acc_grouped: Dict[str, Dict[str, int]] = {}
        acc_surged: Dict[str, Dict[str, int]] = {}

        try:
            start_ts = int(self.start.timestamp())
            end_ts   = int(self.end.timestamp())
            logger.debug(f"🔎 Quest ⏱️ Time range: start_ts={start_ts}, end_ts={end_ts}")

            request_start = time.monotonic()
            sha = await self._load_script(client)

            for pattern in self._build_key_patterns():
                raw = await client.evalsha(
                    sha,
                    0,
                    pattern,
                    str(start_ts),
                    str(end_ts),
                    self.mode
                )
                data = convert_redis_result(raw)

                if self.mode == "sum":
                    # data: { full_key: count, ... }
                    for k, v in data.items():
                        acc_sum[k] = acc_sum.get(k, 0) + int(v)
                elif self.mode == "grouped":
                    # data: { full_key: { ts_bucket: count, ... }, ... }
                    for k, groups in data.items():
                        bucket = acc_grouped.setdefault(k, {})
                        # groups might be list -> convert to dict
                        if isinstance(groups, list):
                            groups = {groups[i]: groups[i+1] for i in range(0, len(groups), 2)}
                        for tb, cnt in groups.items():
                            bucket[tb] = bucket.get(tb, 0) + int(cnt)
                elif self.mode == "surged":
                    # data: { full_key: { hour: count, ... }, ... }
                    for k, hours in data.items():
                        bucket = acc_surged.setdefault(k, {})
                        if isinstance(hours, list):
                            hours = {hours[i]: hours[i+1] for i in range(0, len(hours), 2)}
                        for h, cnt in hours.items():
                            bucket[h] = bucket.get(h, 0) + int(cnt)

            # Final formatting
            if self.mode == "sum":
                if self.area.lower() in ["all", "global"]:
                    area_totals: Dict[str, int] = {}
                    quest_grand_total = 0
                    for key, v in acc_sum.items():
                        parts = key.split(":")
                        key_area = parts[3] if len(parts) > 3 else "unknown"
                        val = int(v)
                        area_totals[key_area] = area_totals.get(key_area, 0) + val
                        quest_grand_total += val
                    pokestops_data = global_state.cached_pokestops or {"areas": {}, "pokestop_grand_total": 0}
                    formatted = {
                        "areas": area_totals,
                        "quest_grand_total": quest_grand_total,
                        "total pokestops": pokestops_data,
                    }
                else:
                    total_sum = sum(int(v) for v in acc_sum.values())
                    formatted = {"total": total_sum}

            elif self.mode == "grouped":
                formatted = {}
                for k, groups in acc_grouped.items():
                    ordered = dict(sorted(groups.items(), key=lambda x: int(x[0])))
                    formatted[k] = ordered
                formatted = dict(sorted(formatted.items(), key=lambda item: item[0]))

            elif self.mode == "surged":
                formatted = {}
                for k, hours in acc_surged.items():
                    labeled = {f"hour {int(h)}": v for h, v in hours.items()}
                    formatted[k] = dict(sorted(labeled.items(), key=lambda x: int(x[0].split()[1])))
                formatted = dict(sorted(formatted.items(), key=lambda item: item[0]))

            else:
                formatted = {}

            elapsed = time.monotonic() - request_start
            logger.info(f"🔎 Quest retrieval execution took ⏱️ {elapsed:.3f} seconds")

            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"❌ 🔎 Quest Lua script execution failed: {e}", exc_info=True)
            return {"mode": self.mode, "data": {}}
