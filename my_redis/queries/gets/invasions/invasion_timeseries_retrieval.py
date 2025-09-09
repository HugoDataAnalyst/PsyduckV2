import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Iterable, Union
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
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        display: Union[str, Iterable[str], None] = "all",
        grunt: Union[str, Iterable[str], None] = "all",
        confirmed: str = "all",  # keep single
    ):
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
        self.confirmed = confirmed
        self.script_sha = None

        def _norm(x):
            if x is None or (isinstance(x, str) and x.lower() == "all"):
                return None
            if isinstance(x, str):
                return {x}
            return set(map(str, x))

        self.displays = _norm(display)
        self.grunts   = _norm(grunt)

        logger.info(
            f"▶️ Initialized 🕴️ InvasionTimeSeries area={self.area}, mode={self.mode}, "
            f"displays={self.displays or 'ALL'}, grunts={self.grunts or 'ALL'}, confirmed={self.confirmed}, "
            f"start={self.start}, end={self.end}"
        )

    async def _load_script(self, client):
        if not self.script_sha:
            logger.debug("🔄 Loading 🕴️ Invasion Lua script into Redis...")
            self.script_sha = await client.script_load(INVASION_TIMESERIES_SCRIPT)
            logger.debug(f"🕴️ Invasion Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.debug("🕴️ Invasion Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_patterns(self) -> list[str]:
        """
        Build cartesian patterns.
        Key: ts:invasion:total:{area}:{display}:{grunt}:{confirmed}
        """
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        displays = list(self.displays) if self.displays is not None else ["*"]
        grunts   = list(self.grunts)   if self.grunts   is not None else ["*"]
        confirmed = "*" if str(self.confirmed).lower() == "all" else str(self.confirmed)

        patterns = []
        for d in displays:
            for g in grunts:
                patterns.append(f"ts:invasion:total:{area}:{d}:{g}:{confirmed}")
        logger.debug(f"Built 🕴️ Invasion {len(patterns)} key pattern(s): {patterns[:5]}{'...' if len(patterns)>5 else ''}")
        return patterns

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

        # Accumulators across multiple patterns
        acc_total = 0
        acc_confirmed: Dict[str, int] = {}
        acc_grouped: Dict[str, Dict[str, int]] = {}
        acc_surged: Dict[str, Dict[str, int]] = {}

        try:
            start_ts = int(self.start.timestamp())
            end_ts   = int(self.end.timestamp())
            logger.debug(f"🕴️ Invasion ⏱️ Time range: start_ts={start_ts}, end_ts={end_ts}")

            request_start = time.monotonic()
            sha = await self._load_script(client)

            for pattern in self._build_key_patterns():
                raw = await client.evalsha(
                    sha, 0,
                    pattern,
                    str(start_ts),
                    str(end_ts),
                    self.mode
                )
                data = convert_redis_result(raw)

                if self.mode == "sum":
                    # {"total": int, "confirmed": { "0": int, "1": int } }
                    acc_total += int(data.get("total", 0) or 0)
                    conf = data.get("confirmed", {}) or {}
                    for k, v in conf.items():
                        acc_confirmed[k] = acc_confirmed.get(k, 0) + int(v)
                elif self.mode == "grouped":
                    # { "display:grunt:confirmed": { "ts": count, ... }, ... }
                    for group_key, groups in data.items():
                        # Ensure 'groups' is dict
                        if isinstance(groups, list):
                            groups = {groups[i]: groups[i+1] for i in range(0, len(groups), 2)}
                        bucket = acc_grouped.setdefault(group_key, {})
                        for ts_str, cnt in groups.items():
                            bucket[ts_str] = bucket.get(ts_str, 0) + int(cnt)
                elif self.mode == "surged":
                    # { "display:grunt:confirmed": { "hour": count, ... }, ... }
                    for group_key, inner in data.items():
                        if isinstance(inner, list):
                            inner = {inner[i]: inner[i+1] for i in range(0, len(inner), 2)}
                        bucket = acc_surged.setdefault(group_key, {})
                        for hour, cnt in inner.items():
                            bucket[hour] = bucket.get(hour, 0) + int(cnt)

            # Final formatting/sorting
            if self.mode == "sum":
                formatted = {"total": acc_total, "confirmed": dict(sorted(acc_confirmed.items()))}
            elif self.mode == "grouped":
                formatted = {}
                for group_key, groups in acc_grouped.items():
                    ordered = dict(sorted(groups.items(), key=lambda x: int(x[0])))
                    formatted[group_key] = ordered
                # sort outer by numeric tuple (display, grunt, confirmed)
                formatted = dict(
                    sorted(
                        formatted.items(),
                        key=lambda item: tuple(int(x) if x.isdigit() else x for x in item[0].split(":"))
                    )
                )
            elif self.mode == "surged":
                formatted = {}
                for group_key, hours in acc_surged.items():
                    labeled = {f"hour {int(h)}": v for h, v in hours.items()}
                    formatted[group_key] = dict(sorted(labeled.items(), key=lambda kv: int(kv[0].split()[1])))
                formatted = dict(
                    sorted(
                        formatted.items(),
                        key=lambda item: tuple(int(x) if x.isdigit() else x for x in item[0].split(":"))
                    )
                )
            else:
                formatted = {}

            elapsed = time.monotonic() - request_start
            logger.info(f"🕴️ Invasion retrieval execution took ⏱️ {elapsed:.3f} seconds")
            return {"mode": self.mode, "data": formatted}
        except Exception as e:
            logger.error(f"❌ 🕴️ Invasion Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}
