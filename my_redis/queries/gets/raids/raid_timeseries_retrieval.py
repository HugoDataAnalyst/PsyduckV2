import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Iterable, Union
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from server_fastapi import global_state
from webhook.filter_data import WebhookFilter

redis_manager = RedisManager()

# Lua script without any offset logic.
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
    for i = 1, #hash_data, 2 do
      local ts = tonumber(hash_data[i])
      local count = tonumber(hash_data[i+1])
      if ts and count and ts >= start_ts and ts < end_ts then
         -- For sum and grouped modes, aggregate counts using the full key.
         sum_results[key] = (sum_results[key] or 0) + count
         grouped_results[key] = (grouped_results[key] or 0) + count
         -- For surged mode, compute the hour from the UTC timestamp
         if mode == 'surged' then
            local hour = math.floor((ts % 86400) / 3600)
            local new_key = key .. ":" .. tostring(hour)
            surged_results[new_key] = (surged_results[new_key] or 0) + count
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
  for k, v in pairs(grouped_results) do
    table.insert(arr, k)
    table.insert(arr, v)
  end
  return arr
elseif mode == 'surged' then
  local arr = {}
  for k, v in pairs(surged_results) do
    table.insert(arr, k)
    table.insert(arr, v)
  end
  return arr
else
  return {}
end
"""

class RaidTimeSeries:
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        raid_pokemon: Union[str, Iterable[str], None] = "all",
        raid_form: Union[str, Iterable[str], None] = "all",
        raid_level: Union[str, Iterable[str], None] = "all",
        raid_type: Union[str, Iterable[str], None] = "all",
    ):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.script_sha = None

        def _norm(x):
            if x is None or (isinstance(x, str) and x.lower() == "all"):
                return None
            if isinstance(x, str):
                return {x}
            return set(map(str, x))

        # store as sets (or None)
        self.raid_types    = _norm(raid_type)
        self.raid_pokemons = _norm(raid_pokemon)
        self.raid_forms    = _norm(raid_form)
        self.raid_levels   = _norm(raid_level)

        logger.info(
            f"▶️ Initialized 👹 RaidTimeSeries area={self.area}, mode={self.mode}, "
            f"raid_types={self.raid_types or 'ALL'}, raid_pokemons={self.raid_pokemons or 'ALL'}, "
            f"raid_levels={self.raid_levels or 'ALL'}, raid_forms={self.raid_forms or 'ALL'}, "
            f"start={self.start}, end={self.end}"
        )

    async def _load_script(self, client):
        if not self.script_sha:
            logger.debug("🔄 Loading 👹 Raid Lua script into Redis...")
            self.script_sha = await client.script_load(RAID_TIMESERIES_SCRIPT)
            logger.debug(f"👹 Raid Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.debug("👹 Raid Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_patterns(self) -> list[str]:
        """
        Key format:
          ts:raids_total:{raid_type}:{area}:{raid_pokemon}:{raid_level}:{raid_form}
        We expand the cartesian product of selected filters; wildcard any slot with None.
        """
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        types  = list(self.raid_types)    if self.raid_types    is not None else ["*"]
        pokes  = list(self.raid_pokemons) if self.raid_pokemons is not None else ["*"]
        levels = list(self.raid_levels)   if self.raid_levels   is not None else ["*"]
        forms  = list(self.raid_forms)    if self.raid_forms    is not None else ["*"]

        patterns = []
        for rt in types:
            for rp in pokes:
                for rl in levels:
                    for rf in forms:
                        patterns.append(f"ts:raids_total:{rt}:{area}:{rp}:{rl}:{rf}")
        logger.debug(f"Built 👹 Raid {len(patterns)} key pattern(s): {patterns[:5]}{'...' if len(patterns)>5 else ''}")
        return patterns

    @staticmethod
    def _transform_timeseries_sum(raw_data: dict) -> dict:
        """
        Transforms raw 'sum' data into a breakdown.
        Expected key format: ts:raids_total:{raid_type}:{area}:{raid_pokemon}:{raid_level}:{raid_form}
        """
        total = 0
        raid_level_totals = {}
        for key, value in raw_data.items():
            parts = key.split(":")
            if len(parts) != 7:
                continue
            raid_level = parts[5]
            try:
                val = int(value)
            except Exception as e:
                logger.error(f"Could not convert value {value} for key {key}: {e}")
                val = 0
            total += val
            raid_level_totals[raid_level] = raid_level_totals.get(raid_level, 0) + val
        return {
            "total": total,
            "raid_level": dict(sorted(raid_level_totals.items(), key=lambda x: int(x[0]) if x[0].isdigit() else x[0]))
        }

    @staticmethod
    def transform_raid_totals_grouped(raw_data: dict) -> dict:
        """
        Grouped breakdown from key->count.
        """
        breakdown = {
            "raid_pokemon+raid_form": {},
            "raid_level": {},
            "total": 0
        }
        for key, value in raw_data.items():
            parts = key.split(":")
            if len(parts) != 7:
                continue
            raid_pokemon = parts[4]
            raid_level = parts[5]
            raid_form = parts[6]
            try:
                val = int(value)
            except Exception as e:
                logger.error(f"Could not convert value {value} for key {key}: {e}")
                val = 0
            breakdown["total"] += val
            pf_key = f"{raid_pokemon}:{raid_form}"
            breakdown["raid_pokemon+raid_form"][pf_key] = breakdown["raid_pokemon+raid_form"].get(pf_key, 0) + val
            breakdown["raid_level"][raid_level] = breakdown["raid_level"].get(raid_level, 0) + val
        breakdown["raid_pokemon+raid_form"] = dict(sorted(breakdown["raid_pokemon+raid_form"].items()))
        breakdown["raid_level"] = dict(sorted(breakdown["raid_level"].items(), key=lambda x: int(x[0]) if x[0].isdigit() else x[0]))
        return breakdown

    @classmethod
    def transform_raids_surged_totals_hourly_by_hour(cls, raw_aggregated: dict) -> dict:
        """
        Input keys have ':<hour>' appended (0..23). Group by hour and reuse grouped transform.
        """
        surged = {}
        for full_key, count in raw_aggregated.items():
            parts = full_key.split(":")
            if len(parts) < 2:
                continue
            hour_str = parts[-1]
            if not hour_str.isdigit():
                continue
            hour_int = int(hour_str)
            if not (0 <= hour_int <= 23):
                continue
            hour_key = f"hour {hour_int}"
            base_key = ":".join(parts[:-1])
            if hour_key not in surged:
                surged[hour_key] = {}
            surged[hour_key][base_key] = surged[hour_key].get(base_key, 0) + int(count)

        result = {}
        for hour, group in surged.items():
            transformed = cls.transform_raid_totals_grouped(group)
            result[hour] = transformed
        return dict(sorted(result.items(), key=lambda item: int(item[0].split()[1])))

    async def raid_retrieve_timeseries(self) -> Dict[str, Any]:
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

        # accumulators across multiple patterns
        acc_sum: Dict[str, int] = {}
        acc_grouped: Dict[str, int] = {}
        acc_surged: Dict[str, int] = {}

        try:
            start_ts = int(self.start.timestamp())
            end_ts   = int(self.end.timestamp())
            logger.debug(f"👹 Raid ⏱️ Time range: start_ts={start_ts}, end_ts={end_ts}")

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
                    for k, v in data.items():
                        acc_sum[k] = acc_sum.get(k, 0) + int(v)
                elif self.mode == "grouped":
                    for k, v in data.items():
                        acc_grouped[k] = acc_grouped.get(k, 0) + int(v)
                elif self.mode == "surged":
                    for k, v in data.items():  # k includes ":<hour>"
                        acc_surged[k] = acc_surged.get(k, 0) + int(v)

            # Final formatting
            if self.mode == "sum":
                formatted = self._transform_timeseries_sum(acc_sum)
            elif self.mode == "grouped":
                formatted = self.transform_raid_totals_grouped(acc_grouped)
            elif self.mode == "surged":
                formatted = self.transform_raids_surged_totals_hourly_by_hour(acc_surged)
            else:
                formatted = {}

            elapsed = time.monotonic() - request_start
            logger.info(f"👹 Raid retrieval execution took ⏱️ {elapsed:.3f} seconds")
            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"❌ 👹 Raid Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}
