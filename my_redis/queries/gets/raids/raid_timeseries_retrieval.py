import asyncio
import time
from datetime import datetime
from typing import Dict, Any
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from server_fastapi import global_state
from webhook.filter_data import WebhookFilter  # Ensure this function is available

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
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 raid_type: str = "all", raid_pokemon: str = "all",
                 raid_level: str = "all", raid_form: str = "all"):
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
            f"▶️ Initialized 👹 RaidTimeSeries with area: {self.area}, Mode: {self.mode}, "
            f"👹 Raid type: {self.raid_type}, Raid pokémon: {self.raid_pokemon}, "
            f"👹 Raid level: {self.raid_level}, Raid form: {self.raid_form}, "
            f"▶️ Start: {self.start}, ⏸️ End: {self.end}"
        )

    async def _load_script(self, client):
        if not self.script_sha:
            logger.debug("🔄 Loading 👹 Raid Lua script into Redis...")
            self.script_sha = await client.script_load(RAID_TIMESERIES_SCRIPT)
            logger.debug(f"👹 Raid Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.debug("👹 Raid Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_pattern(self) -> str:
        # Replace any filter set to "all" (or "global" for area) with a wildcard "*"
        raid_type = "*" if self.raid_type.lower() == "all" else self.raid_type
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        raid_pokemon = "*" if self.raid_pokemon.lower() == "all" else self.raid_pokemon
        raid_level = "*" if self.raid_level.lower() == "all" else self.raid_level
        raid_form = "*" if self.raid_form.lower() == "all" else self.raid_form
        pattern = f"ts:raids_total:{raid_type}:{area}:{raid_pokemon}:{raid_level}:{raid_form}"
        logger.debug(f"Built 👹 Raid 🔑 key pattern: {pattern}")
        return pattern

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
        return {"total": total, "raid_level": raid_level_totals}

    @staticmethod
    def transform_raid_totals_grouped(raw_data: dict) -> dict:
        """
        Transforms raw aggregated raid totals (from grouped mode) into a detailed breakdown.
        Expected key format: ts:raids_total:{raid_type}:{area}:{raid_pokemon}:{raid_level}:{raid_form}
        The breakdown includes:
         - "raid_pokemon+raid_form": aggregated sum for each unique combination.
         - "raid_level": aggregated sum per raid_level.
         - "total": overall total.
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
        Transforms raw surged data (keys with a trailing ":<hour>") into a dictionary keyed by hour.
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
            # Remove the appended hour to get the base key.
            base_key = ":".join(parts[:-1])
            if hour_key not in surged:
                surged[hour_key] = {}
            surged[hour_key][base_key] = surged[hour_key].get(base_key, 0) + int(count)

        result = {}
        for hour, group in surged.items():
            transformed = RaidTimeSeries.transform_raid_totals_grouped(group)
            result[hour] = transformed
        sorted_result = dict(sorted(result.items(), key=lambda item: int(item[0].split()[1])))
        return sorted_result

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

        try:
            pattern = self._build_key_pattern()
            start_ts = int(self.start.timestamp())
            end_ts = int(self.end.timestamp())
            logger.debug(f"👹 Raid ⏱️ Time range for query: start_ts={start_ts}, end_ts={end_ts}")

            request_start = time.monotonic()

            # Note: No offset is computed or passed.
            script_sha = await self._load_script(client)
            logger.debug("Executing 👹 Raid Lua script with evalsha...")
            raw_data = await client.evalsha(
                script_sha,
                0,  # No keys; only ARGV
                pattern,
                str(start_ts),
                str(end_ts),
                self.mode
            )
            logger.debug(f"Raw 👹 Raid data from Lua script (pre-conversion): {raw_data}")
            raw_data = convert_redis_result(raw_data)
            logger.debug(f"Converted 👹 Raid raw data: {raw_data}")

            formatted_data = {}
            if self.mode == "sum":
                formatted_data = self._transform_timeseries_sum(raw_data)
                logger.debug(f"Formatted 👹 Raid 'sum' data: {formatted_data}")
            elif self.mode == "grouped":
                formatted_data = self.transform_raid_totals_grouped(raw_data)
                logger.debug(f"Formatted 👹 Raid 'grouped' data: {formatted_data}")
            elif self.mode == "surged":
                formatted_data = self.transform_raids_surged_totals_hourly_by_hour(raw_data)
                logger.debug(f"Formatted 👹 Raid 'surged' data: {formatted_data}")
            else:
                formatted_data = raw_data
                logger.debug(f"Formatted 👹 Raid data (raw mode): {formatted_data}")

            # End timing after script execution
            request_end = time.monotonic()
            elapsed_time = request_end - request_start
            logger.info(f"👹 Raid retrieval execution took ⏱️ {elapsed_time:.3f} seconds")

            return {"mode": self.mode, "data": formatted_data}
        except Exception as e:
            logger.error(f"❌ 👹 Raid Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}
