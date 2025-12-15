import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Iterable, Union
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from server_fastapi import global_state
from webhook.filter_data import WebhookFilter

redis_manager = RedisManager()

# Chunked Lua script for Raid timeseries
TIMESERIES_RAID_CHUNK_SCRIPT = """
local start_ts = tonumber(ARGV[1])
local end_ts = tonumber(ARGV[2])
local mode = ARGV[3]

local sum_results = {}
local grouped_results = {}
local surged_results = {}

-- Process only the keys passed as KEYS argument
for _, key in ipairs(KEYS) do
    local hash_data = redis.call('HGETALL', key)

    for i = 1, #hash_data, 2 do
        local ts = tonumber(hash_data[i])
        local count = tonumber(hash_data[i+1])
        if ts and count and ts >= start_ts and ts < end_ts then
            if mode == 'sum' or mode == 'grouped' then
                -- For sum and grouped, use the full key
                sum_results[key] = (sum_results[key] or 0) + count
                grouped_results[key] = (grouped_results[key] or 0) + count
            elseif mode == 'surged' then
                -- For surged mode, compute the hour from the UTC timestamp
                local hour = math.floor((ts % 86400) / 3600)
                local new_key = key .. ':' .. tostring(hour)
                surged_results[new_key] = (surged_results[new_key] or 0) + count
            end
        end
    end
end

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
        chunk_size: int = 500,
        chunk_sleep: float = 0.15,
    ):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.chunk_size = chunk_size
        self.chunk_sleep = chunk_sleep
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
            f"start={self.start}, end={self.end}, "
            f"chunk_size={self.chunk_size}, chunk_sleep={self.chunk_sleep}s"
        )

    async def _load_script(self, client):
        """Load Lua script into Redis if not already cached"""
        if not self.script_sha:
            logger.debug("Loading chunked Lua script for Raid into Redis...")
            self.script_sha = await client.script_load(TIMESERIES_RAID_CHUNK_SCRIPT)
            logger.debug(f"Lua script 👹 loaded with SHA: {self.script_sha}")
        return self.script_sha

    async def _scan_keys_by_patterns(self, client) -> list[str]:
        """SCAN for all matching keys (non-blocking, fast)"""
        scan_start = time.monotonic()
        all_keys = []

        for pattern in self._build_key_patterns():
            cursor = 0
            while True:
                cursor, keys = await client.scan(cursor, match=pattern, count=1000)
                all_keys.extend(k.decode() if isinstance(k, bytes) else k for k in keys)
                if cursor == 0:
                    break

        scan_elapsed = time.monotonic() - scan_start
        logger.info(f"👹 SCAN collected {len(all_keys)} keys in {scan_elapsed:.3f}s")
        return all_keys

    def _convert_redis_result(self, res):
        """Convert Redis Lua table (list) into Python dict"""
        if isinstance(res, list):
            if len(res) % 2 == 0:
                return {res[i]: self._convert_redis_result(res[i + 1]) for i in range(0, len(res), 2)}
            else:
                return [self._convert_redis_result(item) for item in res]
        return res

    def _merge_results(self, acc_sum, acc_grouped, acc_surged, chunk_data):
        """Merge chunk results into accumulators"""
        if self.mode in ["sum", "grouped"]:
            for key, cnt in chunk_data.items():
                acc_sum[key] = acc_sum.get(key, 0) + int(cnt)
                acc_grouped[key] = acc_grouped.get(key, 0) + int(cnt)
        elif self.mode == "surged":
            for key, cnt in chunk_data.items():
                acc_surged[key] = acc_surged.get(key, 0) + int(cnt)

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
        """Retrieve timeseries using chunked Lua scripts with yield points"""
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
            return {"mode": self.mode, "data": {}}

        total_start = time.monotonic()

        try:
            # Step 1: SCAN for all keys
            all_keys = await self._scan_keys_by_patterns(client)

            if not all_keys:
                logger.info("👹 No keys found matching patterns")
                return {"mode": self.mode, "data": {}}

            # Step 2: Load Lua script
            sha = await self._load_script(client)

            # Step 3: Split keys into chunks
            chunks = [all_keys[i:i+self.chunk_size] for i in range(0, len(all_keys), self.chunk_size)]
            logger.info(f"👹 Processing {len(all_keys)} keys in {len(chunks)} chunks of ~{self.chunk_size} keys")

            # Accumulators
            acc_sum: Dict[str, int] = {}
            acc_grouped: Dict[str, int] = {}
            acc_surged: Dict[str, int] = {}

            start_ts = int(self.start.timestamp())
            end_ts   = int(self.end.timestamp())

            # Step 4: Process chunks with sleep intervals
            chunk_start = time.monotonic()
            for i, chunk in enumerate(chunks):
                chunk_iter_start = time.monotonic()

                # Run Lua script on this chunk
                raw = await client.evalsha(
                    sha,
                    len(chunk),
                    *chunk,
                    str(start_ts),
                    str(end_ts),
                    self.mode
                )

                chunk_data = self._convert_redis_result(raw)
                self._merge_results(acc_sum, acc_grouped, acc_surged, chunk_data)

                chunk_iter_elapsed = time.monotonic() - chunk_iter_start
                logger.info(f"👹 Chunk {i+1}/{len(chunks)} processed {len(chunk)} keys in {chunk_iter_elapsed:.3f}s")

                # Sleep between chunks to allow writes
                if i < len(chunks) - 1:
                    await asyncio.sleep(self.chunk_sleep)

            chunk_elapsed = time.monotonic() - chunk_start
            logger.info(f"👹 Chunked Lua processing took {chunk_elapsed:.3f}s ({len(chunks)} chunks × ~{self.chunk_sleep}s sleep)")

            # Step 5: Final formatting
            format_start = time.monotonic()
            if self.mode == "sum":
                formatted = self._transform_timeseries_sum(acc_sum)
            elif self.mode == "grouped":
                formatted = self.transform_raid_totals_grouped(acc_grouped)
            elif self.mode == "surged":
                formatted = self.transform_raids_surged_totals_hourly_by_hour(acc_surged)
            else:
                formatted = {}

            format_elapsed = time.monotonic() - format_start
            total_elapsed = time.monotonic() - total_start

            logger.info(f"👹 Final formatting took {format_elapsed:.3f}s")
            logger.info(f"👹 Total retrieval time: {total_elapsed:.3f}s")

            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"❌ 👹 Raid retrieval failed: {e}")
            return {"mode": self.mode, "data": {}}
