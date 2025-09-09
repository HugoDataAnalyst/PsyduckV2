import asyncio
import time
from datetime import datetime
from typing import Dict, Union, Iterable
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

# Lua script as defined above.
TTH_TIMESERIES_SCRIPT = """
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
        -- Extract key parts: ts, tth_pokemon, area, tth_bucket
        local key_parts = {}
        for part in string.gmatch(key, '([^:]+)') do
            table.insert(key_parts, part)
        end
        for i = 1, #hash_data, 2 do
            local ts = tonumber(hash_data[i])
            local count = tonumber(hash_data[i+1])
            if ts and count and ts >= start_ts and ts < end_ts then
                local bucket = key_parts[4] or "unknown"
                -- Sum mode aggregation
                sum_results[bucket] = (sum_results[bucket] or 0) + count
                -- Grouped mode aggregation: group by time bucket (minute-rounded timestamp)
                if not grouped_results[bucket] then
                    grouped_results[bucket] = {}
                end
                local time_bucket = tostring(math.floor(ts / 60) * 60)
                grouped_results[bucket][time_bucket] = (grouped_results[bucket][time_bucket] or 0) + count
                -- Surged mode aggregation: group by hour of day.
                if not surged_results[bucket] then
                    surged_results[bucket] = {}
                end
                local hour = tostring(math.floor((ts % 86400) / 3600))
                surged_results[bucket][hour] = (surged_results[bucket][hour] or 0) + count
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
    for bucket, groups in pairs(grouped_results) do
        local inner = {}
        for time_bucket, count in pairs(groups) do
            table.insert(inner, time_bucket)
            table.insert(inner, count)
        end
        table.insert(arr, bucket)
        table.insert(arr, inner)
    end
    return arr
elseif mode == 'surged' then
    local arr = {}
    for bucket, hours in pairs(surged_results) do
        local inner = {}
        for hour, count in pairs(hours) do
            table.insert(inner, hour)
            table.insert(inner, count)
        end
        table.insert(arr, bucket)
        table.insert(arr, inner)
    end
    return arr
else
    return {}
end
"""

class PokemonTTHTimeSeries:
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        tth_bucket: Union[str, Iterable[str], None] = "all",
        mode: str = "sum",
    ):
        """
        Parameters:
          - area: The area filter. Use "all" or "global" to match every area.
          - tth_bucket: The despawn timer bucket filter (e.g., "10_15"). Use "all" to match any bucket.
          - mode: Aggregation mode ("sum", "grouped", or "surged").
        """
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

        self.tth_buckets = _norm(tth_bucket)

        logger.info(
            f"👻⏱️ Initialized PokemonTTHTimeSeries with area: {self.area}, "
            f"tth_buckets: {self.tth_buckets or 'ALL'}, ▶️ Start: {self.start}, "
            f"⏸️ End: {self.end}, Mode: {self.mode}"
        )

    async def _load_script(self, client):
        if not self.script_sha:
            logger.debug("🔄 Loading 👻⏱️ TTH Lua script into Redis...")
            self.script_sha = await client.script_load(TTH_TIMESERIES_SCRIPT)
            logger.debug(f"👻⏱️ TTH Lua script loaded with SHA: {self.script_sha}")
        else:
            logger.debug("👻⏱️ TTH Lua script already loaded, reusing cached SHA.")
        return self.script_sha

    def _build_key_patterns(self) -> list[str]:
        # "all/global" -> wildcard; expand buckets into multiple patterns
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        buckets = list(self.tth_buckets) if self.tth_buckets is not None else ["*"]
        patterns = [f"ts:tth_pokemon:{area}:{b}" for b in buckets]
        logger.debug(
            f"Built 👻⏱️ TTH {len(patterns)} key pattern(s): "
            f"{patterns[:5]}{'...' if len(patterns)>5 else ''}"
        )
        return patterns

    async def retrieve_timeseries(self) -> Dict:
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
            return {"mode": self.mode, "data": {}}

        def convert_redis_result(res):
            if isinstance(res, list):
                if len(res) % 2 == 0:
                    return {res[i]: convert_redis_result(res[i + 1]) for i in range(0, len(res), 2)}
                else:
                    return [convert_redis_result(item) for item in res]
            return res

        # helpers for sorting
        def _bucket_key(bucket: str) -> int:
            try:
                return int(bucket.split("_")[0])
            except Exception:
                return 10**9  # push unknowns to end

        start_ts = int(self.start.timestamp())
        end_ts   = int(self.end.timestamp())

        # accumulators
        acc_sum: Dict[str, int] = {}
        acc_grouped: Dict[str, Dict[str, int]] = {}  # bucket -> { time_bucket_ts: count }
        acc_surged: Dict[str, Dict[str, int]] = {}   # bucket -> { hour_str: count }

        try:
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
                    # data: { bucket: count }
                    for b, v in data.items():
                        acc_sum[b] = acc_sum.get(b, 0) + int(v)
                elif self.mode == "grouped":
                    # data: { bucket: { time_bucket_ts: count } }
                    for b, groups in data.items():
                        if isinstance(groups, list):
                            groups = {groups[i]: groups[i+1] for i in range(0, len(groups), 2)}
                        bucket = acc_grouped.setdefault(b, {})
                        for t, v in groups.items():
                            bucket[t] = bucket.get(t, 0) + int(v)
                elif self.mode == "surged":
                    # data: { bucket: { hour_str: count } }
                    for b, hours in data.items():
                        if isinstance(hours, list):
                            hours = {hours[i]: hours[i+1] for i in range(0, len(hours), 2)}
                        bucket = acc_surged.setdefault(b, {})
                        for h, v in hours.items():
                            bucket[h] = bucket.get(h, 0) + int(v)

            # Final formatting
            if self.mode == "sum":
                formatted = dict(sorted(acc_sum.items(), key=lambda kv: _bucket_key(kv[0])))

            elif self.mode == "grouped":
                formatted: Dict[str, Dict[str, int]] = {}
                for b, groups in acc_grouped.items():
                    # sort inner by timestamp numeric
                    formatted[b] = dict(sorted(groups.items(), key=lambda kv: int(kv[0])))
                # sort outer by bucket lower bound
                formatted = dict(sorted(formatted.items(), key=lambda kv: _bucket_key(kv[0])))

            elif self.mode == "surged":
                formatted: Dict[str, Dict[str, int]] = {}
                for b, hours in acc_surged.items():
                    labeled = {f"hour {int(h)}": v for h, v in hours.items()}
                    formatted[b] = dict(sorted(labeled.items(), key=lambda kv: int(kv[0].split()[1])))
                formatted = dict(sorted(formatted.items(), key=lambda kv: _bucket_key(kv[0])))

            else:
                formatted = {}

            elapsed = time.monotonic() - request_start
            logger.info(f"👻⏱️ Pokémon TTH retrieval execution took {elapsed:.3f} seconds")
            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"TTH Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}
