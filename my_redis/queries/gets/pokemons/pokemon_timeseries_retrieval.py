import asyncio
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Union, Iterable
from my_redis.connect_redis import RedisManager
from utils.logger import logger
try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    relativedelta = None

redis_manager = RedisManager()

# Optimized Lua script for timeseries retrieval
TIMESERIES_SCRIPT = """
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

        -- Extract key parts (ts:pokemon:metric:area:pokemon_id:form)
        local key_parts = {}
        for part in string.gmatch(key, '([^:]+)') do
            table.insert(key_parts, part)
        end

        for i = 1, #hash_data, 2 do
            local ts = tonumber(hash_data[i])
            local count = tonumber(hash_data[i+1])
            if ts and count and ts >= start_ts and ts < end_ts then
                local metric = key_parts[3] or 'unknown'
                local pokemon_id = key_parts[5] or 'all'
                local form = key_parts[6] or '0'

                -- Sum mode aggregation
                sum_results[metric] = (sum_results[metric] or 0) + count

                -- Grouped mode aggregation
                local group_key = pokemon_id .. ':' .. form
                if not grouped_results[metric] then
                    grouped_results[metric] = {}
                end
                grouped_results[metric][group_key] = (grouped_results[metric][group_key] or 0) + count

                -- Surged mode aggregation
                local hour = tostring(math.floor((ts % 86400) / 3600))
                if not surged_results[metric] then
                    surged_results[metric] = {}
                end
                surged_results[metric][hour] = (surged_results[metric][hour] or 0) + count
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
    for metric, groups in pairs(grouped_results) do
        local inner = {}
        for group_key, count in pairs(groups) do
            table.insert(inner, group_key)
            table.insert(inner, count)
        end
        table.insert(arr, metric)
        table.insert(arr, inner)
    end
    return arr
elseif mode == 'surged' then
    local arr = {}
    for metric, hours in pairs(surged_results) do
        local inner = {}
        for hour, count in pairs(hours) do
            table.insert(inner, hour)
            table.insert(inner, count)
        end
        table.insert(arr, metric)
        table.insert(arr, inner)
    end
    return arr
else
    return {}
end
"""

class PokemonTimeSeries:
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        pokemon_id: Union[str, Iterable[str], None] = "all",
        form: Union[str, Iterable[str], None] = "all",
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

        self.pokemon_ids = _norm(pokemon_id)
        self.forms       = _norm(form)

        logger.info(
            f"👻 Initialized PokemonTimeSeries area={self.area} "
            f"start={self.start} end={self.end} mode={self.mode} "
            f"pokemon_ids={self.pokemon_ids or 'ALL'} forms={self.forms or 'ALL'}"
        )

    async def _load_script(self, client):
        """Load Lua script into Redis if not already cached"""
        if not self.script_sha:
            logger.debug("Loading Lua script into Redis...")
            self.script_sha = await client.script_load(TIMESERIES_SCRIPT)
            logger.debug(f"Lua script 👻 pokemon loaded with SHA: {self.script_sha}")
        else:
            logger.debug("Lua script 👻 pokemon already loaded, reusing cached SHA.")
        return self.script_sha

    async def retrieve_timeseries(self) -> Dict:
        """Retrieve timeseries data using optimized Lua script"""
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
            return {"mode": self.mode, "data": {}}

        # Helper: convert a Redis Lua table (list) into a dictionary.
        def convert_redis_result(res):
            if isinstance(res, list):
                if len(res) % 2 == 0:
                    return {res[i]: convert_redis_result(res[i + 1]) for i in range(0, len(res), 2)}
                else:
                    return [convert_redis_result(item) for item in res]
            return res

        # accumulators
        acc_sum: Dict[str, int] = {}
        acc_grouped: Dict[str, Dict[str, int]] = {}
        acc_surged: Dict[str, Dict[str, int]] = {}

        start_ts = int(self.start.timestamp())
        end_ts   = int(self.end.timestamp())

        try:
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

                # Merge into accumulators
                if self.mode == "sum":
                    for metric, cnt in data.items():
                        acc_sum[metric] = acc_sum.get(metric, 0) + int(cnt)
                elif self.mode == "grouped":
                    # data: { metric: { "pid:form": count, ... } }
                    for metric, groups in data.items():
                        bucket = acc_grouped.setdefault(metric, {})
                        for k, v in groups.items():
                            bucket[k] = bucket.get(k, 0) + int(v)
                elif self.mode == "surged":
                    # data: { metric: { hour_str: count, ... } }  (hours are "0".."23")
                    for metric, hours in data.items():
                        bucket = acc_surged.setdefault(metric, {})
                        for h, v in hours.items():
                            bucket[h] = bucket.get(h, 0) + int(v)

            # Final formatting
            if self.mode == "sum":
                formatted = dict(sorted(acc_sum.items()))
            elif self.mode == "grouped":
                formatted = {}
                for metric, groups in acc_grouped.items():
                    # sort by pid then form numerically if possible
                    def _key(x):
                        pid, frm = x.split(":")
                        try:
                            return (int(pid), int(frm))
                        except:
                            return (pid, frm)
                    formatted[metric] = dict(sorted(groups.items(), key=lambda kv: _key(kv[0])))
            elif self.mode == "surged":
                formatted = {}
                for metric, hours in acc_surged.items():
                    # label "hour N" and sort numerically by N
                    labeled = {f"hour {int(h)}": v for h, v in hours.items()}
                    formatted[metric] = dict(sorted(labeled.items(), key=lambda kv: int(kv[0].split()[1])))
            else:
                formatted = {}

            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"❌ Lua Pokémon 👻 script execution failed: {e}")
            return {"mode": self.mode, "data": {}}

    def _build_key_patterns(self) -> list[str]:
        """
        Build a list of Redis MATCH patterns for SCAN.
        Key format: ts:pokemon:{metric}:{area}:{pokemon_id}:{form}
        We always wildcard metric ('*'). For pokemon_id/form we expand cartesian
        product if sets are provided; otherwise wildcard that slot.
        """
        metric = "*"
        area = "*" if self.area.lower() in ["all", "global"] else self.area

        pids  = list(self.pokemon_ids) if self.pokemon_ids is not None else ["*"]
        forms = list(self.forms)       if self.forms is not None       else ["*"]

        patterns = []
        for pid in pids:
            for frm in forms:
                patterns.append(f"ts:pokemon:{metric}:{area}:{pid}:{frm}")
        logger.debug(f"Built {len(patterns)} key pattern(s): {patterns[:5]}{'...' if len(patterns)>5 else ''}")
        return patterns
