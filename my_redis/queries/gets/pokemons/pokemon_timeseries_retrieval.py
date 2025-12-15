import asyncio
import re
import time
from datetime import datetime
from typing import Dict, Any, Union, Iterable
from my_redis.connect_redis import RedisManager
from utils.logger import logger
try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    relativedelta = None

redis_manager = RedisManager()

# Chunked Lua script - processes only specified keys (no SCAN)
TIMESERIES_CHUNK_SCRIPT = """
local start_ts = tonumber(ARGV[1])
local end_ts = tonumber(ARGV[2])
local mode = ARGV[3]

local sum_results = {}
local grouped_results = {}
local surged_results = {}

-- Process only the keys passed as KEYS argument
for _, key in ipairs(KEYS) do
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
        chunk_size: int = 500,  # Keys per Lua chunk
        chunk_sleep: float = 0.15,  # Sleep between chunks (seconds)
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

        self.pokemon_ids = _norm(pokemon_id)
        self.forms       = _norm(form)

        logger.info(
            f"👻 Initialized PokemonTimeSeries area={self.area} "
            f"start={self.start} end={self.end} mode={self.mode} "
            f"pokemon_ids={self.pokemon_ids or 'ALL'} forms={self.forms or 'ALL'} "
            f"chunk_size={self.chunk_size} chunk_sleep={self.chunk_sleep}s"
        )

    async def _load_script(self, client):
        """Load Lua script into Redis if not already cached"""
        if not self.script_sha:
            logger.debug("Loading chunked Lua script into Redis...")
            self.script_sha = await client.script_load(TIMESERIES_CHUNK_SCRIPT)
            logger.debug(f"Lua script 👻 loaded with SHA: {self.script_sha}")
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
        logger.info(f"👻 SCAN collected {len(all_keys)} keys in {scan_elapsed:.3f}s")
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
        if self.mode == "sum":
            for metric, cnt in chunk_data.items():
                acc_sum[metric] = acc_sum.get(metric, 0) + int(cnt)
        elif self.mode == "grouped":
            for metric, groups in chunk_data.items():
                bucket = acc_grouped.setdefault(metric, {})
                for k, v in groups.items():
                    bucket[k] = bucket.get(k, 0) + int(v)
        elif self.mode == "surged":
            for metric, hours in chunk_data.items():
                bucket = acc_surged.setdefault(metric, {})
                for h, v in hours.items():
                    bucket[h] = bucket.get(h, 0) + int(v)

    async def retrieve_timeseries(self) -> Dict:
        """Retrieve timeseries using chunked Lua scripts with yield points"""
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
            return {"mode": self.mode, "data": {}}

        total_start = time.monotonic()

        try:
            # Step 1: SCAN for all keys (non-blocking, ~0.1s)
            all_keys = await self._scan_keys_by_patterns(client)

            if not all_keys:
                logger.info("No keys found matching patterns")
                return {"mode": self.mode, "data": {}}

            # Step 2: Load Lua script
            sha = await self._load_script(client)

            # Step 3: Split keys into chunks
            chunks = [all_keys[i:i+self.chunk_size] for i in range(0, len(all_keys), self.chunk_size)]
            logger.info(f"👻 Processing {len(all_keys)} keys in {len(chunks)} chunks of ~{self.chunk_size} keys")

            # Accumulators
            acc_sum: Dict[str, int] = {}
            acc_grouped: Dict[str, Dict[str, int]] = {}
            acc_surged: Dict[str, Dict[str, int]] = {}

            start_ts = int(self.start.timestamp())
            end_ts   = int(self.end.timestamp())

            # Step 4: Process chunks with sleep intervals
            chunk_start = time.monotonic()
            for i, chunk in enumerate(chunks):
                chunk_iter_start = time.monotonic()

                # Run Lua script on this chunk
                raw = await client.evalsha(
                    sha,
                    len(chunk),  # Number of KEYS
                    *chunk,      # KEYS array
                    str(start_ts),  # ARGV[1]
                    str(end_ts),    # ARGV[2]
                    self.mode       # ARGV[3]
                )

                chunk_data = self._convert_redis_result(raw)
                self._merge_results(acc_sum, acc_grouped, acc_surged, chunk_data)

                chunk_iter_elapsed = time.monotonic() - chunk_iter_start
                logger.info(f"👻 Chunk {i+1}/{len(chunks)} processed {len(chunk)} keys in {chunk_iter_elapsed:.3f}s")

                # Sleep between chunks to allow writes (except last chunk)
                if i < len(chunks) - 1:
                    await asyncio.sleep(self.chunk_sleep)

            chunk_elapsed = time.monotonic() - chunk_start
            logger.info(f"👻 Chunked Lua processing took {chunk_elapsed:.3f}s ({len(chunks)} chunks × ~{self.chunk_sleep}s sleep)")

            # Step 5: Format results
            format_start = time.monotonic()
            if self.mode == "sum":
                formatted = dict(sorted(acc_sum.items()))
            elif self.mode == "grouped":
                formatted = {}
                for metric, groups in acc_grouped.items():
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
                    labeled = {f"hour {int(h)}": v for h, v in hours.items()}
                    formatted[metric] = dict(sorted(labeled.items(), key=lambda kv: int(kv[0].split()[1])))
            else:
                formatted = {}

            format_elapsed = time.monotonic() - format_start
            total_elapsed = time.monotonic() - total_start

            logger.info(f"👻 Final formatting took {format_elapsed:.3f}s")
            logger.info(f"👻 Total retrieval time: {total_elapsed:.3f}s")

            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"❌ Chunked Lua script execution failed: {e}")
            return {"mode": self.mode, "data": {}}

    def _build_key_patterns(self) -> list[str]:
        """
        Build a list of Redis MATCH patterns for SCAN.
        Key format: ts:pokemon:{metric}:{area}:{pokemon_id}:{form}
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
