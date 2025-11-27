import asyncio
import time
from datetime import datetime
from typing import Dict, Union, Iterable
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

# Chunked Lua script for TTH timeseries
TIMESERIES_TTH_CHUNK_SCRIPT = """
local start_ts = tonumber(ARGV[1])
local end_ts = tonumber(ARGV[2])
local mode = ARGV[3]

local sum_results = {}
local grouped_results = {}
local surged_results = {}

-- Process only the keys passed as KEYS argument
for _, key in ipairs(KEYS) do
    local hash_data = redis.call('HGETALL', key)

    -- Extract key parts (ts:tth_pokemon:area:tth_bucket)
    local key_parts = {}
    for part in string.gmatch(key, '([^:]+)') do
        table.insert(key_parts, part)
    end
    local bucket = key_parts[4] or 'unknown'

    for i = 1, #hash_data, 2 do
        local ts = tonumber(hash_data[i])
        local count = tonumber(hash_data[i+1])
        if ts and count and ts >= start_ts and ts < end_ts then
            -- Sum mode
            sum_results[bucket] = (sum_results[bucket] or 0) + count

            -- Grouped mode: group by time bucket (minute-rounded timestamp)
            if not grouped_results[bucket] then
                grouped_results[bucket] = {}
            end
            local time_bucket = tostring(math.floor(ts / 60) * 60)
            grouped_results[bucket][time_bucket] = (grouped_results[bucket][time_bucket] or 0) + count

            -- Surged mode: group by hour of day
            if not surged_results[bucket] then
                surged_results[bucket] = {}
            end
            local hour = tostring(math.floor((ts % 86400) / 3600))
            surged_results[bucket][hour] = (surged_results[bucket][hour] or 0) + count
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
        chunk_size: int = 500,
        chunk_sleep: float = 0.15,
    ):
        """
        Parameters:
          - area: The area filter. Use "all" or "global" to match every area.
          - tth_bucket: The despawn timer bucket filter (e.g., "10_15"). Use "all" to match any bucket.
          - mode: Aggregation mode ("sum", "grouped", or "surged").
          - chunk_size: Keys per Lua chunk
          - chunk_sleep: Sleep between chunks (seconds)
        """
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

        self.tth_buckets = _norm(tth_bucket)

        logger.info(
            f"👻⏱️ Initialized PokemonTTHTimeSeries with area: {self.area}, "
            f"tth_buckets: {self.tth_buckets or 'ALL'}, ▶️ Start: {self.start}, "
            f"⏸️ End: {self.end}, Mode: {self.mode}, "
            f"chunk_size={self.chunk_size}, chunk_sleep={self.chunk_sleep}s"
        )

    async def _load_script(self, client):
        """Load Lua script into Redis if not already cached"""
        if not self.script_sha:
            logger.debug("Loading chunked Lua script for TTH into Redis...")
            self.script_sha = await client.script_load(TIMESERIES_TTH_CHUNK_SCRIPT)
            logger.debug(f"Lua script 👻⏱️ loaded with SHA: {self.script_sha}")
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
        logger.info(f"👻⏱️ SCAN collected {len(all_keys)} keys in {scan_elapsed:.3f}s")
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
            for bucket, cnt in chunk_data.items():
                acc_sum[bucket] = acc_sum.get(bucket, 0) + int(cnt)
        elif self.mode == "grouped":
            for bucket, groups in chunk_data.items():
                bucket_dict = acc_grouped.setdefault(bucket, {})
                for time_bucket, count in groups.items():
                    bucket_dict[time_bucket] = bucket_dict.get(time_bucket, 0) + int(count)
        elif self.mode == "surged":
            for bucket, hours in chunk_data.items():
                bucket_dict = acc_surged.setdefault(bucket, {})
                for h, v in hours.items():
                    bucket_dict[h] = bucket_dict.get(h, 0) + int(v)

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
        """Retrieve timeseries using chunked Lua scripts with yield points"""
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
            return {"mode": self.mode, "data": {}}

        # helpers for sorting
        def _bucket_key(bucket: str) -> int:
            try:
                return int(bucket.split("_")[0])
            except Exception:
                return 10**9  # push unknowns to end

        total_start = time.monotonic()

        try:
            # Step 1: SCAN for all keys
            all_keys = await self._scan_keys_by_patterns(client)

            if not all_keys:
                logger.info("👻⏱️ No keys found matching patterns")
                return {"mode": self.mode, "data": {}}

            # Step 2: Load Lua script
            sha = await self._load_script(client)

            # Step 3: Split keys into chunks
            chunks = [all_keys[i:i+self.chunk_size] for i in range(0, len(all_keys), self.chunk_size)]
            logger.info(f"👻⏱️ Processing {len(all_keys)} keys in {len(chunks)} chunks of ~{self.chunk_size} keys")

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
                    len(chunk),
                    *chunk,
                    str(start_ts),
                    str(end_ts),
                    self.mode
                )

                chunk_data = self._convert_redis_result(raw)
                self._merge_results(acc_sum, acc_grouped, acc_surged, chunk_data)

                chunk_iter_elapsed = time.monotonic() - chunk_iter_start
                logger.info(f"👻⏱️ Chunk {i+1}/{len(chunks)} processed {len(chunk)} keys in {chunk_iter_elapsed:.3f}s")

                # Sleep between chunks to allow writes
                if i < len(chunks) - 1:
                    await asyncio.sleep(self.chunk_sleep)

            chunk_elapsed = time.monotonic() - chunk_start
            logger.info(f"👻⏱️ Chunked Lua processing took {chunk_elapsed:.3f}s ({len(chunks)} chunks × ~{self.chunk_sleep}s sleep)")

            # Step 5: Final formatting
            format_start = time.monotonic()
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

            format_elapsed = time.monotonic() - format_start
            total_elapsed = time.monotonic() - total_start

            logger.info(f"👻⏱️ Final formatting took {format_elapsed:.3f}s")
            logger.info(f"👻⏱️ Total retrieval time: {total_elapsed:.3f}s")

            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"TTH retrieval failed: {e}")
            return {"mode": self.mode, "data": {}}
