import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Iterable, Union
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

# Chunked Lua script for Invasion timeseries
TIMESERIES_INVASION_CHUNK_SCRIPT = """
local start_ts = tonumber(ARGV[1])
local end_ts = tonumber(ARGV[2])
local mode = ARGV[3]

local total_results = {}
local confirmed_results = {}
local grouped_results = {}
local surged_results = {}

-- Process only the keys passed as KEYS argument
for _, key in ipairs(KEYS) do
    local hash_data = redis.call('HGETALL', key)

    -- Extract key parts (ts:invasion:total:area:display_type:grunt:confirmed)
    local key_parts = {}
    for part in string.gmatch(key, '([^:]+)') do
        table.insert(key_parts, part)
    end

    if #key_parts >= 7 then
        local group_key = key_parts[5] .. ':' .. key_parts[6] .. ':' .. key_parts[7]  -- display:grunt:confirmed
        local confirmed_key = key_parts[7]

        for i = 1, #hash_data, 2 do
            local ts = tonumber(hash_data[i])
            local count = tonumber(hash_data[i+1])
            if ts and count and ts >= start_ts and ts < end_ts then
                -- Sum mode
                total_results['total'] = (total_results['total'] or 0) + count
                confirmed_results[confirmed_key] = (confirmed_results[confirmed_key] or 0) + count

                -- Grouped mode
                if not grouped_results[group_key] then
                    grouped_results[group_key] = {}
                end
                local bucket_str = tostring(ts)
                grouped_results[group_key][bucket_str] = (grouped_results[group_key][bucket_str] or 0) + count

                -- Surged mode
                if not surged_results[group_key] then
                    surged_results[group_key] = {}
                end
                local hour = tostring(math.floor((ts % 86400) / 3600))
                surged_results[group_key][hour] = (surged_results[group_key][hour] or 0) + count
            end
        end
    end
end

if mode == 'sum' then
    local arr = {}
    -- total
    for k, v in pairs(total_results) do
        table.insert(arr, k)
        table.insert(arr, v)
    end
    -- confirmed (nested)
    local confirmed_arr = {}
    for k, v in pairs(confirmed_results) do
        table.insert(confirmed_arr, k)
        table.insert(confirmed_arr, v)
    end
    table.insert(arr, 'confirmed')
    table.insert(arr, confirmed_arr)
    return arr
elseif mode == 'grouped' then
    local arr = {}
    for group_key, groups in pairs(grouped_results) do
        local inner = {}
        for bucket_str, count in pairs(groups) do
            table.insert(inner, bucket_str)
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
        confirmed: str = "all",
        chunk_size: int = 500,
        chunk_sleep: float = 0.15,
    ):
        """
        Parameters:
          - area: Area name filter. Use "all" or "global" to match every area.
          - display_type: Invasion display type (e.g., a numeric value or "all").
          - grunt: Invasion grunt filter (numeric or "all").
          - confirmed: Invasion confirmed flag filter (numeric or "all").
          - mode: Aggregation mode: "sum", "grouped", or "surged".
          - chunk_size: Keys per Lua chunk
          - chunk_sleep: Sleep between chunks (seconds)
        """
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.confirmed = confirmed
        self.chunk_size = chunk_size
        self.chunk_sleep = chunk_sleep
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
            f"start={self.start}, end={self.end}, "
            f"chunk_size={self.chunk_size}, chunk_sleep={self.chunk_sleep}s"
        )

    async def _load_script(self, client):
        """Load Lua script into Redis if not already cached"""
        if not self.script_sha:
            logger.debug("Loading chunked Lua script for Invasion into Redis...")
            self.script_sha = await client.script_load(TIMESERIES_INVASION_CHUNK_SCRIPT)
            logger.debug(f"Lua script 🕴️ loaded with SHA: {self.script_sha}")
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
        logger.info(f"🕴️ SCAN collected {len(all_keys)} keys in {scan_elapsed:.3f}s")
        return all_keys

    def _convert_redis_result(self, res):
        """Convert Redis Lua table (list) into Python dict"""
        if isinstance(res, list):
            if len(res) % 2 == 0:
                return {res[i]: self._convert_redis_result(res[i + 1]) for i in range(0, len(res), 2)}
            else:
                return [self._convert_redis_result(item) for item in res]
        return res

    def _merge_results(self, acc_total, acc_confirmed, acc_grouped, acc_surged, chunk_data):
        """Merge chunk results into accumulators"""
        if self.mode == "sum":
            # chunk_data: {total: X, confirmed: {key: count}}
            if 'total' in chunk_data:
                acc_total['total'] = acc_total.get('total', 0) + int(chunk_data['total'])
            if 'confirmed' in chunk_data:
                for confirmed_key, count in chunk_data['confirmed'].items():
                    acc_confirmed[confirmed_key] = acc_confirmed.get(confirmed_key, 0) + int(count)
        elif self.mode == "grouped":
            for group_key, groups in chunk_data.items():
                bucket = acc_grouped.setdefault(group_key, {})
                for bucket_str, count in groups.items():
                    bucket[bucket_str] = bucket.get(bucket_str, 0) + int(count)
        elif self.mode == "surged":
            for group_key, hours in chunk_data.items():
                bucket = acc_surged.setdefault(group_key, {})
                for hour, count in hours.items():
                    bucket[hour] = bucket.get(hour, 0) + int(count)

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
                logger.info("🕴️ No keys found matching patterns")
                return {"mode": self.mode, "data": {}}

            # Step 2: Load Lua script
            sha = await self._load_script(client)

            # Step 3: Split keys into chunks
            chunks = [all_keys[i:i+self.chunk_size] for i in range(0, len(all_keys), self.chunk_size)]
            logger.info(f"🕴️ Processing {len(all_keys)} keys in {len(chunks)} chunks of ~{self.chunk_size} keys")

            # Accumulators
            acc_total = {}
            acc_confirmed: Dict[str, int] = {}
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
                self._merge_results(acc_total, acc_confirmed, acc_grouped, acc_surged, chunk_data)

                chunk_iter_elapsed = time.monotonic() - chunk_iter_start
                logger.info(f"🕴️ Chunk {i+1}/{len(chunks)} processed {len(chunk)} keys in {chunk_iter_elapsed:.3f}s")

                # Sleep between chunks to allow writes
                if i < len(chunks) - 1:
                    await asyncio.sleep(self.chunk_sleep)

            chunk_elapsed = time.monotonic() - chunk_start
            logger.info(f"🕴️ Chunked Lua processing took {chunk_elapsed:.3f}s ({len(chunks)} chunks × ~{self.chunk_sleep}s sleep)")

            # Step 5: Final formatting/sorting
            format_start = time.monotonic()
            if self.mode == "sum":
                formatted = {"total": acc_total.get('total', 0), "confirmed": dict(sorted(acc_confirmed.items()))}
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

            format_elapsed = time.monotonic() - format_start
            total_elapsed = time.monotonic() - total_start

            logger.info(f"🕴️ Final formatting took {format_elapsed:.3f}s")
            logger.info(f"🕴️ Total retrieval time: {total_elapsed:.3f}s")

            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"❌ 🕴️ Invasion retrieval failed: {e}")
            return {"mode": self.mode, "data": {}}
