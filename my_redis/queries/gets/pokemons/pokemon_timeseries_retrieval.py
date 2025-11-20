import asyncio
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Union, Iterable
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import config as AppConfig
try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    relativedelta = None

redis_manager = RedisManager()

# Pipeline batch size - larger batches for better performance
# Writes can still interleave between batches
PIPELINE_BATCH_SIZE = 500

# Max concurrent pipeline operations for queries
# Based on Redis connection pool: use 1/4 of total (half for queries, half of that for safety)
# Capped at 20 to prevent excessive overhead
MAX_CONCURRENT_PIPELINES = min(max(3, AppConfig.redis_max_connections // 4), 20)

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

    async def retrieve_timeseries(self) -> Dict:
        """Retrieve timeseries data using Python-side filtering with pipelining to avoid blocking Redis"""
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
            return {"mode": self.mode, "data": {}}

        # accumulators with lock for thread-safe concurrent updates
        acc_sum: Dict[str, int] = {}
        acc_grouped: Dict[str, Dict[str, int]] = {}
        acc_surged: Dict[str, Dict[str, int]] = {}
        acc_lock = asyncio.Lock()

        start_ts = int(self.start.timestamp())
        end_ts   = int(self.end.timestamp())

        try:
            request_start = time.monotonic()
            total_keys_processed = 0
            patterns = self._build_key_patterns()

            # Process patterns concurrently for better performance
            async def process_pattern(pattern):
                logger.debug(f"👻 Scanning for pattern: {pattern}")

                # Collect all keys first
                all_keys = []
                async for key in client.scan_iter(match=pattern, count=2000):
                    all_keys.append(key)

                # Process all batches concurrently
                batches = [all_keys[i:i + PIPELINE_BATCH_SIZE] for i in range(0, len(all_keys), PIPELINE_BATCH_SIZE)]

                # Process batches concurrently based on available connections
                for i in range(0, len(batches), MAX_CONCURRENT_PIPELINES):
                    concurrent_batches = batches[i:i + MAX_CONCURRENT_PIPELINES]
                    await asyncio.gather(*[
                        self._process_keys_batch(
                            client, batch, start_ts, end_ts,
                            acc_sum, acc_grouped, acc_surged, acc_lock
                        ) for batch in concurrent_batches
                    ])

                return len(all_keys)

            # Process up to 5 patterns concurrently
            batch_size = 5
            for i in range(0, len(patterns), batch_size):
                pattern_batch = patterns[i:i + batch_size]
                counts = await asyncio.gather(*[process_pattern(p) for p in pattern_batch])
                total_keys_processed += sum(counts)

            elapsed = time.monotonic() - request_start
            logger.info(f"👻 Pokémon retrieval processed {total_keys_processed} keys in {elapsed:.3f}s")

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
            logger.error(f"❌ Pokémon 👻 retrieval failed: {e}")
            return {"mode": self.mode, "data": {}}

    async def _process_keys_batch(
        self,
        client,
        keys: list,
        start_ts: int,
        end_ts: int,
        acc_sum: Dict[str, int],
        acc_grouped: Dict[str, Dict[str, int]],
        acc_surged: Dict[str, Dict[str, int]],
        acc_lock: asyncio.Lock = None
    ):
        """Process a batch of keys using pipelining to fetch data efficiently"""
        if not keys:
            return

        # Use pipeline to fetch all hash data at once
        async with client.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(key)
            results = await pipe.execute()

        # Local accumulators for this batch
        local_sum: Dict[str, int] = {}
        local_grouped: Dict[str, Dict[str, int]] = {}
        local_surged: Dict[str, Dict[str, int]] = {}

        # Process each key's hash data into local accumulators
        for key, hash_data in zip(keys, results):
            if not hash_data:
                continue

            # Decode key if it's bytes
            if isinstance(key, bytes):
                key = key.decode('utf-8')

            # Extract key parts (ts:pokemon:metric:area:pokemon_id:form)
            key_parts = key.split(':')
            if len(key_parts) < 6:
                continue

            metric = key_parts[2]
            pokemon_id = key_parts[4]
            form = key_parts[5]

            # Filter and aggregate hash data
            for ts_field, count_value in hash_data.items():
                # Decode if bytes
                if isinstance(ts_field, bytes):
                    ts_field = ts_field.decode('utf-8')
                if isinstance(count_value, bytes):
                    count_value = count_value.decode('utf-8')

                try:
                    ts = int(ts_field)
                    count = int(count_value)
                except (ValueError, TypeError):
                    continue

                # Filter by time range
                if not (start_ts <= ts < end_ts):
                    continue

                # Aggregate based on mode into local accumulators
                if self.mode == "sum":
                    local_sum[metric] = local_sum.get(metric, 0) + count

                elif self.mode == "grouped":
                    group_key = f"{pokemon_id}:{form}"
                    bucket = local_grouped.setdefault(metric, {})
                    bucket[group_key] = bucket.get(group_key, 0) + count

                elif self.mode == "surged":
                    hour = str((ts % 86400) // 3600)
                    bucket = local_surged.setdefault(metric, {})
                    bucket[hour] = bucket.get(hour, 0) + count

        # Merge local accumulators into global accumulators with lock
        if acc_lock:
            async with acc_lock:
                # Merge sum
                for metric, count in local_sum.items():
                    acc_sum[metric] = acc_sum.get(metric, 0) + count

                # Merge grouped
                for metric, groups in local_grouped.items():
                    bucket = acc_grouped.setdefault(metric, {})
                    for group_key, count in groups.items():
                        bucket[group_key] = bucket.get(group_key, 0) + count

                # Merge surged
                for metric, hours in local_surged.items():
                    bucket = acc_surged.setdefault(metric, {})
                    for hour, count in hours.items():
                        bucket[hour] = bucket.get(hour, 0) + count
        else:
            # No lock needed (sequential processing)
            for metric, count in local_sum.items():
                acc_sum[metric] = acc_sum.get(metric, 0) + count
            for metric, groups in local_grouped.items():
                bucket = acc_grouped.setdefault(metric, {})
                for group_key, count in groups.items():
                    bucket[group_key] = bucket.get(group_key, 0) + count
            for metric, hours in local_surged.items():
                bucket = acc_surged.setdefault(metric, {})
                for hour, count in hours.items():
                    bucket[hour] = bucket.get(hour, 0) + count

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
