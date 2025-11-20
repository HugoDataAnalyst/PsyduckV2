import asyncio
import time
from datetime import datetime
from typing import Dict, Union, Iterable
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

# Pipeline batch size - larger batches for better performance
PIPELINE_BATCH_SIZE = 500

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

    async def _process_keys_batch(
        self,
        client,
        keys: list,
        start_ts: int,
        end_ts: int,
        acc_sum: Dict[str, int],
        acc_grouped: Dict[str, Dict[str, int]],
        acc_surged: Dict[str, Dict[str, int]]
    ):
        """Process a batch of keys using pipelining to fetch data efficiently"""
        if not keys:
            return

        # Use pipeline to fetch all hash data at once
        async with client.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(key)
            results = await pipe.execute()

        # Process each key's hash data
        for key, hash_data in zip(keys, results):
            if not hash_data:
                continue

            # Decode key if it's bytes
            if isinstance(key, bytes):
                key = key.decode('utf-8')

            # Extract key parts: ts:tth_pokemon:area:tth_bucket
            key_parts = key.split(':')
            if len(key_parts) < 4:
                continue

            bucket = key_parts[3]

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

                # Aggregate based on mode
                if self.mode == "sum":
                    acc_sum[bucket] = acc_sum.get(bucket, 0) + count

                elif self.mode == "grouped":
                    # group by time bucket (minute-rounded timestamp)
                    time_bucket = str((ts // 60) * 60)
                    bucket_dict = acc_grouped.setdefault(bucket, {})
                    bucket_dict[time_bucket] = bucket_dict.get(time_bucket, 0) + count

                elif self.mode == "surged":
                    # group by hour of day
                    hour = str((ts % 86400) // 3600)
                    bucket_dict = acc_surged.setdefault(bucket, {})
                    bucket_dict[hour] = bucket_dict.get(hour, 0) + count

    async def retrieve_timeseries(self) -> Dict:
        """Retrieve timeseries data using Python-side filtering with pipelining to avoid blocking Redis"""
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

        start_ts = int(self.start.timestamp())
        end_ts   = int(self.end.timestamp())

        # accumulators
        acc_sum: Dict[str, int] = {}
        acc_grouped: Dict[str, Dict[str, int]] = {}  # bucket -> { time_bucket_ts: count }
        acc_surged: Dict[str, Dict[str, int]] = {}   # bucket -> { hour_str: count }

        try:
            request_start = time.monotonic()
            total_keys_processed = 0

            # Process each pattern separately
            for pattern in self._build_key_patterns():
                logger.debug(f"👻⏱️ Scanning for pattern: {pattern}")

                # Use SCAN to iterate through matching keys
                keys_batch = []
                async for key in client.scan_iter(match=pattern, count=500):
                    keys_batch.append(key)

                    # Process in batches to allow writes to interleave
                    if len(keys_batch) >= PIPELINE_BATCH_SIZE:
                        await self._process_keys_batch(
                            client, keys_batch, start_ts, end_ts,
                            acc_sum, acc_grouped, acc_surged
                        )
                        total_keys_processed += len(keys_batch)
                        keys_batch = []
                        # Small delay to allow write operations to proceed
                        await asyncio.sleep(0.001)

                # Process remaining keys
                if keys_batch:
                    await self._process_keys_batch(
                        client, keys_batch, start_ts, end_ts,
                        acc_sum, acc_grouped, acc_surged
                    )
                    total_keys_processed += len(keys_batch)

            elapsed = time.monotonic() - request_start
            logger.info(f"👻⏱️ Pokémon TTH retrieval processed {total_keys_processed} keys in {elapsed:.3f}s")

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

            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"TTH retrieval failed: {e}")
            return {"mode": self.mode, "data": {}}
