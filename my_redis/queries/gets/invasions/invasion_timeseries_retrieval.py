import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Iterable, Union
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

# Pipeline batch size - larger batches for better performance
PIPELINE_BATCH_SIZE = 500

class InvasionTimeSeries:
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        display: Union[str, Iterable[str], None] = "all",
        grunt: Union[str, Iterable[str], None] = "all",
        confirmed: str = "all",  # keep single
    ):
        """
        Parameters:
          - area: Area name filter. Use "all" or "global" to match every area.
          - display_type: Invasion display type (e.g., a numeric value or "all").
          - grunt: Invasion grunt filter (numeric or "all").
          - confirmed: Invasion confirmed flag filter (numeric or "all").
          - mode: Aggregation mode: "sum", "grouped", or "surged".
        """
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.confirmed = confirmed

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
            f"start={self.start}, end={self.end}"
        )

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

    async def _process_keys_batch(
        self,
        client,
        keys: list,
        start_ts: int,
        end_ts: int
    ):
        """Process a batch of keys using pipelining to fetch data efficiently

        Returns:
            Tuple of (local_total, local_confirmed, local_grouped, local_surged) dictionaries
        """
        # Local accumulators for this batch
        local_total = {}
        local_confirmed: Dict[str, int] = {}
        local_grouped: Dict[str, Dict[str, int]] = {}
        local_surged: Dict[str, Dict[str, int]] = {}

        if not keys:
            return local_total, local_confirmed, local_grouped, local_surged

        # Use pipeline to fetch all hash data at once
        pipeline_start = time.monotonic()
        async with client.pipeline(transaction=False) as pipe:
            for key in keys:
                pipe.hgetall(key)
            results = await pipe.execute()
        pipeline_elapsed = time.monotonic() - pipeline_start
        logger.debug(f"🕴️ Pipeline fetched {len(keys)} keys in {pipeline_elapsed:.3f}s")

        # Process each key's hash data
        processing_start = time.monotonic()
        for key, hash_data in zip(keys, results):
            if not hash_data:
                continue

            # Decode key if it's bytes
            if isinstance(key, bytes):
                key = key.decode('utf-8')

            # Key format: ts:invasion:total:{area}:{display_type}:{grunt}:{confirmed}
            key_parts = key.split(':')
            if len(key_parts) < 7:
                continue

            group_key = f"{key_parts[4]}:{key_parts[5]}:{key_parts[6]}"  # display:grunt:confirmed
            confirmed_key = key_parts[6]

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
                    local_total['total'] = local_total.get('total', 0) + count
                    local_confirmed[confirmed_key] = local_confirmed.get(confirmed_key, 0) + count

                elif self.mode == "grouped":
                    bucket = local_grouped.setdefault(group_key, {})
                    bucket_str = str(ts)
                    bucket[bucket_str] = bucket.get(bucket_str, 0) + count

                elif self.mode == "surged":
                    bucket = local_surged.setdefault(group_key, {})
                    hour = str((ts % 86400) // 3600)
                    bucket[hour] = bucket.get(hour, 0) + count

        processing_elapsed = time.monotonic() - processing_start
        logger.debug(f"🕴️ Processed {len(keys)} keys data in {processing_elapsed:.3f}s")

        return local_total, local_confirmed, local_grouped, local_surged

    async def invasion_retrieve_timeseries(self) -> Dict[str, Any]:
        """Retrieve timeseries data using Python-side filtering with pipelining to avoid blocking Redis"""
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
            return {"mode": self.mode, "data": {}}

        # Global accumulators
        acc_total = {}
        acc_confirmed: Dict[str, int] = {}
        acc_grouped: Dict[str, Dict[str, int]] = {}
        acc_surged: Dict[str, Dict[str, int]] = {}

        try:
            start_ts = int(self.start.timestamp())
            end_ts   = int(self.end.timestamp())
            logger.debug(f"🕴️ Invasion ⏱️ Time range: start_ts={start_ts}, end_ts={end_ts}")

            request_start = time.monotonic()
            total_keys_processed = 0

            # Process each pattern separately
            for pattern in self._build_key_patterns():
                logger.debug(f"🕴️ Scanning for pattern: {pattern}")

                # Collect all keys for this pattern
                scan_start = time.monotonic()
                all_keys = []
                async for key in client.scan_iter(match=pattern, count=2000):
                    all_keys.append(key)
                scan_elapsed = time.monotonic() - scan_start
                logger.debug(f"🕴️ SCAN iteration collected {len(all_keys)} keys in {scan_elapsed:.3f}s")

                # Process in batches
                batch_split_start = time.monotonic()
                batches = [all_keys[i:i + PIPELINE_BATCH_SIZE] for i in range(0, len(all_keys), PIPELINE_BATCH_SIZE)]
                batch_split_elapsed = time.monotonic() - batch_split_start
                logger.debug(f"🕴️ Split into {len(batches)} batches in {batch_split_elapsed:.3f}s")

                # Process batches sequentially to avoid lock contention
                batch_process_start = time.monotonic()
                for batch in batches:
                    local_total, local_confirmed, local_grouped, local_surged = await self._process_keys_batch(
                        client, batch, start_ts, end_ts
                    )

                    # Merge results sequentially (fast CPU operation, no lock needed)
                    for k, v in local_total.items():
                        acc_total[k] = acc_total.get(k, 0) + v

                    for confirmed_key, count in local_confirmed.items():
                        acc_confirmed[confirmed_key] = acc_confirmed.get(confirmed_key, 0) + count

                    for group_key, groups in local_grouped.items():
                        bucket = acc_grouped.setdefault(group_key, {})
                        for bucket_str, count in groups.items():
                            bucket[bucket_str] = bucket.get(bucket_str, 0) + count

                    for group_key, hours in local_surged.items():
                        bucket = acc_surged.setdefault(group_key, {})
                        for hour, count in hours.items():
                            bucket[hour] = bucket.get(hour, 0) + count
                batch_process_elapsed = time.monotonic() - batch_process_start
                logger.debug(f"🕴️ Sequential batch processing took {batch_process_elapsed:.3f}s")

                total_keys_processed += len(all_keys)

            elapsed = time.monotonic() - request_start
            logger.info(f"🕴️ Invasion retrieval processed {total_keys_processed} keys in {elapsed:.3f}s")

            # Final formatting/sorting
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
            logger.debug(f"🕴️ Final formatting took {format_elapsed:.3f}s")

            return {"mode": self.mode, "data": formatted}
        except Exception as e:
            logger.error(f"❌ 🕴️ Invasion retrieval failed: {e}")
            return {"mode": self.mode, "data": {}}
