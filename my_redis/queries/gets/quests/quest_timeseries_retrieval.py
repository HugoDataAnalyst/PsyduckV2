import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Iterable, Union
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from server_fastapi import global_state

redis_manager = RedisManager()

# Pipeline batch size - balance between throughput and allowing writes to interleave
PIPELINE_BATCH_SIZE = 150

class QuestTimeSeries:
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        quest_mode: str = "all",
        field_details: Union[str, Iterable[str], None] = "all",
    ):
        """
        Parameters:
          - area: Area name filter. Use "all" or "global" to match every area.
          - start, end: Datetime objects for the time range.
          - mode: Aggregation mode: "sum", "grouped", or "surged".
          - quest_mode: Quest mode—either "ar" or "normal". If set to "all", a wildcard is used.
          - field_details: The quest type to filter for (i.e. the first field in field_details).
                         If "all", no filtering on quest type is done.

        The keys are stored in the new format:
            ts:quests_total:{quest_mode}:{area}:{field_details}
        For retrieval, if field_details is not "all", we build a pattern as:
            {field_details}:*:*:*:*:*
        so that only keys with that quest type (first field) are returned.
        """
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.quest_mode = quest_mode.lower()

        def _norm(x):
            if x is None or (isinstance(x, str) and x.lower() == "all"):
                return None
            if isinstance(x, str):
                return {x}
            return set(map(str, x))

        # multi-select quest types (we only constrain f1 in the 6-field segment)
        self.quest_types = _norm(field_details)

        logger.info(
            f"▶️ Initialized 🔎 QuestTimeSeries area={self.area}, mode={self.mode}, "
            f"quest_mode={self.quest_mode}, quest_types={self.quest_types or 'ALL'}, "
            f"range={self.start}..{self.end}"
        )

    def _build_key_patterns(self) -> list[str]:
        # Replace "all"/"global" with wildcard for area; wildcard quest_mode if "all"
        area = "*" if self.area.lower() in ["all", "global"] else self.area
        quest_mode = "*" if self.quest_mode == "all" else self.quest_mode

        # field_details pattern: if quest_types is None -> "*:*:*:*:*:*"
        # else -> for each quest_type -> "{quest_type}:*:*:*:*:*"
        if self.quest_types is None:
            fd_patterns = ["*:*:*:*:*:*"]
        else:
            fd_patterns = [f"{qt}:*:*:*:*:*" for qt in self.quest_types]

        patterns = [f"ts:quests_total:{quest_mode}:{area}:{fd}" for fd in fd_patterns]
        logger.debug(f"Built 🔎 Quest {len(patterns)} key pattern(s): {patterns[:5]}{'...' if len(patterns)>5 else ''}")
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
                    acc_sum[key] = acc_sum.get(key, 0) + count

                elif self.mode == "grouped":
                    bucket = acc_grouped.setdefault(key, {})
                    ts_bucket = str(ts)
                    bucket[ts_bucket] = bucket.get(ts_bucket, 0) + count

                elif self.mode == "surged":
                    bucket = acc_surged.setdefault(key, {})
                    hour = str((ts % 86400) // 3600)
                    bucket[hour] = bucket.get(hour, 0) + count

    async def quest_retrieve_timeseries(self) -> Dict[str, Any]:
        """Retrieve timeseries data using Python-side filtering with pipelining to avoid blocking Redis"""
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
            return {"mode": self.mode, "data": {}}

        # accumulators to merge across multiple patterns
        acc_sum: Dict[str, int] = {}
        acc_grouped: Dict[str, Dict[str, int]] = {}
        acc_surged: Dict[str, Dict[str, int]] = {}

        try:
            start_ts = int(self.start.timestamp())
            end_ts   = int(self.end.timestamp())
            logger.debug(f"🔎 Quest ⏱️ Time range: start_ts={start_ts}, end_ts={end_ts}")

            request_start = time.monotonic()
            total_keys_processed = 0

            # Process each pattern separately
            for pattern in self._build_key_patterns():
                logger.debug(f"🔎 Scanning for pattern: {pattern}")

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
            logger.info(f"🔎 Quest retrieval processed {total_keys_processed} keys in {elapsed:.3f}s")

            # Final formatting
            if self.mode == "sum":
                if self.area.lower() in ["all", "global"]:
                    area_totals: Dict[str, int] = {}
                    quest_grand_total = 0
                    for key, v in acc_sum.items():
                        parts = key.split(":")
                        key_area = parts[3] if len(parts) > 3 else "unknown"
                        val = int(v)
                        area_totals[key_area] = area_totals.get(key_area, 0) + val
                        quest_grand_total += val
                    pokestops_data = global_state.cached_pokestops or {"areas": {}, "pokestop_grand_total": 0}
                    formatted = {
                        "areas": area_totals,
                        "quest_grand_total": quest_grand_total,
                        "total pokestops": pokestops_data,
                    }
                else:
                    total_sum = sum(int(v) for v in acc_sum.values())
                    formatted = {"total": total_sum}

            elif self.mode == "grouped":
                formatted = {}
                for k, groups in acc_grouped.items():
                    ordered = dict(sorted(groups.items(), key=lambda x: int(x[0])))
                    formatted[k] = ordered
                formatted = dict(sorted(formatted.items(), key=lambda item: item[0]))

            elif self.mode == "surged":
                formatted = {}
                for k, hours in acc_surged.items():
                    labeled = {f"hour {int(h)}": v for h, v in hours.items()}
                    formatted[k] = dict(sorted(labeled.items(), key=lambda x: int(x[0].split()[1])))
                formatted = dict(sorted(formatted.items(), key=lambda item: item[0]))

            else:
                formatted = {}

            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"❌ 🔎 Quest retrieval failed: {e}", exc_info=True)
            return {"mode": self.mode, "data": {}}
