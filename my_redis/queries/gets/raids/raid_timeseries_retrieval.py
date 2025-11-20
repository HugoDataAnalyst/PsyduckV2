import asyncio
import time
from datetime import datetime
from typing import Dict, Any, Iterable, Union
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from server_fastapi import global_state
from webhook.filter_data import WebhookFilter

redis_manager = RedisManager()

# Pipeline batch size - larger batches for better performance
PIPELINE_BATCH_SIZE = 500

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

        # store as sets (or None)
        self.raid_types    = _norm(raid_type)
        self.raid_pokemons = _norm(raid_pokemon)
        self.raid_forms    = _norm(raid_form)
        self.raid_levels   = _norm(raid_level)

        logger.info(
            f"▶️ Initialized 👹 RaidTimeSeries area={self.area}, mode={self.mode}, "
            f"raid_types={self.raid_types or 'ALL'}, raid_pokemons={self.raid_pokemons or 'ALL'}, "
            f"raid_levels={self.raid_levels or 'ALL'}, raid_forms={self.raid_forms or 'ALL'}, "
            f"start={self.start}, end={self.end}"
        )

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

    async def _process_keys_batch(
        self,
        client,
        keys: list,
        start_ts: int,
        end_ts: int,
        acc_sum: Dict[str, int],
        acc_grouped: Dict[str, int],
        acc_surged: Dict[str, int]
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
                if self.mode in ["sum", "grouped"]:
                    # For sum and grouped, use the full key
                    acc_sum[key] = acc_sum.get(key, 0) + count
                    acc_grouped[key] = acc_grouped.get(key, 0) + count

                elif self.mode == "surged":
                    # For surged mode, compute the hour from the UTC timestamp
                    hour = (ts % 86400) // 3600
                    new_key = f"{key}:{hour}"
                    acc_surged[new_key] = acc_surged.get(new_key, 0) + count

    async def raid_retrieve_timeseries(self) -> Dict[str, Any]:
        """Retrieve timeseries data using Python-side filtering with pipelining to avoid blocking Redis"""
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection failed")
            return {"mode": self.mode, "data": {}}

        # accumulators across multiple patterns
        acc_sum: Dict[str, int] = {}
        acc_grouped: Dict[str, int] = {}
        acc_surged: Dict[str, int] = {}

        try:
            start_ts = int(self.start.timestamp())
            end_ts   = int(self.end.timestamp())
            logger.debug(f"👹 Raid ⏱️ Time range: start_ts={start_ts}, end_ts={end_ts}")

            request_start = time.monotonic()
            total_keys_processed = 0

            # Process each pattern separately
            for pattern in self._build_key_patterns():
                logger.debug(f"👹 Scanning for pattern: {pattern}")

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
            logger.info(f"👹 Raid retrieval processed {total_keys_processed} keys in {elapsed:.3f}s")

            # Final formatting
            if self.mode == "sum":
                formatted = self._transform_timeseries_sum(acc_sum)
            elif self.mode == "grouped":
                formatted = self.transform_raid_totals_grouped(acc_grouped)
            elif self.mode == "surged":
                formatted = self.transform_raids_surged_totals_hourly_by_hour(acc_surged)
            else:
                formatted = {}

            return {"mode": self.mode, "data": formatted}

        except Exception as e:
            logger.error(f"❌ 👹 Raid retrieval failed: {e}")
            return {"mode": self.mode, "data": {}}
