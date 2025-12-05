import asyncio
from datetime import datetime, timedelta
from typing import final
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils import filtering_keys
from my_redis.utils.counter_transformer import CounterTransformer

redis_manager = RedisManager()

# Configuration
USE_SCAN_INSTEAD_OF_KEYS = True  # Use SCAN (non-blocking) instead of KEYS (blocking)
CONCURRENT_BATCH_SIZE = 500  # Batch size for concurrent operations


class PokemonCounterRetrieval(CounterTransformer):
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        pokemon_id: str | set[str] | None = "all",
        form: str | set[str] | None = "all",
        metric: str | set[str] | None = "all",
    ):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode

        def _norm(x):
            # None or "all" => no filtering
            if x is None or (isinstance(x, str) and x.lower() == "all"):
                return None
            if isinstance(x, str):
                # single value -> {value}
                return {x}
            # iterable -> set of str
            return set(map(str, x))

        # store everything as sets (or None) with plural names
        self.pokemon_ids = _norm(pokemon_id)
        self.forms       = _norm(form)
        self.metrics     = _norm(metric)

    async def _get_keys(self, client, pattern: str) -> list:
        """
        Get keys matching pattern using either SCAN (non-blocking) or KEYS.
        SCAN is preferred for production as it doesn't block Redis.
        """
        if USE_SCAN_INSTEAD_OF_KEYS:
            return await filtering_keys.scan_keys(client, pattern)
        else:
            return await client.keys(pattern)

    def _filter_aggregated_data(self, raw_data: dict) -> dict:
        """
        Filters the aggregated data based on self.pokemon_id and self.form.
        Works for both flat (sum mode) and nested (grouped/surged mode) dictionaries.
        Expected key format: "pokemon_id:form:metric".
        If the key does not match this format (or if the pokemon_id is non-numeric),
        the field is left unfiltered.
        """
        def ok(pid, form_val, metric_val):
            return ((self.pokemon_ids is None or pid in self.pokemon_ids) and
                    (self.forms is None or form_val in self.forms) and
                    (self.metrics is None or metric_val in self.metrics))

        if self.mode == "sum":
            filtered = {}
            for field, value in raw_data.items():
                parts = field.split(":")
                if len(parts) < 3 or not parts[0].isdigit():
                    # If the field doesn't follow the expected pattern, keep it as is.
                    filtered[field] = value
                    continue
                pid, form_val, metric_val = parts[:3]
                if ok(pid, form_val, metric_val):
                    filtered[field] = value
            return filtered
        else:
            filtered = {}
            for redis_key, fields in raw_data.items():
                filtered_fields = {}
                for field, value in fields.items():
                    parts = field.split(":")
                    if len(parts) < 3 or not parts[0].isdigit():
                        filtered_fields[field] = value
                        continue
                    pid, form_val, metric_val = parts[:3]
                    if ok(pid, form_val, metric_val):
                        filtered_fields[field] = value
                if filtered_fields:
                    filtered[redis_key] = filtered_fields
            return filtered

    def _filter_weather_fields(self, fields: dict) -> dict:
        """
        Filters weather hash fields based solely on metric.
        The expected inner field format is assumed to be "pokemon_id:form:metric" but
        only the metric component is checked.
        """
        # metrics is a set or None now
        if self.metrics is None:
            return fields
        filtered = {}
        for field, value in fields.items():
            parts = field.split(":")
            metric_val = parts[2] if len(parts) >= 3 else None
            if metric_val is None or metric_val in self.metrics:
                filtered[field] = value
        return filtered

    def _filter_tth_data(self, data: dict) -> dict:
        """
        Filters aggregated TTH data based on the metric.
        For 'sum' or 'grouped' modes, data is a flat dictionary with keys as TTH buckets.
        For 'surged' mode, data is a dictionary with hours as keys and nested dictionaries as values.
        """
        if self.metrics is None:
            return data

        if self.mode in ["sum", "grouped"]:
            return {bucket: v for bucket, v in data.items() if bucket in self.metrics}
        if self.mode == "surged":
            filtered = {}
            for hour, buckets in data.items():
                subset = {b: v for b, v in buckets.items() if b in self.metrics}
                if subset:
                    filtered[hour] = subset
            return filtered

        return data

    # --- Retrieval functions for totals ---

    async def retrieve_totals_hourly(self) -> dict:
        """
        Retrieve hourly totals for Pokémon counters.
        Key format: "counter:pokemon_total:{area}:{YYYYMMDDHH}"
        """
        time_format = "%Y%m%d%H"
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection not available")
            return {"mode": self.mode, "data": {}}
        if self.area.lower() in ["global", "all"]:
            pattern = "counter:pokemon_hourly:*"
        else:
            pattern = f"counter:pokemon_hourly:{self.area}:*"

        keys = await self._get_keys(client, pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        # Apply filtering by pokemon_id and form
        raw_aggregated = self._filter_aggregated_data(raw_aggregated)
        if self.mode in ["sum", "grouped"]:
            final_data = self.transform_aggregated_totals(raw_aggregated, self.mode)
        elif self.mode == "surged":
            final_data = self.transform_surged_totals_hourly_by_hour(raw_aggregated)
        return {"mode": self.mode, "data": final_data}

    async def retrieve_totals_weekly(self) -> dict:
        """
        Retrieve weekly totals for Pokémon counters.
        Key format: "counter:pokemon_total:{area}:{YYYYMMDD}"
        Uses the "retrieval_pool" for Redis operations.

        In SUM mode, the function aggregates all hash fields from all matching keys and then
        groups them by metric (the third component, e.g. "total", "iv100", etc.).

        In GROUPED mode, the function combines data from all keys into a single dictionary
        keyed by the full field (e.g., "1:163:total") summing counts across keys, then sorts
        the final result by pokemon_id (the first component).
        """
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection not available")
            return {"mode": self.mode, "data": {}}

        time_format = "%Y%m%d"
        if self.area.lower() in ["global", "all"]:
            pattern = "counter:pokemon_total:*"
        else:
            pattern = f"counter:pokemon_total:{self.area}:*"

        keys = await self._get_keys(client, pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        raw_aggregated = self._filter_aggregated_data(raw_aggregated)
        final_data = self.transform_aggregated_totals(raw_aggregated, self.mode)
        return {"mode": self.mode, "data": final_data}

    # --- Retrieval functions for TTH ---

    async def retrieve_tth_hourly(self) -> dict:
        """
        Retrieve hourly TTH counters.
        Key format: "counter:tth_pokemon_hourly:{area}:{YYYYMMDDHH}"

        In "sum" mode, values are summed across matching keys.
        In "grouped" mode, only hours that have data are combined into a dictionary keyed by the full hour (e.g. "2025031718")
        and then re-labeled sequentially if needed.
        In "surged" mode, data is grouped by the actual hour of day (e.g. "18") across all keys (regardless of date).
        """
        time_format = "%Y%m%d%H"
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection not available")
            return {"mode": self.mode, "data": {}}

        if self.area.lower() in ["global", "all"]:
            pattern = "counter:tth_pokemon_hourly:*"
        else:
            pattern = f"counter:tth_pokemon_hourly:{self.area}:*"

        keys = await self._get_keys(client, pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)

        if self.mode in ["sum", "grouped"]:
            final_data = self.transform_aggregated_tth(raw_aggregated, self.mode)
            final_data = self._filter_tth_data(final_data)
        elif self.mode == "surged":
            final_data = self.transform_surged_tth_hourly_by_hour(raw_aggregated)
            final_data = self._filter_tth_data(final_data)
        else:
            final_data = raw_aggregated

        return {"mode": self.mode, "data": final_data}


    async def retrieve_tth_weekly(self) -> dict:
        """
        Retrieve weekly TTH counters.
        Key format: "counter:tth_pokemon:{area}:{YYYYMMDD}"
        For mode "grouped", data is grouped by day and the average per field is computed.
        Always returns a complete timeline (each day in the requested range).
        """
        time_format = "%Y%m%d"
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection not available")
            return {"mode": self.mode, "data": {}}

        if self.area.lower() in ["global", "all"]:
            pattern = "counter:tth_pokemon:*"
        else:
            pattern = f"counter:tth_pokemon:{self.area}:*"

        keys = await self._get_keys(client, pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        final_data = self.transform_aggregated_tth(raw_aggregated, self.mode, self.start, self.end)
        final_data = self._filter_tth_data(final_data)
        return {"mode": self.mode, "data": final_data}

    # --- Retrieval function for Weather (monthly) - OPTIMIZED ---

    async def retrieve_weather_monthly(self) -> dict:
        """
        Retrieve monthly weather counters CONCURRENTLY.
        Key format: "counter:pokemon_weather_iv:{area}:{YYYYMM}:{weather_boost}"

        In "sum" mode, for each weather boost flag (0 or 1) the function sums all IV bucket counts.
        In "grouped" mode, it groups keys by month and weather boost and sums the fields.

        Returns a dictionary with aggregated data.

        OPTIMIZED: Uses pipeline/concurrent fetching instead of sequential.
        """
        time_format = "%Y%m"
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Redis connection not available")
            return {"mode": self.mode, "data": {}}

        pattern = "counter:pokemon_weather_iv:*" if self.area.lower() in ["global", "all"] \
                  else f"counter:pokemon_weather_iv:{self.area}:*"

        keys = await self._get_keys(client, pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end, component_index=-2)
        if not keys:
            return {"mode": self.mode, "data": {}}

        # Pre-filter keys by weather boost metric if specified
        if self.metrics is not None:
            keys = [k for k in keys if k.split(":")[-1] in self.metrics]

        if not keys:
            return {"mode": self.mode, "data": {}}

        # Fetch all data concurrently using pipeline
        all_data = await filtering_keys._fetch_keys_pipeline(client, keys)

        if self.mode == "sum":
            aggregated = {}
            for key, data in all_data.items():
                parts = key.split(":")
                if len(parts) < 5:
                    continue
                weather_boost = parts[-1]  # "0".."9"

                # Convert to int and filter fields
                data = {k: int(v) for k, v in data.items()}
                data = self._filter_weather_fields(data)

                if weather_boost not in aggregated:
                    aggregated[weather_boost] = {}
                for field, value in data.items():
                    aggregated[weather_boost][field] = aggregated[weather_boost].get(field, 0) + value
            return {"mode": "sum", "data": aggregated}

        elif self.mode == "grouped":
            grouped = {}
            for key, data in all_data.items():
                parts = key.split(":")
                if len(parts) < 5:
                    continue
                month = parts[-2]
                weather_boost = parts[-1]

                composite_key = f"{month}:{weather_boost}"

                # Convert to int and filter fields
                data = {k: int(v) for k, v in data.items()}
                data = self._filter_weather_fields(data)

                if composite_key not in grouped:
                    grouped[composite_key] = {}
                for field, value in data.items():
                    grouped[composite_key][field] = grouped[composite_key].get(field, 0) + value
            return {"mode": "grouped", "data": grouped}

        return {"mode": self.mode, "data": {}}
