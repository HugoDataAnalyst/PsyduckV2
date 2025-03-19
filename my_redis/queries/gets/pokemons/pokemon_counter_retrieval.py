from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils import filtering_keys
from my_redis.utils.counter_transformer import CounterTransformer

redis_manager = RedisManager()

class PokemonCounterRetrieval(CounterTransformer):
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum"):
        self.area=area
        self.start=start
        self.end=end
        self.mode=mode

    # --- Retrieval functions for totals ---

    async def retrieve_totals_hourly(self) -> dict:
        time_format = "%Y%m%d%H"
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}
        if self.area.lower() == "global":
            pattern = "counter:pokemon_hourly:*"
        else:
            pattern = f"counter:pokemon_hourly:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}
        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
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
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}

        time_format = "%Y%m%d"
        if self.area.lower() == "global":
            pattern = "counter:pokemon_total:*"
        else:
            pattern = f"counter:pokemon_total:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
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
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}

        if self.area.lower() == "global":
            pattern = "counter:tth_pokemon_hourly:*"
        else:
            pattern = f"counter:tth_pokemon_hourly:{self.area}:*"

        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)

        if self.mode in ["sum", "grouped"]:
            final_data = self.transform_aggregated_tth(raw_aggregated, self.mode)
        elif self.mode == "surged":
            final_data = self.transform_surged_tth_hourly_by_hour(raw_aggregated)
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
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}

        if self.area.lower() == "global":
            pattern = "counter:tth_pokemon:*"
        else:
            pattern = f"counter:tth_pokemon:{self.area}:*"

        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        final_data = self.transform_aggregated_tth(raw_aggregated, self.mode, self.start, self.end)
        return {"mode": self.mode, "data": final_data}

    # --- Retrieval function for Weather (monthly) ---

    async def retrieve_weather_monthly(self) -> dict:
        """
        Retrieve monthly weather counters.
        Key format: "counter:pokemon_weather_iv:{area}:{YYYYMM}:{weather_boost}"

        In "sum" mode, for each weather boost flag (0 or 1) the function sums all IV bucket counts.
        In "grouped" mode, it groups keys by month and weather boost and sums the fields.

        Returns a dictionary with aggregated data.
        """
        time_format = "%Y%m"
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}

        if self.area.lower() == "global":
            pattern = "counter:pokemon_weather_iv:*"
        else:
            pattern = f"counter:pokemon_weather_iv:{self.area}:*"

        keys = await client.keys(pattern)
        # For weather keys, use component_index=-2 to extract the YYYYMM part.
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end, component_index=-2)
        if not keys:
            return {"mode": self.mode, "data": {}}

        if self.mode == "sum":
            aggregated = {}
            for key in keys:
                # Expected key format: "counter:pokemon_weather_iv:{area}:{YYYYMM}:{weather_boost}"
                parts = key.split(":")
                if len(parts) < 5:
                    continue
                weather_boost = parts[-1]
                data = await client.hgetall(key)
                data = {k: int(v) for k, v in data.items()}
                if weather_boost not in aggregated:
                    aggregated[weather_boost] = {}
                for field, value in data.items():
                    aggregated[weather_boost][field] = aggregated[weather_boost].get(field, 0) + value
            return {"mode": "sum", "data": aggregated}

        elif self.mode == "grouped":
            grouped = {}
            for key in keys:
                parts = key.split(":")
                if len(parts) < 5:
                    continue
                month = parts[-2]  # the YYYYMM part
                weather_boost = parts[-1]
                composite_key = f"{month}:{weather_boost}"
                data = await client.hgetall(key)
                data = {k: int(v) for k, v in data.items()}
                if composite_key not in grouped:
                    grouped[composite_key] = {}
                for field, value in data.items():
                    grouped[composite_key][field] = grouped[composite_key].get(field, 0) + value
            return {"mode": "grouped", "data": grouped}
