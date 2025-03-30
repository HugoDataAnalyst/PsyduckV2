from datetime import datetime, timedelta
from typing import Counter
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils import filtering_keys
from my_redis.utils.counter_transformer import CounterTransformer

redis_manager = RedisManager()

class InvasionCounterRetrieval(CounterTransformer):
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum"):
        self.area=area
        self.start=start
        self.end=end
        self.mode=mode


    # --- Retrieval functions for totals ---
    async def invasion_retrieve_totals_weekly(self) -> dict:
        """
        Retrieve weekly invasions totals.

        Key format: "counter:invasion:{area}:{YYYYMMDD}"
        Uses the "retrieval_pool" for Redis operations.

        In "sum" mode, it aggregates all fields and groups them by the metric (the last component).
        In "grouped" mode, it combines data from all keys into one dictionary keyed by the full field,
        then sorts the result by the numeric value of the first component.
        """
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}

        time_format = "%Y%m%d"

        if self.area.lower() == "global":
            pattern = "counter:invasion:*"
        else:
            pattern = f"counter:invasion:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        if self.mode == "sum":
            logger.debug("▶️ Transforming weekly 🕴️ invasion_totals SUM")
            final_data = self.transform_invasion_totals_sum(raw_aggregated)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming weekly 🕴️ invasion_totals GROUPED")
            final_data = self.transform_aggregated_totals(raw_aggregated, self.mode)
        else:
            logger.debug("❌ Else Block weekly 🕴️ invasion_totals")
            final_data = raw_aggregated
        return {"mode": self.mode, "data": final_data}

    async def invasion_retrieve_totals_hourly(self) -> dict:
        """
        Retrieve hourly invasions totals.

        Key format: "counter:invasion_hourly:{area}:{YYYYMMDDHH}"
        Uses the "retrieval_pool" for Redis operations.

        In "sum" mode, it aggregates all fields and groups them by the metric.
        In "grouped" mode, it groups the data by the actual hour extracted from the key.
        In "surged" mode (if desired), you could implement similar logic to group by the actual hour across days.
        Here, for demonstration, we'll support "sum" and "grouped".
        """
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}

        time_format = "%Y%m%d%H"
        if self.area.lower() == "global":
            pattern = "counter:invasion_hourly:*"
        else:
            pattern = f"counter:invasion_hourly:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        if self.mode == "sum":
            logger.debug("▶️ Transforming hourly 🕴️ invasion_totals SUM")
            final_data = self.transform_invasion_totals_sum(raw_aggregated)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming hourly 🕴️ invasion_totals GROUPED")
            final_data = self.transform_aggregated_totals(raw_aggregated, self.mode)
        elif self.mode == "surged":
            # If you want a surged mode for raids as well, you can implement a similar helper.
            logger.debug("▶️ Transforming hourly 🕴️ invasion_totals SURGED")
            final_data = self.transform_surged_totals_hourly_by_hour(raw_aggregated)
        else:
            logger.debug("❌ Else Block Hourly 🕴️ invasion_totals")
            final_data = raw_aggregated
        return {"mode": self.mode, "data": final_data}
