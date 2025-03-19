from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from my_redis.utils.counter_transformer import CounterTransformer
from utils.logger import logger
from my_redis.utils import filtering_keys

redis_manager = RedisManager()

class RaidCounterRetrieval(CounterTransformer):
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum"):
        self.area=area
        self.start=start
        self.end=end
        self.mode=mode

    async def raid_retrieve_totals_weekly(self) -> dict:
        """
        Retrieve weekly raid totals.

        Key format: "counter:raid_total:{area}:{YYYYMMDD}"
        Uses the "retrieval_pool" for Redis operations.

        In "sum" mode, it aggregates all fields and groups them by the metric (the last component).
        In "grouped" mode, it combines data from all keys into one dictionary keyed by the full field,
        then sorts the result by the numeric value of the first component.
        """
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}

        time_format = "%Y%m%d"

        if self.area.lower() == "global":
            pattern = "counter:raid_total:*"
        else:
            pattern = f"counter:raid_total:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        if self.mode == "sum":
            logger.debug("▶️ Transforming weekly raid_totals SUM")
            final_data = self.transform_raid_totals_sum(raw_aggregated)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming weekly raid_totals GROUPED")
            final_data = self.transform_aggregated_totals(raw_aggregated, self.mode)
        else:
            logger.debug("❌ Else Block weekly raid_totals")
            final_data = raw_aggregated
        return {"mode": self.mode, "data": final_data}

    async def raid_retrieve_totals_hourly(self) -> dict:
        """
        Retrieve hourly raid totals.

        Key format: "counter:raid_hourly:{area}:{YYYYMMDDHH}"
        Uses the "retrieval_pool" for Redis operations.

        In "sum" mode, it aggregates all fields and groups them by the metric.
        In "grouped" mode, it groups the data by the actual hour extracted from the key.
        In "surged" mode (if desired), you could implement similar logic to group by the actual hour across days.
        Here, for demonstration, we'll support "sum" and "grouped".
        """
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}

        time_format = "%Y%m%d%H"
        if self.area.lower() == "global":
            pattern = "counter:raid_hourly:*"
        else:
            pattern = f"counter:raid_hourly:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        if self.mode == "sum":
            logger.debug("▶️ Transforming hourly raid_totals SUM")
            final_data = self.transform_raid_totals_sum(raw_aggregated)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming hourly raid_totals GROUPED")
            final_data = self.transform_aggregated_totals(raw_aggregated, self.mode)
        elif self.mode == "surged":
            # If you want a surged mode for raids as well, you can implement a similar helper.
            logger.debug("▶️ Transforming hourly raid_totals SURGED")
            final_data = self.transform_surged_totals_hourly_by_hour(raw_aggregated)
        else:
            logger.debug("❌ Else Block Hourly raid_totals")
            final_data = raw_aggregated
        return {"mode": self.mode, "data": final_data}
