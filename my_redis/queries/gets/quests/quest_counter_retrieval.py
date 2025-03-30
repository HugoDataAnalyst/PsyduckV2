from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from my_redis.utils.counter_transformer import CounterTransformer
from utils.logger import logger
from my_redis.utils import filtering_keys

redis_manager = RedisManager()

class QuestCounterRetrieval(CounterTransformer):
    def __init__(self, area: str, start: int, end: int, mode: str = "sum"):
        self.area=area
        self.start=start
        self.end=end
        self.mode=mode

    # --- Retrieval functions for totals ---
    async def quest_retrieve_totals_weekly(self) -> dict:
        """
        Retrieve weekly quest totals.

        Key format: "counter:quest:{area}:{YYYYMMDD}"
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
            pattern = "counter:quest:*"
        else:
            pattern = f"counter:quest:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}  # Return empty data if no keys found

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        if self.mode == "sum":
            logger.debug("▶️ Transforming weekly 🔎 quest_totals SUM")
            final_data = self.transform_quest_totals_sum(raw_aggregated, self.mode)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming weekly 🔎 quest_totals GROUPED")
            final_data = self.transform_quest_totals_sum(raw_aggregated, self.mode)
        else:
            logger.debug("❌ Else Block weekly 🔎 quest_totals")
            final_data = raw_aggregated
        return {"mode": self.mode, "data": final_data}

    async def quest_retrieve_totals_hourly(self) -> dict:
        """
        Retrieve hourly quest totals.

        Key format: "counter:quest_hourly:{area}:{YYYYMMDDHH}"
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
            pattern = "counter:quest_hourly:*"
        else:
            pattern = f"counter:quest_hourly:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        if self.mode == "sum":
            logger.debug("▶️ Transforming hourly 🔎 quest_totals SUM")
            final_data = self.transform_quest_totals_sum(raw_aggregated, self.mode)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming hourly 🔎 quest_totals GROUPED")
            final_data = self.transform_quest_totals_sum(raw_aggregated, self.mode)
        elif self.mode == "surged":
            # If you want a surged mode for quests as well, you can implement a similar helper.
            logger.debug("▶️ Transforming hourly 🔎 quest_totals SURGED")
            final_data = self.transform_quest_totals_sum(raw_aggregated, self.mode)
        else:
            logger.debug("❌ Else Block Hourly 🔎 quest_totals")
            final_data = raw_aggregated
        return {"mode": self.mode, "data": final_data}
