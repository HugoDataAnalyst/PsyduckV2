from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils import filtering_keys
from my_redis.utils.counter_transformer import CounterTransformer

redis_manager = RedisManager()

class InvasionCounterRetrieval(CounterTransformer):
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 display_type: str = "all", character: str = "all", grunt: str = "all", confirmed: str = "all"):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.display_type = display_type
        self.character = character
        self.grunt = grunt
        self.confirmed = confirmed

    def _filter_aggregated_invasions(self, raw_data: dict) -> dict:
        """
        Filters the aggregated invasion data based on the filtering options.
        Expected key format: "display_type:character:grunt:confirmed:total"
        Only keys that match each filter (when not "all") are kept.
        """
        filtered = {}
        for key, value in raw_data.items():
            parts = key.split(":")
            if len(parts) != 5:
                continue
            dt, char, gr, conf, metric = parts
            if (self.display_type != "all" and self.display_type != dt):
                continue
            if (self.character != "all" and self.character != char):
                continue
            if (self.grunt != "all" and self.grunt != gr):
                continue
            if (self.confirmed != "all" and self.confirmed != conf):
                continue
            filtered[key] = value
        return filtered

    async def invasion_retrieve_totals_weekly(self) -> dict:
        """
        Retrieve weekly invasion totals.
        Key format: "counter:invasion:{area}:{YYYYMMDD}"
        """
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}
        time_format = "%Y%m%d"
        if self.area.lower() in ["global", "all"]:
            pattern = "counter:invasion:*"
        else:
            pattern = f"counter:invasion:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}
        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        # If mode is grouped and raw data is nested, flatten it.
        if self.mode == "grouped" and isinstance(raw_aggregated, dict):
            first_val = next(iter(raw_aggregated.values()), None)
            if isinstance(first_val, dict):
                flat = {}
                for k, v in raw_aggregated.items():
                    for field, value in v.items():
                        flat[field] = flat.get(field, 0) + value
                raw_aggregated = flat
        # Apply filtering if any invasion filter is active.
        if (self.display_type == "all" and self.character == "all" and
            self.grunt == "all" and self.confirmed == "all"):
            filtered_data = raw_aggregated
        else:
            filtered_data = self._filter_aggregated_invasions(raw_aggregated)
        logger.debug(f"Filtered weekly 🕴️ invasion data: {filtered_data}")
        if self.mode == "sum":
            logger.debug("▶️ Transforming weekly 🕴️ invasion_totals SUM")
            final_data = self.transform_invasion_totals_sum(filtered_data)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming weekly 🕴️ invasion_totals GROUPED")
            final_data = self.transform_invasion_totals_grouped(filtered_data)
        else:
            logger.debug("❌ Else Block weekly 🕴️ invasion_totals")
            final_data = filtered_data
        return {"mode": self.mode, "data": final_data}

    async def invasion_retrieve_totals_hourly(self) -> dict:
        """
        Retrieve hourly invasion totals.
        Key format: "counter:invasion_hourly:{area}:{YYYYMMDDHH}"
        """
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}
        time_format = "%Y%m%d%H"
        if self.area.lower() in ["global", "all"]:
            pattern = "counter:invasion_hourly:*"
        else:
            pattern = f"counter:invasion_hourly:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}
        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        # For grouped mode, flatten if necessary.
        if self.mode == "grouped" and isinstance(raw_aggregated, dict):
            first_val = next(iter(raw_aggregated.values()), None)
            if isinstance(first_val, dict):
                flat = {}
                for k, v in raw_aggregated.items():
                    for field, value in v.items():
                        flat[field] = flat.get(field, 0) + value
                raw_aggregated = flat
        if (self.display_type == "all" and self.character == "all" and
            self.grunt == "all" and self.confirmed == "all"):
            filtered_data = raw_aggregated
        else:
            filtered_data = self._filter_aggregated_invasions(raw_aggregated)
        logger.debug(f"Filtered hourly 🕴️ invasion data: {filtered_data}")
        if self.mode == "sum":
            logger.debug("▶️ Transforming hourly 🕴️ invasion_totals SUM")
            final_data = self.transform_invasion_totals_sum(filtered_data)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming hourly 🕴️ invasion_totals GROUPED")
            final_data = self.transform_invasion_totals_grouped(filtered_data)
        elif self.mode == "surged":
            logger.debug("▶️ Transforming hourly 🕴️ invasion_totals SURGED")
            final_data = self.transform_invasion_surged_totals_hourly_by_hour(raw_aggregated)
        else:
            logger.debug("❌ Else Block Hourly 🕴️ invasion_totals")
            final_data = filtered_data
        return {"mode": self.mode, "data": final_data}
