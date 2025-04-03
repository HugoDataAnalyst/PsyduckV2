from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from my_redis.utils.counter_transformer import CounterTransformer
from utils.logger import logger
from my_redis.utils import filtering_keys

redis_manager = RedisManager()

class RaidCounterRetrieval(CounterTransformer):
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 raid_pokemon: str = "all", raid_form: str = "all", raid_level: str = "all",
                 raid_costume: str = "all", raid_is_exclusive: str = "all", raid_ex_eligible: str = "all"):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()  # ensure mode is lower-case
        self.raid_pokemon = raid_pokemon
        self.raid_form = raid_form
        self.raid_level = raid_level
        self.raid_costume = raid_costume
        self.raid_is_exclusive = raid_is_exclusive
        self.raid_ex_eligible = raid_ex_eligible

    def _flatten_grouped(self, grouped_data: dict) -> dict:
        """
        Flattens a nested dictionary (grouped by Redis key) into a flat dictionary.
        """
        flat = {}
        for redis_key, fields in grouped_data.items():
            for field, value in fields.items():
                flat[field] = flat.get(field, 0) + value
        return flat

    def _filter_aggregated_raids(self, raw_data: dict) -> dict:
        """
        Filters the aggregated raid data based on the raid filtering options.
        Expected key format:
          "raid_pokemon:raid_level:raid_form:raid_costume:raid_is_exclusive:raid_ex_eligible:metric"
        Only keys that match each filter (when not "all") are kept.
        """
        filtered = {}
        for key, value in raw_data.items():
            parts = key.split(":")
            if len(parts) != 7:
                continue  # Skip keys not following the expected format.
            rp, rl, rf, rc, rie, ree, metric = parts
            if (self.raid_pokemon != "all" and self.raid_pokemon != rp):
                continue
            if (self.raid_form != "all" and self.raid_form != rf):
                continue
            if (self.raid_level != "all" and self.raid_level != rl):
                continue
            if (self.raid_costume != "all" and self.raid_costume != rc):
                continue
            if (self.raid_is_exclusive != "all" and self.raid_is_exclusive != rie):
                continue
            if (self.raid_ex_eligible != "all" and self.raid_ex_eligible != ree):
                continue
            filtered[key] = value
        return filtered

    async def raid_retrieve_totals_weekly(self) -> dict:
        """
        Retrieve weekly raid totals.
        Key format: "counter:raid_total:{area}:{YYYYMMDD}"
        """
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}
        time_format = "%Y%m%d"
        if self.area.lower() in ["global", "all"]:
            pattern = "counter:raid_total:*"
        else:
            pattern = f"counter:raid_total:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}
        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        logger.debug(f"Weekly raid 👹 raw aggregated data: {raw_aggregated}")

        # If mode is grouped and the data is nested, flatten it.
        if self.mode == "grouped" and isinstance(raw_aggregated, dict):
            # Check if values are dicts (i.e. grouped) and flatten if needed.
            first_val = next(iter(raw_aggregated.values()), None)
            if isinstance(first_val, dict):
                raw_aggregated = self._flatten_grouped(raw_aggregated)
                logger.debug(f"Flattened raid 👹 grouped data: {raw_aggregated}")

        # Apply filtering only if any filter is active.
        if (self.raid_pokemon == "all" and self.raid_form == "all" and
            self.raid_level == "all" and self.raid_costume == "all" and
            self.raid_is_exclusive == "all" and self.raid_ex_eligible == "all"):
            filtered_data = raw_aggregated
        else:
            filtered_data = self._filter_aggregated_raids(raw_aggregated)
        logger.debug(f"Filtered weekly 👹 raid data: {filtered_data}")

        if self.mode == "sum":
            logger.debug("▶️ Transforming weekly 👹 raid_totals SUM")
            final_data = self.transform_raid_totals_sum(filtered_data)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming weekly 👹 raid_totals GROUPED")
            final_data = self.transform_raid_totals_grouped(filtered_data)
        else:
            logger.debug("❌ Else Block weekly 👹 raid_totals")
            final_data = filtered_data
        return {"mode": self.mode, "data": final_data}

    async def raid_retrieve_totals_hourly(self) -> dict:
        """
        Retrieve hourly raid totals.
        Key format: "counter:raid_hourly:{area}:{YYYYMMDDHH}"
        """
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}
        time_format = "%Y%m%d%H"
        if self.area.lower() in ["global", "all"]:
            pattern = "counter:raid_hourly:*"
        else:
            pattern = f"counter:raid_hourly:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}
        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        logger.debug(f"Hourly raid 👹 raw aggregated data: {raw_aggregated}")

        if self.mode == "grouped" and isinstance(raw_aggregated, dict):
            first_val = next(iter(raw_aggregated.values()), None)
            if isinstance(first_val, dict):
                raw_aggregated = self._flatten_grouped(raw_aggregated)
                logger.debug(f"Flattened hourly raid 👹 grouped data: {raw_aggregated}")

        if (self.raid_pokemon == "all" and self.raid_form == "all" and
            self.raid_level == "all" and self.raid_costume == "all" and
            self.raid_is_exclusive == "all" and self.raid_ex_eligible == "all"):
            filtered_data = raw_aggregated
        else:
            filtered_data = self._filter_aggregated_raids(raw_aggregated)
        logger.debug(f"Filtered hourly 👹 raid data: {filtered_data}")

        if self.mode == "sum":
            logger.debug("▶️ Transforming hourly 👹 raid_totals SUM")
            final_data = self.transform_raid_totals_sum(filtered_data)
        elif self.mode == "grouped":
            logger.debug("▶️ Transforming hourly 👹 raid_totals GROUPED")
            final_data = self.transform_raid_totals_grouped(filtered_data)
        elif self.mode == "surged":
            logger.debug("▶️ Transforming hourly 👹 raid_totals SURGEDr")
            final_data = self.transform_raids_surged_totals_hourly_by_hour(filtered_data)
        else:
            logger.debug("❌ Else Block Hourly 👹 raid_totals")
            final_data = filtered_data
        return {"mode": self.mode, "data": final_data}
