from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from my_redis.utils.counter_transformer import CounterTransformer
from utils.logger import logger
from my_redis.utils import filtering_keys

redis_manager = RedisManager()

class RaidCounterRetrieval(CounterTransformer):
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        raid_pokemon: str | set[str] | None = "all",
        raid_form: str | set[str] | None = "all",
        raid_level: str | set[str] | None = "all",
        raid_costume: str | set[str] | None = "all",
        raid_is_exclusive: str | set[str] | None = "all",
        raid_ex_eligible: str | set[str] | None = "all",
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

        self.raid_pokemons     = _norm(raid_pokemon)
        self.raid_forms        = _norm(raid_form)
        self.raid_levels       = _norm(raid_level)
        self.raid_costumes     = _norm(raid_costume)
        self.raid_is_exclusive = _norm(raid_is_exclusive)
        self.raid_ex_eligible  = _norm(raid_ex_eligible)

    def _flatten_grouped(self, grouped_data: dict) -> dict:
        """
        Flattens a nested dictionary (grouped by Redis key) into a flat dictionary.
        """
        flat = {}
        for _, fields in grouped_data.items():
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
        def ok(rp, rl, rf, rc, rie, ree):
            return (
                (self.raid_pokemons is None     or rp  in self.raid_pokemons) and
                (self.raid_levels is None       or rl  in self.raid_levels) and
                (self.raid_forms is None        or rf  in self.raid_forms) and
                (self.raid_costumes is None     or rc  in self.raid_costumes) and
                (self.raid_is_exclusive is None or rie in self.raid_is_exclusive) and
                (self.raid_ex_eligible is None  or ree in self.raid_ex_eligible)
            )

        filtered = {}
        for key, value in raw_data.items():
            parts = key.split(":")
            if len(parts) != 7:
                continue
            rp, rl, rf, rc, rie, ree, metric = parts
            if ok(rp, rl, rf, rc, rie, ree):
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
        pattern = "counter:raid_total:*" if self.area.lower() in ["global", "all"] \
                  else f"counter:raid_total:{self.area}:*"

        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        if self.mode == "grouped" and isinstance(next(iter(raw_aggregated.values()), None), dict):
            raw_aggregated = self._flatten_grouped(raw_aggregated)

        filtered_data = self._filter_aggregated_raids(raw_aggregated)
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
        pattern = "counter:raid_hourly:*" if self.area.lower() in ["global", "all"] \
                else f"counter:raid_hourly:{self.area}:*"

        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        logger.debug(f"Hourly raid 👹 raw aggregated data (mode={self.mode}): {type(raw_aggregated)}")

        if self.mode in ["sum", "grouped"]:
            # For grouped, aggregate_keys returns nested; flatten to a single flat dict first.
            if self.mode == "grouped" and isinstance(next(iter(raw_aggregated.values()), None), dict):
                raw_aggregated = self._flatten_grouped(raw_aggregated)
                logger.debug("Flattened hourly raid 👹 grouped data for filtering")

            # Now raw_aggregated is flat: field -> count
            filtered_data = self._filter_aggregated_raids(raw_aggregated)

            if self.mode == "sum":
                logger.debug("▶️ Transforming hourly 👹 raid_totals SUM")
                final_data = self.transform_raid_totals_sum(filtered_data)
            else:
                logger.debug("▶️ Transforming hourly 👹 raid_totals GROUPED")
                final_data = self.transform_raid_totals_grouped(filtered_data)

        elif self.mode == "surged":
            # Keep nested shape; filter each inner dict separately
            # raw_aggregated: { "<redis_key:YYYYMMDDHH>": { "<field7parts>": count, ... } }
            filtered_nested = {}
            for redis_key, fields in raw_aggregated.items():
                if not isinstance(fields, dict):
                    # Defensive: skip weird entries
                    continue
                per_key_filtered = self._filter_aggregated_raids(fields)  # flat dict -> flat dict
                if per_key_filtered:
                    filtered_nested[redis_key] = per_key_filtered

            logger.debug(f"Filtered hourly 👹 raid data (surged, nested keys={len(filtered_nested)}).")
            final_data = self.transform_raids_surged_totals_hourly_by_hour(filtered_nested)

        else:
            # Fallback
            final_data = raw_aggregated

        return {"mode": self.mode, "data": final_data}
