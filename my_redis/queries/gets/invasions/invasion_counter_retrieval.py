from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from my_redis.utils import filtering_keys
from my_redis.utils.counter_transformer import CounterTransformer

redis_manager = RedisManager()

class InvasionCounterRetrieval(CounterTransformer):
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        display_type: str | set[str] | None = "all",
        character: str | set[str] | None = "all",
        grunt: str | set[str] | None = "all",
        confirmed: str = "all",  # keep scalar
    ):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()

        def _norm_set(x):
            if x is None or (isinstance(x, str) and x.lower() == "all"):
                return None
            if isinstance(x, str):
                return {x}
            return set(map(str, x))

        self.display_types = _norm_set(display_type)
        self.characters    = _norm_set(character)
        self.grunts        = _norm_set(grunt)
        self.confirmed     = confirmed

    def _filter_aggregated_invasions(self, raw_data: dict) -> dict:
        """
        Filters the aggregated invasion data based on the filtering options.
        Expected key format: "display_type:character:grunt:confirmed:total"
        Only keys that match each filter (when not "all") are kept.
        """
        def ok(dt, char, gr, conf):
            return ((self.display_types is None or dt   in self.display_types) and
                    (self.characters    is None or char in self.characters)    and
                    (self.grunts        is None or gr   in self.grunts)        and
                    (self.confirmed == "all" or conf == self.confirmed))

        filtered = {}
        for key, value in raw_data.items():
            parts = key.split(":")
            if len(parts) != 5:
                continue
            dt, char, gr, conf, _metric = parts
            if ok(dt, char, gr, conf):
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
        pattern = "counter:invasion:*" if self.area.lower() in ["global", "all"] \
                  else f"counter:invasion:{self.area}:*"

        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)

        # Flatten if grouped nested
        if self.mode == "grouped" and isinstance(raw_aggregated, dict):
            first_val = next(iter(raw_aggregated.values()), None)
            if isinstance(first_val, dict):
                flat = {}
                for _, inner in raw_aggregated.items():
                    for field, value in inner.items():
                        flat[field] = flat.get(field, 0) + value
                raw_aggregated = flat

        filtered_data = self._filter_aggregated_invasions(raw_aggregated)

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
        pattern = "counter:invasion_hourly:*" if self.area.lower() in ["global", "all"] \
                  else f"counter:invasion_hourly:{self.area}:*"

        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}

        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)

        if self.mode in ["sum", "grouped"]:
            if self.mode == "grouped" and isinstance(raw_aggregated, dict):
                first_val = next(iter(raw_aggregated.values()), None)
                if isinstance(first_val, dict):
                    flat = {}
                    for _, inner in raw_aggregated.items():
                        for field, value in inner.items():
                            flat[field] = flat.get(field, 0) + value
                    raw_aggregated = flat

            filtered_data = self._filter_aggregated_invasions(raw_aggregated)
            if self.mode == "sum":
                logger.debug("▶️ Transforming hourly 🕴️ invasion_totals SUM")
                final_data = self.transform_invasion_totals_sum(filtered_data)
            else:
                logger.debug("▶️ Transforming hourly 🕴️ invasion_totals GROUPED")
                final_data = self.transform_invasion_totals_grouped(filtered_data)

        elif self.mode == "surged":
            # Filter per inner dict so SURGED respects filters
            filtered_nested = {}
            for redis_key, fields in raw_aggregated.items():
                filtered_fields = self._filter_aggregated_invasions(fields)
                if filtered_fields:
                    filtered_nested[redis_key] = filtered_fields

            logger.debug(f"Filtered hourly 🕴️ invasion data (surged, nested keys={len(filtered_nested)}).")
            final_data = self.transform_invasion_surged_totals_hourly_by_hour(filtered_nested)

        else:
            final_data = raw_aggregated

        return {"mode": self.mode, "data": final_data}
