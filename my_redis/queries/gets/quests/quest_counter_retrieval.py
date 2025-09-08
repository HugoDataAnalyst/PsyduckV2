from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from my_redis.utils.counter_transformer import CounterTransformer
from utils.logger import logger
from my_redis.utils import filtering_keys

redis_manager = RedisManager()

class QuestCounterRetrieval(CounterTransformer):
    def __init__(
        self,
        area: str,
        start: datetime,
        end: datetime,
        mode: str = "sum",
        with_ar: str = "all",  # 'true' | 'false' | 'all'
        # AR filters (CSV -> set[str] or None)
        ar_type: str | set[str] | None = "all",
        reward_ar_type: str | set[str] | None = "all",
        reward_ar_item_id: str | set[str] | None = "all",
        reward_ar_item_amount: str | set[str] | None = "all",
        reward_ar_poke_id: str | set[str] | None = "all",
        reward_ar_poke_form: str | set[str] | None = "all",
        # Normal filters (CSV -> set[str] or None)
        normal_type: str | set[str] | None = "all",
        reward_normal_type: str | set[str] | None = "all",
        reward_normal_item_id: str | set[str] | None = "all",
        reward_normal_item_amount: str | set[str] | None = "all",
        reward_normal_poke_id: str | set[str] | None = "all",
        reward_normal_poke_form: str | set[str] | None = "all",
    ):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.with_ar = with_ar  # leave scalar

        def _norm(x):
            if x is None or (isinstance(x, str) and x.lower() == "all"):
                return None
            if isinstance(x, str):
                return {x}
            return set(map(str, x))

        # AR side -> sets or None
        self.ar_types               = _norm(ar_type)
        self.reward_ar_types        = _norm(reward_ar_type)
        self.reward_ar_item_ids     = _norm(reward_ar_item_id)
        self.reward_ar_item_amounts = _norm(reward_ar_item_amount)
        self.reward_ar_poke_ids     = _norm(reward_ar_poke_id)
        self.reward_ar_poke_forms   = _norm(reward_ar_poke_form)

        # Normal side -> sets or None
        self.normal_types               = _norm(normal_type)
        self.reward_normal_types        = _norm(reward_normal_type)
        self.reward_normal_item_ids     = _norm(reward_normal_item_id)
        self.reward_normal_item_amounts = _norm(reward_normal_item_amount)
        self.reward_normal_poke_ids     = _norm(reward_normal_poke_id)
        self.reward_normal_poke_forms   = _norm(reward_normal_poke_form)

    def _match_any(self, val: str, allowed: set[str] | None) -> bool:
        return True if allowed is None else val in allowed

    def filter_quest_field(self, parts: list) -> bool:
        """
        Returns True if the given split field (list of parts) passes filtering.
        Expects parts has at least 7 elements:
           [quest_mode, f1, f2, f3, f4, f5, f6, ...]
        """
        if len(parts) < 7:
            return False

        quest_mode_key = parts[0]          # "ar" | "normal"
        f1, f2, f3, f4, f5, f6 = parts[1:7]

        if self.with_ar == "true":
            if quest_mode_key != "ar":
                return False
            return (self._match_any(f1, self.ar_types)
                    and self._match_any(f2, self.reward_ar_types)
                    and self._match_any(f3, self.reward_ar_item_ids)
                    and self._match_any(f4, self.reward_ar_item_amounts)
                    and self._match_any(f5, self.reward_ar_poke_ids)
                    and self._match_any(f6, self.reward_ar_poke_forms))

        if self.with_ar == "false":
            if quest_mode_key != "normal":
                return False
            return (self._match_any(f1, self.normal_types)
                    and self._match_any(f2, self.reward_normal_types)
                    and self._match_any(f3, self.reward_normal_item_ids)
                    and self._match_any(f4, self.reward_normal_item_amounts)
                    and self._match_any(f5, self.reward_normal_poke_ids)
                    and self._match_any(f6, self.reward_normal_poke_forms))

        # with_ar == "all": accept both modes, apply respective filters
        if quest_mode_key == "ar":
            return (self._match_any(f1, self.ar_types)
                    and self._match_any(f2, self.reward_ar_types)
                    and self._match_any(f3, self.reward_ar_item_ids)
                    and self._match_any(f4, self.reward_ar_item_amounts)
                    and self._match_any(f5, self.reward_ar_poke_ids)
                    and self._match_any(f6, self.reward_ar_poke_forms))
        if quest_mode_key == "normal":
            return (self._match_any(f1, self.normal_types)
                    and self._match_any(f2, self.reward_normal_types)
                    and self._match_any(f3, self.reward_normal_item_ids)
                    and self._match_any(f4, self.reward_normal_item_amounts)
                    and self._match_any(f5, self.reward_normal_poke_ids)
                    and self._match_any(f6, self.reward_normal_poke_forms))
        return False

    async def quest_retrieve_totals_weekly(self) -> dict:
        client = await redis_manager.check_redis_connection()
        if not client:
            logger.error("❌ Retrieval pool connection not available")
            return {"mode": self.mode, "data": {}}

        time_format = "%Y%m%d"

        if self.area.lower() in ["global", "all"]:
            pattern = "counter:quest:*"
        else:
            pattern = f"counter:quest:{self.area}:*"
        keys = await client.keys(pattern)
        keys = filtering_keys.filter_keys_by_time(keys, time_format, self.start, self.end)
        if not keys:
            return {"mode": self.mode, "data": {}}
        raw_aggregated = await filtering_keys.aggregate_keys(keys, self.mode)
        if self.mode == "sum":
            logger.debug("▶️ Transforming weekly 🔎 quest_totals SUM")
            final_data = self.transform_quest_totals_new_sum(raw_aggregated)
        elif self.mode in ["grouped", "surged"]:
            logger.debug("▶️ Transforming weekly 🔎 quest_totals GROUPED/SURGED")
            final_data = self.transform_quest_totals_sum(raw_aggregated, self.mode, filter_func=self.filter_quest_field)
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
        if self.area.lower() in ["global", "all"]:
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
            final_data = self.transform_quest_totals_new_sum(raw_aggregated)
        elif self.mode in ["grouped", "surged"]:
            logger.debug("▶️ Transforming hourly 🔎 quest_totals GROUPED/SURGED")
            final_data = self.transform_quest_totals_sum(raw_aggregated, self.mode, filter_func=self.filter_quest_field)
        else:
            logger.debug("❌ Else Block Hourly 🔎 quest_totals")
            final_data = raw_aggregated
        return {"mode": self.mode, "data": final_data}

    def transform_quest_totals_new_sum(self, raw_data: dict) -> dict:
        total = 0
        quest_mode_totals = {}
        for field, value in raw_data.items():
            parts = field.split(":")
            if len(parts) < 7:
                continue
            quest_mode_key = parts[0]
            # Use only the first 7 parts (ignoring extra parts such as date)
            # Ensure that the metric (assumed to be the 8th part if present) is 'total'
            if len(parts) >= 8 and parts[7] != "total":
                continue
            # Apply filtering using our filter_quest_field method:
            if not self.filter_quest_field(parts[:7]):
                continue
            try:
                val = int(value)
            except Exception as e:
                logger.error(f"❌ Could not convert value {value} for 🔎 quest key {field}: {e}")
                val = 0
            total += val
            quest_mode_totals[quest_mode_key] = quest_mode_totals.get(quest_mode_key, 0) + val
        return {"total": total, "quest_mode": quest_mode_totals}

