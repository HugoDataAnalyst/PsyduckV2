from collections import OrderedDict
from datetime import datetime
from my_redis.connect_redis import RedisManager
from my_redis.utils.counter_transformer import CounterTransformer
from utils.logger import logger

redis_manager = RedisManager()

class QuestTimeSeries(CounterTransformer):
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 quest_mode: str = "all", quest_type: str = "all"):
        """
        quest_mode: "ar", "normal", or "all"
        quest_type: a filter on the reward type (as stored in the detailed key’s label "reward"), or "all"
        """
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.quest_mode = quest_mode.lower()
        self.quest_type = quest_type.lower()

    async def quest_retrieve_timeseries(self) -> dict:
        """
        Retrieve Quest TimeSeries data.

        Overall series:
          - Keys are created as:
                ts:quests_total:total:{area}:{mode}
          - When area is global, no area filter is applied.

        Detailed series:
          - If quest_mode=="ar", we query keys with metric "quest_ar_detailed"
          - If quest_mode=="normal", we query keys with metric "quest_normal_detailed"
          - If quest_mode=="all", we query both.
          - If quest_type != "all", a filter is added on the "reward" label.

        Modes:
          - "sum": overall series are summed and split by quest mode;
                   detailed series are summed and then grouped by the full field‐details identifier.
          - "grouped": overall series are aggregated by quest mode (without time breakdown)
                       and detailed series are grouped by their full field details.
          - "surged": (not shown here) would group by hour.
        """
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Redis connection not available for Quest TimeSeries retrieval.")
            return {"mode": self.mode, "data": {}}

        start_ms = int(self.start.timestamp() * 1000)
        end_ms   = int(self.end.timestamp() * 1000)
        logger.info(f"Querying from {start_ms} to {end_ms} for area '{self.area}' using mode '{self.mode}'")

        results = {}

        ### Query Overall Series ###
        overall_filters = ["metric=quest_total"]
        if self.area.lower() != "global":
            overall_filters.append(f"area={self.area}")
        try:
            overall_data = await client.execute_command("TS.MRANGE", start_ms, end_ms, "FILTER", *overall_filters)
            logger.info(f"Raw TS.MRANGE data for overall (metric=quest_total): {overall_data}")
            results["total"] = overall_data
            logger.info("🔑 Retrieved TS.MRANGE data for overall series")
        except Exception as e:
            logger.error(f"❌ Error retrieving overall TS.MRANGE data: {e}")
            results["total"] = 0 if self.mode == "sum" else []

        ### Query Detailed Series ###
        detailed_filters = {}
        if self.quest_mode == "all":
            detailed_filters["ar_detailed"] = ["metric=quest_ar_detailed"]
            detailed_filters["normal_detailed"] = ["metric=quest_normal_detailed"]
        elif self.quest_mode in ("ar", "normal"):
            key_name = "quest_ar_detailed" if self.quest_mode == "ar" else "quest_normal_detailed"
            detailed_filters[self.quest_mode + "_detailed"] = [f"metric={key_name}"]
        # Add area filter if not global.
        for key in detailed_filters:
            if self.area.lower() != "global":
                detailed_filters[key].append(f"area={self.area}")
            # Add reward filter if quest_type is not "all".
            if self.quest_type != "all":
                detailed_filters[key].append(f"reward={self.quest_type}")
        # Query each detailed series.
        for key, filt in detailed_filters.items():
            try:
                data = await client.execute_command("TS.MRANGE", start_ms, end_ms, "FILTER", *filt)
                logger.info(f"Raw TS.MRANGE data for detailed series '{key}': {data}")
                results[key] = data
                logger.info(f"🔑 Retrieved TS.MRANGE data for detailed series '{key}'")
            except Exception as e:
                logger.error(f"❌ Error retrieving TS.MRANGE data for detailed series '{key}': {e}")
                results[key] = 0 if self.mode == "sum" else []

        ### Transformation Phase ###
        transformed = {}
        if self.mode == "sum":
            # Overall: sum each series and group by quest mode.
            overall_by_mode = {}
            for series in results.get("total", []):
                # Expected key format: ts:quests_total:total:{area}:{mode}
                parts = series[0].split(":")
                mode_label = parts[4].lower() if len(parts) >= 5 else "unknown"
                overall_by_mode.setdefault(mode_label, 0)
                overall_by_mode[mode_label] += sum(float(point[1]) for point in series[2])
            transformed["total"] = overall_by_mode

        elif self.mode == "grouped":
            # Overall: aggregate overall series by quest mode (without time breakdown).
            overall_group = {}
            for series in results.get("total", []):
                parts = series[0].split(":")
                mode_label = parts[4].lower() if len(parts) >= 5 else "unknown"
                overall_group.setdefault(mode_label, 0)
                overall_group[mode_label] += sum(float(point[1]) for point in series[2])
            # Detailed: group by full field details.
            detailed_group = {}
            for dkey in detailed_filters.keys():
                for series in results.get(dkey, []):
                    parts = series[0].split(":")
                    if len(parts) >= 6:
                        qmode = parts[4].lower()
                        identifier = ":".join(parts[5:])  # join all parts after the area and mode
                    else:
                        qmode = "unknown"
                        identifier = series[0]
                    detailed_group.setdefault(qmode, {})
                    total_val = sum(float(point[1]) for point in series[2])
                    if total_val:  # only include non‑zero totals
                        detailed_group[qmode][identifier] = detailed_group[qmode].get(identifier, 0) + total_val
            transformed = {"total": overall_group, "detailed": detailed_group}

        elif self.mode == "surged":
            # (Surged mode not modified here; it can be implemented similarly if needed.)
            surged_data = {}
            # For example, grouping overall series by hour.
            for series in results.get("total", []):
                parts = series[0].split(":")
                mode_label = parts[4].lower() if len(parts) >= 5 else "unknown"
                for point in series[2]:
                    ts, val = point
                    dt = datetime.fromtimestamp(int(ts) / 1000)
                    hour_key = dt.strftime("%H")
                    surged_data.setdefault(hour_key, {}).setdefault("overall", {}).setdefault(mode_label, 0)
                    surged_data[hour_key]["overall"][mode_label] += float(val)
            # Detailed: similar grouping by hour using full field details.
            for dkey in detailed_filters.keys():
                for series in results.get(dkey, []):
                    parts = series[0].split(":")
                    if len(parts) >= 6:
                        qmode = parts[4].lower()
                        identifier = ":".join(parts[5:])
                    else:
                        qmode = "unknown"
                        identifier = series[0]
                    for point in series[2]:
                        ts, val = point
                        dt = datetime.fromtimestamp(int(ts) / 1000)
                        hour_key = dt.strftime("%H")
                        surged_data.setdefault(hour_key, {}).setdefault("detailed", {}).setdefault(qmode, {})
                        surged_data[hour_key]["detailed"][qmode][identifier] = surged_data[hour_key]["detailed"][qmode].get(identifier, 0) + float(val)
            transformed = OrderedDict(sorted(surged_data.items(), key=lambda x: int(x[0])))

        logger.info(f"Transformed results: {transformed}")
        return {"mode": self.mode, "data": transformed}

    def _merge_mrange_results(self, mrange_results: list) -> list:
        merged = {}
        for series in mrange_results:
            try:
                series_points = series[1]
                for point in series_points:
                    ts, val = point
                    ts = int(ts)
                    val = float(val)
                    merged[ts] = merged.get(ts, 0) + val
            except Exception as e:
                logger.error(f"❌ Error merging series: {e}")
        merged_list = [[ts, merged[ts]] for ts in sorted(merged.keys())]
        logger.info(f"Merged series: {merged_list}")
        return merged_list
