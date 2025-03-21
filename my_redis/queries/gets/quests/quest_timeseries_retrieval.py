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
        quest_type: a filter on the reward type (as stored in the detailed key), or "all"
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

        We always query the overall series (with metric "quest_total") and one or both of the detailed
        series. For the detailed series:
          - If quest_mode is "ar", we query keys with metric "quest_ar_detailed".
          - If quest_mode is "normal", we query keys with metric "quest_normal_detailed".
          - If quest_mode is "all", we query both.

        We add label filters:
          - Always filter by area.
          - For detailed series, filter by mode (if not "all").
          - Also, if quest_type != "all", add a filter on the reward type label (here we assume
            the label name is "reward" – adjust if needed).

        Note: quest_mode and quest_type are not stored in the overall key (only in the detailed ones).

        Modes:
          - "sum": sum all values.
          - "grouped": group by minute (YYYYMMDDHHMM) for overall; for detailed, group by the identifier
                       extracted from the key (everything after {area}:{mode}:).
          - "surged": group by hour (YYYYMMDDHH) per series.
        """
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Redis connection not available for Quest TimeSeries retrieval.")
            return {"mode": self.mode, "data": {}}

        start_ms = int(self.start.timestamp() * 1000)
        end_ms   = int(self.end.timestamp() * 1000)
        logger.info(f"Querying from {start_ms} to {end_ms} for area '{self.area}' using mode '{self.mode}'")

        # Build our metric mapping.
        # Overall series:
        overall_metric = "quest_total"
        # Detailed metric(s) depend on quest_mode.
        detailed_metrics = {}
        if self.quest_mode == "all":
            detailed_metrics["ar_detailed"] = "quest_ar_detailed"
            detailed_metrics["normal_detailed"] = "quest_normal_detailed"
        elif self.quest_mode in ("ar", "normal"):
            key = "quest_ar_detailed" if self.quest_mode == "ar" else "quest_normal_detailed"
            detailed_metrics[self.quest_mode + "_detailed"] = key

        # We'll build our filters for TS.MRANGE.
        # Overall filter always:
        overall_filters = [f"metric={overall_metric}", f"area={self.area}"]
        # Detailed filters: start with mode filter.
        detailed_filters = {}
        for key, metric_label in detailed_metrics.items():
            # Base filter for detailed series.
            filt = [f"metric={metric_label}", f"area={self.area}"]
            # In our insertion, the "mode" (i.e. "ar" or "normal") was stored as the second label.
            # Add that filter.
            if self.quest_mode != "all":
                filt.append(f"mode={self.quest_mode}")
            # If a specific quest_type is requested, add filter on the reward type label.
            # (Assuming that during insertion the reward type was stored as a label named "reward".)
            if self.quest_type != "all":
                filt.append(f"reward={self.quest_type}")
            detailed_filters[key] = filt

        results = {}

        # Query overall series.
        try:
            overall_data = await client.execute_command("TS.MRANGE", start_ms, end_ms, "FILTER", *overall_filters)
            logger.info(f"Raw TS.MRANGE data for overall (metric={overall_metric}): {overall_data}")
            if self.mode == "sum":
                overall_total = sum(float(point[1]) for series in overall_data for point in series[2])
                results["total"] = overall_total
            else:
                results["total"] = overall_data
            logger.info("🔑 Retrieved TS.MRANGE data for overall series")
        except Exception as e:
            logger.error(f"❌ Error retrieving overall TS.MRANGE data: {e}")
            results["total"] = 0 if self.mode == "sum" else []

        # Query detailed series.
        # If quest_mode == "all", we query both; otherwise only one.
        for key, filt in detailed_filters.items():
            try:
                data = await client.execute_command("TS.MRANGE", start_ms, end_ms, "FILTER", *filt)
                logger.info(f"Raw TS.MRANGE data for detailed series '{key}': {data}")
                # Post‑filtering is possible here if needed (e.g. if quest_type is not stored as a label).
                if self.mode == "sum":
                    total = sum(float(point[1]) for series in data for point in series[2])
                    results[key] = total
                else:
                    results[key] = data
                logger.info(f"🔑 Retrieved TS.MRANGE data for detailed series '{key}'")
            except Exception as e:
                logger.error(f"❌ Error retrieving TS.MRANGE data for detailed series '{key}': {e}")
                results[key] = 0 if self.mode == "sum" else []

        # Transformation
        transformed = {}
        if self.mode == "sum":
            # For sum mode, simply include the overall and detailed sums.
            transformed = results
        elif self.mode == "grouped":
            # Group by minute (YYYYMMDDHHMM).
            grouped = {}
            # Process overall series.
            overall_group = {}
            data = results.get("total", [])
            if data and isinstance(data, list) and data and isinstance(data[0], list) and len(data[0])>=3:
                for series in data:
                    for point in series[2]:
                        ts, val = point
                        dt = datetime.fromtimestamp(int(ts)/1000)
                        minute_key = dt.strftime("%Y%m%d%H%M")
                        overall_group[minute_key] = overall_group.get(minute_key, 0) + float(val)
            else:
                for ts_val in data:
                    ts, val = ts_val
                    dt = datetime.fromtimestamp(int(ts)/1000)
                    minute_key = dt.strftime("%Y%m%d%H%M")
                    overall_group[minute_key] = overall_group.get(minute_key, 0) + float(val)
            grouped["total"] = overall_group

            # Process each detailed series.
            for key in detailed_filters.keys():
                d_group = {}
                ddata = results.get(key, [])
                if ddata and isinstance(ddata, list) and ddata and isinstance(ddata[0], list) and len(ddata[0])>=3:
                    # Use the key from each series to build an identifier.
                    for series in ddata:
                        # Expecting key format: ts:quests_total:total_ar_detailed:{area}:{mode}:{ar_field_details}
                        # We will use the entire field details as the identifier.
                        parts = series[0].split(":")
                        if len(parts) >= 5:
                            identifier = parts[4]  # this is the concatenated field details
                        else:
                            identifier = series[0]
                        total_val = sum(float(point[1]) for point in series[2])
                        if total_val:
                            d_group[identifier] = d_group.get(identifier, 0) + total_val
                else:
                    identifier = "unknown"
                    try:
                        total_val = sum(float(point[1]) for point in ddata)
                    except TypeError:
                        total_val = ddata if isinstance(ddata, (int, float)) else 0
                    if total_val:
                        d_group[identifier] = total_val
                # Only include non‑zero entries.
                if d_group:
                    grouped[key] = dict(sorted(d_group.items()))
            transformed = grouped

        elif self.mode == "surged":
            # Group by hour (YYYYMMDDHH).
            surged = {}
            # Process overall series.
            overall_data = results.get("total", [])
            if overall_data and isinstance(overall_data, list) and overall_data and isinstance(overall_data[0], list) and len(overall_data[0])>=3:
                for series in overall_data:
                    for point in series[2]:
                        ts, val = point
                        dt = datetime.fromtimestamp(int(ts)/1000)
                        hour_key = dt.strftime("%H")
                        if hour_key not in surged:
                            surged[hour_key] = {}
                        surged[hour_key]["total"] = surged[hour_key].get("total", 0) + float(val)
            else:
                for ts_val in overall_data:
                    ts, val = ts_val
                    dt = datetime.fromtimestamp(int(ts)/1000)
                    hour_key = dt.strftime("%H")
                    if hour_key not in surged:
                        surged[hour_key] = {}
                    surged[hour_key]["total"] = surged[hour_key].get("total", 0) + float(val)
            # Process detailed series.
            for key in detailed_filters.keys():
                ddata = results.get(key, [])
                if ddata and isinstance(ddata, list) and ddata and isinstance(ddata[0], list) and len(ddata[0])>=3:
                    for series in ddata:
                        # Use the field details (from the key) as identifier.
                        parts = series[0].split(":")
                        if len(parts) >= 5:
                            identifier = parts[4]
                        else:
                            identifier = series[0]
                        for point in series[2]:
                            ts, val = point
                            dt = datetime.fromtimestamp(int(ts)/1000)
                            hour_key = dt.strftime("%H")
                            if hour_key not in surged:
                                surged[hour_key] = {}
                            # Use the detailed series key (e.g. "ar_detailed" or "normal_detailed") as a sub-key.
                            if key not in surged[hour_key]:
                                surged[hour_key][key] = {}
                            surged[hour_key][key][identifier] = surged[hour_key][key].get(identifier, 0) + float(val)
                else:
                    identifier = "unknown"
                    for ts_val in ddata:
                        ts, val = ts_val
                        dt = datetime.fromtimestamp(int(ts)/1000)
                        hour_key = dt.strftime("%H")
                        if hour_key not in surged:
                            surged[hour_key] = {}
                        if key not in surged[hour_key]:
                            surged[hour_key][key] = {}
                        surged[hour_key][key][identifier] = surged[hour_key][key].get(identifier, 0) + float(val)
            transformed = OrderedDict(sorted(surged.items(), key=lambda x: int(x[0])))

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
