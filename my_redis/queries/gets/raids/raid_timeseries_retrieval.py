from collections import OrderedDict
from datetime import datetime
from my_redis.connect_redis import RedisManager
from my_redis.utils.counter_transformer import CounterTransformer
from utils.logger import logger

redis_manager = RedisManager()

class RaidTimeSeries(CounterTransformer):
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 raid_pokemon: str = "all", raid_form: str = "all", raid_level: str = "all",
                 raid_costume: str = "all", raid_is_exclusive: str = "all", raid_ex_raid_eligible: str = "all"):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.raid_pokemon = raid_pokemon
        self.raid_form = raid_form
        self.raid_level = raid_level
        self.raid_costume = raid_costume
        self.raid_is_exclusive = raid_is_exclusive
        self.raid_ex_raid_eligible = raid_ex_raid_eligible

    async def raid_retrieve_timeseries(self) -> dict:
        """
        Retrieve the Raid TimeSeries data.
        - If area is not "global" and all three parameters (raid_pokemon, raid_form, raid_level)
          are specified (i.e. not "all"), we use TS.RANGE on a single key.
        - Otherwise, we use TS.MRANGE with label filters. (Note: raid_level isn’t stored as a label,
          so if raid_level != "all" we post‑filter the returned series.)
        Supported metrics:
          • total, costume, exclusive, ex_raid_eligible.
        Modes:
          - "sum": sums all values across time.
          - "grouped": groups by the identifier “raid_pokemon:raid_form:raid_level”. (Only non‑zero totals are returned.)
          - "surged": for each metric, groups by hour (using YYYYMMDDHH) per identifier.
        """
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Redis connection not available for Raid TimeSeries retrieval.")
            return {"mode": self.mode, "data": {}}

        metrics_info = {
            "total": "raid_total",
            "costume": "raid_costume",
            "exclusive": "raid_exclusive",
            "ex_raid_eligible": "raid_ex_raid_eligible"
        }

        start_ms = int(self.start.timestamp() * 1000)
        end_ms = int(self.end.timestamp() * 1000)
        logger.info(f"Querying from {start_ms} to {end_ms} for area '{self.area}' using mode '{self.mode}'")

        results = {}
        # Use TS.RANGE only if area is not global and all three parameters are specified.
        if (self.area.lower() != "global" and
            self.raid_pokemon != "all" and
            self.raid_form != "all" and
            self.raid_level != "all"):
            for metric, metric_label in metrics_info.items():
                key = f"ts:raids_total:{metric}:{self.area}:{self.raid_pokemon}:{self.raid_level}:{self.raid_form}"
                logger.info(f"Querying key: {key}")
                try:
                    ts_data = await client.execute_command("TS.RANGE", key, start_ms, end_ms)
                    logger.info(f"Raw data for key {key}: {ts_data}")
                    results[metric] = ts_data
                    logger.info(f"🔑 Retrieved {len(ts_data)} points for key {key}")
                except Exception as e:
                    logger.error(f"❌ Error retrieving TS data for key {key}: {e}")
                    results[metric] = []
        else:
            # Build filter expressions.
            for metric, metric_label in metrics_info.items():
                filter_exprs = [f"metric={metric_label}"]
                if self.area.lower() != "global":
                    filter_exprs.append(f"area={self.area}")
                if self.raid_pokemon != "all":
                    filter_exprs.append(f"raid={self.raid_pokemon}")
                if self.raid_form != "all":
                    filter_exprs.append(f"form={self.raid_form}")
                logger.info(f"Querying TS.MRANGE for metric '{metric}' with filters: {filter_exprs}")
                try:
                    ts_data = await client.execute_command("TS.MRANGE", start_ms, end_ms, "FILTER", *filter_exprs)
                    logger.info(f"Raw TS.MRANGE data for metric {metric}: {ts_data}")
                    # Post-filter by raid_level if needed.
                    if self.raid_level != "all":
                        ts_data = [series for series in ts_data if series[0].split(":")[5] == self.raid_level]
                    if self.mode == "sum":
                        total = sum(float(point[1]) for series in ts_data for point in series[2])
                        results[metric] = total
                    else:
                        results[metric] = ts_data
                    logger.info(f"🔑 Retrieved TS.MRANGE data for metric {metric}")
                except Exception as e:
                    logger.error(f"❌ Error retrieving TS.MRANGE data for metric {metric}: {e}")
                    results[metric] = 0 if self.mode == "sum" else []

        # Transformation
        transformed = {}
        if self.mode == "sum":
            for metric, data in results.items():
                if isinstance(data, list) and data and isinstance(data[0], list) and len(data[0]) >= 3:
                    total = sum(float(point[1]) for series in data for point in series[2])
                elif isinstance(data, list):
                    total = sum(float(point[1]) for point in data)
                else:
                    total = data
                transformed[metric] = total

        elif self.mode == "grouped":
            # Group by identifier "raid_pokemon:raid_form:raid_level"
            grouped_data = {}
            for metric, data in results.items():
                group = {}
                if isinstance(data, list) and data and isinstance(data[0], list) and len(data[0]) >= 3:
                    for series in data:
                        parts = series[0].split(":")
                        if len(parts) >= 7:
                            # Reorder parts: the key is of the form:
                            # ts:raids_total:{metric}:{area}:{raid_pokemon}:{raid_level}:{raid_form}
                            # We want: raid_pokemon:raid_form:raid_level
                            identifier = f"{parts[4]}:{parts[6]}:{parts[5]}"
                        else:
                            identifier = series[0]
                        total_value = sum(float(point[1]) for point in series[2])
                        if total_value:
                            group[identifier] = group.get(identifier, 0) + total_value
                    # Only include non‑zero totals.
                    grouped_data[metric] = dict(sorted(group.items(),
                        key=lambda kv: (int(kv[0].split(":")[0]), int(kv[0].split(":")[1]), int(kv[0].split(":")[2]))))
                else:
                    # If data isn’t in series form, fall back to using the query parameters as identifier.
                    identifier = f"{self.raid_pokemon}:{self.raid_form}:{self.raid_level}"
                    try:
                        total_value = sum(float(point[1]) for point in data)
                    except TypeError:
                        total_value = data if isinstance(data, (int, float)) else 0
                    grouped_data[metric] = {identifier: total_value} if total_value else {}
            transformed = grouped_data

        elif self.mode == "surged":
            # Group by hour (using only the hour part, e.g. "16", "17", etc.)
            # The final structure will be: { hour: { metric: { identifier: total } } }
            surged_data = {}
            for metric, data in results.items():
                # Process series-based data (from TS.MRANGE)
                if data and isinstance(data, list) and data and isinstance(data[0], list) and len(data[0]) >= 3:
                    for series in data:
                        parts = series[0].split(":")
                        if len(parts) >= 7:
                            # Our key is stored as: ts:raids_total:{metric}:{area}:{raid_pokemon}:{raid_level}:{raid_form}
                            # We want the identifier as: "raid_pokemon:raid_form:raid_level"
                            identifier = f"{parts[4]}:{parts[6]}:{parts[5]}"
                        else:
                            identifier = series[0]
                        for point in series[2]:
                            ts, val = point
                            dt = datetime.fromtimestamp(int(ts) / 1000)
                            hour_key = dt.strftime("%H")  # just the hour (zero-padded)
                            # Initialize the outer dictionary for this hour if needed.
                            if hour_key not in surged_data:
                                surged_data[hour_key] = {}
                            # Initialize the metric sub-dictionary for this hour if needed.
                            if metric not in surged_data[hour_key]:
                                surged_data[hour_key][metric] = {}
                            surged_data[hour_key][metric][identifier] = surged_data[hour_key][metric].get(identifier, 0) + float(val)
                else:
                    # Process flat list data (from TS.RANGE)
                    identifier = f"{self.raid_pokemon}:{self.raid_form}:{self.raid_level}"
                    for ts_val in data:
                        ts, val = ts_val
                        dt = datetime.fromtimestamp(int(ts) / 1000)
                        hour_key = dt.strftime("%H")
                        if hour_key not in surged_data:
                            surged_data[hour_key] = {}
                        if metric not in surged_data[hour_key]:
                            surged_data[hour_key][metric] = {}
                        surged_data[hour_key][metric][identifier] = surged_data[hour_key][metric].get(identifier, 0) + float(val)
            # Order the outer hours by numeric value.
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
