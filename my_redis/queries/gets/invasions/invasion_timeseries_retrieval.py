from datetime import datetime
from my_redis.connect_redis import RedisManager
from my_redis.utils.counter_transformer import CounterTransformer
from utils.logger import logger

redis_manager = RedisManager()

class InvasionTimeSeries(CounterTransformer):
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum",
                 display: str = "all", grunt: str = "all", confirmed: str = "all"):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.display = display
        self.grunt = grunt
        self.confirmed = confirmed

    async def invasion_retrieve_timeseries(self) -> dict:
        """
        Retrieve the Invasion time series data.

        - When area is not global and specific display, grunt and confirmed are provided, we use TS.RANGE.
        - Otherwise, we use TS.MRANGE with filters.
        Since TS.MRANGE only supports exact filtering, if confirmed is "all" but grunt (or display) is set,
        we post-filter the returned series by parsing the key (expected format:
          ts:invasion:total:{area}:{display}:{grunt}:{confirmed})
        and only summing series that match the provided display/grunt/confirmed values.

        Supported modes:
          - "sum": Sums all values (applying the above post-filtering).
          - "grouped": Groups by the key identifier (display:grunt:confirmed) and excludes groups with 0 total.
          - "surged": Groups by hour (YYYYMMDDHH).
        """
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Redis connection not available for TimeSeries retrieval.")
            return {"mode": self.mode, "data": {}}

        metric_label = "invasion_total"  # Only one metric for invasions.

        # Convert start and end to milliseconds.
        start_ms = int(self.start.timestamp() * 1000)
        end_ms = int(self.end.timestamp() * 1000)
        logger.info(f"Querying from {start_ms} to {end_ms} for area '{self.area}' using mode '{self.mode}'")

        results = {}

        # If area is not global and all filters are specified (i.e. none are "all"),
        # we can query a single key with TS.RANGE.
        if (self.area.lower() != "global" and
            self.display != "all" and
            self.grunt != "all" and
            self.confirmed != "all"):
            key = f"ts:invasion:total:{self.area}:{self.display}:{self.grunt}:{self.confirmed}"
            logger.info(f"Querying key: {key}")
            try:
                ts_data = await client.execute_command("TS.RANGE", key, start_ms, end_ms)
                logger.info(f"Raw data for key {key}: {ts_data}")
                results["total"] = ts_data
                logger.info(f"🔑 Retrieved {len(ts_data)} points for key {key}")
            except Exception as e:
                logger.error(f"❌ Error retrieving TS data for key {key}: {e}")
                results["total"] = []
        else:
            # Otherwise, use TS.MRANGE with filters.
            filter_exprs = [f"metric={metric_label}"]
            # Only add area filter if area is not global.
            if self.area.lower() != "global":
                filter_exprs.append(f"area={self.area}")
            if self.display != "all":
                filter_exprs.append(f"invasion={self.display}")
            # Note: If grunt is specified but confirmed is "all", we do not add a filter here.
            if self.grunt != "all" and self.confirmed != "all":
                filter_exprs.append(f"grunt={self.grunt}:{self.confirmed}")
            logger.info(f"Querying TS.MRANGE for invasion with filters: {filter_exprs}")
            try:
                ts_data = await client.execute_command(
                    "TS.MRANGE", start_ms, end_ms,
                    "FILTER", *filter_exprs
                )
                logger.info(f"Raw TS.MRANGE data for invasion: {ts_data}")
                if self.mode == "sum":
                    # We may need to post-filter series if grunt or confirmed is "all"
                    total = 0
                    for series in ts_data:
                        key = series[0]
                        parts = key.split(":")
                        # Expected: ts, invasion, total, area, display, grunt, confirmed
                        if len(parts) >= 7:
                            # Apply post-filtering if a specific display/grunt/confirmed is desired.
                            if self.display != "all" and parts[4] != self.display:
                                continue
                            if self.grunt != "all" and parts[5] != self.grunt:
                                continue
                            if self.confirmed != "all" and parts[6] != self.confirmed:
                                continue
                        total += sum(float(point[1]) for point in series[2])
                    results["total"] = total
                else:
                    # For grouped/surged, keep the raw series for later transformation.
                    results["total"] = ts_data
                logger.info("🔑 Retrieved TS.MRANGE data for invasion")
            except Exception as e:
                logger.error(f"❌ Error retrieving TS.MRANGE data for invasion: {e}")
                results["total"] = 0 if self.mode == "sum" else []

        # Transformation
        transformed = {}
        if self.mode == "sum":
            # If TS.RANGE was used, data is a flat list.
            data = results["total"]
            if isinstance(data, list) and data:
                if isinstance(data[0], list) and len(data[0]) == 2:
                    total = sum(float(point[1]) for point in data)
                else:
                    total = data  # Already computed sum.
                transformed["total"] = total
            else:
                transformed["total"] = data

        elif self.mode == "grouped":
            # Group by the key’s identifier: display:grunt:confirmed.
            # We post-filter series as needed.
            grouped = {}
            data = results["total"]
            for series in data:
                key = series[0]
                parts = key.split(":")
                if len(parts) < 7:
                    continue
                disp, gr, conf = parts[4], parts[5], parts[6]
                # Apply post-filtering: if a specific value is desired and not "all", skip mismatches.
                if self.display != "all" and disp != self.display:
                    continue
                if self.grunt != "all" and gr != self.grunt:
                    continue
                if self.confirmed != "all" and conf != self.confirmed:
                    continue
                group_key = f"{disp}:{gr}:{conf}"
                total_value = sum(float(point[1]) for point in series[2])
                grouped[group_key] = grouped.get(group_key, 0) + total_value
            # Sort lexicographically (or apply a custom sort if needed) and filter out groups with a 0 total.
            sorted_group = dict(sorted(grouped.items()))
            filtered_group = {k: v for k, v in sorted_group.items() if v != 0}
            transformed["total"] = filtered_group

        elif self.mode == "surged":
            # Group by hour (YYYYMMDDHH) using TS.MRANGE series.
            surged = {}
            # For both TS.MRANGE and TS.RANGE (if used) we post-filter by key if needed.
            data = results["total"]
            # If data comes as series (list of series)...
            if data and isinstance(data[0], list) and len(data[0]) >= 3:
                for series in data:
                    key = series[0]
                    parts = key.split(":")
                    if len(parts) >= 7:
                        if self.display != "all" and parts[4] != self.display:
                            continue
                        if self.grunt != "all" and parts[5] != self.grunt:
                            continue
                        if self.confirmed != "all" and parts[6] != self.confirmed:
                            continue
                    for point in series[2]:
                        ts, val = point
                        dt = datetime.fromtimestamp(int(ts) / 1000)
                        hour_key = dt.strftime("%H")
                        surged[hour_key] = surged.get(hour_key, 0) + float(val)
                transformed["total"] = dict(sorted(surged.items(), key=lambda x: int(x[0])))
            else:
                # If data is a flat list (TS.RANGE result)
                for ts_val in data:
                    ts, val = ts_val
                    dt = datetime.fromtimestamp(int(ts) / 1000)
                    hour_key = dt.strftime("%H")
                    surged[hour_key] = surged.get(hour_key, 0) + float(val)
                transformed["total"] = dict(sorted(surged.items(), key=lambda x: int(x[0])))
        else:
            transformed = results

        logger.info(f"Transformed results: {transformed}")
        return {"mode": self.mode, "data": transformed}

    def _merge_mrange_results(self, mrange_results: list) -> list:
        """
        Given a TS.MRANGE result (a list of series, each in the form:
            [ key, [ [timestamp, value], ... ], { labels } ]),
        merge them into a single time series by summing values with matching timestamps.
        Returns a sorted list of [timestamp, value] pairs.
        """
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
