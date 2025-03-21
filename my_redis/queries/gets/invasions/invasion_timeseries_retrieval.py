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

        In **sum** mode:
          - If area is not "global" and specific values for display, grunt, and confirmed are provided,
            we use TS.RANGE on the single key.
          - Otherwise, we use TS.MRANGE with filters.
        For **grouped** and **surged** modes we always use TS.MRANGE so that the same transformation
        approach is applied (even if specific filters are provided).

        The underlying key format is:
          ts:invasion:total:{area}:{display}:{grunt}:{confirmed}

        Labels (set during key creation) are:
          - metric: invasion_total
          - area: <area>
          - invasion: <display>
          - grunt: <grunt>:<confirmed>   (if provided)
        """
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Redis connection not available for TimeSeries retrieval.")
            return {"mode": self.mode, "data": {}}

        # For invasions we only have one metric: total.
        metric_label = "invasion_total"

        # Convert start and end datetimes to milliseconds.
        start_ms = int(self.start.timestamp() * 1000)
        end_ms = int(self.end.timestamp() * 1000)
        logger.info(f"Querying from {start_ms} to {end_ms} for area '{self.area}' using mode '{self.mode}'")

        results = {}

        # If area is not global and specific filters for display, grunt and confirmed are provided,
        # use TS.RANGE on the constructed key.
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
            # Use TS.MRANGE with filters.
            filter_exprs = [f"metric={metric_label}"]
            if self.area.lower() != "global":
                filter_exprs.append(f"area={self.area}")
            if self.display != "all":
                filter_exprs.append(f"invasion={self.display}")
            # For grunt and confirmed we expect both to be provided to filter.
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
                    total = sum(
                        float(point[1])
                        for series in ts_data
                        for point in series[2]
                    )
                    results["total"] = total
                else:
                    results["total"] = ts_data
                logger.info(f"🔑 Retrieved TS.MRANGE data for invasion")
            except Exception as e:
                logger.error(f"❌ Error retrieving TS.MRANGE data for invasion: {e}")
                results["total"] = 0 if self.mode == "sum" else []

        # Transformation
        transformed = {}
        if self.mode == "sum":
            # Depending on whether the result is a flat list (from TS.RANGE) or a list of series (from TS.MRANGE)
            data = results["total"]
            if isinstance(data, list) and data:
                if isinstance(data[0], list) and len(data[0]) == 2:
                    total = sum(float(point[1]) for point in data)
                elif isinstance(data[0], list) and len(data[0]) == 3:
                    total = sum(
                        float(point[1])
                        for series in data
                        for point in series[2]
                    )
                else:
                    total = 0
                transformed["total"] = total
            else:
                transformed["total"] = data

        elif self.mode == "grouped":
            # For grouped mode we always use TS.MRANGE and group by the key identifier.
            # When display, grunt and confirmed are all "all", extract the identifier from the key.
            grouped = {}
            data = results["total"]
            for series in data:
                # Expected series format: [ key, <ignored>, points ]
                key = series[0]
                parts = key.split(":")
                # Expected key format: ts:invasion:total:{area}:{display}:{grunt}:{confirmed}
                if len(parts) >= 7:
                    group_key = f"{parts[4]}:{parts[5]}:{parts[6]}"
                else:
                    group_key = key
                total_value = sum(float(point[1]) for point in series[2])
                grouped[group_key] = grouped.get(group_key, 0) + total_value
            # (Optional) If you want to sort the groups, you could sort them lexicographically.
            sorted_group = dict(sorted(grouped.items()))
            # Filter out groups with a 0 total.
            filtered_group = {k: v for k, v in sorted_group.items() if v != 0}
            transformed["total"] = filtered_group

        elif self.mode == "surged":
            # For surged mode we group by hour (YYYYMMDDHH)
            surged = {}
            data = results["total"]
            for series in data:
                for point in series[2]:
                    ts, val = point
                    dt = datetime.fromtimestamp(int(ts) / 1000)
                    hour_key = dt.strftime("%H")
                    surged[hour_key] = surged.get(hour_key, 0) + float(val)
            transformed["total"] = dict(sorted(surged.items(), key=lambda x: int(x[0][-2:])))
        else:
            transformed = results

        logger.info(f"Transformed results: {transformed}")
        return {"mode": self.mode, "data": transformed}

    def _merge_mrange_results(self, mrange_results: list) -> list:
        """
        Given the TS.MRANGE result (a list of series in the form:
          [ key, [ [timestamp, value], ... ], { labels } ]),
        merge them into a single time series by summing values at matching timestamps.
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
