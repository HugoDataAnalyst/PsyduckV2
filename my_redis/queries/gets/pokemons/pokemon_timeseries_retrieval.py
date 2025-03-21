from datetime import datetime
from my_redis.connect_redis import RedisManager
from my_redis.utils.counter_transformer import CounterTransformer
from utils.logger import logger

redis_manager = RedisManager()

class PokemonTimeSeries(CounterTransformer):
    def __init__(self, area: str, start: datetime, end: datetime, mode: str = "sum", pokemon_id="all", form="all"):
        self.area = area
        self.start = start
        self.end = end
        self.mode = mode.lower()
        self.pokemon_id = pokemon_id
        self.form = form

    async def retrieve_timeseries(self) -> dict:
        """
        Retrieve the Pokémon time series data for the given parameters.
        If a specific pokemon_id (and form) is provided and area is not "global", TS.RANGE is used on each key.
        Otherwise, TS.MRANGE is used with filters:
        - When area is "global", we do not filter by area.
        - When specific pokemon_id/form are provided (with a non-global area), filters could include those too.
        Supported metrics include: total, iv100, iv0, pvp_little, pvp_great, pvp_ultra, shiny.
        The data is then transformed based on the chosen mode:
        - "sum": Sum all values.
        - "grouped": Group by minute (YYYYMMDDHHMM) or by "pokemon_id:form" if global.
        - "surged": Group by hour (YYYYMMDDHH).
        """
        client = await redis_manager.check_redis_connection("retrieval_pool")
        if not client:
            logger.error("❌ Redis connection not available for TimeSeries retrieval.")
            return {"mode": self.mode, "data": {}}

        # Define metric information.
        metrics_info = {
            "total": "pokemon_total",
            "iv100": "pokemon_iv100",
            "iv0": "pokemon_iv0",
            "pvp_little": "pokemon_pvp_little",
            "pvp_great": "pokemon_pvp_great",
            "pvp_ultra": "pokemon_pvp_ultra",
            "shiny": "pokemon_shiny",
        }

        # Convert start and end datetimes to milliseconds.
        start_ms = int(self.start.timestamp() * 1000)
        end_ms = int(self.end.timestamp() * 1000)
        logger.info(f"Querying from {start_ms} to {end_ms} for area '{self.area}' using mode '{self.mode}'")

        results = {}

        # If area is not global and a specific pokemon_id and form are provided, use TS.RANGE.
        if self.area.lower() != "global" and self.pokemon_id != "all" and self.form != "all":
            for metric, metric_label in metrics_info.items():
                key = f"ts:pokemon_totals:{metric}:{self.area}:{self.pokemon_id}:{self.form}"
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
            # Otherwise, use TS.MRANGE with filters.
            for metric, metric_label in metrics_info.items():
                filter_exprs = [f"metric={metric_label}"]
                # Only add area filter if area is not global.
                if self.area.lower() != "global":
                    filter_exprs.append(f"area={self.area}")
                # If a specific pokemon is requested, add those filters.
                if self.pokemon_id != "all":
                    filter_exprs.append(f"pokemon_id={self.pokemon_id}")
                if self.form != "all":
                    filter_exprs.append(f"form={self.form}")

                logger.info(f"Querying TS.MRANGE for metric '{metric}' with filters: {filter_exprs}")
                try:
                    ts_data = await client.execute_command(
                        "TS.MRANGE", start_ms, end_ms,
                        "FILTER", *filter_exprs
                    )
                    logger.info(f"Raw TS.MRANGE data for metric {metric}: {ts_data}")
                    if self.mode == "sum":
                        # Sum every value directly across all series.
                        total = sum(
                            float(point[1])
                            for series in ts_data
                            for point in series[2]
                        )
                        results[metric] = total
                    else:
                        # Keep the raw series for further transformation.
                        results[metric] = ts_data
                    logger.info(f"🔑 Retrieved TS.MRANGE data for metric {metric}")
                except Exception as e:
                    logger.error(f"❌ Error retrieving TS.MRANGE data for metric {metric}: {e}")
                    results[metric] = 0 if self.mode == "sum" else []

        # Transform results based on mode.
        transformed = {}
        if self.mode == "sum":
            for metric, data in results.items():
                if isinstance(data, list):
                    total = sum(float(point[1]) for point in data)
                    transformed[metric] = total
                else:
                    transformed[metric] = data
        elif self.mode == "grouped":
            grouped_data = {}
            if self.pokemon_id == "all" and self.form == "all":
                # When grouping globally, extract identifier (pokemon_id:form) from each series key.
                for metric, series_list in results.items():
                    group = {}
                    for series in series_list:
                        # Expected series format: [ key, <ignored>, points ]
                        key = series[0]
                        parts = key.split(":")
                        # Expected key format: ts:pokemon_totals:{metric}:{area}:{pokemon_id}:{form}
                        if len(parts) >= 6:
                            group_key = f"{parts[4]}:{parts[5]}"
                        else:
                            group_key = key
                        # Sum all values for this series (points are at index 2)
                        total_value = sum(float(point[1]) for point in series[2])
                        group[group_key] = group.get(group_key, 0) + total_value

                    # Sort the group by pokemon_id (first part) then by form (second part) numerically.
                    sorted_group = dict(sorted(
                        group.items(),
                        key=lambda kv: (int(kv[0].split(":")[0]), int(kv[0].split(":")[1]))
                    ))
                    grouped_data[metric] = sorted_group
                transformed = grouped_data
            else:
                # Otherwise group by minute.
                for metric, data in results.items():
                    grouped = {}
                    for ts_val in data:
                        ts_ms, val = ts_val
                        dt = datetime.fromtimestamp(int(ts_ms) / 1000)
                        minute_str = dt.strftime("%Y%m%d%H%M")
                        grouped[minute_str] = grouped.get(minute_str, 0) + float(val)
                    grouped_data[metric] = grouped
                transformed = grouped_data
        elif self.mode == "surged":
            surged_data = {}
            # When using TS.MRANGE (i.e. area is global) the result is a list of series.
            if self.area.lower() == "global":
                for metric, series_list in results.items():
                    hour_group = {}
                    for series in series_list:
                        # series is in the form [ key, [ [timestamp, value], ... ], { labels } ]
                        for point in series[2]:
                            ts, val = point
                            dt = datetime.fromtimestamp(int(ts) / 1000)
                            hour_key = dt.strftime("%H")
                            hour_group[hour_key] = hour_group.get(hour_key, 0) + float(val)
                    surged_data[metric] = dict(sorted(hour_group.items(), key=lambda x: int(x[0])))
                transformed = surged_data
            else:
                # Specific key mode (TS.RANGE): data is a list of [timestamp, value] pairs.
                for metric, data in results.items():
                    hour_group = {}
                    for ts_val in data:
                        ts, val = ts_val
                        dt = datetime.fromtimestamp(int(ts) / 1000)
                        hour_key = dt.strftime("%H")
                        hour_group[hour_key] = hour_group.get(hour_key, 0) + float(val)
                    surged_data[metric] = dict(sorted(hour_group.items(), key=lambda x: int(x[0])))
                transformed = surged_data

        logger.info(f"Transformed results: {transformed}")
        return {"mode": self.mode, "data": transformed}


    def _merge_mrange_results(self, mrange_results: list) -> list:
        """
        Given the result from TS.MRANGE (a list of series), merge them into a single time series.
        The result of TS.MRANGE is a list of items where each item is in the form:
          [ key, [ [timestamp, value], ... ], { labels } ]
        We merge them by summing the values for matching timestamps.
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
