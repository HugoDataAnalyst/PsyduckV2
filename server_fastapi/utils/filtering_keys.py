from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import re

redis_manager = RedisManager()

async def aggregate_keys(keys: list, mode: str) -> dict:
    """
    Aggregates hash data from a list of keys.
    For "sum" mode, sums all field values (assuming integer values).
    For "grouped" mode, returns a dictionary mapping each key to its hash data.
    """
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    aggregated = {}
    for key in keys:
        data = await client.hgetall(key)
        logger.info(f"🔑 Aggregating key: {key} with data: {data}")
        if mode == "sum":
            for field, value in data.items():
                try:
                    value = int(value)
                except Exception:
                    value = 0
                aggregated[field] = aggregated.get(field, 0) + value
        elif mode in ["grouped", "surged"]:
            aggregated[key] = {k: int(v) for k, v in data.items()}
    logger.info(f"✅ Mode:{mode} Aggregation complete. Aggregated data: {aggregated}")
    return aggregated

def filter_keys_by_time(keys: list, time_format: str, start: datetime, end: datetime, component_index: int = -1) -> list:
    """
    Filters keys by extracting the time component from the specified component index (default is the last component)
    after splitting the key by a colon, parsing it according to time_format, and returning only those keys
    whose datetime falls between start and end.
    """
    filtered = []
    for key in keys:
        parts = key.split(":")
        if len(parts) < abs(component_index):
            continue
        try:
            timestr = parts[component_index]
            dt = datetime.strptime(timestr, time_format)
            logger.debug(f"Key: {key} parsed time: {dt}")
            if start <= dt <= end:
                logger.debug(f"Key: {key} is in range.")
                filtered.append(key)
            else:
                logger.debug(f"Key: {key} with time {dt} is out of range.")
        except Exception as e:
            logger.warning(f"❌ Could not parse time from key {key}: {e}")
    logger.info(f"✅ Filtered down to {len(filtered)} keys")
    return filtered

def parse_time_input(time_str: str, reference: datetime = None) -> datetime:
    """
    Parses a time input string.

    If time_str can be parsed as an ISO datetime, returns that datetime.
    Otherwise, if time_str is in a relative format like "1 month", "10 days", "1 day", "3 months", "1 year", or "10 hours",
    subtracts that duration from the reference (or now if reference is None) and returns that datetime.
    If time_str is "now", returns datetime.now().
    """
    if reference is None:
        reference = datetime.now()
    time_str = time_str.strip().lower()
    if time_str == "now":
        return datetime.now()
    try:
        # Try ISO datetime first.
        return datetime.fromisoformat(time_str)
    except Exception:
        pass
    # Extend the relative time pattern to support hours.
    pattern = re.compile(r"(\d+)\s*(day|days|month|months|year|years|hour|hours)")
    match = pattern.fullmatch(time_str)
    if match:
        value = int(match.group(1))
        unit = match.group(2)
        from datetime import timedelta
        if unit in ("day", "days"):
            return reference - timedelta(days=value)
        elif unit in ("hour", "hours"):
            return reference - timedelta(hours=value)
        elif unit in ("month", "months"):
            try:
                from dateutil.relativedelta import relativedelta
                return reference - relativedelta(months=value)
            except ImportError:
                # Approximate a month as 30 days if dateutil is not available.
                return reference - timedelta(days=30 * value)
        elif unit in ("year", "years"):
            try:
                from dateutil.relativedelta import relativedelta
                return reference - relativedelta(years=value)
            except ImportError:
                # Approximate a year as 365 days.
                return reference - timedelta(days=365 * value)
    raise ValueError(f"Unrecognized time format: {time_str}")


def transform_aggregated_totals(raw_aggregated: dict, mode: str) -> dict:
    """
    Transforms raw aggregated data for totals retrieval.

    In "sum" mode:
      - raw_aggregated is a flat dictionary (e.g., {"1:163:total": 35, "2:166:total": 1, ...}).
      - This function groups fields by the metric (the third component) and sums the values.
      - Example result: {"total": 36, "iv100": X, ...}

    In "grouped" mode:
      - raw_aggregated is a dictionary with keys representing individual Redis keys,
        and values as dictionaries mapping fields (e.g., "1:163:total") to counts.
      - This function combines all the fields into a single dictionary (summing counts across keys)
        and then sorts the result by the numeric value of the first component (pokemon_id).
      - Example result: {"1:163:total": 35, "2:166:total": 1, ...}
    """
    if mode == "sum":
        final = {}
        # raw_aggregated is flat: field -> value
        for field, value in raw_aggregated.items():
            parts = field.split(":")
            metric = parts[-1] if len(parts) >= 3 else field
            final[metric] = final.get(metric, 0) + value
        sorted_final = dict(sorted(final.items(), key=lambda item: item[0]))
        return sorted_final
    elif mode == "grouped":
        combined = {}
        # raw_aggregated is a dict: redis_key -> {field: value}
        for redis_key, fields in raw_aggregated.items():
            for field, value in fields.items():
                combined[field] = combined.get(field, 0) + value
        sorted_combined = dict(sorted(combined.items(), key=lambda item: int(item[0].split(":")[0])))
        return sorted_combined
    else:
        return raw_aggregated


def transform_aggregated_tth(raw_aggregated: dict, mode: str, start: datetime = None, end: datetime = None) -> dict:
    """
    Transforms raw aggregated data for TTH retrieval.

    In "sum" mode:
      - raw_aggregated is a flat dictionary where keys are TTH buckets (e.g., "0_5", "5_10", etc.)
        possibly along with other identifiers.
      - This function merges the values by TTH bucket. For example, if the keys are
        "1:163:0_5", "2:165:0_5", etc., it sums all values for the "0_5" bucket.
      - The final dictionary is then sorted by TTH bucket order.

    In "grouped" mode:
      - raw_aggregated is a dictionary with keys (e.g., Redis keys) and values as dictionaries.
      - This function combines all inner dictionaries into accumulators: one for the total sum
        and one for the count of hours that provided data for each bucket.
      - It then computes the average for each bucket (total divided by count), rounds the result
        to 3 decimals, and orders the final output by TTH bucket order.
    """
    from datetime import timedelta

    TTH_BUCKETS = [
        (0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
        (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
        (50, 55), (55, 60)
    ]
    bucket_order = {f"{low}_{high}": idx for idx, (low, high) in enumerate(TTH_BUCKETS)}

    if mode == "sum":
        final = {}
        for field, value in raw_aggregated.items():
            parts = field.split(":")
            bucket = parts[-1] if len(parts) >= 3 else field
            final[bucket] = final.get(bucket, 0) + value
        sorted_final = dict(sorted(final.items(), key=lambda item: bucket_order.get(item[0], 9999)))
        return sorted_final

    elif mode == "grouped":
        combined = {}
        counts = {}
        # raw_aggregated is a dict: redis_key -> {field: value}
        for redis_key, fields in raw_aggregated.items():
            for field, value in fields.items():
                parts = field.split(":")
                bucket = parts[-1] if len(parts) >= 3 else field
                combined[bucket] = combined.get(bucket, 0) + value
                counts[bucket] = counts.get(bucket, 0) + 1
        averaged = {}
        for bucket in combined:
            if counts.get(bucket, 0) > 0:
                averaged[bucket] = round(combined[bucket] / counts[bucket], 3)
            else:
                averaged[bucket] = 0
        sorted_averaged = dict(sorted(averaged.items(), key=lambda item: bucket_order.get(item[0], 9999)))
        return sorted_averaged
    else:
        return raw_aggregated

def transform_surged_totals_hourly_by_hour(raw_aggregated: dict) -> dict:
    """
    Transforms raw aggregated totals hourly data (grouped mode) into a dictionary keyed by the hour of day.

    Expected raw_aggregated: a dict where keys are full Redis keys in the format:
         "counter:pokemon_total:{area}:{YYYYMMDDHH}"
    This function:
      - Extracts the hour portion (the last two characters of the time part).
      - Groups and sums the inner dictionary fields for all keys with the same hour.
      - Sorts the inner dictionary by the numeric value of the first component (pokemon_id).
      - Returns a dictionary with keys labeled as "hour {H}".

    Example output:
    {
      "hour 13": { "1:163:total": 35, "2:166:iv100": 1, ... },
      "hour 17": { ... },
      "hour 18": { ... }
    }
    """
    surged = {}
    for full_key, fields in raw_aggregated.items():
        parts = full_key.split(":")
        # Expect at least: counter, pokemon_total, area, time
        if len(parts) < 4:
            continue
        # The last component should be the time string (YYYYMMDDHH)
        time_part = parts[-1]
        if len(time_part) < 2:
            continue
        # Extract just the hour (last two characters)
        hour_only = time_part[-2:]
        if hour_only not in surged:
            surged[hour_only] = {}
        # Sum the fields from this key into the bucket for that hour.
        for field, value in fields.items():
            surged[hour_only][field] = surged[hour_only].get(field, 0) + value

    # Sort each hour's dictionary by the numeric value of the first component in the field.
    for hour in surged:
        try:
            surged[hour] = dict(sorted(surged[hour].items(), key=lambda item: int(item[0].split(":")[0])))
        except Exception as e:
            # If parsing fails, leave unsorted.
            surged[hour] = surged[hour]

    # Now, sort the outer dictionary by the hour (converted to integer) and re-label the keys.
    sorted_grouped = {}
    for hr in sorted(surged.keys(), key=lambda x: int(x)):
        sorted_grouped[f"hour {int(hr)}"] = surged[hr]
    return sorted_grouped

def transform_surged_tth_hourly_by_hour(raw_aggregated: dict) -> dict:
    """
    Transforms raw aggregated TTH hourly data (grouped mode) into a dictionary keyed by the hour of day.

    Expected raw_aggregated: a dict where keys are full Redis keys, e.g.
      "counter:tth_pokemon_hourly:Saarlouis:2025031718"
    This function extracts the hour from the last two digits of the time component,
    then combines (sums) the inner dictionaries for keys with the same hour.

    The inner dictionaries are then sorted by the defined TTH bucket order.
    Returns a dictionary with keys as the hour (e.g., "00" through "23").
    """

    # Define TTH bucket order.
    TTH_BUCKETS = [
        (0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
        (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
        (50, 55), (55, 60)
    ]
    bucket_order = {f"{low}_{high}": idx for idx, (low, high) in enumerate(TTH_BUCKETS)}

    surged = {}
    for full_key, fields in raw_aggregated.items():
        parts = full_key.split(":")
        if len(parts) < 4:
            continue
        # The last component is expected to be a time string in the format "YYYYMMDDHH"
        time_component = parts[-1]
        if len(time_component) < 2:
            continue
        try:
            # Extract the last two digits as the hour and format as a zero-padded string.
            hour_int = int(time_component[-2:])
            hour_str = f"{hour_int:02d}"
        except Exception:
            continue

        if hour_str not in surged:
            surged[hour_str] = {}
        for field, value in fields.items():
            surged[hour_str][field] = surged[hour_str].get(field, 0) + value

    # Sort each hour's inner dictionary by TTH bucket order.
    for hour in surged:
        surged[hour] = dict(sorted(surged[hour].items(), key=lambda item: bucket_order.get(item[0], 9999)))
    # Sort outer dictionary by hour as integer.
    sorted_grouped = dict(sorted(surged.items(), key=lambda item: int(item[0])))
    return sorted_grouped
