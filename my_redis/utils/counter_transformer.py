from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger

class CounterTransformer:
    """Base class providing static methods to transform aggregated counter data."""

    TTH_BUCKETS = [
        (0, 5), (5, 10), (10, 15), (15, 20), (20, 25),
        (25, 30), (30, 35), (35, 40), (40, 45), (45, 50),
        (50, 55), (55, 60)
    ]
    bucket_order = {f"{low}_{high}": idx for idx, (low, high) in enumerate(TTH_BUCKETS)}

    @staticmethod
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

    @classmethod
    def transform_aggregated_tth(cls, raw_aggregated: dict, mode: str, start: datetime = None, end: datetime = None) -> dict:
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

        if mode == "sum":
            final = {}
            for field, value in raw_aggregated.items():
                parts = field.split(":")
                bucket = parts[-1] if len(parts) >= 3 else field
                final[bucket] = final.get(bucket, 0) + value
            sorted_final = dict(sorted(final.items(), key=lambda item: cls.bucket_order.get(item[0], 9999)))
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
            sorted_averaged = dict(sorted(averaged.items(), key=lambda item: cls.bucket_order.get(item[0], 9999)))
            return sorted_averaged
        else:
            return raw_aggregated


    @staticmethod
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


    @classmethod
    def transform_surged_tth_hourly_by_hour(cls, raw_aggregated: dict) -> dict:
        """
        Transforms raw aggregated TTH hourly data (grouped mode) into a dictionary keyed by the hour of day.

        Expected raw_aggregated: a dict where keys are full Redis keys, e.g.
        "counter:tth_pokemon_hourly:Saarlouis:2025031718"
        This function extracts the hour from the last two digits of the time component,
        then combines (sums) the inner dictionaries for keys with the same hour.

        The inner dictionaries are then sorted by the defined TTH bucket order.
        Returns a dictionary with keys as the hour (e.g., "00" through "23").
        """

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
            surged[hour] = dict(sorted(surged[hour].items(), key=lambda item: cls.bucket_order.get(item[0], 9999)))
        # Sort outer dictionary by hour as integer.
        sorted_grouped = dict(sorted(surged.items(), key=lambda item: int(item[0])))
        return sorted_grouped


    @staticmethod
    def transform_raid_totals_sum(raw_aggregated: dict) -> dict:
        """
        Transforms raw aggregated raid totals (in sum mode) into a detailed breakdown.

        The raid totals fields are expected to be in the format:
            "{raid_pokemon}:{raid_level}:{raid_form}:{raid_costume}:{raid_is_exclusive}:{raid_ex_eligible}:total"

        This function returns a dictionary with breakdown:
        - "raid_pokemon+raid_form": aggregated sum for each unique combination of raid_pokemon and raid_form.
        - "raid_level": aggregated sum per raid_level.
        - "raid_costume": aggregated sum per raid_costume.
        - "raid_is_exclusive": aggregated sum per raid_is_exclusive.
        - "raid_ex_eligible": aggregated sum per raid_ex_raid_eligible.
        - "total": overall total.
        """
        breakdown = {
            "raid_pokemon+raid_form": {},
            "raid_level": {},
            "raid_costume": {},
            "raid_is_exclusive": {},
            "raid_ex_eligible": {},
            "total": 0
        }
        logger.debug("▶️ Starting transform_raid_totals_sum")
        # raw_aggregated is assumed to be a flat dictionary: field -> value.
        for field, value in raw_aggregated.items():
            logger.info(f"▶️ Processing field: {field} with value: {value}")
            parts = field.split(":")
            if len(parts) != 7:
                logger.debug(f"⏭️ Skipping field {field} because it does not have 7 parts")
                continue  # Skip keys that don't follow the expected format.
            raid_pokemon, raid_level, raid_form, raid_costume, raid_is_exclusive, raid_ex_eligible, metric = parts
            try:
                val = int(value)
            except Exception as e:
                logger.debug(f"❌ Could not convert value {value} of field {field} to int: {e}")
                val = 0
            breakdown["total"] += val
            logger.debug(f"☑️ Added {val} to total; running total: {breakdown['total']}")

            key_pf = f"{raid_pokemon}:{raid_form}"
            breakdown["raid_pokemon+raid_form"][key_pf] = breakdown["raid_pokemon+raid_form"].get(key_pf, 0) + val
            logger.debug(f"⬆️ Updated raid_pokemon+raid_form for {key_pf}: {breakdown['raid_pokemon+raid_form'][key_pf]}")

            breakdown["raid_level"][raid_level] = breakdown["raid_level"].get(raid_level, 0) + val
            logger.debug(f"⬆️ Updated raid_level for {raid_level}: {breakdown['raid_level'][raid_level]}")

            breakdown["raid_costume"][raid_costume] = breakdown["raid_costume"].get(raid_costume, 0) + val
            logger.debug(f"⬆️ Updated raid_costume for {raid_costume}: {breakdown['raid_costume'][raid_costume]}")

            breakdown["raid_is_exclusive"][raid_is_exclusive] = breakdown["raid_is_exclusive"].get(raid_is_exclusive, 0) + val
            logger.debug(f"⬆️ Updated raid_is_exclusive for {raid_is_exclusive}: {breakdown['raid_is_exclusive'][raid_is_exclusive]}")

            breakdown["raid_ex_eligible"][raid_ex_eligible] = breakdown["raid_ex_eligible"].get(raid_ex_eligible, 0) + val
            logger.debug(f"⬆️ Updated raid_ex_eligible for {raid_ex_eligible}: {breakdown['raid_ex_eligible'][raid_ex_eligible]}")

        logger.debug(f"🔍 Before sorting, breakdown: {breakdown}")
        # Optionally sort each dictionary by key (numeric sort for level if applicable)
        breakdown["raid_pokemon+raid_form"] = dict(sorted(breakdown["raid_pokemon+raid_form"].items()))
        try:
            breakdown["raid_level"] = dict(sorted(breakdown["raid_level"].items(), key=lambda item: int(item[0]) if item[0].isdigit() else item[0]))
        except Exception as e:
            logger.warning(f"❌ Could not sort raid_level numerically: {e}")
            breakdown["raid_level"] = dict(sorted(breakdown["raid_level"].items()))
        breakdown["raid_costume"] = dict(sorted(breakdown["raid_costume"].items()))
        breakdown["raid_is_exclusive"] = dict(sorted(breakdown["raid_is_exclusive"].items()))
        breakdown["raid_ex_eligible"] = dict(sorted(breakdown["raid_ex_eligible"].items()))

        logger.info(f"✅ Transformation complete. Final breakdown: {breakdown}")
        return breakdown


    @staticmethod
    def transform_invasion_totals_sum(raw_aggregated: dict) -> dict:
        """
        Transforms raw aggregated invasion totals (in sum mode) into a detailed breakdown.

        Expected field format:
        "{display_type}:{character}:{grunt}:{confirmed}:total"

        Returns a dictionary with breakdown:
        - "display_type+character": aggregated sum for each unique combination of display_type and character.
        - "grunt": aggregated sum per grunt.
        - "confirmed": aggregated sum per confirmed.
        - "total": overall total.
        """
        breakdown = {
            "display_type+character": {},
            "grunt": {},
            "confirmed": {},
            "total": 0
        }
        logger.info("▶️ Starting transform_invasion_totals_sum")

        # raw_aggregated is expected to be a flat dict: field -> value.
        for field, value in raw_aggregated.items():
            logger.debug(f"Processing field: {field} with value: {value}")
            parts = field.split(":")
            if len(parts) != 5:
                logger.debug(f"⏭️ Skipping field {field} because it does not have 5 parts (found {len(parts)})")
                continue
            display_type, character, grunt, confirmed, metric = parts
            try:
                val = int(value)
            except Exception as e:
                logger.debug(f"❌ Could not convert value '{value}' for field '{field}' to int: {e}")
                val = 0
            breakdown["total"] += val
            logger.debug(f"Added {val} to total; running total: {breakdown['total']}")

            # Combine display_type and character.
            key_dc = f"{display_type}:{character}"
            breakdown["display_type+character"][key_dc] = breakdown["display_type+character"].get(key_dc, 0) + val
            logger.debug(f"Updated display_type+character for {key_dc}: {breakdown['display_type+character'][key_dc]}")

            breakdown["grunt"][grunt] = breakdown["grunt"].get(grunt, 0) + val
            logger.debug(f"Updated grunt for {grunt}: {breakdown['grunt'][grunt]}")

            breakdown["confirmed"][confirmed] = breakdown["confirmed"].get(confirmed, 0) + val
            logger.debug(f"Updated confirmed for {confirmed}: {breakdown['confirmed'][confirmed]}")

        logger.debug(f"Before sorting, breakdown: {breakdown}")
        breakdown["display_type+character"] = dict(sorted(breakdown["display_type+character"].items()))
        try:
            breakdown["grunt"] = dict(sorted(breakdown["grunt"].items(), key=lambda item: int(item[0]) if item[0].isdigit() else item[0]))
        except Exception as e:
            logger.warning(f"❌ Could not sort grunt numerically: {e}")
            breakdown["grunt"] = dict(sorted(breakdown["grunt"].items()))
        breakdown["confirmed"] = dict(sorted(breakdown["confirmed"].items()))
        logger.info(f"✅ Transformation complete. Final breakdown: {breakdown}")
        return breakdown
