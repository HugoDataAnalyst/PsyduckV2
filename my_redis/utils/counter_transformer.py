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

        # Sort each hour's inner dictionary by the first two components (pokemon_id and form)
        for hour in surged:
            try:
                surged[hour] = dict(sorted(
                    surged[hour].items(),
                    key=lambda item: (
                        int(item[0].split(":")[0]),
                        int(item[0].split(":")[1]) if len(item[0].split(":")) > 1 else 0
                    )
                ))
            except Exception as e:
                # If parsing fails, leave unsorted.
                logger.warning(f"❌ Could not sort surged data: {e}")
                surged[hour] = surged[hour]

        # Now, sort the outer dictionary by the hour (converted to integer) and re-label the keys.
        sorted_grouped = {}
        for hr in sorted(surged.keys(), key=lambda x: int(x)):
            sorted_grouped[f"hour {int(hr)}"] = surged[hr]
        return sorted_grouped


    @classmethod
    def transform_raids_surged_totals_hourly_by_hour(cls, raw_aggregated: dict) -> dict:
        """
        Transforms raw aggregated raid totals hourly data (grouped mode) into a dictionary keyed by the hour of day.

        This function expects raw_aggregated to be a dictionary where each key is the original Redis key
        (which includes a time component in the format "...:{YYYYMMDDHH}") and its value is a dictionary of
        aggregated fields (e.g. "raid_pokemon:raid_level:raid_form:raid_costume:raid_is_exclusive:raid_ex_eligible:total" -> count).

        For each Redis key, it extracts the hour (the last two characters of the time portion), merges the inner
        dictionaries for all keys that fall within the same hour, and then applies transform_raid_totals_sum
        to produce the detailed breakdown for that hour.

        The final output is a dictionary keyed as "hour {H}" with the detailed breakdown as its value.
        """
        surged = {}
        # Group fields by hour from the original Redis keys.
        for full_key, fields in raw_aggregated.items():
            parts = full_key.split(":")
            if len(parts) < 4:
                continue
            time_part = parts[-1]  # Expected to be something like YYYYMMDDHH
            if len(time_part) < 2:
                continue
            # Extract the last two digits for the hour.
            hour_only = time_part[-2:]
            if hour_only not in surged:
                surged[hour_only] = {}
            # Merge the fields for this key into the appropriate hour bucket.
            for field, value in fields.items():
                surged[hour_only][field] = surged[hour_only].get(field, 0) + value

        # For each hour, use the detailed transformation (grouped method) to get a breakdown.
        result = {}
        for hour, flat_fields in surged.items():
            # flat_fields is a flat dict with keys like:
            # "raid_pokemon:raid_level:raid_form:raid_costume:raid_is_exclusive:raid_ex_eligible:total"
            transformed = cls.transform_raid_totals_grouped(flat_fields)
            result[f"hour {int(hour)}"] = transformed

        # Optionally, sort the result by hour (numerically)
        sorted_result = dict(sorted(result.items(), key=lambda item: int(item[0].split()[1])))
        return sorted_result


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
    def transform_raid_totals_sum(raw_data: dict) -> dict:
        """
        New SUM transformation for raids.
        Returns a dictionary with:
         - "total": the combined total raids (after filtering)
         - "raid_level": a breakdown of total raids per raid_level.
        Assumes raw_data is already filtered.
        """
        total = 0
        raid_level_totals = {}
        for key, value in raw_data.items():
            parts = key.split(":")
            if len(parts) != 7:
                continue
            # parts: raid_pokemon, raid_level, raid_form, raid_costume, raid_is_exclusive, raid_ex_eligible, metric
            _, rl, _, _, _, _, _ = parts
            try:
                val = int(value)
            except Exception as e:
                logger.error(f"Could not convert value {value} for key {key}: {e}")
                val = 0
            total += val
            raid_level_totals[rl] = raid_level_totals.get(rl, 0) + val
        return {"total": total, "raid_level": raid_level_totals}


    @staticmethod
    def transform_raid_totals_grouped(raw_aggregated: dict) -> dict:
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
            logger.debug(f"▶️ Processing field: {field} with value: {value}")
            parts = field.split(":")
            if len(parts) != 7:
                logger.debug(f"⏭️ Skipping field {field} because it does not have 7 parts")
                continue  # Skip keys that don't follow the expected format.
            raid_pokemon, raid_level, raid_form, raid_costume, raid_is_exclusive, raid_ex_eligible, metric = parts
            try:
                val = int(value)
            except Exception as e:
                logger.error(f"❌ Could not convert value {value} of field {field} to int: {e}")
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

        logger.debug(f"✅ Raid Transformation complete. Final breakdown: {breakdown}")
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
            logger.debug(f"▶️ Processing field: {field} with value: {value}")
            parts = field.split(":")
            if len(parts) != 5:
                logger.debug(f"⏭️ Skipping field {field} because it does not have 5 parts (found {len(parts)})")
                continue
            display_type, character, grunt, confirmed, metric = parts
            try:
                val = int(value)
            except Exception as e:
                logger.error(f"❌ Could not convert value '{value}' for field '{field}' to int: {e}")
                val = 0
            breakdown["total"] += val
            logger.debug(f"☑️ Added {val} to total; running total: {breakdown['total']}")

            # Combine display_type and character.
            key_dc = f"{display_type}:{character}"
            breakdown["display_type+character"][key_dc] = breakdown["display_type+character"].get(key_dc, 0) + val
            logger.debug(f"⬆️ Updated display_type+character for {key_dc}: {breakdown['display_type+character'][key_dc]}")

            breakdown["grunt"][grunt] = breakdown["grunt"].get(grunt, 0) + val
            logger.debug(f"⬆️ Updated grunt for {grunt}: {breakdown['grunt'][grunt]}")

            breakdown["confirmed"][confirmed] = breakdown["confirmed"].get(confirmed, 0) + val
            logger.debug(f"⬆️ Updated confirmed for {confirmed}: {breakdown['confirmed'][confirmed]}")

        logger.debug(f"🔍 Before sorting, breakdown: {breakdown}")
        breakdown["display_type+character"] = dict(sorted(breakdown["display_type+character"].items()))
        try:
            breakdown["grunt"] = dict(sorted(breakdown["grunt"].items(), key=lambda item: int(item[0]) if item[0].isdigit() else item[0]))
        except Exception as e:
            logger.warning(f"❌ Could not sort grunt numerically: {e}")
            breakdown["grunt"] = dict(sorted(breakdown["grunt"].items()))
        breakdown["confirmed"] = dict(sorted(breakdown["confirmed"].items()))
        logger.debug(f"✅ Invasion Transformation complete. Final breakdown: {breakdown}")
        return breakdown


    @staticmethod
    def transform_quest_totals_sum(raw_aggregated: dict, mode: str = "sum") -> dict:
        """
        Transforms raw aggregated quest totals (in sum mode) into a detailed breakdown.

        Expected field format (8 parts):
            "{mode}:{normal_type}:{reward_normal_type}:{reward_normal_item_id}:{reward_normal_item_amount}:{reward_normal_poke_id}:{reward_normal_poke_form}:total"

        In sum mode the function behaves as before.
        In grouped mode the function will return a dictionary keyed by the date portion
        (extracted from the full Redis key) with each date’s breakdown.

        Breakdown keys:
          - "quest_mode": aggregated sum per quest mode (parts[0]).
          - "reward_type": aggregated sum per reward type (parts[2]).
          - "reward_item": aggregated sum per reward item ID (parts[3]).
          - "reward_item_amount": aggregated sum per reward item amount (parts[4]).
          - "reward_poke": aggregated sum per reward poke ID (parts[5]).
          - "reward_poke_form": aggregated sum per reward poke form (parts[6]).
          - "total": overall total.
        """
        if mode == "sum":
            # Flat aggregation (ignoring the date portion)
            breakdown = {
                "quest_mode": {},
                "reward_type": {},
                "reward_item": {},
                "reward_item_amount": {},
                "reward_poke": {},
                "reward_poke_form": {},
                "total": 0
            }
            logger.info("▶️ Starting transform_quest_totals_sum (sum mode)")
            for field, value in raw_aggregated.items():
                logger.debug(f"▶️ Processing field: {field} with value: {value}")
                parts = field.split(":")
                if len(parts) < 7:
                    logger.debug(f"⏭️ Skipping field {field} because it does not have at least 7 parts (found {len(parts)})")
                    continue
                # Use the first 7 parts (ignoring the date if present)
                if len(parts) >= 8:
                    quest_mode = parts[0]
                    reward_type = parts[2]
                    reward_item = parts[3]
                    reward_item_amount = parts[4]
                    reward_poke = parts[5]
                    reward_poke_form = parts[6]
                else:
                    quest_mode, reward_type, reward_item, reward_item_amount, reward_poke, reward_poke_form, _ = parts

                try:
                    val = int(value)
                except Exception as e:
                    logger.error(f"❌ Could not convert value '{value}' for field '{field}' to int: {e}")
                    val = 0

                breakdown["total"] += val
                logger.debug(f"☑️ Added {val} to total; running total: {breakdown['total']}")
                breakdown["quest_mode"][quest_mode] = breakdown["quest_mode"].get(quest_mode, 0) + val
                breakdown["reward_type"][reward_type] = breakdown["reward_type"].get(reward_type, 0) + val
                breakdown["reward_item"][reward_item] = breakdown["reward_item"].get(reward_item, 0) + val
                breakdown["reward_item_amount"][reward_item_amount] = breakdown["reward_item_amount"].get(reward_item_amount, 0) + val
                breakdown["reward_poke"][reward_poke] = breakdown["reward_poke"].get(reward_poke, 0) + val
                breakdown["reward_poke_form"][reward_poke_form] = breakdown["reward_poke_form"].get(reward_poke_form, 0) + val

            # Sort each inner dictionary lexicographically
            breakdown["quest_mode"] = dict(sorted(breakdown["quest_mode"].items()))
            breakdown["reward_type"] = dict(sorted(breakdown["reward_type"].items()))
            breakdown["reward_item"] = dict(sorted(breakdown["reward_item"].items()))
            breakdown["reward_item_amount"] = dict(sorted(breakdown["reward_item_amount"].items()))
            breakdown["reward_poke"] = dict(sorted(breakdown["reward_poke"].items()))
            breakdown["reward_poke_form"] = dict(sorted(breakdown["reward_poke_form"].items()))
            logger.debug(f"✅ Quest Transformation complete (sum mode). Final breakdown: {breakdown}")
            return breakdown

        elif mode == "grouped":
            # Group by date portion (extracted from the raw aggregated key)
            grouped_by_date = {}
            logger.info("▶️ Starting transform_quest_totals_sum (grouped mode)")
            for full_key, flat_fields in raw_aggregated.items():
                # Expect full_key to be in the format: "counter:quest_hourly:{area}:{YYYYMMDDHH}"
                parts = full_key.split(":")
                if len(parts) < 4:
                    continue
                date_str = parts[-1]  # e.g., "2025031601"
                # Initialize the breakdown for this date if not present.
                if date_str not in grouped_by_date:
                    grouped_by_date[date_str] = {
                        "quest_mode": {},
                        "reward_type": {},
                        "reward_item": {},
                        "reward_item_amount": {},
                        "reward_poke": {},
                        "reward_poke_form": {},
                        "total": 0
                    }
                # Process each field from the flat_fields
                for field, value in flat_fields.items():
                    logger.debug(f"▶️ Processing field: {field} with value: {value} (for date {date_str})")
                    parts_field = field.split(":")
                    if len(parts_field) < 7:
                        logger.debug(f"⏭️ Skipping field {field} (date {date_str}) because it does not have at least 7 parts (found {len(parts_field)})")
                        continue
                    if len(parts_field) >= 8:
                        quest_mode = parts_field[0]
                        reward_type = parts_field[2]
                        reward_item = parts_field[3]
                        reward_item_amount = parts_field[4]
                        reward_poke = parts_field[5]
                        reward_poke_form = parts_field[6]
                    else:
                        quest_mode, reward_type, reward_item, reward_item_amount, reward_poke, reward_poke_form, _ = parts_field

                    try:
                        val = int(value)
                    except Exception as e:
                        logger.error(f"❌ Could not convert value '{value}' for field '{field}' to int: {e}")
                        val = 0

                    grouped_by_date[date_str]["total"] += val
                    grouped_by_date[date_str]["quest_mode"][quest_mode] = grouped_by_date[date_str]["quest_mode"].get(quest_mode, 0) + val
                    grouped_by_date[date_str]["reward_type"][reward_type] = grouped_by_date[date_str]["reward_type"].get(reward_type, 0) + val
                    grouped_by_date[date_str]["reward_item"][reward_item] = grouped_by_date[date_str]["reward_item"].get(reward_item, 0) + val
                    grouped_by_date[date_str]["reward_item_amount"][reward_item_amount] = grouped_by_date[date_str]["reward_item_amount"].get(reward_item_amount, 0) + val
                    grouped_by_date[date_str]["reward_poke"][reward_poke] = grouped_by_date[date_str]["reward_poke"].get(reward_poke, 0) + val
                    grouped_by_date[date_str]["reward_poke_form"][reward_poke_form] = grouped_by_date[date_str]["reward_poke_form"].get(reward_poke_form, 0) + val

            # Optionally, sort each inner breakdown dictionary (keys are strings, so lexicographical sort is fine)
            for date in grouped_by_date:
                grouped_by_date[date]["quest_mode"] = dict(sorted(grouped_by_date[date]["quest_mode"].items()))
                grouped_by_date[date]["reward_type"] = dict(sorted(grouped_by_date[date]["reward_type"].items()))
                grouped_by_date[date]["reward_item"] = dict(sorted(grouped_by_date[date]["reward_item"].items()))
                grouped_by_date[date]["reward_item_amount"] = dict(sorted(grouped_by_date[date]["reward_item_amount"].items()))
                grouped_by_date[date]["reward_poke"] = dict(sorted(grouped_by_date[date]["reward_poke"].items()))
                grouped_by_date[date]["reward_poke_form"] = dict(sorted(grouped_by_date[date]["reward_poke_form"].items()))
            logger.debug(f"✅ Quest Transformation complete (grouped mode). Final breakdown: {grouped_by_date}")
            return grouped_by_date

        elif mode == "surged":
            # Group by the hour-of-day extracted from the date portion.
            surged = {}
            logger.info("▶️ Starting transform_quest_totals_sum (surged mode)")
            for full_key, flat_fields in raw_aggregated.items():
                parts_key = full_key.split(":")
                if len(parts_key) < 4:
                    continue
                date_str = parts_key[-1]  # e.g., "2025031601"
                # Extract the hour (last two characters of date_str)
                hour = date_str[-2:]
                if hour not in surged:
                    surged[hour] = {
                        "quest_mode": {},
                        "reward_type": {},
                        "reward_item": {},
                        "reward_item_amount": {},
                        "reward_poke": {},
                        "reward_poke_form": {},
                        "total": 0
                    }
                for field, value in flat_fields.items():
                    logger.debug(f"Processing field: {field} with value: {value} (for hour {hour})")
                    parts_field = field.split(":")
                    if len(parts_field) < 7:
                        logger.debug(f"⏭️ Skipping field {field} (hour {hour}) because it does not have at least 7 parts (found {len(parts_field)})")
                        continue
                    quest_mode = parts_field[0]
                    reward_type = parts_field[2]
                    reward_item = parts_field[3]
                    reward_item_amount = parts_field[4]
                    reward_poke = parts_field[5]
                    reward_poke_form = parts_field[6]
                    try:
                        val = int(value)
                    except Exception as e:
                        logger.error(f"❌ Could not convert value '{value}' for field '{field}' to int: {e}")
                        val = 0
                    surged[hour]["total"] += val
                    surged[hour]["quest_mode"][quest_mode] = surged[hour]["quest_mode"].get(quest_mode, 0) + val
                    surged[hour]["reward_type"][reward_type] = surged[hour]["reward_type"].get(reward_type, 0) + val
                    surged[hour]["reward_item"][reward_item] = surged[hour]["reward_item"].get(reward_item, 0) + val
                    surged[hour]["reward_item_amount"][reward_item_amount] = surged[hour]["reward_item_amount"].get(reward_item_amount, 0) + val
                    surged[hour]["reward_poke"][reward_poke] = surged[hour]["reward_poke"].get(reward_poke, 0) + val
                    surged[hour]["reward_poke_form"][reward_poke_form] = surged[hour]["reward_poke_form"].get(reward_poke_form, 0) + val
            # Sort each inner breakdown dictionary lexicographically
            for hour in surged:
                surged[hour]["quest_mode"] = dict(sorted(surged[hour]["quest_mode"].items()))
                surged[hour]["reward_type"] = dict(sorted(surged[hour]["reward_type"].items()))
                surged[hour]["reward_item"] = dict(sorted(surged[hour]["reward_item"].items()))
                surged[hour]["reward_item_amount"] = dict(sorted(surged[hour]["reward_item_amount"].items()))
                surged[hour]["reward_poke"] = dict(sorted(surged[hour]["reward_poke"].items()))
                surged[hour]["reward_poke_form"] = dict(sorted(surged[hour]["reward_poke_form"].items()))
            logger.debug(f"✅ Transformation complete (surged mode). Final breakdown: {surged}")
            return surged

        else:
            logger.warning("⚠️ Mode not recognized; returning raw aggregated data.")
            return raw_aggregated

