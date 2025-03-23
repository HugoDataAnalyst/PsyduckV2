from sql.queries.pokemon_updates import PokemonUpdatesQueries
from utils.logger import logger
from utils.calc_iv_bucket import get_iv_bucket
from sql.models import Spawnpoint, AggregatedPokemonIVMonthly, ShinyUsernameRates, Spawnpoint, AreaNames
from datetime import datetime

class PokemonSQLProcessor(PokemonUpdatesQueries):
    @classmethod
    async def bulk_upsert_aggregated_aggregated(cls, aggregated_data: dict) -> int:
        total_upsert_count = 0
        records = []
        spawnpoints_to_insert = {}
        # Step 1: Parse the aggregated data into a list of records.
        for composite_key, count in aggregated_data.items():
            try:
                parts = composite_key.split("_")
                if len(parts) != 8:
                    logger.warning(f"Invalid composite key format: {composite_key}")
                    continue
                (spawnpoint_hex, pokemon_id_str, form_str, bucket_iv_str,
                area_id_str, month_year_str, latitude_str, longitude_str) = parts

                spawnpoint_value = int(spawnpoint_hex, 16)
                record = {
                    "spawnpoint": spawnpoint_value,  # Use "spawnpoint" as the field name.
                    "pokemon_id": int(pokemon_id_str),
                    "form": int(form_str) if form_str.isdigit() else form_str,
                    "iv": int(bucket_iv_str),
                    "area_id": int(area_id_str),
                    "month_year": int(month_year_str),
                    "total_count": count,
                }
                records.append(record)
                # Build mapping for spawnpoints to insert.
                if spawnpoint_value not in spawnpoints_to_insert:
                    spawnpoints_to_insert[spawnpoint_value] = (float(latitude_str), float(longitude_str))
            except Exception as e:
                logger.error(f"❌ Failed to pre-process aggregated event {composite_key}: {e}", exc_info=True)

        if not records:
            logger.warning("⚠️ No valid aggregated records to upsert.")
            return 0

        #  Bulk Insert the spawnpoints.
        await cls.bulk_insert_spawnpoints(spawnpoints_to_insert)
        logger.info(f"🔂 ✅ Bulk inserted spawnpoints for {len(spawnpoints_to_insert)} unique spawnpoint values.")


        # Step 2: Build a set of unique keys (tuples) for each record.
        unique_keys = {
            (
                rec["spawnpoint"],
                rec["pokemon_id"],
                rec["form"],
                rec["iv"],
                rec["area_id"],
                rec["month_year"]
            )
            for rec in records
        }

        # Step 3: Retrieve existing records matching these unique keys.
        existing_objs = await AggregatedPokemonIVMonthly.filter(
            spawnpoint__in=[k[0] for k in unique_keys],
            pokemon_id__in=[k[1] for k in unique_keys],
            form__in=[k[2] for k in unique_keys],
            iv__in=[k[3] for k in unique_keys],
            area_id__in=[k[4] for k in unique_keys],
            month_year__in=[k[5] for k in unique_keys],
        )
        # Build a mapping of unique key -> existing object.
        existing_map = {
            (obj.spawnpoint, obj.pokemon_id, obj.form, obj.iv, obj.area_id, obj.month_year): obj
            for obj in existing_objs
        }

        # Step 4: Partition records into new and to-update.
        new_objs = []
        update_objs = []
        for rec in records:
            key = (rec["spawnpoint"], rec["pokemon_id"], rec["form"], rec["iv"], rec["area_id"], rec["month_year"])
            if key in existing_map:
                # Update existing object's total_count.
                obj = existing_map[key]
                obj.total_count += rec["total_count"]
                update_objs.append(obj)
            else:
                # Create a new AggregatedPokemonIVMonthly instance.
                new_objs.append(AggregatedPokemonIVMonthly(**rec))

        # Step 5: Bulk create new records.
        if new_objs:
            await AggregatedPokemonIVMonthly.bulk_create(new_objs)
            logger.success(f"🆕 Bulk created {len(new_objs)} new aggregated rows.")

        # Step 6: Bulk update existing records.
        if update_objs:
            await AggregatedPokemonIVMonthly.bulk_update(update_objs, fields=["total_count"])
            logger.success(f"🔁 Bulk updated {len(update_objs)} existing aggregated rows.")

        total_upsert_count = len(new_objs) + len(update_objs)
        logger.info(f"Aggregated upsert completed: {total_upsert_count} total upserts.")
        return total_upsert_count


    @staticmethod
    async def bulk_insert_spawnpoints(spawn_dict: dict) -> dict:
        try:
            objs = [
                Spawnpoint(spawnpoint=spid, latitude=lat, longitude=lon)
                for spid, (lat, lon) in spawn_dict.items()
            ]
            await Spawnpoint.bulk_create(objs, ignore_conflicts=True)
            logger.info(f"🆕 Attempted to insert {len(objs)} new spawnpoints (ignore_conflicts=True).")
        except Exception as e:
            # If the error is due to duplicate entries, ignore it.
            if "Duplicate entry" in str(e):
                logger.debug("Duplicate entry error ignored during bulk insert of spawnpoints.")
            else:
                logger.error(f"❌ Error during bulk insert of spawnpoints: {e}")
        # Return the original mapping, as the spawnpoint value is used directly.
        return spawn_dict


    @classmethod
    async def bulk_upsert_shiny_rates_aggregated(cls, aggregated_data: dict) -> int:
        total_upsert_count = 0
        records = []
        # Step 1: Parse the aggregated data into a list of record dictionaries.
        for composite_key, count in aggregated_data.items():
            try:
                parts = composite_key.split("_")
                if len(parts) != 6:
                    logger.warning(f"Invalid aggregated key format: {composite_key}")
                    continue
                username, pokemon_id_str, form_str, shiny_str, area_id_str, month_year_str = parts
                record = {
                    "username": username,
                    "pokemon_id": int(pokemon_id_str),
                    "form": form_str,  # Convert if needed; here we keep it as a string.
                    "shiny": int(shiny_str),
                    "area_id": int(area_id_str),
                    "month_year": int(month_year_str),
                    "total_count": count,
                }
                records.append(record)
            except Exception as e:
                logger.error(f"❌ Failed to pre-process aggregated shiny event {composite_key}: {e}", exc_info=True)

        if not records:
            logger.warning("⚠️ No valid aggregated shiny records to upsert.")
            return 0

        # Step 2: Build a set of unique keys (tuples) for each record.
        unique_keys = {
            (rec["username"], rec["pokemon_id"], rec["form"], rec["shiny"], rec["area_id"], rec["month_year"])
            for rec in records
        }

        # Step 3: Retrieve existing records matching these unique keys.
        existing_objs = await ShinyUsernameRates.filter(
            username__in=[k[0] for k in unique_keys],
            pokemon_id__in=[k[1] for k in unique_keys],
            form__in=[k[2] for k in unique_keys],
            shiny__in=[k[3] for k in unique_keys],
            area_id__in=[k[4] for k in unique_keys],
            month_year__in=[k[5] for k in unique_keys],
        )
        # Build a mapping of unique key -> existing object.
        existing_map = {
            (obj.username, obj.pokemon_id, obj.form, obj.shiny, obj.area_id, obj.month_year): obj
            for obj in existing_objs
        }

        # Step 4: Partition records into new objects and objects to update.
        new_objs = []
        update_objs = []
        for rec in records:
            key = (rec["username"], rec["pokemon_id"], rec["form"], rec["shiny"], rec["area_id"], rec["month_year"])
            if key in existing_map:
                obj = existing_map[key]
                obj.total_count += rec["total_count"]
                update_objs.append(obj)
            else:
                new_objs.append(ShinyUsernameRates(**rec))

        # Step 5: Bulk create new records.
        if new_objs:
            await ShinyUsernameRates.bulk_create(new_objs)
            logger.success(f"🆕 Bulk created {len(new_objs)} new aggregated shiny rows.")

        # Step 6: Bulk update existing records.
        if update_objs:
            await ShinyUsernameRates.bulk_update(update_objs, fields=["total_count"])
            logger.success(f"🔁 Bulk updated {len(update_objs)} existing aggregated shiny rows.")

        total_upsert_count = len(new_objs) + len(update_objs)
        logger.info(f"Aggregated shiny upsert completed: {total_upsert_count} total upserts.")
        return total_upsert_count
