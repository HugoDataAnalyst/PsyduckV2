from sql.queries.pokemon_updates import PokemonUpdatesQueries
from utils.logger import logger
from utils.calc_iv_bucket import get_iv_bucket
from sql.models import Spawnpoint, AggregatedPokemonIVMonthly, ShinyUsernameRates, Spawnpoint, AreaNames
from datetime import datetime

class PokemonSQLProcessor(PokemonUpdatesQueries):
    @classmethod
    async def bulk_upsert_aggregated(cls, buffered_events: list) -> int:
        success_count = 0

        # Step 1: Preload all known spawnpoints from the DB
        existing_spawnpoints = await cls.preload_spawnpoints()
        missing_spawnpoints = {}

        # Step 2: First pass to collect known vs unknown spawnpoints
        entries_to_insert = []
        deferred_events = []

        for event in buffered_events:
            try:
                spawnpoint_hex = event.get("spawnpoint")
                if not spawnpoint_hex:
                    continue

                spawnpoint_id = int(spawnpoint_hex, 16)
                latitude = event.get("latitude")
                longitude = event.get("longitude")
                pokemon_id = event.get("pokemon_id")
                form = event.get("form", 0)
                raw_iv = event.get("iv")
                area_id = event.get("area_id")
                first_seen_timestamp = event.get("first_seen")

                if None in [latitude, longitude, pokemon_id, raw_iv, area_id, first_seen_timestamp]:
                    continue

                bucket_iv = get_iv_bucket(raw_iv)
                if bucket_iv is None:
                    continue

                dt = datetime.fromtimestamp(first_seen_timestamp)
                month_year = int(dt.strftime("%y%m"))

                if spawnpoint_id not in existing_spawnpoints:
                    missing_spawnpoints[spawnpoint_id] = (latitude, longitude)
                    deferred_events.append((spawnpoint_id, latitude, longitude, pokemon_id, form, bucket_iv, area_id, month_year))
                    continue

                entries_to_insert.append((existing_spawnpoints[spawnpoint_id], pokemon_id, form, bucket_iv, area_id, month_year))
            except Exception as e:
                logger.error(f"❌ Failed to pre-process Pokémon event: {e}")

        # Step 3: Insert missing spawnpoints and update cache
        if missing_spawnpoints:
            inserted = await cls.bulk_insert_spawnpoints(missing_spawnpoints)
            existing_spawnpoints.update(inserted)
        else:
            inserted = {}

        # Step 4: Retry deferred entries with newly inserted spawnpoints
        for spawnpoint_id, lat, lon, pid, form, bucket_iv, area_id, month_year in deferred_events:
            if spawnpoint_id not in existing_spawnpoints:
                continue
            entries_to_insert.append((existing_spawnpoints[spawnpoint_id], pid, form, bucket_iv, area_id, month_year))

        logger.info(f"🧩 {len(missing_spawnpoints)} missing spawnpoints attempted. Got back {len(inserted)} new entries.")

        # Step 5: Upsert AggregatedPokemonIVMonthly
        logger.info(f"📥 Ready to insert {len(entries_to_insert)} Aggregated rows after processing {len(buffered_events)} events.")

        if len(entries_to_insert) == 0:
            logger.warning("⚠️ No entries to insert. All events may have been skipped or invalid.")
            return 0

        for spawn_fk_id, pid, form, bucket_iv, area_id, month_year in entries_to_insert:
            try:
                obj, created = await AggregatedPokemonIVMonthly.get_or_create(
                    spawnpoint_id=spawn_fk_id,
                    pokemon_id=pid,
                    form=form,
                    iv=bucket_iv,
                    area_id=area_id,
                    month_year=month_year,
                    defaults={"total_count": 1}
                )
                if not created:
                    obj.total_count += 1
                    await obj.save()
                    logger.debug(f"🔁 Updated: {pid} | {form} | IV {bucket_iv} | Area {area_id}")
                else:
                    logger.debug(f"🆕 Created: {pid} | {form} | IV {bucket_iv} | Area {area_id}")
                success_count += 1
            except Exception as e:
                logger.error(f"❌ Failed to insert Aggregated row: {e}")

        return success_count

    @classmethod
    async def bulk_upsert_aggregated_aggregated(cls, aggregated_data: dict) -> int:
        # Step 1: Parse the aggregated data into a list of records.
        records = []
        for composite_key, count in aggregated_data.items():
            try:
                parts = composite_key.split("_")
                if len(parts) != 8:
                    logger.warning(f"Invalid composite key format: {composite_key}")
                    continue
                (spawnpoint_hex, pokemon_id_str, form_str, bucket_iv_str,
                area_id_str, month_year_str, latitude_str, longitude_str) = parts

                record = {
                    "spawnpoint_id": int(spawnpoint_hex, 16),
                    "pokemon_id": int(pokemon_id_str),
                    "form": int(form_str) if form_str.isdigit() else form_str,
                    "iv": int(bucket_iv_str),
                    "area_id": int(area_id_str),
                    "month_year": int(month_year_str),
                    "total_count": count,
                }
                records.append(record)
            except Exception as e:
                logger.error(f"❌ Failed to pre-process aggregated event {composite_key}: {e}", exc_info=True)

        if not records:
            logger.warning("⚠️ No valid aggregated records to upsert.")
            return 0

        # Step 2: Build a set of unique keys (tuples) for each record.
        unique_keys = {
            (
                rec["spawnpoint_id"],
                rec["pokemon_id"],
                rec["form"],
                rec["iv"],
                rec["area_id"],
                rec["month_year"]
            )
            for rec in records
        }

        # Step 3: Retrieve existing records matching these unique keys.
        # (This assumes your database has a unique constraint on these fields.)
        # Note: Depending on your DB backend and the size of unique_keys, you might need to chunk this query.
        existing_objs = await AggregatedPokemonIVMonthly.filter(
            spawnpoint_id__in=[k[0] for k in unique_keys],
            pokemon_id__in=[k[1] for k in unique_keys],
            form__in=[k[2] for k in unique_keys],
            iv__in=[k[3] for k in unique_keys],
            area_id__in=[k[4] for k in unique_keys],
            month_year__in=[k[5] for k in unique_keys],
        )
        # Build a mapping of unique key -> existing object.
        existing_map = {
            (obj.spawnpoint_id, obj.pokemon_id, obj.form, obj.iv, obj.area_id, obj.month_year): obj
            for obj in existing_objs
        }

        # Step 4: Partition records into new and to-update.
        new_objs = []
        update_objs = []
        for rec in records:
            key = (rec["spawnpoint_id"], rec["pokemon_id"], rec["form"], rec["iv"], rec["area_id"], rec["month_year"])
            if key in existing_map:
                # Update existing object's total_count.
                obj = existing_map[key]
                obj.total_count += rec["total_count"]
                update_objs.append(obj)
            else:
                # Create a new model instance.
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
    async def preload_spawnpoints() -> dict:
        spawnpoints = await Spawnpoint.all()
        return {s.spawnpoint: s.id for s in spawnpoints}

    @staticmethod
    async def bulk_insert_spawnpoints(spawn_dict: dict) -> dict:
        inserted = {}
        try:
            objs = [Spawnpoint(spawnpoint=spid, latitude=lat, longitude=lon)
                    for spid, (lat, lon) in spawn_dict.items()]
            await Spawnpoint.bulk_create(objs, ignore_conflicts=True)
            # Reload inserted IDs
            refreshed = await Spawnpoint.filter(spawnpoint__in=spawn_dict.keys())
            for sp in refreshed:
                inserted[sp.spawnpoint] = sp.id
            logger.info(f"🆕 Attempted to insert {len(objs)} new spawnpoints (ignore_conflicts=True).")
        except Exception as e:
            logger.error(f"❌ Error during bulk insert of spawnpoints: {e}")
        return inserted

    @classmethod
    async def bulk_upsert_shiny_rates(cls, buffered_events: list) -> int:
        # Existing non-aggregated approach for shiny rates remains available
        success_count = 0
        for event in buffered_events:
            try:
                username = event.get("username")
                pokemon_id = event.get("pokemon_id")
                form = event.get("form", 0)
                shiny = int(event.get("shiny", 0))
                area_id = event.get("area_id")
                first_seen_timestamp = event.get("first_seen")
                if None in [username, pokemon_id, area_id, first_seen_timestamp]:
                    continue

                result = await cls.upsert_shiny_username_rate(
                    username=username,
                    pokemon_id=pokemon_id,
                    form=form,
                    shiny=shiny,
                    area_id=area_id,
                    first_seen_timestamp=first_seen_timestamp,
                    increment=1
                )
                if result:
                    success_count += 1
            except Exception as e:
                logger.error(f"❌ Failed to upsert shiny rate event: {e}")
        return success_count

    @classmethod
    async def bulk_upsert_shiny_rates_aggregated(cls, aggregated_data: dict) -> int:
        """
        Expects aggregated_data as a dictionary with keys formatted as:
        "username_pokemonId_form_shiny_areaId_monthYear"
        and values as the count to increment.
        """
        success_count = 0
        for composite_key, count in aggregated_data.items():
            try:
                # Parse the composite key to extract values
                parts = composite_key.split("_")
                if len(parts) != 6:
                    logger.warning(f"Invalid aggregated key format: {composite_key}")
                    continue

                username, pokemon_id_str, form_str, shiny_str, area_id_str, month_year_str = parts
                pokemon_id = int(pokemon_id_str)
                form = form_str  # or int(form_str) if your schema expects an integer
                shiny = int(shiny_str)
                area_id = int(area_id_str)
                month_year = int(month_year_str)

                # Upsert into ShinyUsernameRates model
                obj, created = await ShinyUsernameRates.get_or_create(
                    username=username,
                    pokemon_id=pokemon_id,
                    form=form,
                    shiny=shiny,
                    area_id=area_id,
                    month_year=month_year,
                    defaults={"total_count": count}
                )
                if not created:
                    obj.total_count += count
                    await obj.save()
                    logger.debug(f"🔁 Updated shiny rate: {composite_key} by {count}")
                else:
                    logger.debug(f"🆕 Created shiny rate: {composite_key} with count {count}")
                success_count += 1
            except Exception as e:
                logger.error(f"❌ Failed to upsert aggregated shiny rate for key {composite_key}: {e}")
        return success_count
