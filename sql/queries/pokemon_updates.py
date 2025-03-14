from typing import Optional
from datetime import datetime
from utils.logger import logger
from tortoise.exceptions import DoesNotExist
from utils.calc_iv_bucket import get_iv_bucket
from sql.models import AggregatedPokemonIVMonthly, ShinyUsernameRates, Spawnpoint

class PokemonUpdatesQueries:
    @staticmethod
    async def upsert_spawnpoint(spawnpoint_id: int, latitude: float, longitude: float) -> Spawnpoint:
        """
        Insert the spawnpoint into the Spawnpoints table if it doesn't exist.
        Returns the Spawnpoint object.
        """
        # Use get_or_create to avoid duplicates.
        obj, created = await Spawnpoint.get_or_create(
            spawnpoint=spawnpoint_id,
            defaults={"latitude": latitude, "longitude": longitude}
        )
        if not created:
            updated = False
            # Update the latitude/longitude if they've changed.
            if obj.latitude != latitude or obj.longitude != longitude:
                obj.latitude = latitude
                obj.longitude = longitude
                updated = True
            if updated:
                await obj.save()
            logger.info(f"⏭️ Spawnpoint exists: id={obj.id}, spawnpoint={obj.spawnpoint}, lat={obj.latitude}, lon={obj.longitude}")
        else:
            logger.debug(f"✅ Created new spawnpoint: id={obj.id}, spawnpoint={obj.spawnpoint}, lat={obj.latitude}, lon={obj.longitude}")
        return obj

    @classmethod
    async def upsert_aggregated_pokemon_iv_monthly(
        cls,
        spawnpoint_id: int,
        latitude: float,
        longitude: float,
        pokemon_id: int,
        form: int,
        raw_iv: int,
        area_id: int,
        first_seen_timestamp: int,
        increment: int = 1
    ):
        """
        Insert or update the AggregatedPokemonIVMonthly record.

        The record is uniquely identified by:
        spawnpoint_id, pokemon_id, form, iv (bucket), area, and month_year.
        The month_year is derived from the first_seen timestamp in YYMM format.
        """
        # Convert the raw IV to its bucket representation.
        bucket_iv = get_iv_bucket(raw_iv)
        if bucket_iv is None:
            logger.warning("⚠️ Bucket conversion returned None; skipping upsert.")
            return None

        # Convert first_seen timestamp to a month-year value.
        dt = datetime.fromtimestamp(first_seen_timestamp)
        month_year = int(dt.strftime("%y%m"))

        # Upsert the spawnpoint (or retrieve existing).
        spawnpoint_obj = await cls.upsert_spawnpoint(spawnpoint_id, latitude, longitude)
        # Use the spawnpoint_obj's ID for the foreign key.
        sp_obj_id = spawnpoint_obj.id

        # Use get_or_create to fetch an existing record or create a new one.
        obj, created = await AggregatedPokemonIVMonthly.get_or_create(
            spawnpoint_id=sp_obj_id,
            pokemon_id=pokemon_id,
            form=form,
            iv=bucket_iv,
            area_id=area_id,
            month_year=month_year,
            defaults={
                "total_count": increment
            }
        )

        if not created:
            obj.total_count += increment
            await obj.save()
            logger.debug(f"⬆️ Updated AggregatedPokemonIVMonthly: Pokémon={obj.pokemon_id}, Form={obj.form}, IV={obj.iv}, Area={obj.area}")
        else:
            logger.debug(f"✅ Created new AggregatedPokemonIVMonthly: Pokémon={obj.pokemon_id}, Form={obj.form}, IV={obj.iv}, Area={obj.area}")

        return obj

    @classmethod
    async def upsert_shiny_username_rate(
        cls,
        username: str,
        pokemon_id: int,
        form: int,
        shiny: int,
        area_id: int,
        first_seen_timestamp: int,
        increment: int = 1
    ):
        """
        Insert or update the ShinyUsernameRates record.

        The record is uniquely identified by:
        username, pokemon_id, form, shiny, area, and month_year.
        The month_year is derived from the first_seen timestamp in YYMM format.
        """
        dt = datetime.fromtimestamp(first_seen_timestamp)
        month_year = int(dt.strftime("%y%m"))

        # Use get_or_create to fetch or create the record atomically.
        obj, created = await ShinyUsernameRates.get_or_create(
            username=username,
            pokemon_id=pokemon_id,
            form=form,
            shiny=shiny,
            area_id=area_id,
            month_year=month_year,
            defaults={'total_count': increment}
        )

        if not created:
            obj.total_count += increment
            await obj.save()
            logger.debug(f"⬆️ Updated ShinyUsernameRates: {obj}")
        else:
            logger.debug(f"✅ Created new ShinyUsernameRates: {obj}")

        return obj
