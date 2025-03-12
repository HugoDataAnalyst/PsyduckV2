from typing import Optional
from datetime import datetime
from utils.logger import logger
from tortoise.exceptions import DoesNotExist
from utils.calc_iv_bucket import get_iv_bucket
from sql.models import AggregatedPokemonIVMonthly, ShinyUsernameRates

class PokemonUpdatesQueries:
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
            logger.warning("Bucket conversion returned None; skipping upsert.")
            return None

        # Convert first_seen timestamp to a month-year value.
        dt = datetime.fromtimestamp(first_seen_timestamp)
        month_year = int(dt.strftime("%y%m"))

        try:
            obj = await AggregatedPokemonIVMonthly.get(
                spawnpoint_id=spawnpoint_id,
                pokemon_id=pokemon_id,
                form=form,
                iv=bucket_iv,
                area_id=area_id,
                month_year=month_year
            )
            obj.total_count += increment
            await obj.save()
            logger.debug(f"⬆️ Updated AggregatedPokemonIVMonthly: {obj}")
            return obj
        except DoesNotExist:
            obj = await AggregatedPokemonIVMonthly.create(
                spawnpoint_id=spawnpoint_id,
                latitude=latitude,
                longitude=longitude,
                pokemon_id=pokemon_id,
                form=form,
                iv=bucket_iv,
                area_id=area_id,
                month_year=month_year,
                total_count=increment
            )
            logger.debug(f"✅ Created new AggregatedPokemonIVMonthly: {obj}")
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

        try:
            obj = await ShinyUsernameRates.get(
                username=username,
                pokemon_id=pokemon_id,
                form=form,
                shiny=shiny,
                area_id=area_id,
                month_year=month_year
            )
            obj.total_count += increment
            await obj.save()
            logger.debug(f"⬆️ Updated ShinyUsernameRates: {obj}")
            return obj
        except DoesNotExist:
            obj = await ShinyUsernameRates.create(
                username=username,
                pokemon_id=pokemon_id,
                form=form,
                shiny=shiny,
                area_id=area_id,
                month_year=month_year,
                total_count=increment
            )
            logger.debug(f"✅ Created new ShinyUsernameRates: {obj}")
            return obj
