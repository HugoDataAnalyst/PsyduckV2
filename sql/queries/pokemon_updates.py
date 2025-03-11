from typing import Optional
from datetime import datetime
from utils.logger import logger
from tortoise.exceptions import DoesNotExist
from sql.models import AggregatedPokemonIVMonthly

class PokemonUpdatesQueries:
    @staticmethod
    def get_iv_bucket(iv: int) -> Optional[int]:
        """
        Convert a raw IV (0-100) into a bucket.

        Buckets:
        - Exactly 0:         0
        - >0 and <=25:      25
        - >25 and <=50:     50
        - >50 and <=75:     75
        - >75 and <=90:     90
        - >90 and <100:     95
        - Exactly 100:      100

        If the IV is out of the 0–100 range, returns None.
        """
        if iv < 0 or iv > 100:
            logger.warning(f"IV value {iv} out of range (0-100); returning None.")
            return None

        bucket = None
        if iv == 0:
            bucket = 0
        elif iv == 100:
            bucket = 100
        elif iv <= 25:
            bucket = 25
        elif iv <= 50:
            bucket = 50
        elif iv <= 75:
            bucket = 75
        elif iv <= 90:
            bucket = 90
        elif iv < 100:
            bucket = 95

        logger.debug(f"Raw IV {iv} mapped to bucket {bucket}.")
        return bucket

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
        bucket_iv = cls.get_iv_bucket(raw_iv)
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
