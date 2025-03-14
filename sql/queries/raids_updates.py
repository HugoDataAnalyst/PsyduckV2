from datetime import datetime
from utils.logger import logger
from tortoise.exceptions import DoesNotExist
from sql.models import AggregatedRaids, Gyms

class RaidsSQLProcessor:
    @staticmethod
    async def upsert_gym(gym_id: str, gym_name: str, latitude: float, longitude: float):
        """
        Insert the gym into the Gyms table if it doesn't exist.
        Returns the Gyms object.
        """
        obj, created = await Gyms.get_or_create(
            gym=gym_id,
            defaults={"gym_name": gym_name, "latitude": latitude, "longitude": longitude}
        )
        if not created:
            updated = False
            if obj.gym_name != gym_name:
                obj.gym_name = gym_name
                updated = True
            if obj.latitude != latitude or obj.longitude != longitude:
                obj.latitude = latitude
                obj.longitude = longitude
                updated = True
            if updated:
                await obj.save()
            logger.info(f"Gym exists: id={obj.id}, gym={obj.gym}, gym_name={obj.gym_name}")
        else:
            logger.info(f"✅ Created new gym: id={obj.id}, gym={obj.gym}, gym_name={obj.gym_name}")
        return obj

    @classmethod
    async def upsert_aggregated_raid(
        cls,
        gym_id: str,
        gym_name: str,
        latitude: float,
        longitude: float,
        raid_pokemon: int,
        raid_level: int,
        raid_form: int,
        raid_team: int,
        raid_costume: int,
        raid_is_exclusive: int,
        raid_ex_raid_eligible: int,
        area_id: int,
        first_seen_timestamp: int,
        increment: int = 1
    ):
        """
        Insert or update the AggregatedRaids record.

        The record is uniquely identified by:
          gym (FK), raid_pokemon, raid_level, raid_form, raid_team, raid_costume,
          raid_is_exclusive, raid_ex_raid_eligible, area, and month_year.
        The month_year is derived from the first_seen timestamp in YYMM format.
        """
        dt = datetime.fromtimestamp(first_seen_timestamp)
        month_year = int(dt.strftime("%y%m"))

        # Upsert the gym (or retrieve existing)
        gym_obj = await cls.upsert_gym(gym_id, gym_name, latitude, longitude)
        gym_obj_id = gym_obj.id

        try:
            obj = await AggregatedRaids.get(
                gym=gym_obj_id,
                raid_pokemon=raid_pokemon,
                raid_level=raid_level,
                raid_form=raid_form,
                raid_team=raid_team,
                raid_costume=raid_costume,
                raid_is_exclusive=raid_is_exclusive,
                raid_ex_raid_eligible=raid_ex_raid_eligible,
                area_id=area_id,
                month_year=month_year
            )
            obj.total_count += increment
            await obj.save()
            logger.debug(f"⬆️ Updated AggregatedRaids: {obj}")
            return obj
        except DoesNotExist:
            obj = await AggregatedRaids.create(
                gym=gym_obj_id,
                raid_pokemon=raid_pokemon,
                raid_level=raid_level,
                raid_form=raid_form,
                raid_team=raid_team,
                raid_costume=raid_costume,
                raid_is_exclusive=raid_is_exclusive,
                raid_ex_raid_eligible=raid_ex_raid_eligible,
                area_id=area_id,
                month_year=month_year,
                total_count=increment
            )
            logger.debug(f"✅ Created new AggregatedRaids: {obj}")
            return obj
