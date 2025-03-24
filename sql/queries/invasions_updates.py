from datetime import datetime
from utils.logger import logger
from sql.models import AggreagatedInvasions, Pokestops

class InvasionsSQLProcessor:
    @staticmethod
    async def upsert_pokestop(pokestop_id: str, pokestop_name: str, latitude: float, longitude: float):
        """
        Insert the pokestop into the Pokestops table if it doesn't exist.
        Returns the Pokestops object.
        """
        obj, created = await Pokestops.get_or_create(
            pokestop=pokestop_id,
            defaults={"pokestop_name": pokestop_name, "latitude": latitude, "longitude": longitude}
        )
        if not created:
            # Optionally update details if they've changed.
            updated = False
            if obj.pokestop_name != pokestop_name:
                obj.pokestop_name = pokestop_name
                updated = True
            if obj.latitude != latitude or obj.longitude != longitude:
                obj.latitude = latitude
                obj.longitude = longitude
                updated = True
            if updated:
                await obj.save()
            logger.debug(f"⏭️ Pokestop exists: id={obj.id}, pokestop={obj.pokestop}, name={obj.pokestop_name}, lat={obj.latitude}, lon={obj.longitude}")
        else:
            logger.debug(f"✅ Created new Pokestop: id={obj.id}, pokestop={obj.pokestop}, name={obj.pokestop_name}, lat={obj.latitude}, lon={obj.longitude}")
        return obj

    @classmethod
    async def upsert_aggregated_invasion(
        cls,
        pokestop_id: str,
        pokestop_name: str,
        latitude: float,
        longitude: float,
        display_type: int,
        character: int,
        grunt: int,
        confirmed: int,
        area_id: int,
        first_seen_timestamp: int,
        increment: int = 1
    ):
        """
        Insert or update the AggreagatedInvasions record.
        The record is uniquely identified by:
        pokestop (FK), display_type, character, grunt, confirmed, area, and month_year.
        The month_year is derived from first_seen (in YYMM format).
        """
        dt = datetime.fromtimestamp(first_seen_timestamp)
        month_year = int(dt.strftime("%y%m"))

        # Upsert the pokestop first.
        pokestop_obj = await cls.upsert_pokestop(pokestop_id, pokestop_name, latitude, longitude)
        ps_obj_id = pokestop_obj.id

        obj, created = await AggreagatedInvasions.get_or_create(
            pokestop_id=ps_obj_id,
            display_type=display_type,
            character=character,
            grunt=grunt,
            confirmed=confirmed,
            area_id=area_id,
            month_year=month_year,
            defaults={"total_count": increment}
        )

        if not created:
            obj.total_count += increment
            await obj.save()
            logger.debug(f"⬆️ Updated AggreagatedInvasions: Pokestop={obj.pokestop}, Display Type={obj.display_type}, Character={obj.character}, Grunt={obj.grunt}, Confirmed={obj.confirmed}, Area={obj.area}, Month Year={obj.month_year}")
        else:
            logger.debug(f"✅ Created new AggreagatedInvasions: Pokestop={obj.pokestop}, Display Type={obj.display_type}, Character={obj.character}, Grunt={obj.grunt}, Confirmed={obj.confirmed}, Area={obj.area}, Month Year={obj.month_year}")
        return obj
