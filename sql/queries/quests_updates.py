from datetime import datetime
from utils.logger import logger
from tortoise.exceptions import DoesNotExist
from sql.models import AggregatedQuests, Pokestops

class QuestsSQLProcessor:
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
            logger.info(f"⏭️ Pokestop exists: id={obj.id}, pokestop={obj.pokestop}, name={obj.pokestop_name}, lat={obj.latitude}, lon={obj.longitude}")
        else:
            logger.info(f"✅ Created new Pokestop: id={obj.id}, pokestop={obj.pokestop}, name={obj.pokestop_name}, lat={obj.latitude}, lon={obj.longitude}")
        return obj

    @classmethod
    async def upsert_aggregated_quest(
        cls,
        pokestop_id: str,
        pokestop_name: str,
        latitude: float,
        longitude: float,
        ar_type: int,
        normal_type: int,
        reward_ar_type: int,
        reward_normal_type: int,
        reward_ar_item_id: int,
        reward_ar_item_amount: int,
        reward_normal_item_id: int,
        reward_normal_item_amount: int,
        reward_ar_poke_id: int,
        reward_ar_poke_form: str,
        reward_normal_poke_id: int,
        reward_normal_poke_form: str,
        area_id: int,
        first_seen_timestamp: int,
        increment: int = 1
    ):
        """
        Insert or update the AggregatedQuests record.
        The record is uniquely identified by the combination of:
        pokestop (FK), ar_type, normal_type, reward_ar_type, reward_normal_type,
        reward_ar_item_id, reward_ar_item_amount, reward_normal_item_id, reward_normal_item_amount,
        reward_ar_poke_id, reward_ar_poke_form, reward_normal_poke_id, reward_normal_poke_form,
        area, and month_year.
        The month_year is derived from first_seen (in YYMM format).
        """
        dt = datetime.fromtimestamp(first_seen_timestamp)
        month_year = int(dt.strftime("%y%m"))

        # Upsert the pokestop first.
        pokestop_obj = await cls.upsert_pokestop(pokestop_id, pokestop_name, latitude, longitude)
        ps_obj_id = pokestop_obj.id

        obj, created = await AggregatedQuests.get_or_create(
            pokestop_id=ps_obj_id,
            ar_type=ar_type,
            normal_type=normal_type,
            reward_ar_type=reward_ar_type,
            reward_normal_type=reward_normal_type,
            reward_ar_item_id=reward_ar_item_id,
            reward_ar_item_amount=reward_ar_item_amount,
            reward_normal_item_id=reward_normal_item_id,
            reward_normal_item_amount=reward_normal_item_amount,
            reward_ar_poke_id=reward_ar_poke_id,
            reward_ar_poke_form=reward_ar_poke_form,
            reward_normal_poke_id=reward_normal_poke_id,
            reward_normal_poke_form=reward_normal_poke_form,
            area_id=area_id,
            month_year=month_year,
            defaults={"total_count": increment}
        )

        if not created:
            obj.total_count += increment
            await obj.save()
            logger.debug(f"⬆️ Updated AggregatedQuests: Pokestop={obj.pokestop}, AR Type={obj.ar_type}, Normal Type={obj.normal_type}, Reward AR Type={obj.reward_ar_type}, Reward Normal Type={obj.reward_normal_type}, Reward AR Item ID={obj.reward_ar_item_id}, Reward AR Item Amount={obj.reward_ar_item_amount}, Reward Normal Item ID={obj.reward_normal_item_id}, Reward Normal Item Amount={obj.reward_normal_item_amount}, Reward AR Poke ID={obj.reward_ar_poke_id}, Reward AR Poke Form={obj.reward_ar_poke_form}, Reward Normal Poke ID={obj.reward_normal_poke_id}, Reward Normal Poke Form={obj.reward_normal_poke_form}, Area={obj.area}, Month Year={obj.month_year}")
        else:
            logger.debug(f"✅ Created new AggregatedQuests: Pokestop={obj.pokestop}, AR Type={obj.ar_type}, Normal Type={obj.normal_type}, Reward AR Type={obj.reward_ar_type}, Reward Normal Type={obj.reward_normal_type}, Reward AR Item ID={obj.reward_ar_item_id}, Reward AR Item Amount={obj.reward_ar_item_amount}, Reward Normal Item ID={obj.reward_normal_item_id}, Reward Normal Item Amount={obj.reward_normal_item_amount}, Reward AR Poke ID={obj.reward_ar_poke_id}, Reward AR Poke Form={obj.reward_ar_poke_form}, Reward Normal Poke ID={obj.reward_normal_poke_id}, Reward Normal Poke Form={obj.reward_normal_poke_form}, Area={obj.area}, Month Year={obj.month_year}")

        return obj
