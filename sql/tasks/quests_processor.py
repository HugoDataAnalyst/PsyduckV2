from sql.queries.quests_updates import QuestsSQLProcessor
from utils.logger import logger

class QuestSQLProcessor(QuestsSQLProcessor):
    @classmethod
    async def upsert_aggregated_quest_from_filtered(cls, filtered_data, increment: int = 1):
        try:
            pokestop = filtered_data.get('pokestop_id')
            pokestop_name = filtered_data.get('pokestop_name')
            latitude = filtered_data.get('latitude')
            longitude = filtered_data.get('longitude')
            # Extract quest-specific fields:
            ar_type = filtered_data.get('ar_type')
            normal_type = filtered_data.get('normal_type')
            reward_ar_type = filtered_data.get('reward_ar_type')
            reward_normal_type = filtered_data.get('reward_normal_type')
            reward_ar_item_id = filtered_data.get('reward_ar_item_id')
            reward_ar_item_amount = filtered_data.get('reward_ar_item_amount')
            reward_normal_item_id = filtered_data.get('reward_normal_item_id')
            reward_normal_item_amount = filtered_data.get('reward_normal_item_amount')
            reward_ar_poke_id = filtered_data.get('reward_ar_poke_id')
            reward_ar_poke_form = filtered_data.get('reward_ar_poke_form')
            reward_normal_poke_id = filtered_data.get('reward_normal_poke_id')
            reward_normal_poke_form = filtered_data.get('reward_normal_poke_form')
            area_id = filtered_data.get('area_id')
            first_seen_timestamp = filtered_data.get('first_seen')

            result = await cls.upsert_aggregated_quest(
                pokestop_id=pokestop,
                pokestop_name=pokestop_name,
                latitude=latitude,
                longitude=longitude,
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
                first_seen_timestamp=first_seen_timestamp,
                increment=increment
            )
            logger.info(f"✅ Upserted Aggregated Quest for pokestop {pokestop} in area {area_id} - Updates: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ Error in upsert_aggregated_quest_from_filtered: {e}")
            raise
