from sql.queries.invasions_updates import InvasionsSQLProcessor
from utils.logger import logger

class InvasionSQLProcessor(InvasionsSQLProcessor):
    @classmethod
    async def upsert_aggregated_invasion_from_filtered(cls, filtered_data, increment: int = 1):
        try:
            pokestop = filtered_data.get('invasion_pokestop_id')
            pokestop_name = filtered_data.get('invasion_pokestop_name')
            latitude = filtered_data.get('invasion_latitude')
            longitude = filtered_data.get('invasion_longitude')
            display_type = filtered_data.get('invasion_type')
            character = filtered_data.get('invasion_character')
            grunt = filtered_data.get('invasion_grunt_type')
            confirmed = filtered_data.get('invasion_confirmed')
            area_id = filtered_data.get('area_id')
            first_seen_timestamp = filtered_data.get('invasion_first_seen')

            result = await cls.upsert_aggregated_invasion(
                pokestop_id=pokestop,
                pokestop_name=pokestop_name,
                latitude=latitude,
                longitude=longitude,
                display_type=display_type,
                character=character,
                grunt=grunt,
                confirmed=confirmed,
                area_id=area_id,
                first_seen_timestamp=first_seen_timestamp,
                increment=increment
            )
            logger.debug(f"✅ Upserted Aggregated Invasion for pokestop {pokestop} in area {area_id} - Updates: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ Error in upsert_aggregated_invasion_from_filtered: {e}")
            raise
