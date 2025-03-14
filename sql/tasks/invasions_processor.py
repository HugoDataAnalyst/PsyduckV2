from sql.queries.invasions_updates import InvasionsSQLProcessor
from utils.logger import logger

class InvasionSQLProcessor(InvasionsSQLProcessor):
    @classmethod
    async def upsert_aggregated_invasion_from_filtered(cls, filtered_data, increment: int = 1):
        try:
            pokestop = filtered_data.get('pokestop')
            pokestop_name = filtered_data.get('pokestop_name')
            latitude = filtered_data.get('latitude')
            longitude = filtered_data.get('longitude')
            display_type = filtered_data.get('display_type')
            character = filtered_data.get('character')
            grunt = filtered_data.get('grunt')
            confirmed = filtered_data.get('confirmed')
            area_id = filtered_data.get('area_id')
            first_seen_timestamp = filtered_data.get('first_seen')

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
            logger.info(f"✅ Upserted Aggregated Invasion for pokestop {pokestop} in area {area_id} - Updates: {result}")
            return result
        except Exception as e:
            logger.error(f"❌ Error in upsert_aggregated_invasion_from_filtered: {e}")
            raise
