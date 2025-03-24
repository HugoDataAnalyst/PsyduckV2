from sql.queries.raids_updates import RaidsSQLProcessor
from utils.logger import logger

class RaidSQLProcessor(RaidsSQLProcessor):
    @classmethod
    async def upsert_aggregated_raid_from_filtered(cls, filtered_data, increment: int = 1):
        """
        Extract values from filtered_data and call the upsert method for AggregatedRaids.

        Expected keys in filtered_data:
          - 'gym': the gym ID as a string
          - 'gym_name': the gym name
          - 'latitude': gym latitude
          - 'longitude': gym longitude
          - 'raid_pokemon': the raid boss Pokémon ID
          - 'raid_level': the raid level
          - 'raid_form': the raid boss form (default 0)
          - 'raid_team': the team controlling the gym (default 0)
          - 'raid_costume': the raid boss costume (default 0)
          - 'raid_is_exclusive': whether the raid is exclusive (boolean/int)
          - 'raid_ex_raid_eligible': whether the raid is EX eligible (boolean/int)
          - 'area_id': the area ID
          - 'first_seen': the UTC timestamp of the raid spawn
        """
        try:
            gym = filtered_data.get('raid_gym_id')
            # If gym is provided as a hex string, convert it:
            gym_name = filtered_data.get('raid_gym_name')
            latitude = filtered_data.get('raid_latitude')
            longitude = filtered_data.get('raid_longitude')
            raid_pokemon = filtered_data.get('raid_pokemon')
            raid_level = filtered_data.get('raid_level')
            raid_form = filtered_data.get('raid_form', 0)
            raid_team = filtered_data.get('raid_team_id', 0)
            raid_costume = filtered_data.get('raid_costume', 0)
            raid_is_exclusive = int(filtered_data.get('raid_is_exclusive', 0))
            raid_ex_raid_eligible = int(filtered_data.get('raid_ex_raid_eligible', 0))
            area_id = filtered_data.get('area_id')
            first_seen_timestamp = filtered_data.get('raid_first_seen')

            result = await cls.upsert_aggregated_raid(
                gym_id=gym,
                gym_name=gym_name,
                latitude=latitude,
                longitude=longitude,
                raid_pokemon=raid_pokemon,
                raid_level=raid_level,
                raid_form=raid_form,
                raid_team=raid_team,
                raid_costume=raid_costume,
                raid_is_exclusive=raid_is_exclusive,
                raid_ex_raid_eligible=raid_ex_raid_eligible,
                area_id=area_id,
                first_seen_timestamp=first_seen_timestamp,
                increment=increment
            )
            logger.debug(f"✅ Upserted Aggregated Raid for gym {gym} in area {area_id} - Updates: {result}")
            return result

        except Exception as e:
            logger.error(f"❌ Error in upsert_aggregated_raid_from_filtered: {e}")
            raise
