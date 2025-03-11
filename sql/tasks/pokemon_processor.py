from sql.queries.pokemon_updates import PokemonUpdatesQueries
from utils.logger import logger

class PokemonSQLProcessor(PokemonUpdatesQueries):
    @classmethod
    async def upsert_aggregated_from_filtered(cls, filtered_data, increment: int = 1):
        """
        Extract values from filtered_data and call the upsert method.

        Expected keys in filtered_data:
          - 'spawnpoint'
          - 'latitude'
          - 'longitude'
          - 'pokemon_id'
          - 'form'
          - 'iv'
          - 'area_id'
          - 'first_seen'
        """
        try:
            spawnpoint_id = filtered_data.get('spawnpoint')
            latitude = filtered_data.get('latitude')
            longitude = filtered_data.get('longitude')
            pokemon_id = filtered_data.get('pokemon_id')
            form = filtered_data.get('form', 0)
            raw_iv = filtered_data.get('iv')
            area_id = filtered_data.get('area_id')
            first_seen_timestamp = filtered_data.get('first_seen')

            result = await cls.upsert_aggregated_pokemon_iv_monthly(
                spawnpoint_id=spawnpoint_id,
                latitude=latitude,
                longitude=longitude,
                pokemon_id=pokemon_id,
                form=form,
                raw_iv=raw_iv,
                area_id=area_id,
                first_seen_timestamp=first_seen_timestamp,
                increment=increment
            )

            logger.debug(f"✅ Upserted Pokémon {pokemon_id} in area {area_id} - Updates: {result}")
            return result

        except Exception as e:
            logger.error(f"Error in upsert_aggregated_from_filtered: {e}")
            raise
