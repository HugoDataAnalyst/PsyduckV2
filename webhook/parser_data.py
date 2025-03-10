import asyncio
import json
from my_redis.queries import (
    pokemon_timeseries,
    pokemon_counterseries,
    pokemon_tth_counterseries,
    pokemon_tth_timeseries,
)
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()

async def process_pokemon_data(filtered_data):
    """
    Process the filtered Pokémon event by updating both the time series and the counter series in a single Redis transaction.
    """
    if not filtered_data:
        logger.error("❌ No data provided to process_pokemon_data.")
        return None

    redis_status = await redis_manager.check_redis_connection()
    if not redis_status:
        logger.error("❌ Redis is not connected. Cannot process Pokémon data.")
        return None

    try:
        client = redis_manager.redis_client
        async with client.pipeline() as pipe:
            # Add all Redis operations to the pipeline
            await pokemon_timeseries.add_timeseries_total_pokemon_event(filtered_data, pipe)
            await pokemon_counterseries.update_total_pokemon_counter(filtered_data, pipe)
            await pokemon_tth_timeseries.add_tth_timeseries_pokemon_event(filtered_data, pipe)
            await pokemon_tth_counterseries.update_tth_pokemon_counter(filtered_data, pipe)

            # Execute all Redis commands in a single batch
            results = await pipe.execute()

        combined_result = json.dumps(results, indent=2)
        logger.info("✅ Successfully processed Pokémon event data in a single Redis transaction.")

        return combined_result

    except Exception as e:
        logger.error(f"❌ Error processing Pokémon event data in parser_data: {e}")
        return None
