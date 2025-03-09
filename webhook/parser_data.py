import asyncio
from my_redis.queries import pokemon_timeseries, pokemon_counterseries
from utils.logger import logger

async def process_pokemon_data(filtered_data):
    """
    Process the filtered Pokémon event by updating both the time series and the counter series concurrently.
    """
    if not filtered_data:
        logger.error("❌ No data provided to process_pokemon_data.")
        return None

    try:
        ts_task = pokemon_timeseries.add_timeseries_total_pokemon_event(filtered_data)
        counter_task = pokemon_counterseries.update_total_pokemon_counter(filtered_data)

        # Run both concurrently
        ts_result, counter_result = await asyncio.gather(ts_task, counter_task)

        combined_result = {
            "timeseries": ts_result,
            "counter": counter_result
        }
        logger.info("✅ Successfully processed Pokémon event data in parser_data.")
        return combined_result

    except Exception as e:
        logger.error(f"❌ Error processing Pokémon event data in parser_data: {e}")
        return None
