import asyncio
import json
from my_redis.queries import pokemon_timeseries, pokemon_counterseries, pokemon_tth_counterseries, pokemon_tth_timeseries
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
        ts_tth_task = pokemon_tth_timeseries.add_tth_timeseries_pokemon_event(filtered_data)
        counter_tth_task = pokemon_tth_counterseries.update_tth_pokemon_counter(filtered_data)

        # Run both concurrently
        ts_result, counter_result, ts_tth__result, counter_tth_result = await asyncio.gather(ts_task, counter_task, ts_tth_task, counter_tth_task)

        combined_result = (
            "Total Timeseries: " + json.dumps(ts_result, indent=2)
            + "\nTotal Counter: " + json.dumps(counter_result, indent=2)
            + "\nTTH Timeseries: " + json.dumps(ts_tth__result, indent=2)
            + "\nTTH Counter: " + json.dumps(counter_tth_result, indent=2)
        )
        logger.info("✅ Successfully processed Pokémon event data in parser_data.")
        return combined_result

    except Exception as e:
        logger.error(f"❌ Error processing Pokémon event data in parser_data: {e}")
        return None
