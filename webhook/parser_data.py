import asyncio
import json
from textwrap import indent
import config as AppConfig
from my_redis.queries.pokemons import (
    pokemon_timeseries,
    pokemon_counterseries,
    pokemon_hourly_counterseries,
    pokemon_tth_counterseries,
    pokemon_tth_timeseries,
    pokemon_weather_iv_counterseries
)
from my_redis.queries.raids import (
    raids_timeseries,
    raids_counterseries,
    raids_hourly_counterseries
)
from sql.tasks.pokemon_processor import PokemonSQLProcessor
from sql.tasks.raids_processor import RaidSQLProcessor
from my_redis.connect_redis import RedisManager
from utils.logger import logger

redis_manager = RedisManager()
pokemon_sql = PokemonSQLProcessor()
raid_sql = RaidSQLProcessor()

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
            pokemon_timeseries_update = await pokemon_timeseries.add_timeseries_total_pokemon_event(filtered_data, pipe)
            pokemon_counterseries_update = await pokemon_counterseries.update_total_pokemon_counter(filtered_data, pipe)
            pokemon_hourly_counterseries_update = await pokemon_hourly_counterseries.update_pokemon_hourly_counter(filtered_data, pipe)
            pokemon_tth_timeseries_update = await pokemon_tth_timeseries.add_tth_timeseries_pokemon_event(filtered_data, pipe)
            pokemon_tth_counterseries_update = await pokemon_tth_counterseries.update_tth_pokemon_counter(filtered_data, pipe)
            pokemon_weather_counterseries_update = await pokemon_weather_iv_counterseries.update_pokemon_weather_iv(filtered_data, pipe)

            # Execute all Redis commands in a single batch
            await pipe.execute()

        # Execute SQL commands if Enabled
        if AppConfig.store_sql_pokemon_aggregation:
            logger.info("🔃 Processing Pokémon Aggregation...")
            await pokemon_sql.upsert_aggregated_from_filtered(filtered_data)
        else:
            logger.info("⚠️ SQL Pokémon Aggregation is disabled.")

        if AppConfig.store_sql_pokemon_shiny:
            logger.info("🔃 Processing Pokémon Shiny Rates...")
            await pokemon_sql.upsert_shiny_rate_from_filtered(filtered_data)
        else:
            logger.info("⚠️ SQL Pokémon Shiny Rates is disabled.")

        # Map results to Meaningful Information.
        structured_result = (
            f"Pokémon ID: {filtered_data['pokemon_id']}\n"
            f"Form: {filtered_data['form']}\n"
            f"Area: {filtered_data['area_name']}\n"
            "Updates:\n"
            f"  - Timeseries Total: {json.dumps(pokemon_timeseries_update, indent=2)}\n"
            f"  - Counter Total: {json.dumps(pokemon_counterseries_update, indent=2)}\n"
            f"  - Counter Hourly Total: {json.dumps(pokemon_hourly_counterseries_update, indent=2)}\n"
            f"  - TTH Timeseries: {json.dumps(pokemon_tth_timeseries_update, indent=2)}\n"
            f"  - TTH Counter: {json.dumps(pokemon_tth_counterseries_update, indent=2)}\n"
            f"  - Counter Weather: {json.dumps(pokemon_weather_counterseries_update, indent=2)}\n"
        )

        logger.debug(f"✅ Processed Pokémon {filtered_data['pokemon_id']} in area {filtered_data['area_name']} - Updates: {structured_result}")
        return structured_result

    except Exception as e:
        logger.error(f"❌ Error processing Pokémon event data in parser_data: {e}")
        return None

async def process_raid_data(filtered_data):
    """
    Process the filtered Raid event by updating both the time series and the counter series in a single Redis transaction.
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
            raid_timeseries_update = await raids_timeseries.add_timeseries_raid_event(filtered_data, pipe)
            raid_counterseries_update = await raids_counterseries.update_raid_counter(filtered_data, pipe)
            raid_hourly_counterseries_update = await raids_hourly_counterseries.update_raid_hourly_counter(filtered_data, pipe)

            # Execute all Redis commands in a single batch
            await pipe.execute()

        # Execute SQl commands if Enabled
        if AppConfig.store_sql_raid_aggregation:
            logger.info("🔃 Processing Raid Aggregation...")
            await raid_sql.upsert_aggregated_raid_from_filtered(filtered_data)
        else:
            logger.info("⚠️ SQL Raid Aggregation is disabled.")

                # Map results to Meaningful Information.
        structured_result = (
            f"Raid Pokémon ID: {filtered_data['raid_pokemon']}\n"
            f"Raid Level: {filtered_data['raid_level']}\n"
            f"Raid Form: {filtered_data['raid_form']}\n"
            f"Area: {filtered_data['area_name']}\n"
            "Updates:\n"
            f"  - Raid Timeseries Total: {json.dumps(raid_timeseries_update, indent=2)}\n"
            f"  - Raid Counter Total: {json.dumps(raid_counterseries_update, indent=2)}\n"
            f"  - Raid Counter Hourly Total: {json.dumps(raid_hourly_counterseries_update, indent=2)}\n"
        )

        logger.debug(f"✅ Processed Raid {filtered_data['raid_pokemon']} in area {filtered_data['area_name']} - Updates: {structured_result}")
        return structured_result

    except Exception as e:
        logger.error(f"❌ Error processing Pokémon event data in parser_data: {e}")
        return None
