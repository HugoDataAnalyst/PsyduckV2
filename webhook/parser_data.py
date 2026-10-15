import json
import config as AppConfig
from my_redis.queries.buffer import raids_bulk_buffer
from my_redis.queries.updates.pokemons import (
    pokemon_timeseries,
    pokemon_counterseries,
    pokemon_hourly_counterseries,
    pokemon_daily_counterseries,
    pokemon_tth_counterseries,
    pokemon_tth_timeseries,
    pokemon_tth_hourly_counterseries,
    pokemon_tth_daily_counterseries,
    pokemon_weather_iv_counterseries,
    pokemon_active
)
from my_redis.queries.updates.raids import (
    raids_timeseries,
    raids_counterseries,
    raids_hourly_counterseries,
    raids_daily_counterseries
)
from my_redis.queries.updates.invasions import (
    invasions_timeseries,
    invasions_counterseries,
    invasions_hourly_counterseries,
    invasions_daily_counterseries
)
from my_redis.queries.updates.quests import (
    quests_timeseries,
    quests_counterseries,
    quests_hourly_counterseries,
    quests_daily_counterseries
)
from my_redis.connect_redis import RedisManager
from my_redis.queries.buffer.pokemon_bulk_buffer import PokemonIVRedisBuffer, ShinyRateRedisBuffer
from my_redis.queries.buffer.quests_bulk_buffer import QuestsRedisBuffer
from my_redis.queries.buffer.raids_bulk_buffer import RaidsRedisBuffer
from my_redis.queries.buffer.invasions_bulk_buffer import InvasionsRedisBuffer
from utils.logger import logger
from utils.retry_functions import retry

redis_manager = RedisManager()
pokemon_buffer = PokemonIVRedisBuffer()
shiny_buffer = ShinyRateRedisBuffer()
quests_buffer = QuestsRedisBuffer()
raids_buffer = RaidsRedisBuffer()
invasions_buffer = InvasionsRedisBuffer()

# The per-event summary (ten json.dumps per Pokémon) is only ever logged at
# DEBUG, so it is only built at DEBUG.
_DEBUG = str(AppConfig.log_level).upper() == "DEBUG"
_DISABLED = {"status": "DISABLED"}


def _summary(header: str, updates: dict) -> str:
    lines = [header, "Updates:"]
    lines += [f"  - {name}: {json.dumps(value, indent=2)}" for name, value in updates.items()]
    return "\n".join(lines) + "\n"


async def _execute(pipe):
    results = await retry(pipe.execute, max_attempts=5, delay=2)
    for res in results or ():
        if isinstance(res, Exception):
            raise res


async def process_pokemon_data(filtered_data):
    """
    Process one filtered Pokémon event: timeseries, counters, live set and the
    SQL buffers, all queued on one pipeline - a single Redis round trip.
    """
    if not filtered_data:
        logger.error("❌ No data provided to process_pokemon_data.")
        return None

    # Use fast retry for webhook processing to avoid data loss
    client = await redis_manager.get_connection_with_retry(max_attempts=3, delay=0.3)
    if not client:
        logger.error("❌ Redis is not connected after retries. Cannot process Pokémon data.")
        return None

    updates = {}
    async with client.pipeline(transaction=False) as pipe:
        updates["Timeseries Total"] = (await pokemon_timeseries.add_pokemon_timeseries_event(filtered_data, pipe)
                                       if AppConfig.store_pokemon_timeseries else _DISABLED)
        updates["Counter Weekly Total"] = await pokemon_counterseries.update_total_pokemon_counter(filtered_data, pipe)
        updates["Counter Hourly Total"] = await pokemon_hourly_counterseries.update_pokemon_hourly_counter(filtered_data, pipe)
        updates["Counter Daily Total"] = await pokemon_daily_counterseries.update_daily_pokemon_counter(filtered_data, pipe)
        updates["TTH Timeseries"] = (await pokemon_tth_timeseries.add_tth_timeseries_pokemon_event(filtered_data, pipe)
                                     if AppConfig.store_pokemon_tth_timeseries else _DISABLED)
        updates["TTH Weekly Counter"] = await pokemon_tth_counterseries.update_tth_pokemon_counter(filtered_data, pipe)
        updates["TTH Hourly Counter"] = await pokemon_tth_hourly_counterseries.update_tth_pokemon_hourly_counter(filtered_data, pipe)
        updates["TTH Daily Counter"] = await pokemon_tth_daily_counterseries.update_tth_pokemon_daily_counter(filtered_data, pipe)
        updates["Counter Weather"] = await pokemon_weather_iv_counterseries.update_pokemon_weather_iv(filtered_data, pipe)
        # Live set: sorted set scored by disappear_time
        updates["Active Live Set"] = (await pokemon_active.add_pokemon_active_event(filtered_data, pipe)
                                      if AppConfig.store_active_pokemon else _DISABLED)
        # SQL buffers ride on the same pipeline (no round trip of their own)
        if AppConfig.store_sql_pokemon_aggregation:
            updates["IV Buffer"] = pokemon_buffer.stage(pipe, filtered_data)
        if AppConfig.store_sql_pokemon_shiny:
            updates["Shiny Buffer"] = shiny_buffer.stage(pipe, filtered_data)

        await _execute(pipe)

    if not _DEBUG:
        return "OK"
    structured_result = _summary(
        f"Pokémon ID: {filtered_data['pokemon_id']}\nForm: {filtered_data['form']}\nArea: {filtered_data['area_name']}",
        updates,
    )
    logger.debug(f"✅ Processed Pokémon {filtered_data['pokemon_id']} in area {filtered_data['area_name']} - Updates: {structured_result}")
    return structured_result


async def process_raid_data(filtered_data):
    """
    Process one filtered Raid event: timeseries, counters and the SQL buffer on
    one pipeline - a single Redis round trip.
    """
    if not filtered_data:
        logger.error("❌ No data provided to process_raid_data.")
        return None

    # Use fast retry for webhook processing to avoid data loss
    client = await redis_manager.get_connection_with_retry(max_attempts=3, delay=0.3)
    if not client:
        logger.error("❌ Redis is not connected after retries. Cannot process Raid data.")
        return None

    try:
        updates = {}
        async with client.pipeline(transaction=False) as pipe:
            updates["Raid Timeseries Total"] = (await raids_timeseries.add_raid_timeseries_event(filtered_data, pipe)
                                                if AppConfig.store_raids_timeseries else _DISABLED)
            updates["Raid Weekly Counter Total"] = await raids_counterseries.update_raid_counter(filtered_data, pipe)
            updates["Raid Hourly Counter Total"] = await raids_hourly_counterseries.update_raid_hourly_counter(filtered_data, pipe)
            updates["Raid Daily Counter Total"] = await raids_daily_counterseries.update_raid_daily_counter(filtered_data, pipe)
            if AppConfig.store_sql_raid_aggregation:
                updates["Raid Buffer"] = raids_buffer.stage(pipe, filtered_data)
            await _execute(pipe)

        if not _DEBUG:
            return "OK"
        structured_result = _summary(
            f"Raid Pokémon ID: {filtered_data['raid_pokemon']}\nRaid Level: {filtered_data['raid_level']}\n"
            f"Raid Form: {filtered_data['raid_form']}\nArea: {filtered_data['area_name']}",
            updates,
        )
        logger.debug(f"✅ Processed Raid {filtered_data['raid_pokemon']} in area {filtered_data['area_name']} - Updates: {structured_result}")
        return structured_result

    except Exception as e:
        logger.error(f"❌ Error processing Raid event data in parser_data: {e}")
        return None


async def process_quest_data(filtered_data):
    """
    Process one filtered Quest event: timeseries, counters and the SQL buffer on
    one pipeline - a single Redis round trip.
    """
    if not filtered_data:
        logger.error("❌ No data provided to process_quest_data.")
        return None

    # Use fast retry for webhook processing to avoid data loss
    client = await redis_manager.get_connection_with_retry(max_attempts=3, delay=0.3)
    if not client:
        logger.error("❌ Redis is not connected after retries. Cannot process Quest data.")
        return None

    try:
        updates = {}
        async with client.pipeline(transaction=False) as pipe:
            updates["Quest Timeseries Total"] = (await quests_timeseries.add_timeseries_quest_event(filtered_data, pipe)
                                                 if AppConfig.store_quests_timeseries else _DISABLED)
            updates["Quest Weekly Counter Total"] = await quests_counterseries.update_quest_counter(filtered_data, pipe)
            updates["Quest Hourly Counter Total"] = await quests_hourly_counterseries.update_quest_hourly_counter(filtered_data, pipe)
            updates["Quest Daily Counter Total"] = await quests_daily_counterseries.update_quest_daily_counter(filtered_data, pipe)
            if AppConfig.store_sql_quest_aggregation:
                updates["Quest Buffer"] = quests_buffer.stage(pipe, filtered_data)
            await _execute(pipe)

        if not _DEBUG:
            return "OK"
        with_ar = filtered_data.get("ar_type") is not None
        mode = "AR" if with_ar else "Normal"
        reward_type = filtered_data.get("reward_ar_type" if with_ar else "reward_normal_type")
        structured_result = _summary(
            f"Quest Type: {mode}\nQuest Reward Type: {reward_type}\nArea: {filtered_data['area_name']}",
            updates,
        )
        logger.debug(f"✅ Processed Quest {mode} in area {filtered_data['area_name']} - Updates: {structured_result}")
        return structured_result

    except Exception as e:
        logger.error(f"❌ Error processing Quest event data in parser_data: {e}")
        return None


async def process_invasion_data(filtered_data):
    """
    Process one filtered Invasion event: timeseries, counters and the SQL
    buffer on one pipeline - a single Redis round trip.
    """
    if not filtered_data:
        logger.error("❌ No data provided to process_invasion_data.")
        return None

    # Use fast retry for webhook processing to avoid data loss
    client = await redis_manager.get_connection_with_retry(max_attempts=3, delay=0.3)
    if not client:
        logger.error("❌ Redis is not connected after retries. Cannot process Invasion data.")
        return None

    try:
        updates = {}
        async with client.pipeline(transaction=False) as pipe:
            updates["Invasion Timeseries Total"] = (await invasions_timeseries.add_timeseries_invasion_event(filtered_data, pipe)
                                                    if AppConfig.store_invasions_timeseries else _DISABLED)
            updates["Invasion Weekly Counter Total"] = await invasions_counterseries.update_invasion_counter(filtered_data, pipe)
            updates["Invasion Hourly Counter Total"] = await invasions_hourly_counterseries.update_invasion_hourly_counter(filtered_data, pipe)
            updates["Invasion Daily Counter Total"] = await invasions_daily_counterseries.update_invasion_daily_counter(filtered_data, pipe)
            if AppConfig.store_sql_invasion_aggregation:
                updates["Invasion Buffer"] = invasions_buffer.stage(pipe, filtered_data)
            await _execute(pipe)

        if not _DEBUG:
            return "OK"
        structured_result = _summary(
            f"Invasion Type ID: {filtered_data['invasion_type']}\nInvasion Grunt Type: {filtered_data['invasion_grunt_type']}\n"
            f"Invasion Confirmed: {filtered_data['invasion_confirmed']}\nArea: {filtered_data['area_name']}",
            updates,
        )
        logger.debug(f"✅ Processed Invasion {filtered_data['invasion_type']} in area {filtered_data['area_name']} - Updates: {structured_result}")
        return structured_result

    except Exception as e:
        logger.error(f"❌ Error processing Invasion event data in parser_data: {e}")
        return None
