import config as AppConfig
from datetime import datetime
from server_fastapi.utils import secure_api
from sql.utils.time_parser import parse_time_input
from server_fastapi import global_state
from fastapi import APIRouter, Depends, Header, HTTPException, Query, dependencies
from typing import Optional
from my_redis.utils import filtering_keys
from my_redis.queries.gets.pokemons.pokemon_counter_retrieval import PokemonCounterRetrieval
from my_redis.queries.gets.raids.raid_counter_retrieval import RaidCounterRetrieval
from my_redis.queries.gets.invasions.invasion_counter_retrieval import InvasionCounterRetrieval
from my_redis.queries.gets.quests.quest_counter_retrieval import QuestCounterRetrieval
from my_redis.queries.gets.pokemons.pokemon_timeseries_retrieval import PokemonTimeSeries
from my_redis.queries.gets.pokemons.pokemon_tth_timeseries_retrieval import PokemonTTHTimeSeries
from my_redis.queries.gets.invasions.invasion_timeseries_retrieval import InvasionTimeSeries
from my_redis.queries.gets.raids.raid_timeseries_retrieval import RaidTimeSeries
from my_redis.queries.gets.quests.quest_timeseries_retrieval import QuestTimeSeries
from sql.queries.pokemon_gets import PokemonSQLQueries
from sql.queries.pokemon_shiny_gets import ShinySQLQueries
from sql.queries.raid_gets import RaidSQLQueries
from sql.queries.invasion_gets import InvasionSQLQueries
from sql.queries.quest_gets import QuestSQLQueries
from sql.tasks.golbat_pokestops import GolbatSQLPokestops

router = APIRouter()

dependencies_list = [
    Depends(secure_api.validate_path),
    Depends(secure_api.validate_ip)
]
if AppConfig.api_secret_key:
    dependencies_list.append(Depends(secure_api.verify_token))


@router.get(
    "/api/redis/get_cached_pokestops",
    tags=["Pokestops"],
    dependencies=dependencies_list
)
async def get_cached_pokestops(
    response_format: str = Query("json", description="Response format: json or text"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    """
    Retrieve the cached pokestops counts from Redis.
    """
    await secure_api.check_secret_header_value(api_secret_header)
    result = await GolbatSQLPokestops.get_cached_pokestops()
    if result is None:
        raise HTTPException(status_code=404, detail="Cached pokestops not found")

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/redis/get_cached_geofences",
    tags=["Koji Geofences"],
    dependencies=dependencies_list
)
async def get_cached_geofences(
    response_format: str = Query("json", description="Response format: json or text"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    """
    Retrieve the cached Koji geofences from the global state.
    """
    await secure_api.check_secret_header_value(api_secret_header)
    result = global_state.geofences
    if not result:
        raise HTTPException(status_code=404, detail="Cached geofences not found")
    if response_format.lower() == "json":
        return result
    else:
        if isinstance(result, dict):
            text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        elif isinstance(result, list):
            text_output = "\n".join(str(item) for item in result)
        else:
            text_output = str(result)
        return text_output


@router.get(
    "/api/redis/get_pokemon_counterseries",
    tags=["Pokémon Counter Series"],
    dependencies=dependencies_list
)
async def get_pokemon_counterseries(
    counter_type: str = Query(..., description="Type of counter series: totals, tth, or weather"),
    interval: str = Query(..., description="Interval: hourly or weekly for totals and tth; monthly for weather"),
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter counters"),
    metric: str = Query("all", description="Filter by metric. For totals: allowed values are total, iv100, iv0, pvp_little, pvp_great, pvp_ultra, shiny. For weather: allowed values are 0 to 9. For TTH: allowed values are e.g. 0_5, 5_10, etc."),
    pokemon_id: str = Query("all", description="ONLY IN TOTALS. Filter by Pokémon ID. Use 'all' to show all Pokémon."),
    form: str = Query("all", description="ONLY IN TOTALS. Filter by form. Use 'all' to show all forms."),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
    #api_secret_key: Optional[str] = secure_api.get_secret_key_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)
    #await secure_api.check_secret_key_value(api_secret_key)

    # Normalize and validate inputs
    counter_type = counter_type.lower()
    interval = interval.lower()
    mode = mode.lower()
    if counter_type not in ["totals", "tth", "weather"]:
        raise HTTPException(status_code=400, detail="❌ Invalid counter_type. Must be totals, tth, or weather.")
    if counter_type in ["totals", "tth"] and interval not in ["hourly", "weekly"]:
        raise HTTPException(status_code=400, detail="❌ For totals and tth, interval must be hourly or weekly.")
    if counter_type == "weather" and interval != "monthly":
        raise HTTPException(status_code=400, detail="❌ For weather, interval must be monthly.")
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if mode == "surged" and interval != "hourly":
        raise HTTPException(status_code=400, detail="❌ Surged mode is only supported for hourly intervals.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        # Get timezone offset (None means use machine timezone)
        area_offset = filtering_keys.get_area_offset(area, global_state.geofences)

        start_dt = filtering_keys.parse_time_input(start_time, area_offset)
        end_dt = filtering_keys.parse_time_input(end_time, area_offset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    pokemon_counter_retrieval = PokemonCounterRetrieval(area, start_dt, end_dt, mode, pokemon_id, form, metric)

    # Retrieve data dynamically based on counter type and interval
    retrieval_methods = {
        ("totals", "hourly"): pokemon_counter_retrieval.retrieve_totals_hourly,
        ("totals", "weekly"): pokemon_counter_retrieval.retrieve_totals_weekly,
        ("tth", "hourly"): pokemon_counter_retrieval.retrieve_tth_hourly,
        ("tth", "weekly"): pokemon_counter_retrieval.retrieve_tth_weekly,
        ("weather", "monthly"): pokemon_counter_retrieval.retrieve_weather_monthly,
    }

    retrieval_method = retrieval_methods.get((counter_type, interval))
    result = await retrieval_method() if retrieval_method else {}

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/redis/get_raids_counterseries",
    tags=["Raid Counter Series"],
    dependencies=dependencies_list
)
async def get_counter_raids(
    counter_type: str = Query("totals", description="Type of counter series: totals"),
    interval: str = Query(..., description="Interval: hourly or weekly."),
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter counters"),
    raid_pokemon: str = Query("all", description="Filter by raid_pokemon. Use 'all' to show all."),
    raid_form: str = Query("all", description="Filter by raid_form. Use 'all' to show all."),
    raid_level: str = Query("all", description="Filter by raid_level. Use 'all' to show all."),
    raid_costume: str = Query("all", description="Filter by raid_costume. Use 'all' to show all."),
    raid_is_exclusive: str = Query("all", description="Filter by raid_is_exclusive. Use 'all' to show all."),
    raid_ex_eligible: str = Query("all", description="Filter by raid_ex_eligible. Use 'all' to show all."),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    counter_type = counter_type.lower()
    interval = interval.lower()
    mode = mode.lower()
    if counter_type not in ["totals"]:
        raise HTTPException(status_code=400, detail="❌ Invalid counter_type. Must be totals.")
    if counter_type in ["totals", "tth"] and interval not in ["hourly", "weekly"]:
        raise HTTPException(status_code=400, detail="❌ Interval must be hourly or weekly.")
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if mode == "surged" and interval != "hourly":
        raise HTTPException(status_code=400, detail="❌ Surged mode is only supported for hourly intervals.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        # Get timezone offset (None means use machine timezone)
        area_offset = filtering_keys.get_area_offset(area, global_state.geofences)

        start_dt = filtering_keys.parse_time_input(start_time, area_offset)
        end_dt = filtering_keys.parse_time_input(end_time, area_offset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    raid_counter_retrieval = RaidCounterRetrieval(area, start_dt, end_dt, mode, raid_pokemon, raid_form, raid_level, raid_costume, raid_is_exclusive, raid_ex_eligible)

    # Retrieve data dynamically based on counter type and interval
    retrieval_methods = {
        ("totals", "hourly"): raid_counter_retrieval.raid_retrieve_totals_hourly,
        ("totals", "weekly"): raid_counter_retrieval.raid_retrieve_totals_weekly
    }

    retrieval_method = retrieval_methods.get((counter_type, interval))
    result = await retrieval_method() if retrieval_method else {}

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output

@router.get(
    "/api/redis/get_invasions_counterseries",
    tags=["Invasion Counter Series"],
    dependencies=dependencies_list
)
async def get_counter_invasions(
    counter_type: str = Query("totals", description="Type of counter series: totals"),
    interval: str = Query(..., description="Interval: hourly or weekly."),
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter counters"),
    display_type: str = Query("all", description="all or invasion display type"),
    character: str = Query("all", description="all or invasion character"),
    grunt: str = Query("all", description="all or grunt type"),
    confirmed: str = Query("all", description="all or confirmed status (0 or 1)"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    counter_type = counter_type.lower()
    interval = interval.lower()
    mode = mode.lower()
    if counter_type not in ["totals"]:
        raise HTTPException(status_code=400, detail="❌ Invalid counter_type. Must be totals.")
    if counter_type in ["totals", "tth"] and interval not in ["hourly", "weekly"]:
        raise HTTPException(status_code=400, detail="❌ Interval must be hourly or weekly.")
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if mode == "surged" and interval != "hourly":
        raise HTTPException(status_code=400, detail="❌ Surged mode is only supported for hourly intervals.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        # Get timezone offset (None means use machine timezone)
        area_offset = filtering_keys.get_area_offset(area, global_state.geofences)

        start_dt = filtering_keys.parse_time_input(start_time, area_offset)
        end_dt = filtering_keys.parse_time_input(end_time, area_offset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    invasion_counter_retrieval = InvasionCounterRetrieval(area, start_dt, end_dt, mode, display_type, character, grunt, confirmed)

    # Retrieve data dynamically based on counter type and interval
    retrieval_methods = {
        ("totals", "hourly"): invasion_counter_retrieval.invasion_retrieve_totals_hourly,
        ("totals", "weekly"): invasion_counter_retrieval.invasion_retrieve_totals_weekly
    }

    retrieval_method = retrieval_methods.get((counter_type, interval))
    result = await retrieval_method() if retrieval_method else {}

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/redis/get_quest_counterseries",
    tags=["Quest Counter Series"],
    dependencies=dependencies_list
)
async def get_counter_quests(
    counter_type: str = Query("totals", description="Type of counter series: totals"),
    interval: str = Query(..., description="Interval: hourly or weekly."),
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter counters"),
    with_ar: str = Query("false", description="Filter by AR quests: true, false, or all"),
    ar_type: str = Query("all", description="Filter by AR quest type"),
    reward_ar_type: str = Query("all", description="Filter by AR reward type"),
    reward_ar_item_id: str = Query("all", description="Filter by AR reward item ID"),
    reward_ar_item_amount: str = Query("all", description="Filter by AR reward item amount"),
    reward_ar_poke_id: str = Query("all", description="Filter by AR reward Pokémon ID"),
    reward_ar_poke_form: str = Query("all", description="Filter by AR reward Pokémon form"),
    normal_type: str = Query("all", description="Filter by normal quest type"),
    reward_normal_type: str = Query("all", description="Filter by normal reward type"),
    reward_normal_item_id: str = Query("all", description="Filter by normal reward item ID"),
    reward_normal_item_amount: str = Query("all", description="Filter by normal reward item amount"),
    reward_normal_poke_id: str = Query("all", description="Filter by normal reward Pokémon ID"),
    reward_normal_poke_form: str = Query("all", description="Filter by normal reward Pokémon form"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    counter_type = counter_type.lower()
    interval = interval.lower()
    mode = mode.lower()
    if counter_type not in ["totals"]:
        raise HTTPException(status_code=400, detail="❌ Invalid counter_type. Must be totals.")
    if counter_type in ["totals", "tth"] and interval not in ["hourly", "weekly"]:
        raise HTTPException(status_code=400, detail="❌ Interval must be hourly or weekly.")
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if mode == "surged" and interval != "hourly":
        raise HTTPException(status_code=400, detail="❌ Surged mode is only supported for hourly intervals.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        # Get timezone offset (None means use machine timezone)
        area_offset = filtering_keys.get_area_offset(area, global_state.geofences)

        start_dt = filtering_keys.parse_time_input(start_time, area_offset)
        end_dt = filtering_keys.parse_time_input(end_time, area_offset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    quest_counter_retrieval = QuestCounterRetrieval(
        area, start_dt, end_dt, mode, with_ar,
        ar_type, reward_ar_type, reward_ar_item_id, reward_ar_item_amount, reward_ar_poke_id, reward_ar_poke_form,
        normal_type, reward_normal_type, reward_normal_item_id, reward_normal_item_amount, reward_normal_poke_id, reward_normal_poke_form
    )

    # Retrieve data dynamically based on counter type and interval
    retrieval_methods = {
        ("totals", "hourly"): quest_counter_retrieval.quest_retrieve_totals_hourly,
        ("totals", "weekly"): quest_counter_retrieval.quest_retrieve_totals_weekly
    }

    retrieval_method = retrieval_methods.get((counter_type, interval))
    result = await retrieval_method() if retrieval_method else {}

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/redis/get_pokemon_timeseries",
    tags=["Pokémon TimeSeries"],
    dependencies=dependencies_list
)
async def get_pokemon_timeseries(
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    pokemon_id: str = Query("all", description="Pokémon ID"),
    form: str = Query("all", description="Pokémon form"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    mode = mode.lower()
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        # Get timezone offset (None means use machine timezone)
        area_offset = filtering_keys.get_area_offset(area, global_state.geofences)

        start_dt = filtering_keys.parse_time_input(start_time, area_offset)
        end_dt = filtering_keys.parse_time_input(end_time, area_offset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    pokemon_timeseries = PokemonTimeSeries(area, start_dt, end_dt, mode, pokemon_id, form)

    # Retrieve data dynamically based on counter type and interval

    result = await pokemon_timeseries.retrieve_timeseries()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/redis/get_pokemon_tth_timeseries",
    tags=["Pokémon TTH TimeSeries"],
    dependencies=dependencies_list
)
async def get_pokemon_tth_timeseries(
    start_time: str = Query(..., description="Start time as ISO format or relative (e.g., '1 month')"),
    end_time: str = Query(..., description="End time as ISO format or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum', 'grouped', or 'surged'"),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    tth_bucket: str = Query("all", description="TTH bucket filter (e.g., '10_15'; use 'all' to match any)"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    await secure_api.check_secret_header_value(api_secret_header)

    mode = mode.lower()
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be 'sum', 'grouped', or 'surged'.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        # Get timezone offset (None means use machine timezone)
        area_offset = filtering_keys.get_area_offset(area, global_state.geofences)

        start_dt = filtering_keys.parse_time_input(start_time, area_offset)
        end_dt = filtering_keys.parse_time_input(end_time, area_offset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize TTH retrieval object.
    pokemon_tth_timeseries = PokemonTTHTimeSeries(area, start_dt, end_dt, tth_bucket, mode)
    result = await pokemon_tth_timeseries.retrieve_timeseries()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/redis/get_raid_timeseries",
    tags=["Raid TimeSeries"],
    dependencies=dependencies_list
)
async def get_raid_timeseries(
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    raid_pokemon: str = Query("all", description="all or Pokémon ID"),
    raid_form: str = Query("all", description="all or Form ID"),
    raid_level: str = Query("all", description="all or Raid Level"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    mode = mode.lower()
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        # Get timezone offset (None means use machine timezone)
        area_offset = filtering_keys.get_area_offset(area, global_state.geofences)

        start_dt = filtering_keys.parse_time_input(start_time, area_offset)
        end_dt = filtering_keys.parse_time_input(end_time, area_offset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    raid_timeseries = RaidTimeSeries(area, start_dt, end_dt, mode, raid_pokemon, raid_form, raid_level)

    # Retrieve data dynamically based on counter type and interval

    result = await raid_timeseries.raid_retrieve_timeseries()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/redis/get_invasion_timeseries",
    tags=["Invasion TimeSeries"],
    dependencies=dependencies_list
)
async def get_invasion_timeseries(
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    display: str = Query("all", description="all or Invasion Display ID"),
    grunt: str = Query("all", description="all or Grunt ID"),
    confirmed: str = Query("all", description="0 or 1 (confirmed or not details)."),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    mode = mode.lower()
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        # Get timezone offset (None means use machine timezone)
        area_offset = filtering_keys.get_area_offset(area, global_state.geofences)

        start_dt = filtering_keys.parse_time_input(start_time, area_offset)
        end_dt = filtering_keys.parse_time_input(end_time, area_offset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    invasion_timeseries = InvasionTimeSeries(area, start_dt, end_dt, mode, display, grunt, confirmed)

    # Retrieve data dynamically based on counter type and interval

    result = await invasion_timeseries.invasion_retrieve_timeseries()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/redis/get_quest_timeseries",
    tags=["Quest TimeSeries"],
    dependencies=dependencies_list
)
async def get_quest_timeseries(
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    quest_mode: str = Query("all", description="all or AR or NORMAL"),
    quest_type: str = Query("all", description="all or Quest Type ID"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    mode = mode.lower()
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        # Get timezone offset (None means use machine timezone)
        area_offset = filtering_keys.get_area_offset(area, global_state.geofences)

        start_dt = filtering_keys.parse_time_input(start_time, area_offset)
        end_dt = filtering_keys.parse_time_input(end_time, area_offset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    quest_timeseries = QuestTimeSeries(area, start_dt, end_dt, mode, quest_mode, quest_type)

    # Retrieve data dynamically based on counter type and interval

    result = await quest_timeseries.quest_retrieve_timeseries()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output

# SQL section
@router.get(
    "/api/sql/get_pokemon_heatmap_data",
    tags=["Pokémon HeatMap Data"],
    dependencies=dependencies_list
)
async def get_pokemon_heatmap_data(
    start_time: str = Query(..., description="Start time as 202503 (2025 year month 03)"),
    end_time: str = Query(..., description="End time as 202504 (2025 year month 04)"),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    pokemon_id: str = Query("all", description="all or Pokémon ID"),
    form: str = Query("all", description="all or Pokémon Form ID"),
    iv_bucket: str = Query("all", description="all or IV specific bucket(0, 25, 50, 75, 90, 100), choose one."),
    limit: Optional[int] = Query(0, description="Optional row limit for preview in the UI, 1000 advised."),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        start_dt = parse_time_input(start_time)
        end_dt = parse_time_input(end_time)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time format: {e}")

    # Initialize the counter retrieval object
    pokemon_heatmap = PokemonSQLQueries(area, start_dt, end_dt, pokemon_id, form, iv_bucket, limit)

    # Retrieve data dynamically based on counter type and interval

    result = await pokemon_heatmap.pokemon_sql_heatmap()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/sql/get_shiny_rate_data",
    tags=["Shiny Rate Data"],
    dependencies=dependencies_list
)
async def get_shiny_rate_data(
    start_time: str = Query(..., description="Start time as 202503 (2025 year month 03)"),
    end_time: str = Query(..., description="End time as 202504 (2025 year month 04)"),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    username: str = Query("all", description="all or specific username"),
    pokemon_id: str = Query("all", description="all or Pokémon ID"),
    form: str = Query("all", description="all or Pokémon Form ID"),
    shiny: str = Query("all", description="all or shiny status (0=non-shiny, 1=shiny)"),
    limit: Optional[int] = Query(0, description="Optional row limit for preview in the UI, 1000 advised."),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        start_dt = parse_time_input(start_time)
        end_dt = parse_time_input(end_time)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time format: {e}")

    if shiny not in ["all", "0", "1"]:
        raise HTTPException(status_code=400, detail="❌ Invalid shiny value. Must be all, 0, or 1.")

    # Initialize the shiny rate retrieval object
    shiny_rates = ShinySQLQueries(area, start_dt, end_dt, username, pokemon_id, form, shiny, limit)

    # Retrieve data
    result = await shiny_rates.shiny_sql_rates()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/sql/get_raid_data",
    tags=["Raid SQL Data"],
    dependencies=dependencies_list
)
async def get_raid_data(
    start_time: str = Query(..., description="Start time as 202503 (2025 year month 03)"),
    end_time: str = Query(..., description="End time as 202504 (2025 year month 04)"),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    gym_id: str = Query("all", description="all or specific gym ID"),
    raid_pokemon: str = Query("all", description="all or raid boss Pokémon ID"),
    raid_level: str = Query("all", description="all or raid level (1-5)"),
    raid_form: str = Query("all", description="all or raid boss form"),
    raid_team: str = Query("all", description="all or controlling team ID"),
    raid_costume: str = Query("all", description="all or costume ID"),
    raid_is_exclusive: str = Query("all", description="all or exclusive status (0 or 1)"),
    raid_ex_raid_eligible: str = Query("all", description="all or EX eligibility (0 or 1)"),
    limit: Optional[int] = Query(0, description="Optional row limit for preview in the UI, 1000 advised."),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        start_dt = parse_time_input(start_time)
        end_dt = parse_time_input(end_time)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time format: {e}")

    # Initialize the raid retrieval object
    raid_data = RaidSQLQueries(
        area=area,
        start=start_dt,
        end=end_dt,
        gym_id=gym_id,
        raid_pokemon=raid_pokemon,
        raid_level=raid_level,
        raid_form=raid_form,
        raid_team=raid_team,
        raid_costume=raid_costume,
        raid_is_exclusive=raid_is_exclusive,
        raid_ex_raid_eligible=raid_ex_raid_eligible,
        limit=limit
    )

    # Retrieve data
    result = await raid_data.raid_sql_data()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/sql/get_invasion_data",
    tags=["Invasion SQL Data"],
    dependencies=dependencies_list
)
async def get_invasion_data(
    start_time: str = Query(..., description="Start time as 202503 (2025 year month 03)"),
    end_time: str = Query(..., description="End time as 202504 (2025 year month 04)"),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    pokestop_id: str = Query("all", description="all or specific pokestop ID"),
    display_type: str = Query("all", description="all or invasion display type"),
    character: str = Query("all", description="all or invasion character"),
    grunt: str = Query("all", description="all or grunt type"),
    confirmed: str = Query("all", description="all or confirmed status (0 or 1)"),
    limit: Optional[int] = Query(0, description="Optional row limit for preview in the UI, 1000 advised."),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        start_dt = parse_time_input(start_time)
        end_dt = parse_time_input(end_time)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time format: {e}")

    # Initialize the invasion retrieval object
    invasion_data = InvasionSQLQueries(
        area=area,
        start=start_dt,
        end=end_dt,
        pokestop_id=pokestop_id,
        display_type=display_type,
        character=character,
        grunt=grunt,
        confirmed=confirmed,
        limit=limit
    )

    # Retrieve data
    result = await invasion_data.invasion_sql_data()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output


@router.get(
    "/api/sql/get_quest_data",
    tags=["Quest SQL Data"],
    dependencies=dependencies_list
)
async def get_quest_data(
    start_time: str = Query(..., description="Start time as 202503 (2025 year month 03)"),
    end_time: str = Query(..., description="End time as 202504 (2025 year month 04)"),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter"),
    pokestop_id: str = Query("all", description="all or specific pokestop ID"),
    ar_type: str = Query("all", description="all or AR quest type"),
    normal_type: str = Query("all", description="all or normal quest type"),
    reward_ar_type: str = Query("all", description="all or AR reward type"),
    reward_normal_type: str = Query("all", description="all or normal reward type"),
    reward_ar_item_id: str = Query("all", description="all or AR reward item ID"),
    reward_normal_item_id: str = Query("all", description="all or normal reward item ID"),
    reward_ar_poke_id: str = Query("all", description="all or AR reward Pokémon ID"),
    reward_normal_poke_id: str = Query("all", description="all or normal reward Pokémon ID"),
    limit: Optional[int] = Query(0, description="Optional row limit for preview in the UI, 1000 advised."),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)

    # Normalize and validate inputs
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        start_dt = parse_time_input(start_time)
        end_dt = parse_time_input(end_time)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time format: {e}")

    # Initialize the quest retrieval object
    quest_data = QuestSQLQueries(
        area=area,
        start=start_dt,
        end=end_dt,
        pokestop_id=pokestop_id,
        ar_type=ar_type,
        normal_type=normal_type,
        reward_ar_type=reward_ar_type,
        reward_normal_type=reward_normal_type,
        reward_ar_item_id=reward_ar_item_id,
        reward_normal_item_id=reward_normal_item_id,
        reward_ar_poke_id=reward_ar_poke_id,
        reward_normal_poke_id=reward_normal_poke_id,
        limit=limit
    )

    # Retrieve data
    result = await quest_data.quest_sql_data()

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output
