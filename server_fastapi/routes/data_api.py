import config as AppConfig
from datetime import datetime
from server_fastapi.utils import secure_api
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from typing import Optional
from my_redis.utils import filtering_keys
from my_redis.queries.gets.pokemons.pokemon_counter_retrieval import PokemonCounterRetrieval
from my_redis.queries.gets.raids.raid_counter_retrieval import RaidCounterRetrieval
from my_redis.queries.gets.invasions.invasion_counter_retrieval import InvasionCounterRetrieval
from my_redis.queries.gets.quests.quest_counter_retrieval import QuestCounterRetrieval

router = APIRouter()

@router.get(
    "/api/redis/total_pokemons_hourly",
    tags=["Total Hourly Pokémon"],
    dependencies=[
        Depends(secure_api.validate_path),
        Depends(secure_api.validate_ip),
    ]
)
async def get_total_pokemons_hourly(
    # These parameters will now show up in the docs for manual input
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
    api_secret_key: Optional[str] = secure_api.get_secret_key_param()
):
    # Manually perform the secret header and secret key validations
    await secure_api.check_secret_header_value(api_secret_header)
    await secure_api.check_secret_key_value(api_secret_key)
    message = "This is working if it passes the checks"
    return message

@router.get(
    "/api/redis/get_pokemon_counterseries",
    tags=["Pokémon Counter Series"],
    dependencies=[
        Depends(secure_api.validate_path),
        Depends(secure_api.validate_ip),
    ]
)
async def get_pokemon_counterseries(
    counter_type: str = Query(..., description="Type of counter series: totals, tth, or weather"),
    interval: str = Query(..., description="Interval: hourly or weekly for totals and tth; monthly for weather"),
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter counters"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
    api_secret_key: Optional[str] = secure_api.get_secret_key_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)
    await secure_api.check_secret_key_value(api_secret_key)

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
        start_dt = filtering_keys.parse_time_input(start_time)
        end_dt = filtering_keys.parse_time_input(end_time)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    pokemon_counter_retrieval = PokemonCounterRetrieval(area, start_dt, end_dt, mode)

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
    dependencies=[
        Depends(secure_api.validate_path),
        Depends(secure_api.validate_ip),
    ]
)
async def get_counter_raids(
    counter_type: str = Query("totals", description="Type of counter series: totals"),
    interval: str = Query(..., description="Interval: hourly or weekly."),
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter counters"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
    api_secret_key: Optional[str] = secure_api.get_secret_key_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)
    await secure_api.check_secret_key_value(api_secret_key)

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
        start_dt = filtering_keys.parse_time_input(start_time)
        end_dt = filtering_keys.parse_time_input(end_time)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    raid_counter_retrieval = RaidCounterRetrieval(area, start_dt, end_dt, mode)

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
    dependencies=[
        Depends(secure_api.validate_path),
        Depends(secure_api.validate_ip),
    ]
)
async def get_counter_invasions(
    counter_type: str = Query("totals", description="Type of counter series: totals"),
    interval: str = Query(..., description="Interval: hourly or weekly."),
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter counters"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
    api_secret_key: Optional[str] = secure_api.get_secret_key_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)
    await secure_api.check_secret_key_value(api_secret_key)

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
        start_dt = filtering_keys.parse_time_input(start_time)
        end_dt = filtering_keys.parse_time_input(end_time)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    invasion_counter_retrieval = InvasionCounterRetrieval(area, start_dt, end_dt, mode)

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
    dependencies=[
        Depends(secure_api.validate_path),
        Depends(secure_api.validate_ip),
    ]
)
async def get_counter_quests(
    counter_type: str = Query("totals", description="Type of counter series: totals"),
    interval: str = Query(..., description="Interval: hourly or weekly."),
    start_time: str = Query(..., description="Start time as ISO format (e.g., 2023-03-05T00:00:00) or relative (e.g., '1 month', '10 days')"),
    end_time: str = Query(..., description="End time as ISO format (e.g., 2023-03-15T23:59:59) or relative (e.g., 'now')"),
    mode: str = Query("sum", description="Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'."),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("global", description="Area to filter counters"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
    api_secret_key: Optional[str] = secure_api.get_secret_key_param()
):
    # Validate secret parameters
    await secure_api.check_secret_header_value(api_secret_header)
    await secure_api.check_secret_key_value(api_secret_key)

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
        start_dt = filtering_keys.parse_time_input(start_time)
        end_dt = filtering_keys.parse_time_input(end_time)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid time format: {e}")

    # Initialize the counter retrieval object
    quest_counter_retrieval = QuestCounterRetrieval(area, start_dt, end_dt, mode)

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
