import config as AppConfig
from datetime import datetime
from server_fastapi.utils import secure_api
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from typing import Optional
from server_fastapi.retrieval import pokemon_counter_retrieval
from server_fastapi.utils import filtering_keys

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

    # Retrieve data based on parameters and mode
    result = {}
    if counter_type == "totals":
        if interval == "hourly":
            result = await pokemon_counter_retrieval.retrieve_totals_hourly(area, start_dt, end_dt, mode=mode)
        elif interval == "weekly":
            result = await pokemon_counter_retrieval.retrieve_totals_weekly(area, start_dt, end_dt, mode=mode)
    elif counter_type == "tth":
        if interval == "hourly":
            result = await pokemon_counter_retrieval.retrieve_tth_hourly(area, start_dt, end_dt, mode=mode)
        elif interval == "weekly":
            result = await pokemon_counter_retrieval.retrieve_tth_weekly(area, start_dt, end_dt, mode=mode)
    elif counter_type == "weather":
        result = await pokemon_counter_retrieval.retrieve_weather_monthly(area, start_dt, end_dt, mode=mode)

    if response_format.lower() == "json":
        return result
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output
