import config as AppConfig
from datetime import datetime
from server_fastapi.utils import secure_api
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from typing import Optional
from server_fastapi.retrieval import pokemon_counter_retrieval

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
    tags=["Pokemon CounterSeries"],
    dependencies=[
        Depends(secure_api.validate_path),
        Depends(secure_api.validate_ip),
    ]
)
async def get_pokemon_counterseries(
    counter_type: str = Query(..., description="Type of counter series: totals, tth, or weather"),
    interval: str = Query(..., description="Interval: hourly or weekly for totals and tth; monthly for weather"),
    start_time: str = Query(..., description="Start time in ISO format (e.g., 2023-03-05T00:00:00)"),
    end_time: str = Query(..., description="End time in ISO format (e.g., 2023-03-15T23:59:59)"),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str = Query("default_area", description="Area to filter counters"),
    # These parameters will now show up in the docs for manual input
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
    api_secret_key: Optional[str] = secure_api.get_secret_key_param()
):
    # Manually perform the secret header and secret key validations
    await secure_api.check_secret_header_value(api_secret_header)
    await secure_api.check_secret_key_value(api_secret_key)
    # Normalize and validate inputs
    counter_type = counter_type.lower()
    interval = interval.lower()
    if counter_type not in ["totals", "tth", "weather"]:
        raise HTTPException(status_code=400, detail="Invalid counter_type. Must be totals, tth, or weather.")
    if counter_type in ["totals", "tth"] and interval not in ["hourly", "weekly"]:
        raise HTTPException(status_code=400, detail="For totals and tth, interval must be hourly or weekly.")
    if counter_type == "weather" and interval != "monthly":
        raise HTTPException(status_code=400, detail="For weather, interval must be monthly.")
    if response_format.lower() not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="Invalid response_format. Must be json or text.")

    try:
        start_dt = datetime.fromisoformat(start_time)
        end_dt = datetime.fromisoformat(end_time)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid start_time or end_time format. Must be ISO format.")

    # Retrieve data based on the parameters
    result = {}
    if counter_type == "totals":
        if interval == "hourly":
            result = await pokemon_counter_retrieval.retrieve_totals_hourly(area, start_dt, end_dt)
        elif interval == "weekly":
            result = await pokemon_counter_retrieval.retrieve_totals_weekly(area, start_dt, end_dt)
    elif counter_type == "tth":
        if interval == "hourly":
            result = await pokemon_counter_retrieval.retrieve_tth_hourly(area, start_dt, end_dt)
        elif interval == "weekly":
            result = await pokemon_counter_retrieval.retrieve_tth_weekly(area, start_dt, end_dt)
    elif counter_type == "weather":
        result = await pokemon_counter_retrieval.retrieve_weather_monthly(area, start_dt, end_dt)

    if response_format.lower() == "json":
        return result
    else:
        # Convert dictionary to plain text output
        text_output = "\n".join(f"{k}: {v}" for k, v in result.items())
        return text_output
