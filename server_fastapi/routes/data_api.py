from urllib import response
import config as AppConfig
from datetime import datetime
from typing import List
from server_fastapi.utils import secure_api
from sql.utils.time_parser import parse_time_input, month_parse_time_input, parse_time_to_datetime
from sql.utils.area_parser import resolve_area_id_by_name
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
from sql.tasks.golbat_pokestops import GolbatSQLPokestops
from sql.queries.pokemon_gets import HeatmapFilters, fetch_pokemon_heatmap_range
from sql.queries.pokemon_shiny_gets import fetch_shiny_rates_range
from sql.queries.raid_gets import RaidFilters, fetch_raids_range
from sql.queries.invasion_gets import InvasionFilters, fetch_invasions_range
from sql.queries.quest_gets import QuestItemFilters, QuestMonFilters, fetch_quests_range

router = APIRouter()

dependencies_list = [
    Depends(secure_api.validate_path),
    Depends(secure_api.validate_ip)
]
if AppConfig.api_secret_key:
    dependencies_list.append(Depends(secure_api.verify_token))

def _parse_csv_param(v: str):
    """
    Accepts 'all' or a comma-separated list.
    Returns None for 'all' (meaning no filtering), otherwise a set of strings.
    Trims whitespace and drops empty items.
    """
    if v is None:
        return None
    s = v.strip()
    if not s or s.lower() == "all":
        return None
    return {part.strip() for part in s.split(",") if part.strip()}

def _to_int_list(name: str, s: Optional[set[str]]) -> Optional[List[int]]:
    if s is None:
        return None
    try:
        return sorted({int(x) for x in s})
    except Exception:
        raise HTTPException(status_code=400, detail=f"❌ {name} must be integers (CSV).")


@router.get(
    "/api/redis/get_cached_pokestops",
    tags=["Pokestops"],
    dependencies=dependencies_list
)
async def get_cached_pokestops(
    area: Optional[str] = Query("global", description="Filter pokestops by area (default: global)"),
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

    total_value = result.get("grand_total", 0)
    if area.lower() in ["global", "all"]:
        # Return all areas along with the combined total.
        result = {"areas": result.get("areas", {}), "total": total_value}
    else:
        # For a specific area, return only the total for that area.
        # Do a case-insensitive lookup in the cached "areas" dictionary.
        filtered_value = next(
            (v for k, v in result.get("areas", {}).items() if k.lower() == area.lower()),
            0
        )
        result = {"total": filtered_value}

    result = {"data": result}

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
    resp_fmt = response_format.lower()
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
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(400, "❌ Invalid response_format. Must be json or text.")


    # Parse multi-select params (None means "all")
    metrics_set     = _parse_csv_param(metric)
    pokemon_ids_set = _parse_csv_param(pokemon_id)
    forms_set       = _parse_csv_param(form)

    # Area handling
    area_is_global = area.strip().lower() in ["global", "all"]
    area_list = None if area_is_global else _parse_csv_param(area)

    # Resolve areas -> offsets
    if area_is_global:
        # all areas
        area_offsets = filtering_keys.get_area_offset("global", global_state.geofences)
    elif area_list:
        area_offsets = filtering_keys.get_area_offsets_for_list(list(area_list), global_state.geofences)
        if not area_offsets:
            raise HTTPException(400, "❌ None of the requested areas were found.")
    else:
        # single area string
        offset = filtering_keys.get_area_offset(area, global_state.geofences)
        area_offsets = {area: offset}

    # retrieval method mapping
    def _method(obj):
        return {
            ("totals", "hourly"): obj.retrieve_totals_hourly,
            ("totals", "weekly"): obj.retrieve_totals_weekly,
            ("tth", "hourly"): obj.retrieve_tth_hourly,
            ("tth", "weekly"): obj.retrieve_tth_weekly,
            ("weather", "monthly"): obj.retrieve_weather_monthly,
        }.get((counter_type, interval))

    # Run per-area
    results = {}
    for area_name, offset in area_offsets.items():
        try:
            start_dt = filtering_keys.parse_time_input(start_time, offset)
            end_dt   = filtering_keys.parse_time_input(end_time,   offset)

            retr = PokemonCounterRetrieval(
                area=area_name,
                start=start_dt,
                end=end_dt,
                mode=mode,
                pokemon_id=pokemon_ids_set,   # sets or None
                form=forms_set,
                metric=metrics_set,
            )
            method = _method(retr)
            results[area_name] = await method() if method else {}
        except Exception as e:
            results[area_name] = {"error": str(e)}

    if resp_fmt == "json":
        return results if area_is_global or (area_list and len(area_list) > 1) else next(iter(results.values()))
    else:
        text_output = "\n".join(f"{k}: {v}" for k, v in results.items())
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
    resp_fmt = response_format.lower()

    if counter_type not in ["totals"]:
        raise HTTPException(status_code=400, detail="❌ Invalid counter_type. Must be totals.")
    if counter_type in ["totals", "tth"] and interval not in ["hourly", "weekly"]:
        raise HTTPException(status_code=400, detail="❌ Interval must be hourly or weekly.")
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if mode == "surged" and interval != "hourly":
        raise HTTPException(status_code=400, detail="❌ Surged mode is only supported for hourly intervals.")
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    # Parse multi-select filters (None means "all")
    raid_pokemon_set     = _parse_csv_param(raid_pokemon)
    raid_form_set        = _parse_csv_param(raid_form)
    raid_level_set       = _parse_csv_param(raid_level)
    raid_costume_set     = _parse_csv_param(raid_costume)
    raid_is_exc_set      = _parse_csv_param(raid_is_exclusive)
    raid_ex_eligible_set = _parse_csv_param(raid_ex_eligible)

    # Area handling (case-insensitive; returns canonical names)
    area_is_global = area.strip().lower() in ["global", "all"]
    area_list = None if area_is_global else _parse_csv_param(area)

    if area_is_global:
        area_offsets = filtering_keys.get_area_offset("global", global_state.geofences)
    elif area_list:
        area_offsets = filtering_keys.get_area_offsets_for_list(list(area_list), global_state.geofences)
        if not area_offsets:
            raise HTTPException(400, "❌ None of the requested areas were found.")
    else:
        # single area -> resolve via list helper to get canonical name
        resolved = filtering_keys.get_area_offsets_for_list([area], global_state.geofences)
        if not resolved:
            raise HTTPException(400, f"❌ Area not found: {area}")
        area_offsets = resolved

    # Select retrieval method
    def _method(obj):
        return {
            ("totals", "hourly"): obj.raid_retrieve_totals_hourly,
            ("totals", "weekly"): obj.raid_retrieve_totals_weekly,
        }.get((counter_type, interval))

    # Run per-area
    results = {}
    for area_name, offset in area_offsets.items():
        try:
            start_dt = filtering_keys.parse_time_input(start_time, offset)
            end_dt   = filtering_keys.parse_time_input(end_time,   offset)

            retr = RaidCounterRetrieval(
                area=area_name,
                start=start_dt,
                end=end_dt,
                mode=mode,
                raid_pokemon=raid_pokemon_set,
                raid_form=raid_form_set,
                raid_level=raid_level_set,
                raid_costume=raid_costume_set,
                raid_is_exclusive=raid_is_exc_set,
                raid_ex_eligible=raid_ex_eligible_set,
            )
            method = _method(retr)
            results[area_name] = await method() if method else {}
        except Exception as e:
            results[area_name] = {"error": str(e)}

    if resp_fmt == "json":
        # If multiple areas, return the dict; if only one, return that single result.
        return results if len(results) != 1 else next(iter(results.values()))
    else:
        return "\n".join(f"{k}: {v}" for k, v in results.items())

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
    resp_fmt = response_format.lower()

    if counter_type not in ["totals"]:
        raise HTTPException(status_code=400, detail="❌ Invalid counter_type. Must be totals.")
    if counter_type in ["totals", "tth"] and interval not in ["hourly", "weekly"]:
        raise HTTPException(status_code=400, detail="❌ Interval must be hourly or weekly.")
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if mode == "surged" and interval != "hourly":
        raise HTTPException(status_code=400, detail="❌ Surged mode is only supported for hourly intervals.")
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    # Parse multi-select filters (None => no filtering)
    display_types_set = _parse_csv_param(display_type)
    characters_set    = _parse_csv_param(character)
    grunts_set        = _parse_csv_param(grunt)

    # Areas (case-insensitive; canonical names out)
    area_is_global = area.strip().lower() in ["global", "all"]
    area_list = None if area_is_global else _parse_csv_param(area)

    if area_is_global:
        area_offsets = filtering_keys.get_area_offset("global", global_state.geofences)
    elif area_list:
        area_offsets = filtering_keys.get_area_offsets_for_list(list(area_list), global_state.geofences)
        if not area_offsets:
            raise HTTPException(400, "❌ None of the requested areas were found.")
    else:
        resolved = filtering_keys.get_area_offsets_for_list([area], global_state.geofences)
        if not resolved:
            raise HTTPException(400, f"❌ Area not found: {area}")
        area_offsets = resolved

    # Choose retrieval method
    def _method(obj):
        return {
            ("totals", "hourly"): obj.invasion_retrieve_totals_hourly,
            ("totals", "weekly"): obj.invasion_retrieve_totals_weekly,
        }.get((counter_type, interval))

    # Per-area execution (keeps your per-area offset semantics)
    results = {}
    for area_name, offset in area_offsets.items():
        try:
            start_dt = filtering_keys.parse_time_input(start_time, offset)
            end_dt   = filtering_keys.parse_time_input(end_time,   offset)

            retr = InvasionCounterRetrieval(
                area=area_name,
                start=start_dt,
                end=end_dt,
                mode=mode,
                display_type=display_types_set,
                character=characters_set,
                grunt=grunts_set,
                confirmed=confirmed,
            )
            method = _method(retr)
            results[area_name] = await method() if method else {}
        except Exception as e:
            results[area_name] = {"error": str(e)}

    if resp_fmt == "json":
        return results if len(results) != 1 else next(iter(results.values()))
    else:
        return "\n".join(f"{k}: {v}" for k, v in results.items())


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
    with_ar: str = Query("all", description="Filter by AR quests: true, false, or all"),
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
    resp_fmt = response_format.lower()

    if counter_type not in ["totals"]:
        raise HTTPException(status_code=400, detail="❌ Invalid counter_type. Must be totals.")
    if counter_type in ["totals", "tth"] and interval not in ["hourly", "weekly"]:
        raise HTTPException(status_code=400, detail="❌ Interval must be hourly or weekly.")
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if mode == "surged" and interval != "hourly":
        raise HTTPException(status_code=400, detail="❌ Surged mode is only supported for hourly intervals.")
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    with_ar_l = with_ar.strip().lower()
    if with_ar_l not in ("true", "false", "all"):
        raise HTTPException(400, "❌ with_ar must be 'true', 'false', or 'all'.")

    # Parse CSV → sets (None means no filter)
    ar_type_set                 = _parse_csv_param(ar_type)
    reward_ar_type_set          = _parse_csv_param(reward_ar_type)
    reward_ar_item_id_set       = _parse_csv_param(reward_ar_item_id)
    reward_ar_item_amount_set   = _parse_csv_param(reward_ar_item_amount)
    reward_ar_poke_id_set       = _parse_csv_param(reward_ar_poke_id)
    reward_ar_poke_form_set     = _parse_csv_param(reward_ar_poke_form)

    normal_type_set             = _parse_csv_param(normal_type)
    reward_normal_type_set      = _parse_csv_param(reward_normal_type)
    reward_normal_item_id_set   = _parse_csv_param(reward_normal_item_id)
    reward_normal_item_amount_set = _parse_csv_param(reward_normal_item_amount)
    reward_normal_poke_id_set   = _parse_csv_param(reward_normal_poke_id)
    reward_normal_poke_form_set = _parse_csv_param(reward_normal_poke_form)

    # Area handling (case-insensitive; canonical names)
    area_is_global = area.strip().lower() in ["global", "all"]
    area_list = None if area_is_global else _parse_csv_param(area)

    if area_is_global:
        area_offsets = filtering_keys.get_area_offset("global", global_state.geofences)  # dict[name]=offset
    elif area_list:
        area_offsets = filtering_keys.get_area_offsets_for_list(list(area_list), global_state.geofences)
        if not area_offsets:
            raise HTTPException(400, "❌ None of the requested areas were found.")
    else:
        resolved = filtering_keys.get_area_offsets_for_list([area], global_state.geofences)
        if not resolved:
            raise HTTPException(400, f"❌ Area not found: {area}")
        area_offsets = resolved

    # Method
    def _method(obj):
        return {
            ("totals", "hourly"): obj.quest_retrieve_totals_hourly,
            ("totals", "weekly"): obj.quest_retrieve_totals_weekly,
        }.get((counter_type, interval))

    # Per-area run
    results = {}
    for area_name, offset in area_offsets.items():
        try:
            start_dt = filtering_keys.parse_time_input(start_time, offset)
            end_dt   = filtering_keys.parse_time_input(end_time,   offset)

            retr = QuestCounterRetrieval(
                area=area_name,
                start=start_dt,
                end=end_dt,
                mode=mode,
                with_ar=with_ar_l,
                ar_type=ar_type_set,
                reward_ar_type=reward_ar_type_set,
                reward_ar_item_id=reward_ar_item_id_set,
                reward_ar_item_amount=reward_ar_item_amount_set,
                reward_ar_poke_id=reward_ar_poke_id_set,
                reward_ar_poke_form=reward_ar_poke_form_set,
                normal_type=normal_type_set,
                reward_normal_type=reward_normal_type_set,
                reward_normal_item_id=reward_normal_item_id_set,
                reward_normal_item_amount=reward_normal_item_amount_set,
                reward_normal_poke_id=reward_normal_poke_id_set,
                reward_normal_poke_form=reward_normal_poke_form_set,
            )
            method = _method(retr)
            results[area_name] = await method() if method else {}
        except Exception as e:
            results[area_name] = {"error": str(e)}

    if resp_fmt == "json":
        return results if len(results) != 1 else next(iter(results.values()))
    else:
        return "\n".join(f"{k}: {v}" for k, v in results.items())


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
    resp_fmt = response_format.lower()

    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(400, "❌ Invalid response_format. Must be json or text.")

    # Parse multi-select (None means "all")
    pokemon_ids_set = _parse_csv_param(pokemon_id)
    forms_set       = _parse_csv_param(form)

    # Area handling (case-insensitive; returns canonical names)
    area_is_global = area.strip().lower() in ["global", "all"]
    area_list = None if area_is_global else _parse_csv_param(area)

    if area_is_global:
        area_offsets = filtering_keys.get_area_offset("global", global_state.geofences)
    elif area_list:
        area_offsets = filtering_keys.get_area_offsets_for_list(list(area_list), global_state.geofences)
        if not area_offsets:
            raise HTTPException(400, "❌ None of the requested areas were found.")
    else:
        resolved = filtering_keys.get_area_offsets_for_list([area], global_state.geofences)
        if not resolved:
            raise HTTPException(400, f"❌ Area not found: {area}")
        area_offsets = resolved

    results = {}
    for area_name, offset in area_offsets.items():
        try:
            start_dt = filtering_keys.parse_time_input(start_time, offset)
            end_dt   = filtering_keys.parse_time_input(end_time,   offset)

            ts = PokemonTimeSeries(
                area=area_name,
                start=start_dt,
                end=end_dt,
                mode=mode,
                pokemon_id=pokemon_ids_set,
                form=forms_set,
            )
            results[area_name] = await ts.retrieve_timeseries()
        except Exception as e:
            results[area_name] = {"mode": mode, "error": str(e)}

    if resp_fmt == "json":
        return results if len(results) != 1 else next(iter(results.values()))
    else:
        return "\n".join(f"{k}: {v}" for k, v in results.items())


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
    resp_fmt = response_format.lower()

    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be 'sum', 'grouped', or 'surged'.")
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    # CSV -> set[str] or None (None == no filtering/"all")
    tth_buckets_set = _parse_csv_param(tth_bucket)

    # Areas (case-insensitive; resolve to canonical names)
    area_is_global = area.strip().lower() in ["global", "all"]
    area_list = None if area_is_global else _parse_csv_param(area)

    if area_is_global:
        area_offsets = filtering_keys.get_area_offset("global", global_state.geofences)
    elif area_list:
        area_offsets = filtering_keys.get_area_offsets_for_list(list(area_list), global_state.geofences)
        if not area_offsets:
            raise HTTPException(400, "❌ None of the requested areas were found.")
    else:
        resolved = filtering_keys.get_area_offsets_for_list([area], global_state.geofences)
        if not resolved:
            raise HTTPException(400, f"❌ Area not found: {area}")
        area_offsets = resolved

    results = {}
    for area_name, offset in area_offsets.items():
        try:
            start_dt = filtering_keys.parse_time_input(start_time, offset)
            end_dt   = filtering_keys.parse_time_input(end_time,   offset)

            ts = PokemonTTHTimeSeries(
                area=area_name,
                start=start_dt,
                end=end_dt,
                tth_bucket=tth_buckets_set,
                mode=mode,
            )
            results[area_name] = await ts.retrieve_timeseries()
        except Exception as e:
            results[area_name] = {"mode": mode, "error": str(e)}

    if resp_fmt == "json":
        return results if len(results) != 1 else next(iter(results.values()))
    else:
        return "\n".join(f"{k}: {v}" for k, v in results.items())


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
    resp_fmt = response_format.lower()

    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    # Parse multi-select (None means "all")
    raid_pokemon_set = _parse_csv_param(raid_pokemon)
    raid_form_set    = _parse_csv_param(raid_form)
    raid_level_set   = _parse_csv_param(raid_level)

    # Area handling (case-insensitive; returns canonical names)
    area_is_global = area.strip().lower() in ["global", "all"]
    area_list = None if area_is_global else _parse_csv_param(area)

    if area_is_global:
        area_offsets = filtering_keys.get_area_offset("global", global_state.geofences)
    elif area_list:
        area_offsets = filtering_keys.get_area_offsets_for_list(list(area_list), global_state.geofences)
        if not area_offsets:
            raise HTTPException(400, "❌ None of the requested areas were found.")
    else:
        resolved = filtering_keys.get_area_offsets_for_list([area], global_state.geofences)
        if not resolved:
            raise HTTPException(400, f"❌ Area not found: {area}")
        area_offsets = resolved

    results = {}
    for area_name, offset in area_offsets.items():
        try:
            start_dt = filtering_keys.parse_time_input(start_time, offset)
            end_dt   = filtering_keys.parse_time_input(end_time,   offset)

            raid_timeseries = RaidTimeSeries(
                area=area_name,
                start=start_dt,
                end=end_dt,
                mode=mode,
                raid_pokemon=raid_pokemon_set,
                raid_form=raid_form_set,
                raid_level=raid_level_set,
            )
            results[area_name] = await raid_timeseries.raid_retrieve_timeseries()
        except Exception as e:
            results[area_name] = {"mode": mode, "error": str(e)}

    if resp_fmt == "json":
        return results if len(results) != 1 else next(iter(results.values()))
    else:
        return "\n".join(f"{k}: {v}" for k, v in results.items())


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
    resp_fmt = response_format.lower()

    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    # Parse multi-select (None means "all")
    displays_set = _parse_csv_param(display)
    grunts_set   = _parse_csv_param(grunt)

    # Area handling (case-insensitive; returns canonical names)
    area_is_global = area.strip().lower() in ["global", "all"]
    area_list = None if area_is_global else _parse_csv_param(area)

    if area_is_global:
        area_offsets = filtering_keys.get_area_offset("global", global_state.geofences)
    elif area_list:
        area_offsets = filtering_keys.get_area_offsets_for_list(list(area_list), global_state.geofences)
        if not area_offsets:
            raise HTTPException(400, "❌ None of the requested areas were found.")
    else:
        resolved = filtering_keys.get_area_offsets_for_list([area], global_state.geofences)
        if not resolved:
            raise HTTPException(400, f"❌ Area not found: {area}")
        area_offsets = resolved

    results = {}
    for area_name, offset in area_offsets.items():
        try:
            start_dt = filtering_keys.parse_time_input(start_time, offset)
            end_dt   = filtering_keys.parse_time_input(end_time,   offset)

            invasion_timeseries = InvasionTimeSeries(
                area=area_name,
                start=start_dt,
                end=end_dt,
                mode=mode,
                display=displays_set,
                grunt=grunts_set,
                confirmed=confirmed,
            )

            results[area_name] = await invasion_timeseries.invasion_retrieve_timeseries()
        except Exception as e:
            results[area_name] = {"mode": mode, "error": str(e)}

    if resp_fmt == "json":
        return results if len(results) != 1 else next(iter(results.values()))
    else:
        return "\n".join(f"{k}: {v}" for k, v in results.items())


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
    resp_fmt = response_format.lower()
    if mode not in ["sum", "grouped", "surged"]:
        raise HTTPException(status_code=400, detail="❌ Invalid mode. Must be one of 'sum', 'grouped', or 'surged'.")
    if resp_fmt not in ["json", "text"]:
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    # Parse multi-select (None means "all")
    quest_types_set = _parse_csv_param(quest_type)

    # Area handling (canonical + case-insensitive)
    area_is_global = area.strip().lower() in ["global", "all"]
    area_list = None if area_is_global else _parse_csv_param(area)

    if area_is_global:
        area_offsets = filtering_keys.get_area_offset("global", global_state.geofences)
    elif area_list:
        area_offsets = filtering_keys.get_area_offsets_for_list(list(area_list), global_state.geofences)
        if not area_offsets:
            raise HTTPException(400, "❌ None of the requested areas were found.")
    else:
        resolved = filtering_keys.get_area_offsets_for_list([area], global_state.geofences)
        if not resolved:
            raise HTTPException(400, f"❌ Area not found: {area}")
        area_offsets = resolved

    results = {}
    for area_name, offset in area_offsets.items():
        try:
            start_dt = filtering_keys.parse_time_input(start_time, offset)
            end_dt   = filtering_keys.parse_time_input(end_time,   offset)

            quest_timeseries = QuestTimeSeries(
                area=area_name,
                start=start_dt,
                end=end_dt,
                mode=mode,
                quest_mode=quest_mode,
                field_details=quest_types_set,
            )

            results[area_name] = await quest_timeseries.quest_retrieve_timeseries()
        except Exception as e:
            results[area_name] = {"mode": mode, "error": str(e)}

    if resp_fmt == "json":
        return results if len(results) != 1 else next(iter(results.values()))
    else:
        return "\n".join(f"{k}: {v}" for k, v in results.items())

# SQL section
@router.get(
    "/api/sql/get_pokemon_heatmap_data",
    tags=["Pokémon HeatMap Data"],
    dependencies=dependencies_list
)
async def get_pokemon_heatmap_data(
    start_time: str = Query(..., description="ISO or relative (e.g., '10 hours')"),
    end_time: str   = Query(..., description="ISO or 'now' / relative"),
    response_format: str = Query("json", description="json or text"),
    area: str       = Query(..., description="Single area name (exactly one; no lists)"),
    pokemon_id: str = Query("all", description="CSV of Pokémon IDs or 'all'"),
    form: str       = Query("all", description="CSV of forms or 'all'"),
    iv: str         = Query("all", description="IV conditions: '>=90,==0' or 'all'"),
    level: str      = Query("all", description="Level conditions: '>=30' or 'all'"),
    limit: Optional[int] = Query(0, description="Optional per-day limit; 0 = no limit"),
    concurrency: Optional[int] = Query(4, description="Max parallel day-queries"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
):
    await secure_api.check_secret_header_value(api_secret_header)

    fmt = (response_format or "json").lower()
    if fmt not in ("json", "text"):
        raise HTTPException(status_code=400, detail="❌ response_format must be json or text")

    # area to id - one area only
    area_norm = (area or "").strip()
    if area_norm.lower() in ("all", "global") or "," in area_norm:
        raise HTTPException(status_code=400, detail="❌ Provide exactly one area name (no lists, no 'all/global').")
    area_id = await resolve_area_id_by_name(area_norm)

    # parse time to DATETIME - precise seen_at window
    try:
        seen_from = parse_time_to_datetime(start_time)
        seen_to   = parse_time_to_datetime(end_time)
        if seen_to < seen_from:
            raise ValueError("end_time before start_time")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time range: {e}")

    # parse CSVs
    pid_set  = _parse_csv_param(pokemon_id)
    form_set = _parse_csv_param(form)
    try:
        pid_list  = sorted({int(x) for x in pid_set}) if pid_set else None
    except Exception:
        raise HTTPException(status_code=400, detail="❌ pokemon_id must be integers (CSV).")
    form_list = sorted(form_set) if form_set else None

    filters = HeatmapFilters(
        pokemon_ids=pid_list,
        forms=form_list,
        iv_expr=None if (iv or "all").lower() == "all" else iv,
        level_expr=None if (level or "all").lower() == "all" else level,
    )

    try:
        result = await fetch_pokemon_heatmap_range(
            area_id=int(area_id),
            seen_from=seen_from,
            seen_to=seen_to,
            filters=filters,
            limit_per_day=int(limit or 0),
            concurrency=int(concurrency or 4),
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"❌ {ve}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Query failed: {e}")

    if fmt == "json":
        return result

    # text fallback
    lines = [f"range={result['start_time']}..{result['end_time']} area={area_norm} rows={result['rows']}"]
    for r in result["data"]:
        lines.append(f"{r['latitude']},{r['longitude']} -> {r['count']} (#{r['pokemon_id']}/{r['form']} @ {r['spawnpoint']})")
    return "\n".join(lines)


@router.get(
    "/api/sql/get_shiny_rate_data",
    tags=["Shiny Rate Data"],
    dependencies=dependencies_list
)
async def get_shiny_rate_data(
    start_time: str = Query(..., description="Start month as 202503 (YYYYMM or YYMM)"),
    end_time: str   = Query(..., description="End month as 202504 (YYYYMM or YYMM)"),
    response_format: str = Query("json", description="Response format: json or text"),
    area: str       = Query("global", description="Area name or 'all'/'global'"),
    username: str   = Query("all", description="All or a specific username"),
    pokemon_id: str = Query("all", description="All or CSV of Pokémon IDs"),
    form: str       = Query("all", description="All or CSV of form strings"),
    min_user_n: int = Query(0, description="Per-user minimum encounters to include (noise control)"),
    limit: Optional[int] = Query(0, description="Limit rows in the final output; 0 = no limit"),
    concurrency: Optional[int] = Query(4, description="Parallel month queries"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    await secure_api.check_secret_header_value(api_secret_header)

    resp_fmt = (response_format or "json").lower()
    if resp_fmt not in ("json", "text"):
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    try:
        start_month_date = month_parse_time_input(start_time)  # returns date(y, m, 1)
        end_month_date   = month_parse_time_input(end_time)
        if end_month_date < start_month_date:
            raise ValueError("end_time before start_time")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time format: {e}")

    try:
        result = await fetch_shiny_rates_range(
            start_month_date=start_month_date,
            end_month_date=end_month_date,
            area_name=area,
            usernames_csv=username,
            pokemon_id=pokemon_id,
            form=form,
            min_user_n=int(min_user_n or 0),
            limit=int(limit or 0),
            concurrency=int(concurrency or 4),
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"❌ {ve}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Query failed: {e}")

    if resp_fmt == "json":
        return result

    # text fallback
    lines = [
        f"range={result['start_month']}..{result['end_month']}",
        f"area={result['area']}",
        f"rows={result['rows']}",
    ]
    for r in result["data"]:
        lines.append(
            f"{r['pokemon_id']}/{r['form']} "
            f"macro={r['shiny_pct_macro']}% pooled={r['shiny_pct_pooled']}% "
            f"users={r['users_contributing']} n={r['total_encounters']}"
        )
    return "\n".join(lines)



@router.get(
    "/api/sql/get_raid_data",
    tags=["Raid SQL Data"],
    dependencies=dependencies_list
)
async def get_raid_data(
    start_time: str = Query(..., description="ISO or relative (e.g., '10 hours')"),
    end_time: str   = Query(..., description="ISO or 'now' / relative"),
    response_format: str = Query("json", description="json or text"),
    area: str = Query(..., description="Single area name (exactly one; no lists)"),
    gym_id: str = Query("all"), raid_pokemon: str = Query("all"),
    raid_level: str = Query("all"), raid_form: str = Query("all"),
    raid_team: str = Query("all"), raid_costume: str = Query("all"),
    raid_is_exclusive: str = Query("all"), raid_ex_raid_eligible: str = Query("all"),
    limit: Optional[int] = Query(0, description="Optional per-day limit; 0 = no limit."),
    concurrency: Optional[int] = Query(4, description="Max parallel day-queries"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    await secure_api.check_secret_header_value(api_secret_header)

    fmt = (response_format or "json").lower()
    if fmt not in ("json", "text"):
        raise HTTPException(status_code=400, detail="❌ response_format must be json or text.")

    # Area to id - exactly one area
    area_norm = (area or "").strip()
    if area_norm.lower() in ("all", "global") or "," in area_norm:
        raise HTTPException(status_code=400, detail="❌ Provide exactly one area name (no lists, no 'all/global').")
    try:
        area_id = await resolve_area_id_by_name(area_norm)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Unknown area: {area_norm}")

    # Parse times to DATETIME
    try:
        seen_from = parse_time_to_datetime(start_time)
        seen_to   = parse_time_to_datetime(end_time)
        if seen_to < seen_from:
            raise ValueError("end_time before start_time")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time range: {e}")

    # CSVs parsing
    gyms_set               = _parse_csv_param(gym_id)
    raid_pokemon_set       = _parse_csv_param(raid_pokemon)
    raid_form_set          = _parse_csv_param(raid_form)
    raid_level_set         = _parse_csv_param(raid_level)
    raid_team_set          = _parse_csv_param(raid_team)
    raid_costume_set       = _parse_csv_param(raid_costume)
    raid_is_exclusive_set  = _parse_csv_param(raid_is_exclusive)
    raid_ex_eligible_set   = _parse_csv_param(raid_ex_raid_eligible)

    # Convert to proper types
    gyms_list              = sorted(list(gyms_set)) if gyms_set else None
    raid_pokemon_list      = _to_int_list("raid_pokemon", raid_pokemon_set)
    raid_form_list         = _to_int_list("raid_form", raid_form_set)
    raid_level_list        = _to_int_list("raid_level", raid_level_set)
    raid_team_list         = _to_int_list("raid_team", raid_team_set)
    raid_costume_list      = _to_int_list("raid_costume", raid_costume_set)
    raid_is_exclusive_list = _to_int_list("raid_is_exclusive", raid_is_exclusive_set)
    raid_ex_eligible_list  = _to_int_list("raid_ex_raid_eligible", raid_ex_eligible_set)

    # Build filters dataclass
    filters = RaidFilters(
        gyms=gyms_list,
        raid_pokemon=raid_pokemon_list,
        raid_form=raid_form_list,
        raid_level=raid_level_list,
        raid_team=raid_team_list,
        raid_costume=raid_costume_list,
        raid_is_exclusive=raid_is_exclusive_list,
        raid_ex_raid_eligible=raid_ex_eligible_list,
    )

    try:
        result = await fetch_raids_range(
            area_id=int(area_id),
            area_name=area_norm,
            seen_from=seen_from,
            seen_to=seen_to,
            filters=filters,
            limit_per_day=int(limit or 0),
            concurrency=int(concurrency or 4),
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"❌ {ve}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Query failed: {e}")

    if fmt == "json":
        return result

    # Text fallback
    lines = [f"range={result['start_time']}..{result['end_time']} area={result['area']} rows={result['rows']}"]
    for r in result["data"]:
        lines.append(
            f"{r['latitude']},{r['longitude']} -> {r['count']} "
            f"(gym={r['gym']} boss={r['raid_pokemon']}/{r['raid_form']} lvl={r['raid_level']})"
        )
    return "\n".join(lines)



@router.get(
    "/api/sql/get_invasion_data",
    tags=["Invasion SQL Data"],
    dependencies=dependencies_list
)
async def get_invasion_data(
    start_time: str = Query(..., description="ISO or relative (e.g., '10 hours')"),
    end_time: str   = Query(..., description="ISO or 'now' / relative"),
    response_format: str = Query("json", description="json or text"),
    area: str = Query(..., description="Single area name (exactly one; no lists)"),
    pokestop_id: str = Query("all", description="CSV of pokestop ids or 'all'"),
    display_type: str = Query("all", description="CSV of invasion display types or 'all'"),
    character: str = Query("all", description="CSV of invasion characters or 'all'"),
    grunt: str = Query("all", description="CSV of grunt ids or 'all'"),
    confirmed: str = Query("all", description="CSV of 0/1 or 'all'"),
    limit: Optional[int] = Query(0, description="Optional per-day limit; 0 = no limit."),
    concurrency: Optional[int] = Query(4, description="Max parallel day-queries"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    await secure_api.check_secret_header_value(api_secret_header)

    fmt = (response_format or "json").lower()
    if fmt not in ("json", "text"):
        raise HTTPException(status_code=400, detail="❌ response_format must be json or text.")

    # one area only
    area_norm = (area or "").strip()
    if area_norm.lower() in ("all", "global") or "," in area_norm:
        raise HTTPException(status_code=400, detail="❌ Provide exactly one area name (no lists, no 'all/global').")
    try:
        area_id = await resolve_area_id_by_name(area_norm)
    except Exception:
        raise HTTPException(status_code=400, detail=f"❌ Unknown area: {area_norm}")

    # time window to DATETIME
    try:
        seen_from = parse_time_to_datetime(start_time)
        seen_to   = parse_time_to_datetime(end_time)
        if seen_to < seen_from:
            raise ValueError("end_time before start_time")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time range: {e}")

    # CSV params
    pstop_set     = _parse_csv_param(pokestop_id)
    dtype_set     = _parse_csv_param(display_type)
    char_set      = _parse_csv_param(character)
    grunt_set     = _parse_csv_param(grunt)
    confirmed_set = _parse_csv_param(confirmed)

    pokestops_list = sorted(list(pstop_set)) if pstop_set else None  # strings
    dtype_list     = _to_int_list("display_type", dtype_set)
    char_list      = _to_int_list("character", char_set)
    grunt_list     = _to_int_list("grunt", grunt_set)
    confirmed_list = _to_int_list("confirmed", confirmed_set)

    # build filters and run
    filters = InvasionFilters(
        pokestops=pokestops_list,
        display_types=dtype_list,
        characters=char_list,
        grunts=grunt_list,
        confirmed=confirmed_list,
    )

    try:
        result = await fetch_invasions_range(
            area_id=int(area_id),
            area_name=area_norm,
            seen_from=seen_from,
            seen_to=seen_to,
            filters=filters,
            limit_per_day=int(limit or 0),
            concurrency=int(concurrency or 4),
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"❌ {ve}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Query failed: {e}")

    if fmt == "json":
        return result

    # text fallback
    lines = [f"range={result['start_time']}..{result['end_time']} area={result['area']} rows={result['rows']}"]
    for r in result["data"]:
        lines.append(
            f"{r['latitude']},{r['longitude']} -> {r['count']} "
            f"(pokestop={r['pokestop']} type={r['display_type']} char={r['character']})"
        )
    return "\n".join(lines)


@router.get(
    "/api/sql/get_quest_data",
    tags=["Quest SQL Data"],
    dependencies=dependencies_list
)
async def get_quest_data(
    start_time: str = Query(..., description="ISO or relative (e.g., '10 hours')"),
    end_time: str   = Query(..., description="ISO or 'now' / relative"),
    response_format: str = Query("json", description="json or text"),
    area: str = Query(..., description="Single area name (exactly one; no lists)"),
    pokestop_id: str = Query("all", description="CSV of pokestop IDs or 'all'"),
    quest_type: str = Query("all", description="'all' | 'ar' | 'normal'"),
    reward_ar_type: str = Query("all", description="CSV of AR task_types (items) or 'all'"),
    reward_normal_type: str = Query("all", description="CSV of normal task_types (items) or 'all'"),
    reward_ar_item_id: str = Query("all", description="CSV of AR reward item IDs or 'all'"),
    reward_normal_item_id: str = Query("all", description="CSV of normal reward item IDs or 'all'"),
    reward_ar_poke_id: str = Query("all", description="CSV of AR reward Pokémon IDs or 'all'"),
    reward_normal_poke_id: str = Query("all", description="CSV of normal reward Pokémon IDs or 'all'"),
    limit: Optional[int] = Query(0, description="Optional per-day limit; 0 = no limit."),
    concurrency: Optional[int] = Query(4, description="Max parallel day-queries"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param()
):
    await secure_api.check_secret_header_value(api_secret_header)

    fmt = (response_format or "json").lower()
    if fmt not in ("json", "text"):
        raise HTTPException(status_code=400, detail="❌ Invalid response_format. Must be json or text.")

    # area
    area_norm = (area or "").strip()
    if area_norm.lower() in ("all", "global") or "," in area_norm:
        raise HTTPException(status_code=400, detail="❌ Provide exactly one area name (no lists, no 'all/global').")
    try:
        area_id = await resolve_area_id_by_name(area_norm)
    except Exception:
        raise HTTPException(status_code=400, detail=f"❌ Unknown area: {area_norm}")

    # time window
    try:
        seen_from = parse_time_to_datetime(start_time)
        seen_to   = parse_time_to_datetime(end_time)
        if seen_to < seen_from:
            raise ValueError("end_time before start_time")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"❌ Invalid time range: {e}")

    # CSV parser
    ps_set            = _parse_csv_param(pokestop_id)
    r_ar_type_set     = _parse_csv_param(reward_ar_type)
    r_norm_type_set   = _parse_csv_param(reward_normal_type)
    ar_item_set       = _parse_csv_param(reward_ar_item_id)
    normal_item_set   = _parse_csv_param(reward_normal_item_id)
    ar_poke_set       = _parse_csv_param(reward_ar_poke_id)
    normal_poke_set   = _parse_csv_param(reward_normal_poke_id)

    def _to_int_list(name: str, s: Optional[set[str]]) -> Optional[List[int]]:
        if s is None: return None
        try: return sorted({int(x) for x in s})
        except Exception:
            raise HTTPException(status_code=400, detail=f"❌ {name} must be integers (CSV).")

    pokestops_list = sorted(list(ps_set)) if ps_set else None

    # quest_type to allowed modes
    qt = (quest_type or "all").strip().lower()
    if qt not in ("all", "ar", "normal"):
        raise HTTPException(status_code=400, detail="❌ quest_type must be 'all', 'ar', or 'normal'.")
    allowed_modes = None if qt == "all" else ([1] if qt == "ar" else [0])

    # Items filters task_type only provided via reward_*_type
    items_ar_types     = _to_int_list("reward_ar_type", r_ar_type_set)
    items_normal_types = _to_int_list("reward_normal_type", r_norm_type_set)
    items_ar_ids       = _to_int_list("reward_ar_item_id", ar_item_set)
    items_norm_ids     = _to_int_list("reward_normal_item_id", normal_item_set)

    # Pokémon filters no task_type CSV here; only per-mode poke ids
    mons_ar_types      = None
    mons_normal_types  = None
    mons_ar_poke_ids   = _to_int_list("reward_ar_poke_id", ar_poke_set)
    mons_norm_poke_ids = _to_int_list("reward_normal_poke_id", normal_poke_set)

    items_filters = QuestItemFilters(
        pokestops=pokestops_list,
        allowed_modes=allowed_modes,
        ar_task_types=items_ar_types,
        normal_task_types=items_normal_types,
        ar_item_ids=items_ar_ids,
        normal_item_ids=items_norm_ids,
    )
    mon_filters = QuestMonFilters(
        pokestops=pokestops_list,
        allowed_modes=allowed_modes,
        ar_task_types=mons_ar_types,
        normal_task_types=mons_normal_types,
        ar_poke_ids=mons_ar_poke_ids,
        normal_poke_ids=mons_norm_poke_ids,
    )

    try:
        result = await fetch_quests_range(
            area_id=int(area_id),
            area_name=area_norm,
            seen_from=seen_from,
            seen_to=seen_to,
            items_filters=items_filters,
            mon_filters=mon_filters,
            limit_per_day=int(limit or 0),
            concurrency=int(concurrency or 4),
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"❌ {ve}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"❌ Query failed: {e}")

    if fmt == "json":
        return result

    # text fallback
    lines = [
        f"range={result['start_time']}..{result['end_time']} area={result['area']}",
        f"[items] rows={result['items']['rows']}"
    ]
    for r in result["items"]["data"]:
        lines.append(f"{r['latitude']},{r['longitude']} -> {r['count']} "
                     f"(pokestop={r['pokestop']} mode={r['mode']} task_type={r['task_type']})")
    lines.append(f"[pokemon] rows={result['pokemon']['rows']}")
    for r in result["pokemon']['data"]:
        lines.append(f"{r['latitude']},{r['longitude']} -> {r['count']} "
                     f"(pokestop={r['pokestop']} mode={r['mode']} task_type={r['task_type']})")
    return "\n".join(lines)

