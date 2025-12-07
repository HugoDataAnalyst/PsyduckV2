import requests
import pandas as pd
import plotly.graph_objects as go
import config as AppConfig
from urllib.parse import quote
import json
import os
import time
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.logger import logger

API_BASE_URL = AppConfig.api_base_url
ICON_BASE_URL = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon"
ICON_CACHE_DIR = Path(__file__).parent / "assets" / "pokemon_icons"

# Ensure cache directory exists
ICON_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_POKEDEX_FORMS = None
def _load_pokedex_forms():
    """Load and cache the pokedex forms mapping."""
    global _POKEDEX_FORMS
    if _POKEDEX_FORMS is None:
        try:
            pokedex_path = os.path.join(os.path.dirname(__file__), 'assets', 'pokedex.json')
            with open(pokedex_path, 'r') as f:
                forms_by_name = json.load(f)
                # Create reverse mapping: form_id -> form_name
                _POKEDEX_FORMS = {v: k for k, v in forms_by_name.items()}
        except Exception as e:
            logger.info(f"Error loading pokedex.json: {e}")
            _POKEDEX_FORMS = {}
    return _POKEDEX_FORMS

def get_api_headers():
    headers = {}
    if AppConfig.api_secret_key:
        headers["Authorization"] = f"Bearer {AppConfig.api_secret_key}"
    elif AppConfig.api_header_name and AppConfig.api_header_secret:
        headers[AppConfig.api_header_name] = AppConfig.api_header_secret
    return headers

def get_cached_geofences():
    """Fetch geofences from the FastAPI endpoint."""
    try:
        url = f"{API_BASE_URL}/api/redis/get_cached_geofences"
        response = requests.get(url, headers=get_api_headers(), params={"response_format": "json"})
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.info(f"Error fetching geofences: {e}")
    return []

def get_pokemon_stats(endpoint_type="counter", params=None):
    """
    Generic fetcher for pokemon stats.
    endpoint_type: 'counter' or 'timeseries' or 'tth_timeseries' or "sql_heatmap"
    """
    if params is None:
        params = {}

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    if endpoint_type == "counter":
        endpoint = "/api/redis/get_pokemon_counterseries"
    elif endpoint_type == "tth_timeseries":
        endpoint = "/api/redis/get_pokemon_tth_timeseries"
    elif endpoint_type == "sql_heatmap":
        endpoint = "/api/sql/get_pokemon_heatmap_data"
    elif endpoint_type == "sql_shiny_rate":
        endpoint = "/api/sql/get_shiny_rate_data"
    else:
        # Default standard timeseries
        endpoint = "/api/redis/get_pokemon_timeseries"

    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=get_api_headers(), params=params)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                return data.get("data", data)
            return data
    except Exception as e:
        logger.info(f"Error fetching stats: {e}")
    return {}


def get_raids_stats(endpoint_type="counter", params=None):
    """
    Generic fetcher for raids stats.
    endpoint_type: 'counter' or 'timeseries' or 'raid_sql_data'
    """
    if params is None:
        params = {}

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint based on type
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_raids_counterseries"
    elif endpoint_type == "raid_sql_data":
        endpoint = "/api/sql/get_raid_data"
    else:
        # Default standard timeseries
        endpoint = "/api/redis/get_raid_timeseries"

    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=get_api_headers(), params=params)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                return data.get("data", data)
            return data
    except Exception as e:
        logger.info(f"Error fetching raid stats: {e}")
    return {}

def get_invasions_stats(endpoint_type="counter", params=None):
    """
    Generic fetcher for invasions stats.
    endpoint_type: 'counter' or 'timeseries' or 'invasion_sql_data'
    """
    if params is None:
        params = {}

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint based on type
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_invasions_counterseries"
    elif endpoint_type == "invasion_sql_data":
        endpoint = "/api/sql/get_invasion_data"
    else:
        # Default standard timeseries
        endpoint = "/api/redis/get_invasion_timeseries"

    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=get_api_headers(), params=params)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                return data.get("data", data)
            return data
    except Exception as e:
        logger.info(f"Error fetching invasion stats: {e}")
    return {}

def get_quests_stats(endpoint_type="counter", params=None):
    """
    Generic fetcher for quests stats.
    endpoint_type: 'counter' or 'timeseries' or 'quest_sql_data'
    """
    if params is None:
        params = {}

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint based on type
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_quest_counterseries"
    elif endpoint_type == "quest_sql_data":
        endpoint = "/api/sql/get_quest_data"
    else:
        # Default standard timeseries
        endpoint = "/api/redis/get_quest_timeseries"

    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=get_api_headers(), params=params)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict):
                return data.get("data", data)
            return data
    except Exception as e:
        logger.info(f"Error fetching quests stats: {e}")
    return {}

def _download_pokemon_icon(pid, form=0):
    """
    Downloads a single Pokemon icon to local cache.
    Returns True if successful, False otherwise.

    If a form-specific image returns 404, falls back to base {pid}.webp
    """
    try:
        form_int = int(form)

        # Determine filename based on form
        if form_int == 0:
            filename = f"{pid}.webp"
            fallback_filename = None  # No fallback needed for base form
        else:
            # Check if this form is a _NORMAL variant
            pokedex_forms = _load_pokedex_forms()
            form_name = pokedex_forms.get(form_int, "")

            if form_name.endswith("_NORMAL"):
                filename = f"{pid}.webp"
                fallback_filename = None
            else:
                filename = f"{pid}_f{form_int}.webp"
                fallback_filename = f"{pid}.webp"  # Fallback to base image

        local_path = ICON_CACHE_DIR / filename

        # Skip if already cached
        if local_path.exists():
            return True

        # Download from remote
        remote_url = f"{ICON_BASE_URL}/{filename}"
        logger.debug(f"📥 Attempting to download icon: {filename}")
        response = requests.get(remote_url, timeout=10)

        if response.status_code == 200:
            local_path.write_bytes(response.content)
            logger.debug(f"✅ Successfully downloaded icon: {filename}")
            return True
        elif response.status_code == 404 and fallback_filename:
            # Form-specific image not found, try fallback to base image
            logger.debug(f"⚠️ Form icon not found ({filename}), trying fallback: {fallback_filename}")

            fallback_path = ICON_CACHE_DIR / fallback_filename

            # Check if fallback already cached
            if fallback_path.exists():
                logger.debug(f"✅ Fallback icon already cached: {fallback_filename}")
                return True

            # Try downloading fallback
            fallback_url = f"{ICON_BASE_URL}/{fallback_filename}"
            fallback_response = requests.get(fallback_url, timeout=10)

            if fallback_response.status_code == 200:
                fallback_path.write_bytes(fallback_response.content)
                logger.debug(f"✅ Successfully downloaded fallback icon: {fallback_filename}")
                return True
            else:
                logger.warning(f"❌ Fallback icon also not found ({fallback_filename}): HTTP {fallback_response.status_code}")
                return False
        else:
            logger.warning(f"❌ Failed to download icon {filename}: HTTP {response.status_code}")
            return False

    except Exception as e:
        logger.warning(f"❌ Error downloading icon for Pokemon {pid} form {form}: {e}")
        return False

def _get_pokemon_list_from_uicons():
    """
    Extracts Pokemon list from UICONS_index.json AND pokedex.json.
    Returns list of dicts with 'pid' and 'form' keys.

    This ensures we cache ALL forms that might be displayed in the dashboard,
    including forms from pokedex.json that might not be in UICONS_index.json.
    """
    pokemon_dict = {}  # Use dict to deduplicate: key=(pid, form), value={'pid': X, 'form': Y}

    # Step 1: Get forms from UICONS_index.json
    try:
        uicons_path = os.path.join(os.path.dirname(__file__), 'assets', 'UICONS_index.json')
        with open(uicons_path, 'r') as f:
            uicons_data = json.load(f)

        pokemon_icons = uicons_data.get('pokemon', [])

        for icon in pokemon_icons:
            # Icons are in format: "1.webp" or "1_f2.webp"
            filename = icon.replace('.webp', '')

            if '_f' in filename:
                # Form variant: e.g., "1_f2" -> pid=1, form=2
                parts = filename.split('_f')
                pid = int(parts[0])
                form = int(parts[1])
            else:
                # Base form: e.g., "1" -> pid=1, form=0
                pid = int(filename)
                form = 0

            pokemon_dict[(pid, form)] = {'pid': pid, 'form': form}

        logger.info(f"Loaded {len(pokemon_dict)} Pokemon icons from UICONS_index.json")

    except Exception as e:
        logger.warning(f"Error loading Pokemon list from UICONS_index.json: {e}")

    # Step 2: Add forms from pokedex.json that might not be in UICONS
    try:
        pokedex_path = os.path.join(os.path.dirname(__file__), 'assets', 'pokedex.json')
        species_path = os.path.join(os.path.dirname(__file__), 'assets', 'pokedex_id.json')

        # Load species map (name -> pid)
        with open(species_path, 'r') as f:
            species_map = json.load(f)

        # Load forms map (form_name -> form_id)
        with open(pokedex_path, 'r') as f:
            forms_map = json.load(f)

        # Add base forms for all species
        for _, pid in species_map.items():
            pokemon_dict[(pid, 0)] = {'pid': pid, 'form': 0}

        # Add all forms
        for form_name, form_id in forms_map.items():
            if form_id == 0:
                continue

            # Match form to Pokemon ID by finding species name prefix
            matched_pid = None
            for species_name, pid in species_map.items():
                if form_name.startswith(species_name + "_"):
                    if matched_pid is None or len(species_name) > len([n for n, p in species_map.items() if p == matched_pid][0]):
                        matched_pid = pid

            if matched_pid:
                pokemon_dict[(matched_pid, form_id)] = {'pid': matched_pid, 'form': form_id}

        logger.info(f"Total {len(pokemon_dict)} unique Pokemon forms to cache (including pokedex.json forms)")

    except Exception as e:
        logger.warning(f"Error loading forms from pokedex.json: {e}")

    # Fallback: Gen 1-9 base forms if everything failed
    if not pokemon_dict:
        logger.warning("All loading methods failed, using fallback base forms")
        pokemon_dict = {(i, 0): {'pid': i, 'form': 0} for i in range(1, 1026)}

    return list(pokemon_dict.values())

def precache_pokemon_icons(pokemon_list=None, max_workers=10):
    """
    Pre-downloads Pokemon icons to local cache for faster loading.

    Args:
        pokemon_list: List of dicts with 'pid' and 'form' keys. If None, loads from UICONS_index.json.
        max_workers: Number of parallel download threads.

    Returns:
        Tuple of (successful_count, failed_count)
    """
    if pokemon_list is None:
        pokemon_list = _get_pokemon_list_from_uicons()

    successful = 0
    failed = 0
    total = len(pokemon_list)

    logger.info(f"Starting icon pre-cache for {total} Pokemon...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_pokemon = {
            executor.submit(_download_pokemon_icon, p['pid'], p['form']): p
            for p in pokemon_list
        }

        # Process completed downloads
        for future in as_completed(future_to_pokemon):
            if future.result():
                successful += 1
            else:
                failed += 1

            # Log progress every 100 icons
            if (successful + failed) % 100 == 0:
                logger.info(f"Icon cache progress: {successful + failed}/{total} ({successful} succeeded, {failed} failed)")

    logger.info(f"Icon pre-cache complete: {successful}/{total} succeeded, {failed} failed")
    return successful, failed

def get_pokemon_icon_url(pid, form=0):
    """
    Generates the URL for the Pokemon icon, preferring local cache.
    Falls back to remote URL if not cached.

    - If form ends with _NORMAL in pokedex: {pid}.webp
    - Otherwise: {pid}_f{form}.webp
    - If form-specific file doesn't exist locally, falls back to base {pid}.webp
    """
    try:
        form_int = int(form)

        # Determine filename
        if form_int == 0:
            filename = f"{pid}.webp"
            fallback_filename = None
        else:
            # Load pokedex forms and check if this form is a _NORMAL variant
            pokedex_forms = _load_pokedex_forms()
            form_name = pokedex_forms.get(form_int, "")

            if form_name.endswith("_NORMAL"):
                filename = f"{pid}.webp"
                fallback_filename = None
            else:
                filename = f"{pid}_f{form_int}.webp"
                fallback_filename = f"{pid}.webp"

        # Check if cached locally
        local_path = ICON_CACHE_DIR / filename
        if local_path.exists():
            # Return relative path for Dash assets
            return f"/assets/pokemon_icons/{filename}"

        # If form-specific file not found, try fallback to base image
        if fallback_filename:
            fallback_path = ICON_CACHE_DIR / fallback_filename
            if fallback_path.exists():
                return f"/assets/pokemon_icons/{fallback_filename}"

        # Fallback to remote URL (try form-specific first, browser will handle 404)
        # But prefer base image URL if we know form-specific doesn't exist locally
        if fallback_filename:
            # Return base image URL since form-specific likely doesn't exist
            return f"{ICON_BASE_URL}/{fallback_filename}"

        return f"{ICON_BASE_URL}/{filename}"

    except Exception as e:
        # Fallback to base image on any error
        return f"{ICON_BASE_URL}/{pid}.webp"

def create_geofence_figure(geofence_data):
    """
    Creates a map visualization with OpenStreetMap background for geofence.
    Uses Scattermap (not Scattermapbox) to avoid WebGL limits while still showing a proper map.
    """
    if not geofence_data or "coordinates" not in geofence_data:
        return go.Figure()

    name = geofence_data.get("name", "Unknown")

    try:
        coords = geofence_data["coordinates"]

        if coords and isinstance(coords[0], list):
            if isinstance(coords[0][0], (int, float)):
                 ring = coords
            else:
                 ring = coords[0]
        else:
            return go.Figure()

        lons = [float(c[0]) for c in ring]
        lats = [float(c[1]) for c in ring]

        if not lats or not lons:
            return go.Figure()

        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)

        # Calculate zoom level based on bounding box
        lat_range = max(lats) - min(lats)
        lon_range = max(lons) - min(lons)
        max_range = max(lat_range, lon_range)

        if max_range > 1: zoom = 8
        elif max_range > 0.5: zoom = 9
        elif max_range > 0.1: zoom = 10
        elif max_range > 0.05: zoom = 11
        else: zoom = 12

        # Use Scattergeo with OpenStreetMap tiles
        fig = go.Figure()

        # Add the polygon with map background
        fig.add_trace(go.Scattermapbox(
            lon=lons + [lons[0]],
            lat=lats + [lats[0]],
            mode='lines',
            fill='toself',
            fillcolor='rgba(0, 123, 255, 0.3)',
            line=dict(color='#007bff', width=3),
            name=name,
            hoverinfo='name',
            showlegend=False
        ))

        fig.update_layout(
            mapbox=dict(
                style="open-street-map",
                center=dict(lat=center_lat, lon=center_lon),
                zoom=zoom
            ),
            margin={"r":0,"t":0,"l":0,"b":0},
            showlegend=False,
            height=150,
            paper_bgcolor='rgba(0,0,0,0)',
            hovermode='closest'
        )

        return fig
    except Exception as e:
        logger.info(f"Error creating figure for {name}: {e}")
        return go.Figure()

# Global Tasks Functions.
def get_global_pokemon_task(endpoint_type="counter", params=None):
    """
    Hybrid Function:
    1. If params=None: Acts as Background Task (fetches global totals -> aggregates -> returns summary).
    2. If params provided: Acts as Generic Fetcher (fetches endpoint -> returns raw data).
    """

    # Setup Defaults for Background Task if needed
    if params is None:
        params = {
            "counter_type": "totals",
            "area": "global",
            "start_time": "24 hours",
            "end_time": "now",
            "mode": "sum",
            "interval": "hourly",
            "metric": "all",
            "pokemon_id": "all",
            "form_id": "all",
            "response_format": "json"
        }

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine Endpoint
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_pokemon_counterseries"
    elif endpoint_type == "tth_timeseries":
        endpoint = "/api/redis/get_pokemon_tth_timeseries"
    elif endpoint_type == "sql_heatmap":
        endpoint = "/api/sql/get_pokemon_heatmap_data"
    elif endpoint_type == "sql_shiny_rate":
        endpoint = "/api/sql/get_shiny_rate_data"
    else:
        # Default standard timeseries
        endpoint = "/api/redis/get_pokemon_timeseries"

    # Execute Request
    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=get_api_headers(), params=params)

        if response.status_code != 200:
            logger.info(f"Error fetching global pokemon: HTTP {response.status_code}")
            return None

        raw_data = response.json()
        if isinstance(raw_data, dict) and "data" in raw_data:
            raw_data = raw_data.get("data", {})

        if not raw_data:
            return None

        # Perform specific aggregation
        if endpoint_type == "counter":
            aggregated = defaultdict(int)
            for area_name, content in raw_data.items():
                if not isinstance(content, dict): continue
                stats = content.get('data', {})
                if not stats: continue
                for key, value in stats.items():
                    if isinstance(value, (int, float)):
                        aggregated[key] += value

            final_data = dict(aggregated)
            final_data['last_updated'] = time.time()
            return final_data

        # Otherwise, return raw data
        return raw_data

    except Exception as e:
        logger.error(f"Error in get_global_pokemon_task: {e}")
        return None


def get_global_raids_task(endpoint_type="counter", params=None):
    """
    Hybrid Function: Background Task (defaults) OR Generic Fetcher.
    """

    if params is None:
        params = {
            "counter_type": "totals",
            "area": "global",
            "start_time": "24 hours",
            "end_time": "now",
            "mode": "sum",
            "interval": "hourly",
            "raid_pokemon": "all",
            "raid_form": "all",
            "raid_level": "all",
            "response_format": "json"
        }

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_raids_counterseries"
    elif endpoint_type == "raid_sql_data":
        endpoint = "/api/sql/get_raid_data"
    else:
        # Default standard timeseries
        endpoint = "/api/redis/get_raid_timeseries"

    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=get_api_headers(), params=params)

        if response.status_code != 200:
            logger.info(f"Error fetching global raids: HTTP {response.status_code}")
            return None

        raw_data = response.json()
        if isinstance(raw_data, dict) and "data" in raw_data:
            raw_data = raw_data.get("data", {})

        if not raw_data:
            return None

        # Background Task Aggregation
        if endpoint_type == "counter":
            total_raids = 0
            raid_levels_agg = defaultdict(int)

            for area_name, content in raw_data.items():
                if not isinstance(content, dict): continue
                stats = content.get('data', {})
                if not stats: continue

                total_raids += stats.get('total', 0)
                raid_levels = stats.get('raid_level', {})
                if isinstance(raid_levels, dict):
                    for level, count in raid_levels.items():
                        raid_levels_agg[str(level)] += count

            final_data = {
                "total": total_raids,
                "raid_level": dict(raid_levels_agg),
                "last_updated": time.time()
            }
            return final_data

        # Generic Return
        return raw_data

    except Exception as e:
        logger.error(f"Error in get_global_raids_task: {e}")
        return None


def get_global_invasions_task(endpoint_type="counter", params=None):
    """
    Hybrid Function: Background Task (defaults) OR Generic Fetcher.
    """

    if params is None:
        params = {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "24 hours",
            "end_time": "now",
            "mode": "sum",
            "response_format": "json",
            "area": "global",
            "display_type": "all",
            "character": "all",
            "grunt": "all",
            "confirmed": "all"
        }

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_invasions_counterseries"
    elif endpoint_type == "invasion_sql_data":
        endpoint = "/api/sql/get_invasion_data"
    else:
        # Default standard timeseries
        endpoint = "/api/redis/get_invasion_timeseries"

    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=get_api_headers(), params=params)

        if response.status_code != 200:
            logger.info(f"Error fetching global invasions: HTTP {response.status_code}")
            return None

        raw_data = response.json()
        if isinstance(raw_data, dict) and "data" in raw_data:
            raw_data = raw_data.get("data", {})

        if not raw_data:
            return None

        # Background Task Aggregation
        if endpoint_type == "counter":
            total_invasions = 0
            confirmed_count = 0
            unconfirmed_count = 0

            for area_name, content in raw_data.items():
                if not isinstance(content, dict): continue
                stats = content.get('data', {})
                if not stats: continue

                total_invasions += stats.get('total', 0)
                confirmed_data = stats.get('confirmed', {})
                if isinstance(confirmed_data, dict):
                    confirmed_count += confirmed_data.get('1', 0)
                    unconfirmed_count += confirmed_data.get('0', 0)

            final_data = {
                "total": total_invasions,
                "stats": {
                    "confirmed": confirmed_count,
                    "unconfirmed": unconfirmed_count
                },
                "last_updated": time.time()
            }
            return final_data

        # Generic Return
        return raw_data

    except Exception as e:
        logger.error(f"Error in get_global_invasions_task: {e}")
        return None

def get_global_pokestops_task(params=None):
    if params is None:
        params = {
            "area": "global",
            "response_format": "json"
        }

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    endpoint = "/api/redis/get_cached_pokestops"

    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=get_api_headers(), params=params)

        if response.status_code != 200:
            logger.info(f"Error fetching global pokestops: HTTP {response.status_code}")
            return None

        raw_data = response.json()

        # Extract the inner 'data' which contains 'areas' and 'total'
        if isinstance(raw_data, dict) and "data" in raw_data:
            final_data = raw_data.get("data", {})
        else:
            return None

        if not final_data:
            return None

        # Add timestamp to the existing structure
        final_data["last_updated"] = time.time()

        return final_data

    except Exception as e:
        logger.error(f"Error in get_global_pokestops_task: {e}")
        return None

def get_global_quests_task(endpoint_type="counter", params=None):
    """
    Hybrid Function: Background Task (defaults) OR Generic Fetcher.
    """

    if params is None:
        params = {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "24 hours",
            "end_time": "now",
            "mode": "sum",
            "response_format": "json",
            "area": "global",
            "with_ar": "all", "ar_type": "all", "reward_ar_type": "all",
            "reward_ar_item_id": "all", "reward_ar_item_amount": "all",
            "reward_ar_poke_id": "all", "reward_ar_poke_form": "all",
            "normal_type": "all", "reward_normal_type": "all",
            "reward_normal_item_id": "all", "reward_normal_item_amount": "all",
            "reward_normal_poke_id": "all", "reward_normal_poke_form": "all"
        }

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_quest_counterseries"
    elif endpoint_type == "quest_sql_data":
        endpoint = "/api/sql/get_quest_data"
    else:
        # Default standard timeseries
        endpoint = "/api/redis/get_quest_timeseries"

    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=get_api_headers(), params=params)

        if response.status_code != 200:
            logger.info(f"Error fetching global quests: HTTP {response.status_code}")
            return None

        raw_data = response.json()
        if isinstance(raw_data, dict) and "data" in raw_data:
            raw_data = raw_data.get("data", {})

        if not raw_data:
            return None

# Background Task Aggregation
        if endpoint_type == "counter":
            total_ar_quests = 0
            total_normal_quests = 0

            # 1. Calculate Quest Stats from API response
            for area_name, content in raw_data.items():
                if not isinstance(content, dict): continue
                stats = content.get('data', {})
                if not stats: continue

                # Note: We skip 'total' here as we want the pokestop count from the file
                quest_modes = stats.get('quest_mode', {})
                if isinstance(quest_modes, dict):
                    total_ar_quests += quest_modes.get('ar', 0)
                    total_normal_quests += quest_modes.get('normal', 0)

            # 2. Retrieve Total Pokestops from cached file
            total_pokestops = 0
            try:
                with open('dashboard/data/global_pokestops.json', 'r') as f:
                    cached_stops_data = json.load(f)
                    # Support both direct 'total' or nested structure if file format varies
                    total_pokestops = cached_stops_data.get('total', 0)
            except FileNotFoundError:
                logger.warning("global_pokestops.json not found, defaulting stops to 0")
            except Exception as e:
                logger.error(f"Error reading global_pokestops.json: {e}")

            final_data = {
                "total_pokestops": total_pokestops,
                "quests": {
                    "ar": total_ar_quests,
                    "normal": total_normal_quests
                },
                "last_updated": time.time()
            }
            return final_data

        # Generic Return
        return raw_data

    except Exception as e:
        logger.error(f"Error in get_global_quests_task: {e}")
        return None
