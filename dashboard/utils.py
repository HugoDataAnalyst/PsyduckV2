import requests
import pandas as pd
import plotly.graph_objects as go
import config as AppConfig
from urllib.parse import quote
import json
import os
from pathlib import Path
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
    endpoint_type: 'counter' or 'timeseries'
    """
    if params is None:
            params = {}

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint based on type
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_raids_counterseries"
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

def get_raids_stats(endpoint_type="counter", params=None):
    """
    Generic fetcher for raids stats.
    endpoint_type: 'counter' or 'timeseries'
    """
    if params is None:
            params = {}

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint based on type
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_raids_counterseries"
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
    endpoint_type: 'counter' or 'timeseries'
    """
    if params is None:
            params = {}

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint based on type
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_invasions_counterseries"
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
    endpoint_type: 'counter' or 'timeseries'
    """
    if params is None:
            params = {}

    if "area" not in params: params["area"] = "global"
    if "response_format" not in params: params["response_format"] = "json"

    # Determine endpoint based on type
    if endpoint_type == "counter":
        endpoint = "/api/redis/get_quest_counterseries"
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
    """
    try:
        form_int = int(form)

        # Determine filename based on form
        if form_int == 0:
            filename = f"{pid}.webp"
        else:
            # Check if this form is a _NORMAL variant
            pokedex_forms = _load_pokedex_forms()
            form_name = pokedex_forms.get(form_int, "")

            if form_name.endswith("_NORMAL"):
                filename = f"{pid}.webp"
            else:
                filename = f"{pid}_f{form_int}.webp"

        local_path = ICON_CACHE_DIR / filename

        # Skip if already cached
        if local_path.exists():
            return True

        # Download from remote
        remote_url = f"{ICON_BASE_URL}/{filename}"
        response = requests.get(remote_url, timeout=10)

        if response.status_code == 200:
            local_path.write_bytes(response.content)
            return True
        else:
            logger.warning(f"Failed to download icon {filename}: HTTP {response.status_code}")
            return False

    except Exception as e:
        logger.warning(f"Error downloading icon for Pokemon {pid} form {form}: {e}")
        return False

def _get_pokemon_list_from_uicons():
    """
    Extracts Pokemon list from UICONS_index.json AND pokedex.json.
    Returns list of dicts with 'pid' and 'form' keys.

    This ensures we cache ALL forms that might be displayed in the dashboard,
    including forms from pokedex.json that might not be in UICONS_index.json.
    """
    pokemon_dict = {}  # Use dict to deduplicate: key=(pid, form), value={'pid': X, 'form': Y}

    # Get forms from UICONS_index.json
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

    # Add forms from pokedex.json that might not be in UICONS
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
    """
    try:
        form_int = int(form)

        # Determine filename
        if form_int == 0:
            filename = f"{pid}.webp"
        else:
            # Load pokedex forms and check if this form is a _NORMAL variant
            pokedex_forms = _load_pokedex_forms()
            form_name = pokedex_forms.get(form_int, "")

            if form_name.endswith("_NORMAL"):
                filename = f"{pid}.webp"
            else:
                filename = f"{pid}_f{form_int}.webp"

        # Check if cached locally
        local_path = ICON_CACHE_DIR / filename
        if local_path.exists():
            # Return relative path for Dash assets
            return f"/assets/pokemon_icons/{filename}"

        # Fallback to remote URL
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
