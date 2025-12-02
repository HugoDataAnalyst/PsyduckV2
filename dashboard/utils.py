import requests
import pandas as pd
import plotly.graph_objects as go
import config as AppConfig
from urllib.parse import quote
import json
import os
from utils.logger import logger

API_BASE_URL = AppConfig.api_base_url
ICON_BASE_URL = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main/pokemon"

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

def get_pokemon_icon_url(pid, form=0):
    """
    Generates the URL for the Pokemon icon using pokedex form mapping.
    - If form ends with _NORMAL in pokedex: {pid}.webp
    - Otherwise: {pid}_f{form}.webp
    """
    try:
        form_int = int(form)

        # Special case: form 0 is always the base form
        if form_int == 0:
            return f"{ICON_BASE_URL}/{pid}.webp"

        # Load pokedex forms and check if this form is a _NORMAL variant
        pokedex_forms = _load_pokedex_forms()
        form_name = pokedex_forms.get(form_int, "")

        # If form name ends with _NORMAL, use base image without form suffix
        if form_name.endswith("_NORMAL"):
            return f"{ICON_BASE_URL}/{pid}.webp"

        # Otherwise use the form-specific image
        return f"{ICON_BASE_URL}/{pid}_f{form_int}.webp"
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
