import dash
from dash import html, dcc, callback, Input, Output, State, ALL, ctx, MATCH, ClientsideFunction
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, date
from dashboard.utils import get_cached_geofences, get_raids_stats, get_pokemon_icon_url, get_raids_daily_timeseries, REMOTE_ROOT_URL
from utils.logger import logger
import config as AppConfig
import json
import re
import os
from dashboard.translations.manager import translate, translate_pokemon

dash.register_page(__name__, path='/raids', title='Raid Analytics')

_SPECIES_MAP = None
_FORM_MAP = None

def safe_int(value):
    """Safely converts a value to int, handling 'None' strings and NoneType."""
    if value is None: return 0
    if isinstance(value, str):
        if value.lower() == "none" or value == "": return 0
        try: return int(float(value))
        except: return 0
    if isinstance(value, (int, float)): return int(value)
    return 0

def _get_species_map():
    """Loads pokedex_id.json: ID -> Species Name (e.g. 1 -> BULBASAUR)"""
    global _SPECIES_MAP
    if _SPECIES_MAP is None:
        try:
            path = os.path.join(os.path.dirname(__file__), '..', 'assets', 'pogo_mapping', 'pokemons', 'pokedex_id.json')
            if not os.path.exists(path): path = os.path.join(os.getcwd(), 'assets', 'pogo_mapping', 'pokemons', 'pokedex_id.json')
            with open(path, 'r') as f:
                data = json.load(f)
                _SPECIES_MAP = {v: k.replace("_", " ").title() for k, v in data.items()}
        except Exception as e:
            logger.info(f"Error loading pokedex_id.json: {e}")
            _SPECIES_MAP = {}
    return _SPECIES_MAP

def _get_form_map():
    """Loads pokedex.json: Form ID -> Form Name (e.g. 46 -> RATTATA_ALOLA)"""
    global _FORM_MAP
    if _FORM_MAP is None:
        try:
            path = os.path.join(os.path.dirname(__file__), '..', 'assets', 'pogo_mapping', 'pokemons', 'pokedex.json')
            if not os.path.exists(path): path = os.path.join(os.getcwd(), 'assets', 'pogo_mapping', 'pokemons', 'pokedex.json')
            with open(path, 'r') as f:
                data = json.load(f)
                _FORM_MAP = {v: k.replace("_", " ").title() for k, v in data.items()}
        except Exception as e:
            logger.info(f"Error loading pokedex.json: {e}")
            _FORM_MAP = {}
    return _FORM_MAP

def resolve_pokemon_name(pid, form_id, lang="en"):
    """
    Returns the Pokemon name with form suffix if applicable.
    Uses translation system for localized Pokemon names.
    """
    species_map = _get_species_map()
    form_map = _get_form_map()

    pid = safe_int(pid)
    form_id = safe_int(form_id)

    # Get translated species name
    base_name = translate_pokemon(pid, lang)

    # If no form or form is 0 or Unset, just return species
    if form_id <= 0:
        return base_name

    # Try to find specific form name from form_map
    form_name_full = form_map.get(form_id)

    if form_name_full:
        # If the specific form mapping is just the Normal version, ignore the suffix
        # unless it is Alola/Galar/Hisui/etc
        if "Normal" in form_name_full and not any(x in form_name_full for x in ["Alola", "Galar", "Hisui"]):
             return base_name
        # Extract form suffix from the full form name and append to translated base name
        for species_name in species_map.values():
            species_upper = species_name.upper().replace(" ", "_")
            if form_name_full.upper().replace(" ", "_").startswith(species_upper + "_"):
                form_suffix = form_name_full[len(species_name):].strip()
                if form_suffix.startswith("_"):
                    form_suffix = form_suffix[1:]
                form_suffix = form_suffix.replace("_", " ").title()
                return f"{base_name} ({form_suffix})" if form_suffix else base_name
        return form_name_full

    return base_name

# Raid Info Colors/Icons
def get_raid_info(level_str, lang="en"):
    lvl = str(level_str)
    color = "#e0e0e0" # default
    label = f"{translate('Level', lang)} {lvl}"
    file_suffix = lvl

    # Standard levels
    if lvl == "1": color = "#e0e0e0" # Gray/White
    elif lvl == "3": color = "#f0ad4e" # Orange
    elif lvl == "5": color = "#dc3545" # Red (Legendary)
    elif lvl == "6": label, color = translate("Mega", lang), "#a020f0" # Purple
    elif lvl == "7": label, color = translate("Mega 5", lang), "#7fce83" # Greenish
    elif lvl == "8": label, color = translate("Ultra Beast", lang), "#e881f1" # Pinkish
    elif lvl == "9": label, color = translate("Extended Egg", lang), "#ce2c2c" # Dark Red
    elif lvl == "10": label, color = translate("Primal", lang), "#ad5b2c" # Brown/Orange
    elif lvl == "11": label, color = translate("Shadow Level 1", lang), "#0a0a0a"
    elif lvl == "12": label, color = translate("Shadow Level 2", lang), "#0a0a0a"
    elif lvl == "13": label, color = translate("Shadow Level 3", lang), "#0a0a0a"
    elif lvl == "14": label, color = translate("Shadow Level 4", lang), "#0a0a0a"
    elif lvl == "15": label, color = translate("Shadow Level 5", lang), "#0a0a0a"

# Extra Attributes
    lvl_lower = lvl.lower()
    if "costume" in lvl_lower:
        icon_url = f"{REMOTE_ROOT_URL}/reward/avatar_clothing/0.webp"
        color = "#f8f9fa"
        label = translate("Costume", lang)
    elif "exclusive" in lvl_lower:
        icon_url = f"{REMOTE_ROOT_URL}/misc/sponsor.webp"
        color = "#198754"
        label = translate("Exclusive", lang)
    elif "ex eligible" in lvl_lower:
        icon_url = f"{REMOTE_ROOT_URL}/misc/ex.webp"
        color = "#0d6efd"
        label = translate("EX Eligible", lang)
    else:
        # Fallback for standard levels - Egg Image
        icon_url = f"{REMOTE_ROOT_URL}/raid/egg/{file_suffix}.webp"

    return label, color, icon_url

# Layout

def generate_area_cards(geofences, selected_area_name, lang="en"):
    cards = []
    for idx, geo in enumerate(geofences):
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', geo['name'])
        is_selected = (selected_area_name == geo['name'])

        map_children = [html.Div("✓ " + translate("Selected", lang), style={'position': 'absolute', 'top': '10px', 'right': '10px', 'backgroundColor': '#28a745', 'color': 'white', 'padding': '4px 8px', 'borderRadius': '4px', 'fontWeight': 'bold', 'zIndex': '1000'})] if is_selected else []

        card = dbc.Card([
            html.Div(map_children, id=f"raids-area-map-{safe_name}", **{'data-map-geofence': json.dumps(geo)}, style={'height': '150px', 'backgroundColor': '#1a1a1a', 'position': 'relative'}),
            dbc.CardBody([
                html.H5(geo['name'], className="card-title text-truncate", style={'color': '#28a745' if is_selected else 'inherit'}),
                dbc.Button("✓ " + translate("Selected", lang) if is_selected else translate("Select", lang), href=f"/raids?area={geo['name']}", color="success" if is_selected else "primary", size="sm", className="w-100", disabled=is_selected)
            ])
        ], style={"width": "14rem", "margin": "10px", "border": f"3px solid {'#28a745' if is_selected else 'transparent'}"}, className="shadow-sm")

        if is_selected: card.id = "selected-area-card"
        cards.append(card)
    return cards if cards else html.Div(translate("No areas match your search.", lang), className="text-center text-muted my-4")

def layout(area=None, **kwargs):
    geofences = get_cached_geofences() or []
    initial_cards = generate_area_cards(geofences, area, "en")
    area_options = [{"label": g["name"], "value": g["name"]} for g in geofences]
    area_label = area if area else "No Area Selected"

    return dbc.Container([
        dcc.Store(id="raids-raw-data-store"),
        dcc.Store(id="raids-table-sort-store", data={"col": "total", "dir": "desc"}),
        dcc.Store(id="raids-table-page-store", data={"current_page": 1, "rows_per_page": 25}),
        dcc.Store(id="raids-total-pages-store", data=1),
        dcc.Store(id="raids-clientside-dummy-store"),
        dcc.Dropdown(id="raids-area-selector", options=area_options, value=area, style={'display': 'none'}),
        dcc.Store(id="raids-mode-persistence-store", storage_type="local"),
        dcc.Store(id="raids-source-persistence-store", storage_type="local"),
        dcc.Store(id="raids-combined-source-store", data="live"),
        dcc.Store(id="raids-heatmap-data-store", data=[]),
        dcc.Store(id="raids-heatmap-mode-store", data="markers"),
        dcc.Store(id="raids-heatmap-hidden-pokemon", data=[]),

        # Header
        dbc.Row([
            dbc.Col(html.H2("Raid Analytics", id="raids-page-title", className="text-white"), width=12, className="my-4"),
        ]),

        # Notification Area
        html.Div(id="raids-notification-area"),

        # Main Control Card
        dbc.Card([
            dbc.CardHeader("⚙️ Analysis Settings", id="raids-settings-header", className="fw-bold"),
            dbc.CardBody([
                dbc.Row([
                    # Area Selection
                    dbc.Col([
                        dbc.Label("Selected Area", id="raids-label-selected-area", className="fw-bold"),
                        dbc.InputGroup([
                            dbc.InputGroupText("🗺️"),
                            dbc.Input(id="raids-selected-area-display", value=area_label, disabled=True, style={"backgroundColor": "#fff", "color": "#333", "fontWeight": "bold"}),
                            dbc.Button("Change", id="raids-open-area-modal", color="primary")
                        ], className="mb-3")
                    ], width=12, md=6),

                    # Data Source
                    dbc.Col([
                        dbc.Label("Data Source", id="raids-label-data-source", className="fw-bold"),
                        html.Div([
                            # Row 1: Stats (Live & Historical & TimeSeries)
                            html.Div([
                                html.Span("Stats: ", id="raids-label-stats", className="text-muted small me-2", style={"minWidth": "45px"}),
                                dbc.RadioItems(
                                    id="raids-data-source-selector",
                                    options=[
                                        {"label": "Live", "value": "live"},
                                        {"label": "Historical", "value": "historical"},
                                        {"label": "TimeSeries", "value": "timeseries"},
                                    ],
                                    value="live", inline=True, inputClassName="btn-check",
                                    labelClassName="btn btn-outline-info btn-sm",
                                    labelCheckedClassName="active"
                                ),
                            ], className="d-flex align-items-center mb-1"),
                            # Row 2: SQL Sources (Heatmap)
                            html.Div([
                                html.Span("SQL: ", id="raids-label-sql", className="text-muted small me-2", style={"minWidth": "45px"}),
                                dbc.RadioItems(
                                    id="raids-data-source-sql-selector",
                                    options=[
                                        {"label": "Heatmap", "value": "sql_heatmap"},
                                    ],
                                    value=None, inline=True, inputClassName="btn-check",
                                    labelClassName="btn btn-outline-success btn-sm",
                                    labelCheckedClassName="active"
                                ),
                            ], className="d-flex align-items-center"),
                        ], className="mb-3")
                    ], width=12, md=6)
                ], className="g-3"),

                html.Hr(className="my-3"),

                # Controls Row
                dbc.Row([
                    # Time Control
                    dbc.Col([
                        html.Div(id="raids-live-controls", children=[
                            dbc.Label("📅 Time Window (Hours)", id="raids-label-time-window"),
                            dbc.InputGroup([
                                dbc.Input(id="raids-live-time-input", type="number", min=1, max=72, value=1),
                                dbc.InputGroupText("hours")
                            ])
                        ]),
                        html.Div(id="raids-historical-controls", style={"display": "none"}, children=[
                            dbc.Label("📅 Date Range", id="raids-label-date-range"),
                            dcc.DatePickerRange(id="raids-historical-date-picker", start_date=date.today(), end_date=date.today(), className="d-block w-100", persistence=True, persistence_type="local")
                        ])
                    ], width=6, md=3),

                    # Interval
                    dbc.Col([
                        html.Div(id="raids-interval-control-container", style={"display": "none"}, children=[
                            dbc.Label("⏱️ Interval", id="raids-label-interval"),
                            dcc.Dropdown(id="raids-interval-selector", options=[{"label": "Hourly", "value": "hourly"}], value="hourly", clearable=False, className="text-dark")
                        ])
                    ], width=6, md=3),

                    # Mode
                    dbc.Col([
                        dbc.Label("📊 View Mode", id="raids-label-view-mode"),
                        dcc.Dropdown(
                            id="raids-mode-selector",
                            options=[],
                            value=None,
                            clearable=False,
                            className="text-dark"
                        )
                    ], width=6, md=3),

                    # Actions
                    dbc.Col([
                        dbc.Label("Actions", id="raids-label-actions", style={"visibility": "hidden"}),
                        dbc.Button("Run Analysis", id="raids-submit-btn", color="success", className="w-100 fw-bold")
                    ], width=6, md=3)
                ], className="align-items-end g-3"),

                # Heatmap Display Mode only visible for heatmap
                html.Div(id="raids-heatmap-filters-container", style={"display": "none"}, children=[
                    html.Hr(className="my-3"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("🗺️ Heatmap Display Mode", id="raids-label-heatmap-display-mode", className="fw-bold"),
                            dbc.RadioItems(
                                id="raids-heatmap-display-mode",
                                options=[
                                    {"label": "Markers (Gyms)", "value": "markers"},
                                    {"label": "Density Heatmap", "value": "density"},
                                    {"label": "Grid Overlay", "value": "grid"}
                                ],
                                value="markers",
                                inline=True,
                                inputClassName="btn-check",
                                labelClassName="btn btn-outline-primary btn-sm",
                                labelCheckedClassName="active"
                            )
                        ], width=12, md=6),
                    ], className="g-3")
                ])
            ])
        ], className="shadow-sm border-0 mb-4"),

        # Area Selection Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Select an Area", id="raids-modal-title-area")),
            dbc.ModalBody([
                html.Div(
                    dbc.Input(id="raids-area-filter-input", placeholder="Filter areas by name...", className="mb-3", autoFocus=True),
                    style={"position": "sticky", "top": "-16px", "zIndex": "1020", "backgroundColor": "var(--bs-modal-bg, #fff)", "paddingTop": "16px", "paddingBottom": "10px", "marginBottom": "10px", "borderBottom": "1px solid #dee2e6"}
                ),
                html.Div(initial_cards, id="raids-area-cards-container", className="d-flex flex-wrap justify-content-center")
            ]),
            dbc.ModalFooter(dbc.Button("Close", id="raids-close-area-modal", className="ms-auto"))
        ], id="raids-area-modal", size="xl", scrollable=True),

        # Results Container
        html.Div(id="raids-stats-container", style={"display": "none"}, children=[
            dbc.Row([
                # Sidebar
                dbc.Col(dbc.Card([
                    dbc.CardHeader("📈 Total Counts", id="raids-card-header-total-counts"),
                    dbc.CardBody(
                        dcc.Loading(html.Div(id="raids-total-counts-display"))
                    )
                ], className="shadow-sm border-0 h-100"), width=12, lg=4, className="mb-4"),

                # Activity Data
                dbc.Col(dbc.Card([
                    dbc.CardHeader("📋 Activity Data", id="raids-card-header-activity"),
                    dbc.CardBody([
                        # Search Input: debounce=False for fluid search, outside Loading
                        dcc.Input(
                            id="raids-search-input",
                            type="text",
                            placeholder="🔍 Search Bosses...",
                            debounce=False,  # Fluid search
                            className="form-control mb-3",
                            style={"display": "none"}
                        ),
                        dcc.Loading(html.Div(id="raids-main-visual-container"))
                    ])
                ], className="shadow-sm border-0 h-100"), width=12, lg=8, className="mb-4"),
            ]),

            dbc.Row([dbc.Col(dbc.Card([
                dbc.CardHeader("🛠️ Raw Data Inspector", id="raids-card-header-raw"),
                dbc.CardBody(html.Pre(id="raids-raw-data-display", style={"maxHeight": "300px", "overflow": "scroll"}))
            ], className="shadow-sm border-0"), width=12)])
        ]),

        # Heatmap Container separate from stats container
        html.Div(id="raids-heatmap-container", style={"display": "none"}, children=[
            dbc.Row([
                # Left Column - Quick Filter
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            dbc.Row([
                                dbc.Col([
                                    html.Span("🎯 Raid Boss Filter", id="raids-card-header-quick-filter", className="me-2"),
                                    html.Span(id="raids-quick-filter-count", className="text-muted small")
                                ], width="auto", className="d-flex align-items-center"),
                                dbc.Col([
                                    dbc.ButtonGroup([
                                        dbc.Button("All", id="raids-quick-filter-show-all", title="Show All", size="sm", color="success", outline=True),
                                        dbc.Button("None", id="raids-quick-filter-hide-all", title="Hide All", size="sm", color="danger", outline=True),
                                    ], size="sm")
                                ], width="auto")
                            ], className="align-items-center justify-content-between g-0")
                        ]),
                        dbc.CardBody([
                            # Fluid Search for Quick Filter already outside loading
                            dbc.Input(id="raids-quick-filter-search", placeholder="Search Pokemon...", size="sm", className="mb-2"),
                            html.P(id="raids-quick-filter-instructions", children="Click to hide/show Pokémon from map", className="text-muted small mb-2"),
                            html.Div(id="raids-quick-filter-grid",
                                     style={"display": "flex", "flexWrap": "wrap", "gap": "4px", "justifyContent": "center", "maxHeight": "500px", "overflowY": "auto"})
                        ])
                    ], className="shadow-sm border-0 h-100")
                ], width=12, lg=3, className="mb-4"),

                # Right Column - Map
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Span("🗺️ Raid Heatmap", id="raids-card-header-heatmap", className="fw-bold"),
                            html.Span(id="raids-heatmap-stats", className="ms-3 text-muted small")
                        ]),
                        dbc.CardBody([
                            html.Div(id="raids-heatmap-map-container", style={"height": "600px", "backgroundColor": "#1a1a1a"})
                        ])
                    ], className="shadow-sm border-0 h-100")
                ], width=12, lg=9, className="mb-4")
            ])
        ])
    ])

# 0. Static Translation Callback
@callback(
    [Output("raids-page-title", "children"), Output("raids-settings-header", "children"),
     Output("raids-label-selected-area", "children"), Output("raids-open-area-modal", "children"),
     Output("raids-label-data-source", "children"), Output("raids-label-stats", "children"),
     Output("raids-label-sql", "children"), Output("raids-label-time-window", "children"),
     Output("raids-label-date-range", "children"), Output("raids-label-interval", "children"),
     Output("raids-label-view-mode", "children"), Output("raids-label-actions", "children"),
     Output("raids-submit-btn", "children"), Output("raids-label-heatmap-display-mode", "children"),
     Output("raids-modal-title-area", "children"), Output("raids-close-area-modal", "children"),
     Output("raids-card-header-total-counts", "children"), Output("raids-card-header-activity", "children"),
     Output("raids-card-header-raw", "children"), Output("raids-card-header-quick-filter", "children"),
     Output("raids-card-header-heatmap", "children"), Output("raids-selected-area-display", "value"),
     Output("raids-area-filter-input", "placeholder"), Output("raids-quick-filter-search", "placeholder"),
     Output("raids-quick-filter-show-all", "children"), Output("raids-quick-filter-show-all", "title"),
     Output("raids-quick-filter-hide-all", "children"), Output("raids-quick-filter-hide-all", "title"),
     Output("raids-quick-filter-instructions", "children")],
    [Input("language-store", "data"),  Input("raids-area-selector", "value")],
)
def update_static_translations(lang, current_area):
    lang = lang or "en"

    if current_area:
        area_text = current_area
    else:
        area_text = translate("No Area Selected", lang)

    return (
        translate("Raid Analytics", lang),
        translate("Analysis Settings", lang),
        translate("Selected Area", lang), translate("Change", lang),
        translate("Data Source", lang), translate("Stats", lang),
        translate("SQL", lang),
        translate("Time Window", lang), translate("Date Range", lang),
        translate("Interval", lang), translate("View Mode", lang),
        translate("Actions", lang), translate("Run Analysis", lang),
        translate("Heatmap Display Mode", lang),
        translate("Select an Area", lang), translate("Close", lang),
        translate("Total Counts", lang), translate("Activity Data", lang),
        translate("Raw Data Inspector", lang), translate("Raid Boss Filter", lang), translate("Raid Heatmap", lang),
        area_text,
        translate("Filter areas by name...", lang),
        translate("Search Pokemon...", lang),
        translate("All", lang), translate("Show All", lang),
        translate("None", lang), translate("Hide All", lang),
        translate("Click to hide/show Pokémon from map", lang)
    )

# Parsing Logic

def parse_data_to_df(data, mode, source, lang="en"):
    records = []

    # Parse nested dictionaries like raid_level, raid_costume
    def flatten_nested_dict(key_name, nested_data):
        if isinstance(nested_data, dict):
            for k, v in nested_data.items():
                if isinstance(v, (int, float)):
                    records.append({"metric": str(k), "count": v, "pid": "All", "form": "All", "key": "All", "time_bucket": "Total"})

    if mode == "sum":
        if isinstance(data, dict):
            if "total" in data:
                records.append({"metric": "total", "count": data["total"], "pid": "All", "form": "All", "key": "All", "time_bucket": "Total"})
            if "raid_level" in data:
                flatten_nested_dict("level", data["raid_level"])
            if "raid_costume" in data:
                costume_data = data["raid_costume"]
                if "1" in costume_data:
                    records.append({"metric": "Costume", "count": costume_data["1"], "pid": "All", "form": "All", "key": "All", "time_bucket": "Total"})
            if "raid_is_exclusive" in data:
                exclusive_data = data["raid_is_exclusive"]
                if "1" in exclusive_data:
                    records.append({"metric": "Exclusive", "count": exclusive_data["1"], "pid": "All", "form": "All", "key": "All", "time_bucket": "Total"})
            if "raid_ex_eligible" in data:
                ex_data = data["raid_ex_eligible"]
                if "1" in ex_data:
                    records.append({"metric": "EX Eligible", "count": ex_data["1"], "pid": "All", "form": "All", "key": "All", "time_bucket": "Total"})

    elif (mode == "surged") or ("historical" in source and mode != "grouped"):
        if isinstance(data, dict):
            for time_key, content in data.items():
                h_val = time_key
                if "hour" in str(time_key):
                    try: h_val = int(time_key.replace("hour ", ""))
                    except: pass

                if isinstance(content, dict):
                    # Check for nested raid_level
                    if "raid_level" in content and isinstance(content["raid_level"], dict):
                        for lvl, count in content["raid_level"].items():
                            records.append({"metric": str(lvl), "time_bucket": h_val, "count": count, "pid": "All", "form": "All", "key": "All"})
                    # Check for flattened keys
                    else:
                        for k, v in content.items():
                            if isinstance(v, (int, float)) and k != "total":
                                records.append({"metric": str(k), "time_bucket": h_val, "count": v, "pid": "All", "form": "All", "key": "All"})

    elif mode == "grouped":
        if isinstance(data, dict) and "data" in data:
            data = data['data']

        if isinstance(data, dict):
            poke_data = data.get("raid_pokemon+raid_form", {})
            if poke_data and isinstance(poke_data, dict):
                for key_str, count in poke_data.items():
                    if not isinstance(count, (int, float)): continue
                    parts = key_str.split(":")
                    if len(parts) >= 2:
                        pid, form = parts[0], parts[1]
                        # Resolve Name here for search filtering using Maps
                        name = resolve_pokemon_name(pid, form, lang)
                        records.append({"metric": "count", "pid": int(pid), "form": int(form), "key": name, "count": count, "time_bucket": "Total"})

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=["metric", "count", "pid", "form", "key", "time_bucket"])
    return df

# Callbacks

# Callback to combine both data source selectors into one value
@callback(
    [Output("raids-combined-source-store", "data", allow_duplicate=True),
     Output("raids-data-source-selector", "value", allow_duplicate=True),
     Output("raids-data-source-sql-selector", "value", allow_duplicate=True)],
    [Input("raids-data-source-selector", "value"),
     Input("raids-data-source-sql-selector", "value")],
    prevent_initial_call=True
)
def combine_data_sources(stats_val, sql_val):
    trigger = ctx.triggered_id
    if trigger == "raids-data-source-selector" and stats_val:
        return stats_val, stats_val, None
    elif trigger == "raids-data-source-sql-selector" and sql_val:
        return sql_val, None, sql_val
    return dash.no_update, dash.no_update, dash.no_update

@callback(
    [Output("raids-live-controls", "style"), Output("raids-historical-controls", "style"),
     Output("raids-interval-control-container", "style"), Output("raids-heatmap-filters-container", "style")],
    Input("raids-combined-source-store", "data")
)
def toggle_source_controls(source):
    live_s = {"display": "none"}
    hist_s = {"display": "none"}
    int_s = {"display": "none"}
    heat_s = {"display": "none"}

    if source and "live" in source:
        live_s = {"display": "block"}
    elif source == "sql_heatmap":
        hist_s = {"display": "block", "position": "relative", "zIndex": 1002}
        heat_s = {"display": "block"}
    elif source == "timeseries":
        # TimeSeries uses date picker but no interval selector
        hist_s = {"display": "block", "position": "relative", "zIndex": 1002}
    elif source == "historical":
        hist_s = {"display": "block", "position": "relative", "zIndex": 1002}
        int_s = {"display": "block"}

    return live_s, hist_s, int_s, heat_s

@callback(
    [Output("raids-mode-selector", "options"), Output("raids-mode-selector", "value"),
     Output("raids-data-source-selector", "options"), Output("raids-data-source-sql-selector", "options"),
     Output("raids-heatmap-display-mode", "options"), Output("raids-interval-selector", "options")],
    [Input("raids-combined-source-store", "data"), Input("language-store", "data")],
    [State("raids-mode-persistence-store", "data"), State("raids-mode-selector", "value")]
)
def restrict_modes(source, lang, stored_mode, current_ui_mode):
    lang = lang or "en"
    full_options = [
        {"label": translate("Surged (Hourly)", lang), "value": "surged"},
        {"label": translate("Grouped (Table)", lang), "value": "grouped"},
        {"label": translate("Sum (Totals)", lang), "value": "sum"},
    ]
    # TimeSeries mode only supports sum (totals per day)
    timeseries_options = [{"label": translate("Sum (Totals)", lang), "value": "sum"}]
    heatmap_options = [{"label": translate("Map View", lang), "value": "map"}]

    if source == "timeseries": allowed = timeseries_options
    elif source == "sql_heatmap": allowed = heatmap_options
    else: allowed = full_options

    allowed_values = [o['value'] for o in allowed]
    if current_ui_mode in allowed_values: final_value = current_ui_mode
    elif stored_mode in allowed_values: final_value = stored_mode
    else: final_value = allowed_values[0]

    # Translate Source Selectors (with TimeSeries option)
    source_opts = [
        {"label": translate("Live", lang), "value": "live"},
        {"label": translate("Historical", lang), "value": "historical"},
        {"label": translate("TimeSeries", lang), "value": "timeseries"}
    ]
    sql_opts = [{"label": translate("Heatmap", lang), "value": "sql_heatmap"}]

    # Translate Heatmap Mode Options
    heatmap_mode_opts = [
        {"label": translate("Markers (Gyms)", lang), "value": "markers"},
        {"label": translate("Density Heatmap", lang), "value": "density"},
        {"label": translate("Grid Overlay", lang), "value": "grid"}
    ]

    # Interval Options
    interval_opts = [
        {"label": translate("Hourly", lang), "value": "hourly"}
    ]

    return allowed, final_value, source_opts, sql_opts, heatmap_mode_opts, interval_opts

@callback(Output("raids-mode-persistence-store", "data"), Input("raids-mode-selector", "value"), prevent_initial_call=True)
def save_mode(val): return val

@callback(Output("raids-source-persistence-store", "data"), Input("raids-combined-source-store", "data"), prevent_initial_call=True)
def save_source(val): return val

@callback(
    [Output("raids-data-source-selector", "value"),
     Output("raids-data-source-sql-selector", "value"),
     Output("raids-combined-source-store", "data")],
    Input("raids-source-persistence-store", "modified_timestamp"),
    State("raids-source-persistence-store", "data"),
    prevent_initial_call=False
)
def load_persisted_source(ts, stored_source):
    """Load persisted data source on page load and set appropriate selector."""
    if ts is not None and ts > 0:
        raise dash.exceptions.PreventUpdate

    stats_sources = ["live", "historical", "timeseries"]
    sql_sources = ["sql_heatmap"]

    if stored_source in stats_sources:
        return stored_source, None, stored_source
    elif stored_source in sql_sources:
        return None, stored_source, stored_source
    # Default fallback
    return "live", None, "live"

@callback(Output("raids-heatmap-mode-store", "data"), Input("raids-heatmap-display-mode", "value"))
def update_heatmap_mode_store(val): return val

# Quick Filter Callbacks
@callback(
    [Output("raids-quick-filter-grid", "children"), Output("raids-quick-filter-count", "children")],
    [Input("raids-heatmap-data-store", "data"),
     Input("raids-quick-filter-search", "value"),
     Input("language-store", "data")],
    [State("raids-combined-source-store", "data"),
     State("raids-heatmap-hidden-pokemon", "data")]
)
def populate_raids_quick_filter(heatmap_data, search_term, lang, source, hidden_pokemon):
    """Populate Pokemon image grid for quick filtering raid bosses - fluid search"""
    lang = lang or "en"
    if source != "sql_heatmap" or not heatmap_data:
        return [], ""

    # 1. Process Data - aggregate by Pokemon (not gym)
    pokemon_set = {}
    for record in heatmap_data:
        pid = record.get('raid_pokemon')
        form = record.get('raid_form') or 0
        key = f"{pid}:{form}"
        if key not in pokemon_set:
            pokemon_set[key] = {
                'pid': int(pid) if pid else 0,
                'form': int(form) if form else 0,
                'count': record.get('count', 0),
                'icon_url': record.get('icon_url')
            }
        else:
            pokemon_set[key]['count'] += record.get('count', 0)

    # 2. Sort (by count descending, then ID)
    sorted_pokemon = sorted(pokemon_set.items(), key=lambda x: (-x[1]['count'], x[1]['pid'], x[1]['form']))

    # 3. Filter (Search)
    search_lower = search_term.lower() if search_term else ""
    filtered_list = []

    for key, data in sorted_pokemon:
        if search_lower:
            name = resolve_pokemon_name(data['pid'], data['form'], lang).lower()
            if search_lower not in name:
                continue
        filtered_list.append((key, data))

    # 4. Generate UI
    hidden_set = set(hidden_pokemon or [])
    pokemon_images = []

    for key, data in filtered_list:
        is_hidden = key in hidden_set
        icon_url = data.get('icon_url') or get_pokemon_icon_url(data['pid'], data['form'])
        pokemon_name = resolve_pokemon_name(data['pid'], data['form'], lang)

        style = {
            "cursor": "pointer",
            "borderRadius": "8px",
            "padding": "4px",
            "margin": "2px",
            "backgroundColor": "#2a2a2a",
            "opacity": "0.3" if is_hidden else "1",
            "border": "2px solid transparent",
            "transition": "all 0.2s"
        }

        pokemon_images.append(html.Div([
            html.Img(src=icon_url,
                    style={"width": "40px", "height": "40px", "display": "block"}),
            html.Div(f"{data['count']}",
                    style={"fontSize": "10px", "textAlign": "center", "marginTop": "2px", "color": "#aaa"})
        ], id={"type": "raids-quick-filter-icon", "index": key}, style=style,
           title=f"{pokemon_name}: {data['count']} raids"))

    count_text = f"({len(filtered_list)}/{len(sorted_pokemon)})" if search_lower else f"({len(sorted_pokemon)})"

    return pokemon_images, count_text

# Clientside callback to update icon opacity without rebuilding the grid
dash.clientside_callback(
    """
    function(hiddenPokemon) {
        if (!hiddenPokemon) hiddenPokemon = [];
        var hiddenSet = new Set(hiddenPokemon);

        // Find the grid container and iterate its children
        var grid = document.getElementById('raids-quick-filter-grid');
        if (!grid) return window.dash_clientside.no_update;

        var icons = grid.children;
        for (var i = 0; i < icons.length; i++) {
            var icon = icons[i];
            try {
                var idObj = JSON.parse(icon.id);
                if (idObj.type === 'raids-quick-filter-icon') {
                    var key = idObj.index;
                    icon.style.opacity = hiddenSet.has(key) ? '0.3' : '1';
                }
            } catch(e) {}
        }

        return window.dash_clientside.no_update;
    }
    """,
    Output("raids-quick-filter-grid", "className"),  # Dummy output
    Input("raids-heatmap-hidden-pokemon", "data"),
    prevent_initial_call=True
)

@callback(
    Output("raids-heatmap-hidden-pokemon", "data", allow_duplicate=True),
    [Input({"type": "raids-quick-filter-icon", "index": ALL}, "n_clicks"),
     Input("raids-quick-filter-show-all", "n_clicks"),
     Input("raids-quick-filter-hide-all", "n_clicks")],
    [State("raids-heatmap-hidden-pokemon", "data"),
     State("raids-heatmap-data-store", "data")],
    prevent_initial_call=True
)
def toggle_raids_pokemon_visibility(icon_clicks, show_clicks, hide_clicks, hidden_list, heatmap_data):
    """Toggle Pokemon visibility in quick filter"""
    trigger = ctx.triggered_id
    if not trigger:
        return dash.no_update

    # Button Logic
    if trigger == "raids-quick-filter-show-all":
        return []

    if trigger == "raids-quick-filter-hide-all":
        if not heatmap_data: return []
        all_keys = set()
        for record in heatmap_data:
            pid = record.get('raid_pokemon')
            form = record.get('raid_form') or 0
            all_keys.add(f"{pid}:{form}")
        return list(all_keys)

    # Icon Click Logic - must verify an actual click occurred
    if isinstance(trigger, dict) and trigger.get('type') == 'raids-quick-filter-icon':
        # Check if any icon was actually clicked (n_clicks > 0)
        # When grid rebuilds, all n_clicks are None or 0
        if not icon_clicks or not any(c and c > 0 for c in icon_clicks):
            return dash.no_update

        hidden_list = hidden_list or []
        clicked_key = trigger['index']
        if clicked_key in hidden_list:
            return [k for k in hidden_list if k != clicked_key]
        else:
            return hidden_list + [clicked_key]

    return dash.no_update

@callback(
    Output("raids-heatmap-hidden-pokemon", "data", allow_duplicate=True),
    Input("raids-heatmap-data-store", "data"),
    prevent_initial_call=True
)
def reset_raids_hidden_pokemon_on_new_data(heatmap_data):
    """Reset hidden Pokemon list when new heatmap data arrives"""
    if heatmap_data:
        return []
    return dash.no_update

@callback(
    [Output("raids-area-modal", "is_open"), Output("raids-search-input", "style")],
    [Input("raids-open-area-modal", "n_clicks"), Input("raids-close-area-modal", "n_clicks"), Input("raids-mode-selector", "value")],
    [State("raids-area-modal", "is_open")]
)
def handle_modals_and_search(ao, ac, mode, isa):
    search_style = {"display": "block", "width": "100%"} if mode == "grouped" else {"display": "none"}
    tid = ctx.triggered_id
    if tid in ["raids-open-area-modal", "raids-close-area-modal"]: return not isa, search_style
    return isa, search_style

# Area Cards Filter & Scroll
@callback(Output("raids-area-cards-container", "children"),
            [
                Input("raids-area-filter-input", "value"),
                Input("language-store", "data"),
                Input("raids-area-selector", "value")
            ]
        )
def filter_area_cards(search_term, lang, selected_area):
    geofences = get_cached_geofences() or []
    if search_term: geofences = [g for g in geofences if search_term.lower() in g['name'].lower()]
    return generate_area_cards(geofences, selected_area, lang or "en")

dash.clientside_callback(
    ClientsideFunction(namespace='clientside', function_name='scrollToSelected'),
    Output("raids-clientside-dummy-store", "data"), Input("raids-area-modal", "is_open")
)

@callback(
    [Output("raids-raw-data-store", "data"), Output("raids-stats-container", "style"),
     Output("raids-heatmap-data-store", "data"), Output("raids-heatmap-container", "style"),
     Output("raids-heatmap-stats", "children"), Output("raids-notification-area", "children")],
    [Input("raids-submit-btn", "n_clicks"), Input("raids-combined-source-store", "data")],
    [State("raids-area-selector", "value"), State("raids-live-time-input", "value"),
     State("raids-historical-date-picker", "start_date"), State("raids-historical-date-picker", "end_date"),
     State("raids-interval-selector", "value"), State("raids-mode-selector", "value"),
     State("language-store", "data")]
)
def fetch_data(n, source, area, live_h, start, end, interval, mode, lang):
    lang = lang or "en"
    if not n:
        return {}, {"display": "none"}, [], {"display": "none"}, "", None
    if not area:
        return {}, {"display": "none"}, [], {"display": "none"}, "", dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), translate("Please select an Area first.", lang)], color="warning", dismissable=True, duration=4000)

    try:
        if source == "sql_heatmap":
            # SQL Heatmap - fetch raid data grouped by gym
            logger.info(f"🔍 Starting Raid Heatmap Fetch for Area: {area}")
            params = {
                "start_time": f"{start}T00:00:00" if start else None,
                "end_time": f"{end}T23:59:59" if end else None,
                "area": area,
                "response_format": "json"
            }
            raw_data = get_raids_stats("raid_sql_data", params)

            if isinstance(raw_data, dict) and "data" in raw_data:
                heatmap_list = raw_data.get("data", [])
            elif isinstance(raw_data, list):
                heatmap_list = raw_data
            else:
                heatmap_list = []

            # Add icon URLs to each point
            for point in heatmap_list:
                pid = point.get("raid_pokemon", 0)
                form = point.get("raid_form", 0)
                point["icon_url"] = get_pokemon_icon_url(pid, form)

            total_raids = sum(p.get("count", 0) for p in heatmap_list)
            unique_gyms = len(set(p.get("gym_name", "") for p in heatmap_list))
            gyms_word = translate("gyms", lang)
            raids_word = translate("total raids", lang)
            stats_text = f"{unique_gyms:,} {gyms_word} • {total_raids:,} {raids_word}"

            logger.info(f"✅ Raid Heatmap: {len(heatmap_list)} data points, {unique_gyms} gyms, {total_raids} raids")
            return {}, {"display": "none"}, heatmap_list, {"display": "block"}, stats_text, None

        elif source == "timeseries":
            # Daily timeseries for Raids - fetch each day's sum and build timeseries
            logger.info(f"🔍 Fetching Raids TimeSeries for {area}: {start} to {end}")
            data = get_raids_daily_timeseries(start, end, area)
        elif source == "live":
            hours = max(1, min(int(live_h or 1), 72))
            params = {"start_time": f"{hours} hours", "end_time": "now", "mode": mode, "area": area, "response_format": "json"}
            data = get_raids_stats("timeseries", params)
        else:
            params = {"counter_type": "totals", "interval": interval, "start_time": f"{start}T00:00:00", "end_time": f"{end}T23:59:59", "mode": mode, "area": area, "response_format": "json"}
            data = get_raids_stats("counter", params)

        if not data:
            return {}, {"display": "block"}, [], {"display": "none"}, "", dbc.Alert(translate("No data found for this period.", lang), color="warning", dismissable=True, duration=4000)
        return data, {"display": "block"}, [], {"display": "none"}, "", None
    except Exception as e:
        logger.info(f"Fetch error: {e}")
        return {}, {"display": "none"}, [], {"display": "none"}, "", dbc.Alert(f"Error: {str(e)}", color="danger", dismissable=True)

# Sorting
@callback(
    Output("raids-table-sort-store", "data"), Input({"type": "raids-sort-header", "index": ALL}, "n_clicks"), State("raids-table-sort-store", "data"), prevent_initial_call=True
)
def update_sort_order(n_clicks, current_sort):
    if not ctx.triggered_id or not any(n_clicks): return dash.no_update
    col = ctx.triggered_id['index']
    return {"col": col, "dir": "asc" if current_sort['col'] == col and current_sort['dir'] == "desc" else "desc"}

# Pagination
@callback(
    Output("raids-table-page-store", "data"),
    [Input("raids-first-page-btn", "n_clicks"), Input("raids-prev-page-btn", "n_clicks"), Input("raids-next-page-btn", "n_clicks"), Input("raids-last-page-btn", "n_clicks"), Input("raids-rows-per-page-selector", "value"), Input("raids-goto-page-input", "value")],
    [State("raids-table-page-store", "data"), State("raids-total-pages-store", "data")],
    prevent_initial_call=True
)
def update_pagination(first, prev, next, last, rows, goto, state, total_pages):
    trigger = ctx.triggered_id
    if not trigger: return dash.no_update
    current = state.get('current_page', 1)
    total_pages = total_pages or 1
    new_page = current
    if trigger == "raids-first-page-btn": new_page = 1
    elif trigger == "raids-last-page-btn": new_page = total_pages
    elif trigger == "raids-prev-page-btn": new_page = max(1, current - 1)
    elif trigger == "raids-next-page-btn": new_page = min(total_pages, current + 1)
    elif trigger == "raids-goto-page-input":
        if goto is not None: new_page = min(total_pages, max(1, goto))
    elif trigger == "raids-rows-per-page-selector": return {"current_page": 1, "rows_per_page": rows}
    return {**state, "current_page": new_page, "rows_per_page": state.get('rows_per_page', 25)}

# Visuals Update
@callback(
    [Output("raids-total-counts-display", "children"), Output("raids-main-visual-container", "children"), Output("raids-raw-data-display", "children"), Output("raids-total-pages-store", "data"), Output("raids-main-visual-container", "style")],
    [Input("raids-raw-data-store", "data"), Input("raids-search-input", "value"), Input("raids-table-sort-store", "data"), Input("raids-table-page-store", "data"), Input("language-store", "data")],
    [State("raids-mode-selector", "value"), State("raids-combined-source-store", "data")]
)
def update_visuals(data, search_term, sort, page, lang, mode, source):
    lang = lang or "en"
    if not data: return [], html.Div(), "", 1, {"display": "block"}

    # Handle TimeSeries data format (dates + metrics)
    if isinstance(data, dict) and "dates" in data and "metrics" in data:
        dates = data.get("dates", [])
        metrics = data.get("metrics", {})
        raw_text = json.dumps(data, indent=2)

        if not dates or not metrics:
            return "No Data", html.Div(), raw_text, 1, {"display": "block"}

        # Build total counts sidebar
        total_val = sum(sum(v) for v in metrics.values())
        sidebar_items = [html.H1(f"{total_val:,}", className="text-primary")]

        # Sort raid levels numerically
        def raid_level_sort_key(level_str):
            # level_1, level_3, etc -> extract number
            if level_str.startswith("level_"):
                try:
                    return (0, int(level_str.replace("level_", "")))
                except ValueError:
                    return (1, level_str)
            if level_str == "total":
                return (-1, 0)
            return (2, level_str)

        sorted_metrics = sorted(metrics.keys(), key=raid_level_sort_key)

        for metric_key in sorted_metrics:
            values = metrics[metric_key]
            metric_total = sum(values)
            # Extract level number for display
            if metric_key.startswith("level_"):
                level_num = metric_key.replace("level_", "")
                label, color, icon_url = get_raid_info(level_num, lang)
            else:
                label, color, icon_url = get_raid_info(metric_key, lang)

            sidebar_items.append(html.Div([
                html.Img(src=icon_url, style={"width": "28px", "marginRight": "8px", "verticalAlign": "middle"}),
                html.Span(f"{metric_total:,}", style={"fontSize": "1.1em", "fontWeight": "bold", "color": color}),
                html.Span(f" {label}", style={"fontSize": "0.8em", "color": "#aaa", "marginLeft": "5px"})
            ], className="d-flex align-items-center mb-1"))

        # Create TimeSeries line chart
        fig = go.Figure()

        for metric_key in sorted_metrics:
            values = metrics[metric_key]
            if metric_key.startswith("level_"):
                level_num = metric_key.replace("level_", "")
                label, color, _ = get_raid_info(level_num, lang)
            else:
                label, color, _ = get_raid_info(metric_key, lang)

            fig.add_trace(go.Scatter(
                x=dates,
                y=values,
                name=label,
                line=dict(color=color, width=2),
                marker=dict(size=6)
            ))

        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            title=translate("Raids TimeSeries", lang),
            xaxis_title=translate("Date", lang),
            yaxis_title=translate("Count", lang),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified",
            hoverlabel=dict(
                bgcolor="#1a1a1a",
                font_size=12,
                font_color="white",
                bordercolor="#333"
            )
        )

        visual_content = dcc.Graph(figure=fig, id="raids-main-graph")
        return sidebar_items, visual_content, raw_text, 1, {"display": "block"}

    df = parse_data_to_df(data, mode, source, lang)
    if df.empty: return "No Data", html.Div(), json.dumps(data, indent=2), 1, {"display": "block"}

    # Search Logic Grouped Mode
    if mode == "grouped" and search_term:
        df = df[df['key'].str.lower().str.contains(search_term.lower(), na=False)]

    total_div = html.P("No data.")

    # Sidebar
    sidebar_metrics = []

    # Time Series / Nested
    if (mode in ["surged", "live"] or "historical" in source) and isinstance(data, dict):
        total_sum, level_counts = 0, {}
        costume_count, exclusive_count, ex_count = 0, 0, 0
        is_ts = False
        for k, content in data.items():
             if isinstance(content, dict) and ("total" in content or "raid_level" in content):
                is_ts = True
                total_sum += content.get("total", 0)
                if "raid_level" in content:
                    for lvl, cnt in content["raid_level"].items(): level_counts[str(lvl)] = level_counts.get(str(lvl), 0) + cnt
                if "raid_costume" in content: costume_count += content["raid_costume"].get("1", 0)
                if "raid_is_exclusive" in content: exclusive_count += content["raid_is_exclusive"].get("1", 0)
                if "raid_ex_eligible" in content: ex_count += content["raid_ex_eligible"].get("1", 0)

        if is_ts:
             sidebar_metrics.append({'metric': 'total', 'count': total_sum})
             for lvl, cnt in level_counts.items(): sidebar_metrics.append({'metric': str(lvl), 'count': cnt})
             if costume_count: sidebar_metrics.append({'metric': 'Costume', 'count': costume_count})
             if exclusive_count: sidebar_metrics.append({'metric': 'Exclusive', 'count': exclusive_count})
             if ex_count: sidebar_metrics.append({'metric': 'EX Eligible', 'count': ex_count})

    # Standard Sum - Flat
    if not sidebar_metrics and isinstance(data, dict):
        raw_inner = data.get('data', data) if 'data' in data else data
        if 'total' in raw_inner: sidebar_metrics.append({'metric': 'total', 'count': raw_inner['total']})
        if 'raid_level' in raw_inner:
             for k, v in raw_inner['raid_level'].items(): sidebar_metrics.append({'metric': str(k), 'count': v})
        if 'raid_costume' in raw_inner and '1' in raw_inner['raid_costume']: sidebar_metrics.append({'metric': 'Costume', 'count': raw_inner['raid_costume']['1']})
        if 'raid_is_exclusive' in raw_inner and '1' in raw_inner['raid_is_exclusive']: sidebar_metrics.append({'metric': 'Exclusive', 'count': raw_inner['raid_is_exclusive']['1']})

    if sidebar_metrics:
        total_val = next((item['count'] for item in sidebar_metrics if item['metric'] == 'total'), 0)
        total_div = [html.H1(f"{total_val:,}", className="text-primary")]

        def sidebar_sort_key(item):
            m = str(item['metric'])
            if m.isdigit(): return (0, int(m))
            if m == 'total': return (-1, 0)
            return (1, m)

        sorted_metrics = sorted(sidebar_metrics, key=sidebar_sort_key)
        for item in sorted_metrics:
            m = str(item['metric'])
            if m != 'total':
                label, color, icon_url = get_raid_info(m, lang)
                total_div.append(html.Div([
                    html.Img(src=icon_url, style={"width": "28px", "marginRight": "8px", "verticalAlign": "middle"}),
                    html.Span(f"{item['count']:,}", style={"fontSize": "1.1em", "fontWeight": "bold", "color": color}),
                    html.Span(f" {label}" if not m.isdigit() else "", style={"fontSize": "0.8em", "color": "#aaa", "marginLeft": "5px"})
                ], className="d-flex align-items-center mb-1"))

    visual_content = html.Div("No data")
    total_pages_val = 1

    # Grouped Table
    if mode == "grouped" and not df.empty:
        col, ascending = sort['col'], sort['dir'] == "asc"
        if col in df.columns: df = df.sort_values(col, ascending=ascending)
        else: df = df.sort_values('count', ascending=False)

        rows_per_page = page['rows_per_page']
        total_rows = len(df)
        total_pages_val = max(1, (total_rows + rows_per_page - 1) // rows_per_page)
        current_page = min(max(1, page['current_page']), total_pages_val)
        page_df = df.iloc[(current_page - 1) * rows_per_page : current_page * rows_per_page]

        header_row = html.Tr([
            html.Th(translate("Image", lang), style={"backgroundColor": "#1a1a1a", "zIndex": "10", "position": "sticky", "top": "0", "textAlign": "center", "width": "60px"}),
            html.Th(html.Span([translate("Pokémon", lang), html.Span(" ▲" if col == 'key' and ascending else (" ▼" if col == 'key' else ""), style={"color": "#aaa", "marginLeft": "5px"})], id={"type": "raids-sort-header", "index": "key"}, style={"cursor": "pointer"}), style={"backgroundColor": "#1a1a1a", "zIndex": "10", "position": "sticky", "top": "0", "textAlign": "center"}),
            html.Th(html.Span([translate("Count", lang), html.Span(" ▲" if col == 'count' and ascending else (" ▼" if col == 'count' else ""), style={"color": "#aaa", "marginLeft": "5px"})], id={"type": "raids-sort-header", "index": "count"}, style={"cursor": "pointer"}), style={"backgroundColor": "#1a1a1a", "zIndex": "10", "position": "sticky", "top": "0", "textAlign": "center"})
        ])

        rows = []
        for i, r in enumerate(page_df.iterrows()):
            _, r = r
            bg = "#1a1a1a" if i % 2 == 0 else "#242424"
            rows.append(html.Tr([
                html.Td(html.Img(src=get_pokemon_icon_url(r['pid'], r['form']), style={"width":"40px", "height":"40px", "display":"block", "margin":"auto"}), style={"backgroundColor":bg, "verticalAlign": "middle", "textAlign": "center"}),
                html.Td(f"{r['key']}", style={"backgroundColor":bg, "verticalAlign": "middle", "textAlign": "center"}),
                html.Td(f"{int(r['count']):,}", style={"textAlign":"center", "backgroundColor":bg, "verticalAlign": "middle"})
            ]))

        controls = html.Div([
            dbc.Row([
                dbc.Col([html.Span(f"Total: {total_rows} | Rows: ", className="me-2 align-middle"), dcc.Dropdown(id="raids-rows-per-page-selector", options=[{'label': str(x), 'value': x} for x in [10, 25, 50, 100]] + [{'label': 'All', 'value': total_rows}], value=rows_per_page, clearable=False, className="rows-per-page-selector", style={"width":"80px", "display":"inline-block", "color":"black", "verticalAlign": "middle"})], width="auto", className="d-flex align-items-center"),
                dbc.Col([
                    dbc.ButtonGroup([dbc.Button("<<", id="raids-first-page-btn", size="sm", disabled=current_page <= 1), dbc.Button("<", id="raids-prev-page-btn", size="sm", disabled=current_page <= 1)], className="me-2"),
                    html.Span("Page ", className="align-middle me-1"), dcc.Input(id="raids-goto-page-input", type="number", min=1, max=total_pages_val, value=current_page, debounce=True, style={"width": "60px", "textAlign": "center", "display": "inline-block", "color": "black"}), html.Span(f" of {total_pages_val}", className="align-middle ms-1 me-2"),
                    dbc.ButtonGroup([dbc.Button(">", id="raids-next-page-btn", size="sm", disabled=current_page >= total_pages_val), dbc.Button(">>", id="raids-last-page-btn", size="sm", disabled=current_page >= total_pages_val)]),
                ], width="auto", className="d-flex align-items-center justify-content-end ms-auto")
            ], className="g-0")
        ], className="p-2 bg-dark rounded mb-2 border border-secondary")

        visual_content = html.Div([controls, html.Div(html.Table([html.Thead(header_row), html.Tbody(rows)], style={"width":"100%", "color":"#fff"}), style={"overflowX":"auto", "maxHeight":"600px"})])

    # Charts Sum / Surged
    elif mode in ["surged", "sum"]:
        graph_df = df[df['metric'] != 'total'].copy()
        fig = go.Figure()
        sorter = lambda x: int(x) if str(x).isdigit() else 999

        if mode == "sum":
            d = graph_df.copy()
            d['sort'] = d['metric'].apply(sorter)
            d = d.sort_values('sort')
            bar_colors = []
            for m in d['metric']:
                _, c, _ = get_raid_info(m)
                bar_colors.append(c)
            fig.add_trace(go.Bar(x=d['metric'], y=d['count'], marker=dict(color=bar_colors)))

            max_y = d['count'].max() if not d.empty else 10
            icon_size_y = max_y * 0.15
            for i, (idx, row) in enumerate(d.iterrows()):
                _, _, icon_url = get_raid_info(row['metric'])
                fig.add_layout_image(dict(source=icon_url, x=i, y=row['count'], xref="x", yref="y", sizex=0.6, sizey=icon_size_y, xanchor="center", yanchor="bottom"))
            fig.update_layout(margin=dict(t=50))
            fig.update_yaxes(range=[0, max_y * 1.25])
            fig.update_xaxes(type='category')

        else:
            agg = graph_df if "live" in source else graph_df.groupby(["time_bucket", "metric"])["count"].sum().reset_index()
            agg['time_bucket'] = pd.to_numeric(agg['time_bucket'], errors='coerce').fillna(0).astype(int)
            for m in sorted(agg['metric'].unique(), key=sorter):
                d = agg[agg['metric'] == m].sort_values("time_bucket")
                _, c, _ = get_raid_info(m)
                fig.add_trace(go.Scatter(x=d['time_bucket'], y=d['count'], mode='lines+markers', name=str(m), line=dict(color=c)))
            fig.update_xaxes(range=[-0.5, 23.5], dtick=1)

        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', title=f"{translate(mode.title(), lang)} {translate('Data', lang)}")
        visual_content = dcc.Graph(figure=fig, id="raids-main-graph")

    return total_div, visual_content, json.dumps(data, indent=2), total_pages_val, {"display": "block"}

# Clientside callback for raid heatmap rendering
dash.clientside_callback(
    ClientsideFunction(namespace='clientside', function_name='triggerRaidHeatmapRenderer'),
    Output("raids-clientside-dummy-store", "data", allow_duplicate=True),
    [Input("raids-heatmap-data-store", "data"),
     Input("raids-heatmap-hidden-pokemon", "data"),
     Input("raids-heatmap-mode-store", "data")],
    prevent_initial_call=True
)

# Global Area Store Sync - persist area selection across pages
@callback(
    Output("raids-area-selector", "value"),
    Input("global-area-store", "data"),
    State("raids-area-selector", "value"),
    prevent_initial_call=False
)
def init_raids_area_from_global_store(global_area, current_area):
    if not current_area and global_area:
        return global_area
    return dash.no_update
