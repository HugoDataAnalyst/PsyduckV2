import dash
from dash import html, dcc, callback, Input, Output, State, ALL, ctx, MATCH, ClientsideFunction
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, date
from dashboard.utils import get_cached_geofences, get_pokemon_stats, get_pokemon_icon_url
from utils.logger import logger
import config as AppConfig
import json
import re
import os
from dashboard.translations.manager import translate, translate_pokemon

dash.register_page(__name__, path='/pokemon', title='Pokémon Analytics')

try:
    RETENTION_MS = AppConfig.timeseries_pokemon_retention_ms
    MAX_RETENTION_HOURS = int(RETENTION_MS / 3600000)
except:
    MAX_RETENTION_HOURS = 72

try:
    SHINY_RETENTION_MONTHS = AppConfig.clean_pokemon_shiny_older_than_x_months
except:
    SHINY_RETENTION_MONTHS = 3

#  Data Loading
_SPECIES_MAP = None
_FORM_MAP = None
_ALL_POKEMON_OPTIONS = None

def _load_pokedex_data():
    global _SPECIES_MAP, _FORM_MAP, _ALL_POKEMON_OPTIONS

    if _SPECIES_MAP is None:
        try:
            path = os.path.join(os.path.dirname(__file__), '..', 'assets', 'pogo_mapping', 'pokemons', 'pokedex_id.json')
            if not os.path.exists(path): path = os.path.join(os.getcwd(), 'assets', 'pogo_mapping', 'pokemons', 'pokedex_id.json')
            with open(path, 'r') as f:
                _SPECIES_MAP = json.load(f)
        except: _SPECIES_MAP = {}

    if _FORM_MAP is None:
        try:
            path = os.path.join(os.path.dirname(__file__), '..', 'assets', 'pogo_mapping', 'pokemons', 'pokedex.json')
            if not os.path.exists(path): path = os.path.join(os.getcwd(), 'assets', 'pogo_mapping', 'pokemons', 'pokedex.json')
            with open(path, 'r') as f:
                _FORM_MAP = json.load(f)
        except: _FORM_MAP = {}

    if _ALL_POKEMON_OPTIONS is None and _SPECIES_MAP:
        options = []
        name_to_id = {k: v for k, v in _SPECIES_MAP.items()}

        # 1. Add Base Forms Form 0 - exclude MISSINGNO pid 0
        for name, pid in _SPECIES_MAP.items():
            if pid == 0:  # Skip MISSINGNO
                continue
            options.append({
                "pid": pid,
                "form": 0,
                "name": name.title().replace("_", " "),
                "key": f"{pid}:0",
                "search_key": name.lower()
            })

        # 2. Add Forms from pokedex.json
        for form_key, form_val in _FORM_MAP.items():
            if form_val == 0: continue
            # Skip _NORMAL forms they're duplicates of form 0
            if form_key.endswith("_NORMAL"):
                continue

            matched_name = None
            for species_name in _SPECIES_MAP.keys():
                if form_key.startswith(species_name + "_"):
                    if matched_name is None or len(species_name) > len(matched_name):
                        matched_name = species_name

            if matched_name:
                pid = _SPECIES_MAP[matched_name]
                if pid == 0:  # Skip MISSINGNO forms
                    continue
                pretty_name = form_key.replace(matched_name + "_", "").replace("_", " ").title()
                full_name = f"{matched_name.title().replace('_', ' ')} ({pretty_name})"

                options.append({
                    "pid": pid,
                    "form": form_val,
                    "name": full_name,
                    "key": f"{pid}:{form_val}",
                    "search_key": form_key.lower().replace("_", " ")
                })

        options.sort(key=lambda x: (x['pid'], x['form']))
        _ALL_POKEMON_OPTIONS = options

    return _ALL_POKEMON_OPTIONS or []

def safe_int(value):
    if value is None: return 0
    if isinstance(value, str):
        if value.lower() == "none" or value == "": return 0
        try: return int(float(value))
        except: return 0
    if isinstance(value, (int, float)): return int(value)
    return 0

def resolve_pokemon_name(pid, form_id, lang="en"):
    """
    Returns the Pokemon name with form suffix if applicable.
    Uses translation system for localized Pokemon names.
    """
    _load_pokedex_data()
    pid = int(pid) if isinstance(pid, str) and str(pid).isdigit() else (pid if isinstance(pid, int) else 0)
    form_id = int(form_id) if isinstance(form_id, str) and str(form_id).isdigit() else (form_id if isinstance(form_id, int) else 0)

    # Get translated species name
    species_name = translate_pokemon(pid, lang)

    # For form 0, return just the species name
    if form_id == 0:
        return species_name

    # Try to find form name from pokedex.json (FORM_MAP)
    form_name = None
    for form_key, fid in _FORM_MAP.items():
        if fid == form_id:
            # Check if this form key starts with our species
            for name_key in _SPECIES_MAP.keys():
                if form_key.startswith(name_key + "_") and _SPECIES_MAP[name_key] == pid:
                    # Extract form part after species name
                    form_part = form_key.replace(name_key + "_", "")
                    form_name = form_part.replace("_", " ").title()
                    break
            if form_name:
                break

    if form_name:
        return f"{species_name} ({form_name})"

    return f"{species_name} (Form {form_id})" if form_id != 0 else species_name

def resolve_pokemon_name_parts(pid, form_id, lang="en"):
    """Returns tuple of (species_name, form_name) for display purposes.
    Uses translation system for localized Pokemon names."""
    _load_pokedex_data()
    pid = int(pid) if isinstance(pid, str) and pid.isdigit() else (pid if isinstance(pid, int) else 0)
    form_id = int(form_id) if isinstance(form_id, str) and str(form_id).isdigit() else (form_id if isinstance(form_id, int) else 0)

    # Get translated species name
    species_name = translate_pokemon(pid, lang)

    # For form 0, return just the species name with no form label
    if form_id == 0:
        return (species_name, None)

    # Try to find form name from pokedex.json (FORM_MAP)
    form_name = None

    for form_key, fid in _FORM_MAP.items():
        if fid == form_id:
            # Check if this form key starts with our species
            for name_key in _SPECIES_MAP.keys():
                if form_key.startswith(name_key + "_") and _SPECIES_MAP[name_key] == pid:
                    # Extract form part after species name
                    form_part = form_key.replace(name_key + "_", "")
                    form_name = form_part.replace("_", " ").title()
                    break
            if form_name:
                break

    if not form_name:
        form_name = f"Form {form_id}"

    return (species_name, form_name)

# Layout

def generate_area_cards(geofences, selected_area_name, lang="en"):
    cards = []
    for idx, geo in enumerate(geofences):
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', geo['name'])
        is_selected = (selected_area_name == geo['name'])
        map_children = [html.Div("✓ " + translate("Selected", lang), style={'position': 'absolute', 'top': '10px', 'right': '10px', 'backgroundColor': '#28a745', 'color': 'white', 'padding': '4px 8px', 'borderRadius': '4px', 'fontWeight': 'bold', 'zIndex': '1000'})] if is_selected else []

        card = dbc.Card([
            html.Div(map_children, id=f"poke-area-map-{safe_name}", **{'data-map-geofence': json.dumps(geo)}, style={'height': '150px', 'backgroundColor': '#1a1a1a', 'position': 'relative'}),
            dbc.CardBody([
                html.H5(geo['name'], className="card-title text-truncate", style={'color': '#28a745' if is_selected else 'inherit'}),
                dbc.Button("✓ " + translate("Selected", lang) if is_selected else translate("Select", lang), href=f"/pokemon?area={geo['name']}", color="success" if is_selected else "primary", size="sm", className="w-100", disabled=is_selected)
            ])
        ], style={"width": "14rem", "margin": "10px", "border": f"3px solid {'#28a745' if is_selected else 'transparent'}"}, className="shadow-sm")

        if is_selected: card.id = "selected-area-card"
        cards.append(card)
    return cards if cards else html.Div(translate("No areas match your search.", lang), className="text-center text-muted my-4")

def layout(area=None, **kwargs):
    geofences = get_cached_geofences() or []
    initial_cards = generate_area_cards(geofences, area, "en") # Default to EN
    area_options = [{"label": g["name"], "value": g["name"]} for g in geofences]
    area_label = area if area else "No Area Selected"

    _load_pokedex_data()

    return dbc.Container([
        dcc.Store(id="raw-data-store"),
        dcc.Store(id="table-sort-store", data={"col": "total", "dir": "desc"}),
        dcc.Store(id="table-page-store", data={"current_page": 1, "rows_per_page": 25}),
        dcc.Store(id="total-pages-store", data=1),
        dcc.Store(id="clientside-dummy-store"),
        dcc.Store(id="heatmap-data-store", data=[]),
        dcc.Store(id="heatmap-mode-store", data="markers"),
        dcc.Store(id="heatmap-hidden-pokemon", data=[]),
        dcc.Dropdown(id="area-selector", options=area_options, value=area, style={'display': 'none'}),
        dcc.Store(id="mode-persistence-store", storage_type="local"),
        dcc.Store(id="source-persistence-store", storage_type="local"),
        dcc.Store(id="combined-source-store", data="live"),

        # New Selection Stores
        dcc.Store(id="selection-store", data=[]),
        dcc.Store(id="selection-page-store", data=1),

        dbc.Row([
            dbc.Col(html.H2("Pokémon Analytics", id="page-title-text", className="text-white"), width=12, className="my-4"),
        ]),
        html.Div(id="notification-area"),

        # Main Control Card
        dbc.Card([
            dbc.CardHeader("⚙️ Analysis Settings", id="settings-header-text", className="fw-bold"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Selected Area", id="label-selected-area", className="fw-bold"),
                        dbc.InputGroup([
                            dbc.InputGroupText("🗺️"),
                            dbc.Input(id="pokemons-selected-area-display", value=area_label, disabled=True, style={"backgroundColor": "#fff", "color": "#333", "fontWeight": "bold"}),
                            dbc.Button("Change", id="open-area-modal", color="primary")
                        ], className="mb-3")
                    ], width=12, md=6),
                    dbc.Col([
                        dbc.Label("Data Source", id="label-data-source", className="fw-bold"),
                        html.Div([
                            # Row 1: Stats Live & Historical
                            html.Div([
                                html.Span("Stats: ", id="label-stats", className="text-muted small me-2", style={"minWidth": "45px"}),
                                dbc.RadioItems(
                                    id="data-source-selector",
                                    options=[
                                        {"label": "Live", "value": "live"},
                                        {"label": "Historical", "value": "historical"},
                                    ],
                                    value="live", inline=True, inputClassName="btn-check",
                                    labelClassName="btn btn-outline-info btn-sm",
                                    labelCheckedClassName="active"
                                ),
                            ], className="d-flex align-items-center mb-1"),
                            # Row 2: TTH Live & Historical
                            html.Div([
                                html.Span("TTH: ", id="label-tth", className="text-muted small me-2", style={"minWidth": "45px"}),
                                dbc.RadioItems(
                                    id="data-source-tth-selector",
                                    options=[
                                        {"label": "Live", "value": "live_tth"},
                                        {"label": "Historical", "value": "historical_tth"},
                                    ],
                                    value=None, inline=True, inputClassName="btn-check",
                                    labelClassName="btn btn-outline-warning btn-sm",
                                    labelCheckedClassName="active"
                                ),
                            ], className="d-flex align-items-center mb-1"),
                            # Row 3: SQL Sources Heatmap & Shiny Odds
                            html.Div([
                                html.Span("SQL: ", id="label-sql", className="text-muted small me-2", style={"minWidth": "45px"}),
                                dbc.RadioItems(
                                    id="data-source-sql-selector",
                                    options=[
                                        {"label": "Heatmap", "value": "sql_heatmap"},
                                        {"label": "Shiny Odds", "value": "sql_shiny"},
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

                dbc.Row([
                    dbc.Col([
                        html.Div(id="live-controls", children=[
                            dbc.Label(f"📅 Time Window (Hours)", id="label-time-window"),
                            dbc.InputGroup([
                                dbc.Input(id="live-time-input", type="number", min=1, max=MAX_RETENTION_HOURS, value=1),
                                dbc.InputGroupText("hours")
                            ])
                        ]),
                        html.Div(id="historical-controls", style={"display": "none"}, children=[
                            dbc.Label("📅 Date Range", id="label-date-range"),
                            dcc.DatePickerRange(id="historical-date-picker", start_date=date.today(), end_date=date.today(), className="d-block w-100", persistence=True, persistence_type="local")
                        ]),
                        html.Div(id="shiny-month-controls", style={"display": "none"}, children=[
                            dbc.Label("📅 Month Range", id="label-month-range"),
                            dbc.Row([
                                dbc.Col([
                                    dbc.Label("Start", id="label-start", className="small text-muted"),
                                    dcc.Dropdown(
                                        id="shiny-start-month",
                                        options=[],
                                        placeholder="Start Month",
                                        clearable=False,
                                        className="text-dark"
                                    )
                                ], width=6),
                                dbc.Col([
                                    dbc.Label("End", id="label-end", className="small text-muted"),
                                    dcc.Dropdown(
                                        id="shiny-end-month",
                                        options=[],
                                        placeholder="End Month",
                                        clearable=False,
                                        className="text-dark"
                                    )
                                ], width=6)
                            ])
                        ])
                    ], width=6, md=3),
                    dbc.Col([
                        html.Div(id="interval-control-container", style={"display": "none"}, children=[
                            dbc.Label("⏱️ Interval", id="label-interval"),
                            dcc.Dropdown(id="interval-selector", options=[{"label": "Hourly", "value": "hourly"}], value="hourly", clearable=False, className="text-dark")
                        ])
                    ], width=6, md=3),
                    dbc.Col([
                        dbc.Label("📊 View Mode", id="label-view-mode"),
                        html.Div(
                            dbc.RadioItems(
                                id="mode-selector",
                                options=[],
                                value=None,
                                inline=True,
                                inputClassName="btn-check",
                                labelClassName="btn btn-outline-primary",
                                labelCheckedClassName="active"
                            ), className="btn-group", style={"width": "100%", "gap": "0"}
                        )
                    ], width=6, md=3),
                    dbc.Col([
                        dbc.Label("Actions", id="label-actions", style={"visibility": "hidden"}),
                        html.Div([
                            dbc.Button("Selection Filter", id="open-selection-modal", color="info", className="w-100 mb-2", style={"display": "none"}),
                            dbc.Button("Run Analysis", id="submit-btn", color="success", className="w-100 fw-bold mb-2"),
                        ])
                    ], width=6, md=3)
                ], className="align-items-end g-3"),

                # HEATMAP FILTERS
                html.Div(id="heatmap-filters-container", style={"display": "none"}, children=[
                    html.Hr(className="my-3"),
                    dbc.Card([
                        dbc.CardBody([
                            dbc.Row([
                                dbc.Col([
                                    html.Div([
                                        dbc.Label("IV Filter", id="label-iv-filter", className="fw-bold text-white mb-2"),
                                        html.Div(id="iv-range-display", className="text-muted small mb-2", style={"minHeight": "20px"}),
                                        dcc.RangeSlider(
                                            0, 100, 1, value=[0, 100], id='iv-slider',
                                            marks={0:'0%', 25:'25%', 50:'50%', 75:'75%', 100:'100%'},
                                            tooltip={"placement": "bottom", "always_visible": True},
                                            className="mb-2"
                                        )
                                    ])
                                ], width=12, lg=6, className="mb-3"),
                                dbc.Col([
                                    html.Div([
                                        dbc.Label("Level Filter", id="label-level-filter", className="fw-bold text-white mb-2"),
                                        html.Div(id="level-range-display", className="text-muted small mb-2", style={"minHeight": "20px"}),
                                        dcc.RangeSlider(
                                            1, 50, 1, value=[1, 50], id='level-slider',
                                            marks={1:'1', 10:'10', 20:'20', 30:'30', 35:'35', 40:'40', 45:'45', 50:'50'},
                                            tooltip={"placement": "bottom", "always_visible": True},
                                            className="mb-2"
                                        )
                                    ])
                                ], width=12, lg=6, className="mb-3")
                            ])
                        ])
                    ], className="border-secondary mb-3", style={"backgroundColor": "rgba(0,0,0,0.3)"}),

                    # Heatmap Display Mode
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("🗺️ Heatmap Display Mode", id="label-heatmap-display-mode", className="fw-bold"),
                            dbc.RadioItems(
                                id="heatmap-display-mode",
                                options=[
                                    {"label": "Markers", "value": "markers"},
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

        # AREA MODAL
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Select an Area", id="modal-title-area")),
            dbc.ModalBody([
                html.Div(
                    dbc.Input(id="area-filter-input", placeholder="Filter areas by name...", className="mb-3", autoFocus=True),
                    style={"position": "sticky", "top": "-16px", "zIndex": "1020", "backgroundColor": "var(--bs-modal-bg, #fff)", "paddingTop": "16px", "paddingBottom": "10px", "marginBottom": "10px", "borderBottom": "1px solid #dee2e6"}
                ),
                html.Div(initial_cards, id="area-cards-container", className="d-flex flex-wrap justify-content-center")
            ]),
            dbc.ModalFooter(dbc.Button("Close", id="close-area-modal", className="ms-auto"))
        ], id="area-modal", size="xl", scrollable=True),

        # SELECTION MODAL
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Select Pokémon", id="modal-title-selection")),
            dbc.ModalBody([
                dbc.Row([
                    dbc.Col(dbc.Input(id="selection-search", placeholder="Search Pokémon...", className="mb-3"), width=8),
                    dbc.Col(dbc.Button("Select All", id="selection-select-all", color="success", className="w-100"), width=2),
                    dbc.Col(dbc.Button("Clear", id="selection-clear", color="danger", className="w-100"), width=2),
                ]),
                html.P(id="pokemons-heatmap-filter-instruction", children="Select specific Pokémon to include. If >75% are selected, 'All' is queried.", className="text-muted small"),
                html.Div(id="select-all-hint", className="small mb-2 fw-bold"),
                html.Div(id="selection-count-display", className="text-warning small mb-2 fw-bold"),

                # Pagination Controls Top
                dbc.Row([
                    dbc.Col(dbc.Button("Prev", id="sel-prev-top", size="sm"), width="auto"),
                    dbc.Col(html.Span(id="sel-page-display-top", className="align-middle mx-2"), width="auto"),
                    dbc.Col(dbc.Button("Next", id="sel-next-top", size="sm"), width="auto"),
                ], className="mb-2 justify-content-center align-items-center"),

                dcc.Loading(html.Div(id="selection-grid", className="d-flex flex-wrap justify-content-center gap-2"))
            ]),
            dbc.ModalFooter(dbc.Button("Done", id="close-selection-modal", color="primary"))
        ], id="selection-modal", size="xl", scrollable=True),

        # Results Container
        html.Div(id="stats-container", style={"display": "none"}, children=[
            dbc.Row([
                # Sidebar
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("📈 Total Counts", id="card-header-total-counts"),
                        dbc.CardBody(dcc.Loading(html.Div(id="total-counts-display")))
                    ], className="shadow-sm border-0 mb-3"),

                    # Quick Filter Card Left Column
                    html.Div(id="quick-filter-container", style={"display": "none"}, children=[
                        dbc.Card([
                            dbc.CardHeader([
                                dbc.Row([
                                    dbc.Col([
                                        html.Span("🎯 Pokémon Filter", id="card-header-quick-filter", className="me-2"),
                                        html.Span(id="pokemon-quick-filter-count", className="text-muted small")
                                    ], width="auto", className="d-flex align-items-center"),
                                    dbc.Col([
                                        dbc.ButtonGroup([
                                            dbc.Button("All", id="pokemon-quick-filter-show-all", title="Show All", size="sm", color="success", outline=True),
                                            dbc.Button("None", id="pokemon-quick-filter-hide-all", title="Hide All", size="sm", color="danger", outline=True),
                                        ], size="sm")
                                    ], width="auto")
                                ], className="align-items-center justify-content-between g-0")
                            ]),
                            dbc.CardBody([
                                dbc.Input(id="pokemon-quick-filter-search", placeholder="Search Pokémon...", size="sm", className="mb-2"),
                                html.P(id="pokemon-quick-filter-instructions", children="Click to hide/show Pokémon from map", className="text-muted small mb-2"),
                                html.Div(id="pokemon-quick-filter-grid",
                                         style={"display": "flex", "flexWrap": "wrap", "gap": "4px", "justifyContent": "center", "maxHeight": "500px", "overflowY": "auto"})
                            ])
                        ], className="shadow-sm border-0 h-100")
                    ]),
                ], width=12, lg=4, className="mb-4"),

                # Main Data Column
                dbc.Col(dbc.Card([
                    dbc.CardHeader([
                        html.Div("📋 Activity Data", id="card-header-activity", className="d-inline-block me-auto"),
                        html.Div(
                            dbc.RadioItems(
                                id="heatmap-display-mode-visual",
                                value="markers", inline=True, className="ms-2"
                            ), id="heatmap-toggle-container", style={"display": "none", "float": "right"}
                        )
                    ]),
                    dbc.CardBody([
                        dcc.Input(
                            id="table-search-input",
                            type="text",
                            placeholder="🔍 Search Table...",
                            debounce=False,
                            className="form-control mb-3",
                            style={"display": "none"}
                        ),
                        # Wrapped inner content in Loading
                        dcc.Loading(html.Div(id="main-visual-container")),
                        html.Div(id="heatmap-map-container", style={"height": "600px", "width": "100%", "display": "none"})
                    ])
                ], className="shadow-sm border-0 h-100"), width=12, lg=8, className="mb-4"),
            ]),
            dbc.Row([dbc.Col(dbc.Card([
                dbc.CardHeader("🛠️ Raw Data Inspector", id="card-header-raw"),
                dbc.CardBody(html.Pre(id="raw-data-display", style={"maxHeight": "300px", "overflow": "scroll"}))
            ], className="shadow-sm border-0"), width=12)])
        ]),

        # Heatmap Container Map + Quick Filter - OUTSIDE Loading
        html.Div(id="heatmap-container", style={"display": "none"}, children=[
             dbc.Row([
                # Left Column - Quick Filter
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            dbc.Row([
                                dbc.Col([
                                    html.Span("🎯 Pokémon Filter", id="card-header-quick-filter-2", className="me-2"),
                                    html.Span(id="pokemon-quick-filter-count-2", className="text-muted small")
                                ], width="auto", className="d-flex align-items-center"),
                                dbc.Col([
                                    dbc.ButtonGroup([
                                        dbc.Button("All", id="pokemon-quick-filter-show-all-2", title="Show All", size="sm", color="success", outline=True),
                                        dbc.Button("None", id="pokemon-quick-filter-hide-all-2", title="Hide All", size="sm", color="danger", outline=True),
                                    ], size="sm")
                                ], width="auto")
                            ], className="align-items-center justify-content-between g-0")
                        ]),
                        dbc.CardBody([
                            dbc.Input(id="pokemon-quick-filter-search-2", placeholder="Search Pokémon...", size="sm", className="mb-2"),
                            html.P(id="pokemon-quick-filter-search-2-instructions", children="Click to hide/show Pokémon from map", className="text-muted small mb-2"),
                            html.Div(id="pokemon-quick-filter-grid-2",
                                     style={"display": "flex", "flexWrap": "wrap", "gap": "4px", "justifyContent": "center", "maxHeight": "500px", "overflowY": "auto"})
                        ])
                    ], className="shadow-sm border-0 h-100")
                ], width=12, lg=3, className="mb-4"),

                # Right Column - Map
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Span("🗺️ Heatmap", id="card-header-heatmap", className="fw-bold"),
                            html.Span(id="heatmap-stats-header", className="ms-3 text-muted small")
                        ]),
                        dbc.CardBody([
                            html.Div(id="heatmap-map-container-2", style={"height": "600px", "width": "100%", "backgroundColor": "#1a1a1a"})
                        ])
                    ], className="shadow-sm border-0 h-100")
                ], width=12, lg=9, className="mb-4")
            ])
        ])
    ])

# Parsing Logic

def parse_data_to_df(data, mode, source, lang="en"):
    if source == "sql_heatmap": return pd.DataFrame(data) if data else pd.DataFrame()
    records = []

    if mode == "sum":
        if isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, (int, float)):
                    records.append({"metric": key, "count": val, "pid": "All", "form": "All", "key": "All", "time_bucket": "Total"})

    elif mode == "surged" and ("live" in source or "tth" in source):
        if isinstance(data, dict):
            # Check if this is historical TTH format hour - bucket - count
            first_val = next(iter(data.values())) if data else None
            if isinstance(first_val, dict) and "historical" in source and "tth" in source:
                # Historical TTH format: {"10": {"15_20": 1051, "20_25": 8125}, "11": {...}}
                for hour_key, buckets in data.items():
                    if isinstance(buckets, dict):
                        h_val = int(hour_key) if str(hour_key).isdigit() else hour_key
                        for bucket_name, count in buckets.items():
                            records.append({"metric": bucket_name, "time_bucket": h_val, "count": count, "pid": "All", "form": "All", "key": "All"})
            else:
                # Original format: {"metric": {"hour 0": count, "hour 1": count}}
                for metric, hours in data.items():
                    if isinstance(hours, dict):
                        for h_key, count in hours.items():
                            h_val = h_key
                            if "hour" in str(h_key):
                                try: h_val = int(str(h_key).replace("hour ", ""))
                                except: pass
                            records.append({"metric": metric, "time_bucket": h_val, "count": count, "pid": "All", "form": "All", "key": "All"})
                    # Fallback: If no dict buckets (flat structure due to missing interval), treat as totals
                    elif isinstance(hours, (int, float)):
                        records.append({"metric": metric, "time_bucket": 0, "count": hours, "pid": "All", "form": "All", "key": "All"})

    elif mode == "grouped":
        if isinstance(data, dict) and data:
            first_val = next(iter(data.values()))
            if isinstance(first_val, (int, float)):
                for key_str, count in data.items():
                    parts = key_str.split(":")
                    if len(parts) >= 3:
                        pid, form, metric = parts[0], parts[1], parts[2]
                        name = resolve_pokemon_name(pid, form, lang)
                        records.append({"metric": metric, "pid": int(pid) if pid.isdigit() else 0, "form": int(form) if form.isdigit() else 0, "key": name, "count": count, "time_bucket": "Total"})
            elif isinstance(first_val, dict):
                for metric, items in data.items():
                    if isinstance(items, dict):
                        for key_str, count in items.items():
                            parts = key_str.split(":")
                            pid, form = (parts[0], parts[1]) if len(parts) >= 2 else (0, 0)
                            name = resolve_pokemon_name(pid, form, lang)
                            records.append({"metric": metric, "pid": int(pid) if pid.isdigit() else 0, "form": int(form) if form.isdigit() else 0, "key": name, "count": count, "time_bucket": "Total"})

    elif "historical" in source:
        if isinstance(data, dict):
            for hour_key, entries in data.items():
                if isinstance(entries, dict):
                    h_val = hour_key
                    if "hour" in str(hour_key):
                        try: h_val = int(hour_key.replace("hour ", ""))
                        except: pass
                    for key_str, count in entries.items():
                        if ":" in key_str:
                            parts = key_str.split(":")
                            if len(parts) >= 3:
                                pid, form, metric = parts[0], parts[1], parts[2]
                                name = resolve_pokemon_name(pid, form, lang)
                                records.append({"metric": metric, "pid": int(pid) if pid.isdigit() else 0, "form": int(form) if form.isdigit() else 0, "key": name, "count": count, "time_bucket": h_val})
                        else:
                            records.append({"metric": key_str, "pid": "All", "form": "All", "key": "All", "count": count, "time_bucket": h_val})

    df = pd.DataFrame(records)
    if df.empty: return pd.DataFrame(columns=["metric", "count", "pid", "form", "key", "time_bucket"])
    return df

# Callbacks

# 0. Static Translation Callback
@callback(
    [
        Output("page-title-text", "children"), Output("settings-header-text", "children"),
        Output("label-selected-area", "children"), Output("open-area-modal", "children"),
        Output("label-data-source", "children"), Output("label-stats", "children"),
        Output("label-tth", "children"), Output("label-sql", "children"),
        Output("label-time-window", "children"), Output("label-date-range", "children"),
        Output("label-month-range", "children"), Output("label-start", "children"),
        Output("label-end", "children"), Output("label-interval", "children"),
        Output("label-view-mode", "children"), Output("label-actions", "children"),
        Output("open-selection-modal", "children"), Output("submit-btn", "children"),
        Output("label-iv-filter", "children"), Output("label-level-filter", "children"),
        Output("label-heatmap-display-mode", "children"), Output("modal-title-area", "children"),
        Output("close-area-modal", "children"), Output("modal-title-selection", "children"),
        Output("selection-select-all", "children"), Output("selection-clear", "children"),
        Output("close-selection-modal", "children"), Output("card-header-total-counts", "children"),
        Output("card-header-activity", "children"), Output("card-header-raw", "children"),
        Output("card-header-quick-filter", "children"), Output("card-header-heatmap", "children"),
        Output("pokemon-quick-filter-instructions", "children"), Output("pokemons-selected-area-display", "value"),
        Output("area-filter-input", "placeholder"), Output("table-search-input", "placeholder"),
        Output("selection-search", "placeholder"), Output("pokemon-quick-filter-search", "placeholder"),
        Output("pokemon-quick-filter-search-2", "placeholder"), Output("pokemon-quick-filter-search-2-instructions", "placeholder"),
        Output("pokemons-heatmap-filter-instruction", "children"), Output("sel-prev-top", "children"),
        Output("sel-next-top", "children")
    ],
    [
        Input("language-store", "data"),
        Input("area-selector", "value"),
    ]
)
def update_static_translations(lang, current_area):
    lang = lang or "en"

    if current_area:
        area_text = current_area
    else:
        area_text = translate("No Area Selected", lang)


    return (
        translate("Pokémon Analytics", lang),
        translate("Analysis Settings", lang),
        translate("Selected Area", lang), translate("Change", lang),
        translate("Data Source", lang), translate("Stats", lang),
        translate("TTH", lang), translate("SQL", lang),
        translate("Time Window", lang), translate("Date Range", lang),
        translate("Month Range", lang), translate("Start", lang), translate("End", lang),
        translate("Interval", lang), translate("View Mode", lang),
        translate("Actions", lang), translate("Selection Filter", lang), translate("Run Analysis", lang),
        translate("IV Filter", lang), translate("Level Filter", lang),
        translate("Heatmap Display Mode", lang),
        translate("Select an Area", lang), translate("Close", lang),
        translate("Select Pokémon", lang), translate("Select All", lang), translate("Clear", lang), translate("Done", lang),
        translate("Total Counts", lang), translate("Activity Data", lang),
        translate("Raw Data Inspector", lang), translate("Pokémon Filter", lang), translate("Heatmap", lang),
        translate("Click to hide/show Pokémon from map", lang),
        area_text,
        translate("Filter areas by name...", lang),
        f"🔍 {translate('Search Table...', lang)}",
        translate("Search Pokémon...", lang),
        translate("Search Pokémon...", lang),
        translate("Search Pokémon...", lang),
        translate("Click to hide/show Pokémon from map", lang),
        translate("Select specific Pokémon to include. If >75% are selected, 'All' is queried.", lang),
        translate("Prev", lang),
        translate("Next", lang)
    )

# Callback to combine all three data source selectors into one value
@callback(
    [Output("combined-source-store", "data", allow_duplicate=True),
     Output("data-source-selector", "value", allow_duplicate=True),
     Output("data-source-tth-selector", "value", allow_duplicate=True),
     Output("data-source-sql-selector", "value", allow_duplicate=True)],
    [Input("data-source-selector", "value"),
     Input("data-source-tth-selector", "value"),
     Input("data-source-sql-selector", "value")],
    prevent_initial_call=True
)
def combine_data_sources(stats_val, tth_val, sql_val):
    trigger = ctx.triggered_id
    if trigger == "data-source-selector" and stats_val:
        return stats_val, stats_val, None, None
    elif trigger == "data-source-tth-selector" and tth_val:
        return tth_val, None, tth_val, None
    elif trigger == "data-source-sql-selector" and sql_val:
        return sql_val, None, None, sql_val
    return dash.no_update, dash.no_update, dash.no_update, dash.no_update

# Callback to populate shiny month dropdowns
@callback(
    [Output("shiny-start-month", "options"), Output("shiny-start-month", "value"),
     Output("shiny-end-month", "options"), Output("shiny-end-month", "value")],
    Input("combined-source-store", "data")
)
def populate_shiny_months(source):
    # Generate month options for last N months based on retention config
    today = date.today()
    options = []
    for i in range(SHINY_RETENTION_MONTHS):
        # Go back i months
        month = today.month - i
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        month_str = f"{year}{month:02d}"
        month_label = f"{datetime(year, month, 1).strftime('%B %Y')}"
        options.append({"label": month_label, "value": month_str})

    # Default: start = oldest, end = current
    start_val = options[-1]["value"] if options else None
    end_val = options[0]["value"] if options else None

    return options, start_val, options, end_val

@callback(
    [Output("live-controls", "style"), Output("historical-controls", "style"),
     Output("interval-control-container", "style"), Output("heatmap-filters-container", "style"),
     Output("open-selection-modal", "style"), Output("shiny-month-controls", "style")],
    Input("combined-source-store", "data")
)
def toggle_source_controls(source):
    live_s = {"display": "none"}
    hist_s = {"display": "none"}
    int_s = {"display": "none"}
    heat_s = {"display": "none"}
    btn_s = {"display": "none"}
    shiny_s = {"display": "none"}

    if source and "live" in source:
        live_s = {"display": "block"}
    elif source == "sql_heatmap":
        hist_s = {"display": "block", "position": "relative", "zIndex": 1002}
        heat_s = {"display": "block"}
        btn_s = {"display": "block"}
    elif source == "sql_shiny":
        shiny_s = {"display": "block"}
    elif source:
        hist_s = {"display": "block", "position": "relative", "zIndex": 1002}
        int_s = {"display": "block"}

    return live_s, hist_s, int_s, heat_s, btn_s, shiny_s

@callback(
    [Output("mode-selector", "options"), Output("mode-selector", "value"),
     Output("data-source-selector", "options"), Output("data-source-tth-selector", "options"), Output("data-source-sql-selector", "options"),
     Output("heatmap-display-mode", "options"), Output("interval-selector", "options")],
    [Input("combined-source-store", "data"), Input("language-store", "data")],
    [State("mode-persistence-store", "data"), State("mode-selector", "value")]
)
def restrict_modes(source, lang, stored_mode, current_ui_mode):
    lang = lang or "en"

    # Translate Radio Options
    full_options = [
        {"label": translate("Surged (Hourly)", lang), "value": "surged"},
        {"label": translate("Grouped (Table)", lang), "value": "grouped"},
        {"label": translate("Sum (Totals)", lang), "value": "sum"}
    ]
    tth_options = [
        {"label": translate("Surged (Hourly)", lang), "value": "surged"},
        {"label": translate("Sum (Totals)", lang), "value": "sum"}
    ]
    heatmap_options = [{"label": translate("Map View", lang), "value": "map"}]
    shiny_options = [{"label": translate("Grouped (Table)", lang), "value": "grouped"}]

    if source and "tth" in source: allowed = tth_options
    elif source == "sql_heatmap": allowed = heatmap_options
    elif source == "sql_shiny": allowed = shiny_options
    else: allowed = full_options
    allowed_vals = [o['value'] for o in allowed]
    final_val = current_ui_mode if current_ui_mode in allowed_vals else (stored_mode if stored_mode in allowed_vals else allowed_vals[0])

    # Translate Source Selectors
    source_opts = [{"label": translate("Live", lang), "value": "live"}, {"label": translate("Historical", lang), "value": "historical"}]
    tth_opts = [{"label": translate("Live", lang), "value": "live_tth"}, {"label": translate("Historical", lang), "value": "historical_tth"}]
    sql_opts = [{"label": translate("Heatmap", lang), "value": "sql_heatmap"}, {"label": translate("Shiny Odds", lang), "value": "sql_shiny"}]

    heatmap_mode_opts = [
        {"label": translate("Markers", lang), "value": "markers"},
        {"label": translate("Density Heatmap", lang), "value": "density"},
        {"label": translate("Grid Overlay", lang), "value": "grid"}
    ]

    # Interval Options
    interval_opts = [
        {"label": translate("Hourly", lang), "value": "hourly"}
    ]

    return allowed, final_val, source_opts, tth_opts, sql_opts, heatmap_mode_opts, interval_opts

@callback(Output("mode-persistence-store", "data"), Input("mode-selector", "value"), prevent_initial_call=True)
def save_mode(val): return val

@callback(Output("source-persistence-store", "data"), Input("combined-source-store", "data"), prevent_initial_call=True)
def save_source(val): return val

# Use a dummy interval that fires once on page load to trigger source restoration
@callback(
    [Output("data-source-selector", "value"),
     Output("data-source-tth-selector", "value"),
     Output("data-source-sql-selector", "value"),
     Output("combined-source-store", "data")],
    Input("source-persistence-store", "modified_timestamp"),
    State("source-persistence-store", "data"),
    prevent_initial_call=False
)
def load_persisted_source(ts, stored_source):
    """Load persisted data source on page load and set appropriate selector."""
    # Only run on initial page load (ts will be -1 or None initially)
    if ts is not None and ts > 0:
        raise dash.exceptions.PreventUpdate

    stats_sources = ["live", "historical"]
    tth_sources = ["live_tth", "historical_tth"]
    sql_sources = ["sql_heatmap", "sql_shiny"]

    if stored_source in stats_sources:
        return stored_source, None, None, stored_source
    elif stored_source in tth_sources:
        return None, stored_source, None, stored_source
    elif stored_source in sql_sources:
        return None, None, stored_source, stored_source
    # Default fallback
    return "live", None, None, "live"

@callback(Output("heatmap-mode-store", "data"), Input("heatmap-display-mode", "value"))
def update_heatmap_mode_store(val): return val

@callback(Output("iv-range-display", "children"), [Input("iv-slider", "value"), Input("language-store", "data")])
def update_iv_display(value, lang):
    lang = lang or "en"
    if not value or value == [0, 100]:
        return translate("All IVs (0-100%)", lang)
    elif value[0] == value[1]:
        return translate("Exactly {val}% IV", lang).format(val=value[0])
    elif value[1] == 100:
        return translate("IV ≥ {val}%", lang).format(val=value[0])
    else:
        return translate("IV: {min}% - {max}%", lang).format(min=value[0], max=value[1])

@callback(Output("level-range-display", "children"), [Input("level-slider", "value"), Input("language-store", "data")])
def update_level_display(value, lang):
    lang = lang or "en"
    if not value or value == [1, 50]:
        return translate("All Levels (1-50)", lang)
    elif value[0] == value[1]:
        return translate("Exactly Level {val}", lang).format(val=value[0])
    elif value[1] == 50:
        return translate("Level ≥ {val}", lang).format(val=value[0])
    else:
        return translate("Level: {min} - {max}", lang).format(min=value[0], max=value[1])

@callback(
    [Output("area-modal", "is_open"), Output("selection-modal", "is_open"), Output("table-search-input", "style")],
    [Input("open-area-modal", "n_clicks"), Input("close-area-modal", "n_clicks"),
     Input("open-selection-modal", "n_clicks"), Input("close-selection-modal", "n_clicks"),
     Input("mode-selector", "value")],
    [State("area-modal", "is_open"), State("selection-modal", "is_open")]
)
def handle_modals_and_search(ao, ac, so, sc, mode, is_area, is_selection):
    search_style = {"display": "block", "width": "100%"} if mode == "grouped" else {"display": "none"}
    trigger = ctx.triggered_id

    if trigger in ["open-area-modal", "close-area-modal"]:
        return not is_area, is_selection, search_style

    if trigger in ["open-selection-modal", "close-selection-modal"]:
        return is_area, not is_selection, search_style

    return is_area, is_selection, search_style

@callback(Output("area-cards-container", "children"), [Input("area-filter-input", "value"), Input("language-store", "data")], [State("area-selector", "value")])
def filter_area_cards(search, lang, area):
    geos = get_cached_geofences() or []
    if search: geos = [g for g in geos if search.lower() in g['name'].lower()]
    return generate_area_cards(geos, area, lang or "en")

dash.clientside_callback(
    ClientsideFunction(namespace='clientside', function_name='scrollToSelected'),
    Output("clientside-dummy-store", "data"), Input("area-modal", "is_open")
)

# Selection Filter Logic

@callback(
    [Output("selection-grid", "children"),
     Output("sel-page-display-top", "children"),
     Output("selection-page-store", "data"),
     Output("selection-store", "data"),
     Output("selection-select-all", "disabled"),
     Output("select-all-hint", "children"),
     Output("select-all-hint", "className"),
     Output("selection-count-display", "children")],
    [Input("selection-search", "value"),
     Input("sel-prev-top", "n_clicks"),
     Input("sel-next-top", "n_clicks"),
     Input("selection-select-all", "n_clicks"),
     Input("selection-clear", "n_clicks"),
     Input({"type": "selection-item", "index": ALL}, "n_clicks"),
     Input("iv-slider", "value")],
    [State("selection-page-store", "data"),
     State("selection-store", "data"),
     State("language-store", "data")]
)
def update_selection_grid(search, prev_c, next_c, all_c, clear_c, item_clicks, iv_range, page, selected, lang):
    lang = lang or "en"
    trigger = ctx.triggered_id
    options = _load_pokedex_data()
    selected_set = set(selected or [])

    # Calculate IV range span to determine if Select All should be enabled
    iv_span = (iv_range[1] - iv_range[0]) if iv_range else 100
    select_all_enabled = iv_span <= 25

    # If IV range moved outside of 25% and there was a selection, clear it
    if trigger == "iv-slider" and not select_all_enabled and len(selected_set) > 0:
        selected_set = set()
        page = 1

    # Filter options
    if search:
        s = search.lower()
        options = [o for o in options if s in o['search_key'] or s == str(o['pid'])]

    # Handle Bulk Actions
    if trigger == "selection-clear":
        selected_set = set()
        page = 1
    elif trigger == "selection-select-all" and select_all_enabled:
        # Select all CURRENTLY VISIBLE/FILTERED options only if IV range allows
        for o in options:
            selected_set.add(o['key'])
    elif isinstance(trigger, dict) and trigger.get("type") == "selection-item":
        key = trigger["index"]
        if key in selected_set:
            selected_set.remove(key)
        else:
            selected_set.add(key)

    # Pagination
    items_per_page = 100
    total_pages = max(1, (len(options) + items_per_page - 1) // items_per_page)

    if trigger == "sel-prev-top":
        page = max(1, page - 1)
    elif trigger == "sel-next-top":
        page = min(total_pages, page + 1)
    elif trigger == "selection-search":
        page = 1

    # Ensure valid page
    page = max(1, min(page, total_pages))

    start = (page - 1) * items_per_page
    end = start + items_per_page
    visible_items = options[start:end]

    # Render
    grid_children = []
    for item in visible_items:
        key = item['key']
        is_sel = key in selected_set
        cls = "pokemon-filter-item" + (" selected" if is_sel else "")
        display_name = resolve_pokemon_name(item['pid'], item['form'], lang)

        grid_children.append(html.Div(
            [
                html.Img(src=get_pokemon_icon_url(item['pid'], item['form']), style={"width": "48px", "height": "48px", "display": "block"}),
                html.Div(display_name, style={"fontSize": "10px", "textAlign": "center", "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap", "maxWidth": "60px"})
            ],
            id={"type": "selection-item", "index": key},
            className=cls,
            title=display_name
        ))

    page_text = translate("Page {current} / {total} ({count} total)", lang).format(
        current=page, total=total_pages, count=len(options)
    )

    # Dynamic hint message for Select All based on IV range
    if select_all_enabled:
        # Key: "Select All allowed (range: {val}%)"
        hint_str = translate("Select All allowed (range: {val}%)", lang).format(val=iv_span)
        hint_msg = [html.I(className="bi bi-check-circle-fill me-1 text-success"), hint_str]
        hint_class = "text-success"
    else:
        # Key: "Select All disabled (range too wide: {val}%)"
        hint_str = translate("Select All disabled (range too wide: {val}%)", lang).format(val=iv_span)
        hint_msg = [html.I(className="bi bi-x-circle-fill me-1 text-danger"), hint_str]
        hint_class = "text-danger"

    # Selection count message
    selected_count = len(selected_set)
    if selected_count == 0:
        # Key: "No Pokémon selected - selection required to run query"
        count_msg = [html.I(className="bi bi-exclamation-triangle-fill me-1"), translate("No Pokémon selected - selection required to run query", lang)]
    else:
        # Key: "{val} Pokémon selected"
        sel_str = translate("{val} Pokémon selected", lang).format(val=selected_count)
        count_msg = f"✓ {sel_str}"

    return grid_children, page_text, page, list(selected_set), not select_all_enabled, hint_msg, hint_class, count_msg


@callback(
    [Output("raw-data-store", "data"), Output("stats-container", "style"), Output("heatmap-data-store", "data"), Output("notification-area", "children")],
    [Input("submit-btn", "n_clicks"), Input("combined-source-store", "data")],
    [State("area-selector", "value"), State("live-time-input", "value"), State("historical-date-picker", "start_date"),
     State("historical-date-picker", "end_date"), State("interval-selector", "value"),
     State("mode-selector", "value"), State("iv-slider", "value"), State("level-slider", "value"),
     State("selection-store", "data"), State("shiny-start-month", "value"), State("shiny-end-month", "value"),
     State("language-store", "data")]
)
def fetch_data(n, source, area, live_h, start, end, interval, mode, iv_range, level_range, selected_keys, shiny_start, shiny_end, lang):
    lang = lang or "en"
    if not n: return {}, {"display": "none"}, [], None
    if not area: return {}, {"display": "none"}, [], dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), translate("Please select an Area first.", lang)], color="warning", dismissable=True, duration=4000)

    try:
        if source == "sql_shiny":
            logger.info(f"🔍 Starting Shiny Odds Fetch for Area: {area}")

            if not shiny_start or not shiny_end:
                return {}, {"display": "none"}, [], dbc.Alert("Please select start and end months.", color="warning", dismissable=True, duration=4000)

            params = {
                "start_time": shiny_start,
                "end_time": shiny_end,
                "response_format": "json",
                "area": area,
                "username": "all",
                "pokemon_id": "all",
                "form": "all",
                "min_user_n": 0,
                "limit": 0,
                "concurrency": 4
            }

            logger.info("🚀 SENDING SHINY API REQUEST:")
            logger.info(f"   URL: /api/sql/get_shiny_rate_data")
            logger.info(f"   Params: {params}")

            raw_data = get_pokemon_stats("sql_shiny_rate", params)

            if not raw_data:
                return {}, {"display": "block"}, [], dbc.Alert("No Shiny data found for this period.", color="info", dismissable=True, duration=4000)

            return raw_data, {"display": "block"}, [], None

        elif source == "sql_heatmap":
            logger.info(f"🔍 Starting Heatmap Fetch for Area: {area}")

            # IV filter logic
            if iv_range[0] == iv_range[1]:
                iv_str = f"=={iv_range[0]}"
            elif iv_range == [0, 100]:
                iv_str = "all"
            elif iv_range[1] == 100:
                iv_str = f">={iv_range[0]}"
            else:
                iv_str = f">={iv_range[0]},<={iv_range[1]}"

            # Level filter logic
            if level_range[0] == level_range[1]:
                level_str = f"=={level_range[0]}"
            elif level_range == [1, 50]:
                level_str = "all"
            elif level_range[1] == 50:
                level_str = f">={level_range[0]}"
            else:
                level_str = f">={level_range[0]},<={level_range[1]}"

            # Pokemon Selection Logic
            all_options = _load_pokedex_data()
            total_count = len(all_options)
            selected_count = len(selected_keys) if selected_keys else 0

            # Show red warning if no Pokemon selected
            if selected_count == 0:
                return {}, {"display": "none"}, [], dbc.Alert([
                    html.I(className="bi bi-exclamation-triangle-fill me-2"),
                    html.Strong(translate("No Pokémon Selected!", lang)),
                    translate("Please open the Selection Filter and choose at least one Pokémon to query.", lang)
                ], color="danger", dismissable=True, duration=6000)

            # If > 75% selected, query ALL for efficiency
            if (selected_count / total_count) > 0.75:
                pids = "all"
                forms = "all"
            else:
                # Build PID and Form lists
                # For form 0 selections, also include corresponding _NORMAL form for backwards compatibility
                sel_pids = set()
                sel_forms = set()

                # Build a lookup for _NORMAL forms: {pid: normal_form_id}
                normal_form_lookup = {}
                for form_key, form_val in _FORM_MAP.items():
                    if form_key.endswith("_NORMAL"):
                        species_name = form_key.replace("_NORMAL", "")
                        if species_name in _SPECIES_MAP:
                            normal_form_lookup[_SPECIES_MAP[species_name]] = form_val

                for key in selected_keys:
                    p, f = key.split(":")
                    sel_pids.add(p)
                    sel_forms.add(f)

                    # If form 0 is selected, also add the _NORMAL form equivalent
                    if f == "0":
                        pid_int = int(p)
                        if pid_int in normal_form_lookup:
                            sel_forms.add(str(normal_form_lookup[pid_int]))

                pids = ",".join(sel_pids)
                forms = ",".join(sel_forms)

            params = {
                "start_time": f"{start}T00:00:00",
                "end_time": f"{end}T23:59:59",
                "area": area,
                "response_format": "json",
                "iv": iv_str,
                "level": level_str,
                "pokemon_id": pids,
                "form": forms
            }

            logger.info("🚀 SENDING API REQUEST:")
            logger.info(f"   URL: /api/sql/get_pokemon_heatmap_data")
            param_str = str(params)
            logger.info(f"   Params: {param_str}")

            raw_data = get_pokemon_stats("sql_heatmap", params)

            safe_data = []
            if raw_data is not None and isinstance(raw_data, list):
                logger.info(f"✅ API returned {len(raw_data)} records")
                df = pd.DataFrame(raw_data)
                if "pokemon_id" in df.columns:
                    if "form" not in df.columns: df["form"] = 0
                    grouped = df.groupby(['latitude', 'longitude', 'pokemon_id', 'form'])['count'].sum().reset_index()

                    # Add Pokemon names and icon URLs for display in JS
                    def add_pokemon_info(row, lang=lang):
                        species, form_name = resolve_pokemon_name_parts(row['pokemon_id'], row['form'], lang)
                        icon_url = get_pokemon_icon_url(row['pokemon_id'], row['form'])
                        return pd.Series({'species_name': species, 'form_name': form_name or '', 'icon_url': icon_url})

                    info_data = grouped.apply(add_pokemon_info, axis=1)
                    grouped['species_name'] = info_data['species_name']
                    grouped['form_name'] = info_data['form_name']
                    grouped['icon_url'] = info_data['icon_url']

                    safe_data = grouped.to_dict('records')
                    logger.info(f"📉 Aggregated to {len(safe_data)} unique map points")
            else:
                logger.warning(f"⚠️ API returned empty or invalid data: {type(raw_data)}")

            if not safe_data:
                # Still show the map container even if empty, so the user knows the query ran
                return {}, {"display": "block"}, [], dbc.Alert("No Heatmap data found for criteria.", color="info", dismissable=True, duration=4000)

            return {}, {"display": "block"}, safe_data, None

        elif "tth" in source:
            if "live" in source:
                hours = max(1, min(int(live_h or 1), MAX_RETENTION_HOURS))
                params = {"start_time": f"{hours} hours", "end_time": "now", "mode": mode, "area": area, "response_format": "json"}
                data = get_pokemon_stats("tth_timeseries", params)
            else:
                params = {"counter_type": "tth", "interval": interval, "start_time": f"{start}T00:00:00", "end_time": f"{end}T23:59:59", "mode": mode, "area": area, "response_format": "json"}
                data = get_pokemon_stats("counter", params)
        elif source == "live":
            hours = max(1, min(int(live_h or 1), MAX_RETENTION_HOURS))
            params = {"start_time": f"{hours} hours", "end_time": "now", "mode": mode, "area": area, "interval": "hourly", "response_format": "json"}
            data = get_pokemon_stats("timeseries", params)
        else:
            params = {"counter_type": "totals", "interval": interval, "start_time": f"{start}T00:00:00", "end_time": f"{end}T23:59:59", "mode": mode, "area": area, "response_format": "json"}
            data = get_pokemon_stats("counter", params)

        if not data: return {}, {"display": "block"}, [], dbc.Alert(translate("No data found for this period.", lang), color="warning", dismissable=True, duration=4000)
        return data, {"display": "block"}, [], None
    except Exception as e:
        logger.error(f"❌ FETCH ERROR: {e}", exc_info=True)
        return {}, {"display": "none"}, [], dbc.Alert(f"Error: {str(e)}", color="danger", dismissable=True)

@callback(
    Output("table-sort-store", "data"), Input({"type": "sort-header", "index": ALL}, "n_clicks"), State("table-sort-store", "data"), prevent_initial_call=True
)
def update_sort_order(n_clicks, current_sort):
    if not ctx.triggered_id or not any(n_clicks): return dash.no_update
    col = ctx.triggered_id['index']
    return {"col": col, "dir": "asc" if current_sort['col'] == col and current_sort['dir'] == "desc" else "desc"}

@callback(
    Output("table-page-store", "data"),
    [Input("first-page-btn", "n_clicks"), Input("prev-page-btn", "n_clicks"), Input("next-page-btn", "n_clicks"), Input("last-page-btn", "n_clicks"), Input("rows-per-page-selector", "value"), Input("goto-page-input", "value")],
    [State("table-page-store", "data"), State("total-pages-store", "data")],
    prevent_initial_call=True
)
def update_pagination(first, prev, next, last, rows, goto, state, total_pages):
    trigger = ctx.triggered_id
    if not trigger: return dash.no_update
    current = state.get('current_page', 1)
    total_pages = total_pages or 1
    new_page = current
    if trigger == "first-page-btn": new_page = 1
    elif trigger == "last-page-btn": new_page = total_pages
    elif trigger == "prev-page-btn": new_page = max(1, current - 1)
    elif trigger == "next-page-btn": new_page = min(total_pages, current + 1)
    elif trigger == "goto-page-input":
        if goto is not None: new_page = min(total_pages, max(1, goto))
    elif trigger == "rows-per-page-selector": return {"current_page": 1, "rows_per_page": rows}
    return {**state, "current_page": new_page, "rows_per_page": state.get('rows_per_page', 25)}

# Quick Filter Callbacks

@callback(
    [Output("pokemon-quick-filter-grid", "children"), Output("pokemon-quick-filter-count", "children")],
    [Input("heatmap-data-store", "data"),
     Input("pokemon-quick-filter-search", "value"),
     Input("language-store", "data")],
    [State("combined-source-store", "data"),
     State("heatmap-hidden-pokemon", "data")]
)
def populate_pokemon_quick_filter(heatmap_data, search_term, lang, source, hidden_pokemon):
    """Populate Pokemon image grid for quick filtering - fluid search"""
    lang = lang or "en"
    if source != "sql_heatmap" or not heatmap_data:
        return [], ""

    # Process Data
    pokemon_set = {}
    for record in heatmap_data:
        pid = record.get('pokemon_id')
        form = record.get('form') or 0
        key = f"{pid}:{form}"
        if key not in pokemon_set:
            pokemon_set[key] = {
                'pid': int(pid) if pid else 0,
                'form': int(form) if form else 0,
                'count': record.get('count', 0)
            }
        else:
            pokemon_set[key]['count'] += record.get('count', 0)

    # Sort by count descending, then ID
    sorted_pokemon = sorted(pokemon_set.items(), key=lambda x: (-x[1]['count'], x[1]['pid']))

    #  Filter Search
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
            html.Img(src=get_pokemon_icon_url(data['pid'], data['form']),
                    style={"width": "40px", "height": "40px", "display": "block"}),
            html.Div(f"{data['count']}",
                    style={"fontSize": "10px", "textAlign": "center", "marginTop": "2px", "color": "#aaa"})
        ], id={"type": "pokemon-quick-filter-icon", "index": key}, style=style,
           title=f"{resolve_pokemon_name(data['pid'], data['form'], lang)}: {data['count']} spawns"))

    count_text = f"({len(filtered_list)}/{len(sorted_pokemon)})" if search_lower else f"({len(sorted_pokemon)})"

    return pokemon_images, count_text

# Clientside callback to update icon opacity without rebuilding the grid
dash.clientside_callback(
    """
    function(hiddenPokemon) {
        if (!hiddenPokemon) hiddenPokemon = [];
        var hiddenSet = new Set(hiddenPokemon);

        // Find the grid container and iterate its children
        var grid = document.getElementById('pokemon-quick-filter-grid');
        if (!grid) return window.dash_clientside.no_update;

        var icons = grid.children;
        for (var i = 0; i < icons.length; i++) {
            var icon = icons[i];
            try {
                var idObj = JSON.parse(icon.id);
                if (idObj.type === 'pokemon-quick-filter-icon') {
                    var key = idObj.index;
                    icon.style.opacity = hiddenSet.has(key) ? '0.3' : '1';
                }
            } catch(e) {}
        }

        return window.dash_clientside.no_update;
    }
    """,
    Output("pokemon-quick-filter-grid", "className"),
    Input("heatmap-hidden-pokemon", "data"),
    prevent_initial_call=True
)

@callback(
    Output("heatmap-hidden-pokemon", "data", allow_duplicate=True),
    [Input({"type": "pokemon-quick-filter-icon", "index": ALL}, "n_clicks"),
     Input("pokemon-quick-filter-show-all", "n_clicks"),
     Input("pokemon-quick-filter-hide-all", "n_clicks")],
    [State("heatmap-hidden-pokemon", "data"),
     State("heatmap-data-store", "data")],
    prevent_initial_call=True
)
def toggle_pokemon_visibility(icon_clicks, show_clicks, hide_clicks, hidden_list, heatmap_data):
    """Toggle Pokemon visibility in quick filter"""
    trigger = ctx.triggered_id
    if not trigger:
        return dash.no_update

    # Button Logic
    if trigger == "pokemon-quick-filter-show-all":
        return []

    if trigger == "pokemon-quick-filter-hide-all":
        if not heatmap_data: return []
        # Generate list of all keys currently in the heatmap
        all_keys = set()
        for record in heatmap_data:
            pid = record.get('pokemon_id')
            form = record.get('form') or 0
            all_keys.add(f"{pid}:{form}")
        return list(all_keys)

    # Icon Click Logic - must verify an actual click occurred
    if isinstance(trigger, dict) and trigger.get('type') == 'pokemon-quick-filter-icon':
        # Check if any icon was actually clicked (n_clicks > 0)
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
    Output("heatmap-hidden-pokemon", "data", allow_duplicate=True),
    Input("heatmap-data-store", "data"),
    prevent_initial_call=True
)
def reset_hidden_pokemon_on_new_data(heatmap_data):
    """Reset hidden Pokemon list when new heatmap data arrives"""
    # Only reset if we actually have new data (not empty)
    if heatmap_data:
        return []
    return dash.no_update

@callback(
    [Output("total-counts-display", "children"), Output("main-visual-container", "children"), Output("raw-data-display", "children"), Output("total-pages-store", "data"),
     Output("heatmap-map-container", "style"), Output("main-visual-container", "style"), Output("heatmap-toggle-container", "style"), Output("quick-filter-container", "style")],
    [Input("raw-data-store", "data"), Input("table-search-input", "value"), Input("table-sort-store", "data"), Input("table-page-store", "data"), Input("heatmap-data-store", "data"), Input("language-store", "data")],
    [State("mode-selector", "value"), State("combined-source-store", "data")]
)
def update_visuals(data, search_term, sort, page, heatmap_data, lang, mode, source):
    lang = lang or "en"
    if source == "sql_heatmap":
        count = len(heatmap_data) if heatmap_data else 0
        raw_text = json.dumps(heatmap_data, indent=2)
        total_div = [html.H1(f"{count:,} Spawns", className="text-primary")]
        return total_div, html.Div(), raw_text, 1, {"height": "600px", "width": "100%", "display": "block"}, {"display": "none"}, {"display": "block", "float": "right"}, {"display": "block"}

    # Handle Shiny Odds data
    if source == "sql_shiny" and data:
        raw_text = json.dumps(data, indent=2)

        # Parse shiny data
        if isinstance(data, list):
            shiny_df = pd.DataFrame(data)
        else:
            return [], html.Div("Invalid data format"), raw_text, 1, {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}

        if shiny_df.empty:
            return [], html.Div("No shiny data"), raw_text, 1, {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}

        # Search filter
        if search_term:
            def build_search_name(row, lang=lang):
                species, form = resolve_pokemon_name_parts(row['pokemon_id'], int(row['form']) if row['form'] else 0, lang)
                return f"{species} {form}".lower() if form else species.lower()
            shiny_df['search_name'] = shiny_df.apply(build_search_name, axis=1)
            shiny_df = shiny_df[shiny_df['search_name'].str.contains(search_term.lower(), na=False)]

        # Total counts sidebar
        total_encounters = shiny_df['total_encounters'].sum() if 'total_encounters' in shiny_df.columns else 0
        total_pokemon = len(shiny_df)
        shiny_eligible = len(shiny_df[shiny_df['shiny_pct_pooled'] > 0]) if 'shiny_pct_pooled' in shiny_df.columns else 0

        icon_base_url = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main"
        total_div = [
            html.H1(f"{total_pokemon:,}", className="text-primary"),
            html.Div("Pokémon Species", className="text-muted mb-3"),
            html.Div([
                html.Img(src=f"{icon_base_url}/misc/sparkles.webp", style={"width": "28px", "marginRight": "10px", "verticalAlign": "middle"}),
                html.Span(f"{shiny_eligible:,} " + translate("Shiny Eligible", lang), style={"fontWeight": "bold"})
            ], className="d-flex align-items-center mb-2"),
            html.Div([
                html.I(className="bi bi-eye-fill me-2", style={"fontSize": "1.2rem"}),
                html.Span(f"{total_encounters:,} " + translate("Encounters", lang), style={"fontWeight": "bold"})
            ], className="d-flex align-items-center mb-2"),
        ]

        # Sorting
        col, ascending = sort['col'], sort['dir'] == "asc"
        if col in shiny_df.columns:
            shiny_df = shiny_df.sort_values(col, ascending=ascending)
        else:
            shiny_df = shiny_df.sort_values('total_encounters', ascending=False)

        # Pagination
        rows_per_page = page['rows_per_page']
        total_rows = len(shiny_df)
        total_pages_val = max(1, (total_rows + rows_per_page - 1) // rows_per_page)
        current_page = min(max(1, page['current_page']), total_pages_val)
        page_df = shiny_df.iloc[(current_page - 1) * rows_per_page : current_page * rows_per_page]

        # Build table header
        header_style = {"backgroundColor": "#1a1a1a", "position": "sticky", "top": "0", "zIndex": "10", "textAlign": "center", "verticalAlign": "middle"}

        header_cells = [
            html.Th(translate("Image", lang), style={**header_style, "width": "60px"}),
            html.Th(html.Span([translate("Pokémon", lang), html.Span(" ▲" if col == 'pokemon_id' and ascending else (" ▼" if col == 'pokemon_id' else ""), style={"color": "#aaa"})],
                    id={"type": "sort-header", "index": "pokemon_id"}, style={"cursor": "pointer"}), style=header_style),
            html.Th(html.Span([translate("Shiny Rate", lang), html.Span(" ▲" if col == 'shiny_pct_pooled' and ascending else (" ▼" if col == 'shiny_pct_pooled' else ""), style={"color": "#aaa"})],
                    id={"type": "sort-header", "index": "shiny_pct_pooled"}, style={"cursor": "pointer"}), style=header_style),
            html.Th(html.Span([translate("Encounters", lang), html.Span(" ▲" if col == 'total_encounters' and ascending else (" ▼" if col == 'total_encounters' else ""), style={"color": "#aaa"})],
                    id={"type": "sort-header", "index": "total_encounters"}, style={"cursor": "pointer"}), style=header_style),
            html.Th(html.Span([translate("Users", lang), html.Span(" ▲" if col == 'users_contributing' and ascending else (" ▼" if col == 'users_contributing' else ""), style={"color": "#aaa"})],
                    id={"type": "sort-header", "index": "users_contributing"}, style={"cursor": "pointer"}), style=header_style),
        ]

        # Build table rows
        rows = []
        for i, (_, r) in enumerate(page_df.iterrows()):
            bg = "#1a1a1a" if i % 2 == 0 else "#242424"
            cell_style = {"backgroundColor": bg, "verticalAlign": "middle", "textAlign": "center"}

            pid = r['pokemon_id']
            form = int(r['form']) if r['form'] else 0
            species_name, form_name = resolve_pokemon_name_parts(pid, form, lang)

            # Format shiny rate - convert from percentage to 1/X odds
            shiny_pct = r.get('shiny_pct_pooled', 0)
            if shiny_pct and shiny_pct > 0:
                odds = int(100 / shiny_pct)
                shiny_display = html.Span([
                    html.Img(src=f"{icon_base_url}/misc/sparkles.webp", style={"width": "16px", "marginRight": "4px", "verticalAlign": "middle"}),
                    f"1/{odds}"
                ], style={"color": "#ffd700", "fontWeight": "bold"})
            else:
                shiny_display = html.Span("—", style={"color": "#666"})

            # Pokemon name cell
            if form_name:
                name_cell = html.Div([
                    html.Div(species_name, style={"fontWeight": "bold", "lineHeight": "1.2"}),
                    html.Div(form_name, style={"fontSize": "11px", "color": "#aaa", "lineHeight": "1.2"})
                ], style={"textAlign": "center"})
            else:
                name_cell = html.Div(species_name, style={"fontWeight": "bold", "textAlign": "center"})

            rows.append(html.Tr([
                html.Td(html.Img(src=get_pokemon_icon_url(pid, form), style={"width": "40px", "height": "40px", "display": "block", "margin": "auto"}), style=cell_style),
                html.Td(name_cell, style=cell_style),
                html.Td(shiny_display, style=cell_style),
                html.Td(f"{int(r.get('total_encounters', 0)):,}", style=cell_style),
                html.Td(f"{int(r.get('users_contributing', 0)):,}", style=cell_style),
            ]))

        # Pagination controls
        controls = html.Div([
            dbc.Row([
                dbc.Col([
                    html.Span(f"{translate('Total', lang)}: {total_rows} | {translate('Rows', lang)}: ", className="me-2 align-middle"),
                    dcc.Dropdown(
                        id="rows-per-page-selector",
                        options=[{'label': str(x), 'value': x} for x in [10, 25, 50, 100]] + [{'label': translate('All', lang), 'value': total_rows}],
                        value=rows_per_page, clearable=False, className="rows-per-page-selector",
                        style={"width": "80px", "display": "inline-block", "color": "black", "verticalAlign": "middle"}
                    )
                ], width="auto", className="d-flex align-items-center"),
                dbc.Col([
                    dbc.ButtonGroup([
                        dbc.Button("<<", id="first-page-btn", size="sm", disabled=current_page <= 1),
                        dbc.Button("<", id="prev-page-btn", size="sm", disabled=current_page <= 1)
                    ], className="me-2"),
                    html.Span(f"{translate('Page', lang)} ", className="align-middle me-1"),
                    dcc.Input(id="goto-page-input", type="number", min=1, max=total_pages_val, value=current_page, debounce=True,
                              style={"width": "60px", "textAlign": "center", "display": "inline-block", "color": "black"}),
                    html.Span(f" of {total_pages_val}", className="align-middle ms-1 me-2"),
                    dbc.ButtonGroup([
                        dbc.Button(">", id="next-page-btn", size="sm", disabled=current_page >= total_pages_val),
                        dbc.Button(">>", id="last-page-btn", size="sm", disabled=current_page >= total_pages_val)
                    ]),
                ], width="auto", className="d-flex align-items-center justify-content-end ms-auto")
            ], className="g-0")
        ], className="p-2 bg-dark rounded mb-2 border border-secondary")

        visual_content = html.Div([
            controls,
            html.Div(
                html.Table([html.Thead(html.Tr(header_cells)), html.Tbody(rows)], style={"width": "100%", "color": "#fff"}),
                style={"overflowX": "auto", "maxHeight": "600px", "overflowY": "auto"}
            )
        ])

        return total_div, visual_content, raw_text, total_pages_val, {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}

    if not data: return [], html.Div(), "", 1, {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}
    df = parse_data_to_df(data, mode, source, lang)
    if df.empty: return "No Data", html.Div(), json.dumps(data, indent=2), 1, {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}

    if mode == "grouped" and search_term:
        # Build searchable name column using species + form names
        def build_search_name(row, lang=lang):
            species, form = resolve_pokemon_name_parts(row['pid'], row['form'], lang)
            return f"{species} {form}".lower() if form else species.lower()
        df['search_name'] = df.apply(build_search_name, axis=1)
        df = df[df['search_name'].str.contains(search_term.lower(), na=False)]

    total_div = html.P("No data.")
    icon_base_url = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main"
    metric_icons = {
        "iv0": "/assets/images/0iv.png", "iv100": "/assets/images/100iv.png",
        "pvp_little": f"{icon_base_url}/misc/500.webp", "pvp_great": f"{icon_base_url}/misc/1500.webp", "pvp_ultra": f"{icon_base_url}/misc/2500.webp", "shiny": f"{icon_base_url}/misc/sparkles.webp"
    }

    if "metric" in df.columns:
        totals = df.groupby("metric")["count"].sum().reset_index()
        val = totals[totals['metric'] == 'total']['count'].sum() if 'total' in totals['metric'].values else totals['count'].sum()
        total_div = [html.H1(f"{val:,}", className="text-primary")]
        display_df = totals.sort_values('count', ascending=False).head(20) if "tth" in source and len(totals) > 20 else totals
        for _, r in display_df.iterrows():
            if r['metric'] == 'total': continue

            # Updated: Check if count > 0 before displaying
            if r['count'] <= 0: continue

            m_key = str(r['metric']).lower()
            cnt = f"{r['count']:,}"
            if m_key in metric_icons:
                total_div.append(html.Div([html.Img(src=metric_icons[m_key], style={"width": "28px", "marginRight": "10px", "verticalAlign": "middle"}), html.Span(cnt, style={"fontWeight":"bold"})], className="d-flex align-items-center mb-1"))
            else:
                total_div.append(html.Div(f"{r['metric']}: {cnt}", className="text-muted small"))

    visual_content = html.Div("No data")
    total_pages_val = 1

    if mode == "grouped" and "tth" not in source and not df.empty:
        pivot = df.pivot_table(index=['pid', 'form', 'key'], columns='metric', values='count', fill_value=0).reset_index()
        col, ascending = sort['col'], sort['dir'] == "asc"
        if col == 'key': pivot = pivot.sort_values(['pid', 'form'], ascending=ascending)
        elif col in pivot.columns: pivot = pivot.sort_values(col, ascending=ascending)
        else: pivot = pivot.sort_values(pivot.columns[3], ascending=False)

        rows_per_page = page['rows_per_page']
        total_rows = len(pivot)
        total_pages_val = max(1, (total_rows + rows_per_page - 1) // rows_per_page)
        current_page = min(max(1, page['current_page']), total_pages_val)
        page_df = pivot.iloc[(current_page - 1) * rows_per_page : current_page * rows_per_page]

        header_cells = [html.Th(translate("Image", lang), style={"backgroundColor": "#1a1a1a", "width": "60px", "position": "sticky", "top": "0", "zIndex": "10", "textAlign": "center", "verticalAlign": "middle"})]
        header_cells.append(html.Th(html.Span([translate("Pokémon", lang), html.Span(" ▲" if col == 'key' and ascending else (" ▼" if col == 'key' else ""), style={"color": "#aaa", "marginLeft": "5px"})], id={"type": "sort-header", "index": "key"}, style={"cursor": "pointer"}), style={"backgroundColor": "#1a1a1a", "position": "sticky", "top": "0", "zIndex": "10", "textAlign": "center", "verticalAlign": "middle"}))

        for c in [x for x in pivot.columns if x not in ['pid', 'form', 'key']]:
             if c.lower() in metric_icons:
                 label = html.Img(src=metric_icons[c.lower()], style={"width":"24px", "height":"24px", "verticalAlign":"middle"})
             else:
                 label = translate(c.title(), lang)

             arrow = " ▲" if col == c and ascending else (" ▼" if col == c else "")
             header_cells.append(html.Th(html.Span([label, html.Span(arrow, style={"color": "#aaa"})], id={"type": "sort-header", "index": c}, style={"cursor": "pointer", "display":"inline-flex", "alignItems":"center", "justifyContent": "center"}), style={"backgroundColor": "#1a1a1a", "position": "sticky", "top": "0", "zIndex": "10", "textAlign": "center", "verticalAlign": "middle"}))

        header_row = html.Tr(header_cells)

        rows = []
        for i, r in enumerate(page_df.iterrows()):
            _, r = r
            bg = "#1a1a1a" if i % 2 == 0 else "#242424"

            # Get species name and form name parts
            species_name, form_name = resolve_pokemon_name_parts(r['pid'], r['form'], lang)

            # Build Pokemon name cell with bold species name and form name below
            if form_name:
                name_content = html.Div([
                    html.Div(species_name, style={"fontWeight": "bold", "lineHeight": "1.2"}),
                    html.Div(form_name, style={"fontSize": "11px", "color": "#aaa", "lineHeight": "1.2"})
                ], style={"textAlign": "center"})
            else:
                name_content = html.Div(species_name, style={"fontWeight": "bold", "textAlign": "center"})

            cells = [
                html.Td(html.Img(src=get_pokemon_icon_url(r['pid'], r['form']), style={"width":"40px", "height":"40px", "display":"block", "margin":"auto"}), style={"backgroundColor":bg, "textAlign": "center", "verticalAlign": "middle"}),
                html.Td(name_content, style={"backgroundColor":bg, "textAlign": "center", "verticalAlign": "middle"})
            ]
            for m in [x for x in pivot.columns if x not in ['pid', 'form', 'key']]:
                cells.append(html.Td(f"{int(r[m]):,}", style={"backgroundColor":bg, "textAlign": "center", "verticalAlign": "middle"}))
            rows.append(html.Tr(cells))

        controls = html.Div([
            dbc.Row([
                dbc.Col([html.Span(f"{translate('Total', lang)}: {total_rows} | {translate('Rows', lang)}: ", className="me-2 align-middle"), dcc.Dropdown(id="rows-per-page-selector", options=[{'label': str(x), 'value': x} for x in [10, 25, 50, 100]] + [{'label': translate('All', lang), 'value': total_rows}], value=rows_per_page, clearable=False, className="rows-per-page-selector", style={"width":"80px", "display":"inline-block", "color":"black", "verticalAlign": "middle"})], width="auto", className="d-flex align-items-center"),
                dbc.Col([
                    dbc.ButtonGroup([dbc.Button("<<", id="first-page-btn", size="sm", disabled=current_page <= 1), dbc.Button("<", id="prev-page-btn", size="sm", disabled=current_page <= 1)], className="me-2"),
                    html.Span(f"{translate('Page', lang)} ", className="align-middle me-1"), dcc.Input(id="goto-page-input", type="number", min=1, max=total_pages_val, value=current_page, debounce=True, style={"width": "60px", "textAlign": "center", "display": "inline-block", "color": "black"}), html.Span(f" of {total_pages_val}", className="align-middle ms-1 me-2"),
                    dbc.ButtonGroup([dbc.Button(">", id="next-page-btn", size="sm", disabled=current_page >= total_pages_val), dbc.Button(">>", id="last-page-btn", size="sm", disabled=current_page >= total_pages_val)]),
                ], width="auto", className="d-flex align-items-center justify-content-end ms-auto")
            ], className="g-0")
        ], className="p-2 bg-dark rounded mb-2 border border-secondary")

        visual_content = html.Div([controls, html.Div(html.Table([html.Thead(header_row), html.Tbody(rows)], style={"width":"100%", "color":"#fff"}), style={"overflowX":"auto", "maxHeight":"600px"})])

    elif mode in ["surged", "sum"]:
        graph_df = df[df['metric'] != 'total'].copy()
        fig = go.Figure()
        sorter = lambda x: int(x.split('_')[0]) if x.split('_')[0].isdigit() else x

        if mode == "sum":
            d = graph_df.copy()
            d['sort'] = d['metric'].apply(sorter)
            d = d.sort_values('sort') if pd.to_numeric(d['sort'], errors='coerce').notna().all() else d.sort_values('metric')

            bar_colors, border_colors, border_widths, patterns = [], [], [], []
            for m in d['metric']:
                m_lower = str(m).lower()
                c, bc, bw, pat = "#6c757d", "#6c757d", 0, ""
                tth_match = re.match(r"^(\d+)_(\d+)$", m_lower)

                if tth_match:
                    start_val = int(tth_match.group(1))
                    if 0 <= start_val < 30:
                        target, max_dist = 25, 25
                        dist = abs(target - start_val)
                        alpha = max(0.2, min(1.0, 0.3 + 0.7 * (1 - (dist / max_dist))))
                        c = f"rgba(0, 123, 255, {alpha:.2f})"
                    elif 30 <= start_val < 60:
                        target, max_dist = 55, 25
                        dist = abs(target - start_val)
                        alpha = max(0.2, min(1.0, 0.3 + 0.7 * (1 - (dist / max_dist))))
                        c = f"rgba(220, 53, 69, {alpha:.2f})"
                elif "iv0" in m_lower: c = "#28a745"
                elif "iv100" in m_lower: c = "#dc3545"
                elif "great" in m_lower: c = "#007bff"
                elif "little" in m_lower or "litle" in m_lower: c, pat = "#dc3545", "/"
                elif "ultra" in m_lower: c, bc, bw = "#000000", "#FFD700", 3
                elif "shiny" in m_lower: c = "#FFD700"

                bar_colors.append(c)
                border_colors.append(bc)
                border_widths.append(bw)
                patterns.append(pat)

            fig.add_trace(go.Bar(x=d['metric'], y=d['count'], marker=dict(color=bar_colors, line=dict(color=border_colors, width=border_widths), pattern=dict(shape=patterns, bgcolor="#ffffff", fgcolor=bar_colors))))

            max_y = d['count'].max() if not d.empty else 10
            icon_size_y = max_y * 0.15
            for idx, row in d.iterrows():
                m_key = str(row['metric']).lower()
                if m_key in metric_icons:
                    fig.add_layout_image(dict(source=metric_icons[m_key], x=row['metric'], y=row['count'], xref="x", yref="y", sizex=0.6, sizey=icon_size_y, xanchor="center", yanchor="bottom"))
            fig.update_layout(margin=dict(t=50))
            fig.update_yaxes(range=[0, max_y * 1.25])

        else:
            if 'time_bucket' in graph_df.columns:
                 graph_df['time_bucket'] = pd.to_numeric(graph_df['time_bucket'], errors='coerce').fillna(0).astype(int)
                 agg = graph_df if "live" in source else graph_df.groupby(["time_bucket", "metric"])["count"].sum().reset_index()
                 for m in sorted(agg['metric'].unique(), key=sorter):
                     d = agg[agg['metric'] == m].sort_values("time_bucket")
                     fig.add_trace(go.Scatter(x=d['time_bucket'], y=d['count'], mode='lines+markers', name=str(m)))
                 fig.update_xaxes(range=[-0.5, 23.5], dtick=1)

        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', title=f"{translate(mode.title(), lang)} {translate('Data', lang)}")
        visual_content = dcc.Graph(figure=fig, id="main-graph")

    return total_div, visual_content, json.dumps(data, indent=2), total_pages_val, {"display": "none"}, {"display": "block"}, {"display": "none"}, {"display": "none"}

# Update to use the correctly namespaced JS function
dash.clientside_callback(
    ClientsideFunction(namespace='clientside', function_name='triggerHeatmapRenderer'),
    Output("heatmap-map-container", "children"),
    Input("heatmap-data-store", "data"),
    Input("heatmap-hidden-pokemon", "data"),
    Input("heatmap-mode-store", "data"),
)
