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

dash.register_page(__name__, path='/pokemon', title='Pokémon Analytics')

try:
    RETENTION_MS = AppConfig.timeseries_pokemon_retention_ms
    MAX_RETENTION_HOURS = int(RETENTION_MS / 3600000)
except:
    MAX_RETENTION_HOURS = 72

ICON_BASE_URL = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main"

_SPECIES_MAP = None
_FORM_MAP = None
_ALL_VALID_FORMS = None
_HEATMAP_VALID_FORMS = None  # For Heatmap(SQL) - no form 0 entries

def safe_int(value):
    if value is None: return 0
    if isinstance(value, str):
        if value.lower() == "none" or value == "": return 0
        try: return int(float(value))
        except: return 0
    if isinstance(value, (int, float)): return int(value)
    return 0

def _get_species_map():
    global _SPECIES_MAP
    if _SPECIES_MAP is None:
        try:
            path = os.path.join(os.path.dirname(__file__), '..', 'assets', 'pokedex_id.json')
            if not os.path.exists(path): path = os.path.join(os.getcwd(), 'assets', 'pokedex_id.json')
            with open(path, 'r') as f:
                data = json.load(f)
                _SPECIES_MAP = {v: k.replace("_", " ").title() for k, v in data.items()}
        except: _SPECIES_MAP = {}
    return _SPECIES_MAP

def _get_form_map():
    global _FORM_MAP
    if _FORM_MAP is None:
        try:
            path = os.path.join(os.path.dirname(__file__), '..', 'assets', 'pokedex.json')
            if not os.path.exists(path): path = os.path.join(os.getcwd(), 'assets', 'pokedex.json')
            with open(path, 'r') as f:
                data = json.load(f)
                _FORM_MAP = {v: k.replace("_", " ").title() for k, v in data.items()}
        except: _FORM_MAP = {}
    return _FORM_MAP

def _get_all_valid_forms():
    """
    Generates a master list of all Pokemon (PID:0) AND their specific forms from pokedex.json.
    """
    global _ALL_VALID_FORMS
    if _ALL_VALID_FORMS is None:
        species_map = _get_species_map()
        form_map = _get_form_map()

        master_list = []
        # Add Base Forms - Form 0
        for pid, name in species_map.items():
            master_list.append({'pid': pid, 'form': 0, 'name': name, 'key': f"{pid}:0"})

        # Add Mapped Forms
        name_to_pid = {v.upper(): k for k, v in species_map.items()}

        for fid, fname in form_map.items():
            if fid == 0: continue
            fname_upper = fname.upper()
            matched_pid = None
            for sname in sorted(name_to_pid.keys(), key=len, reverse=True):
                if fname_upper.startswith(sname):
                    matched_pid = name_to_pid[sname]
                    break
            if matched_pid:
                master_list.append({'pid': matched_pid, 'form': fid, 'name': fname, 'key': f"{matched_pid}:{fid}"})

        _ALL_VALID_FORMS = master_list

    return _ALL_VALID_FORMS

def _get_heatmap_valid_forms():
    """
    Generates a master list for Heatmap(SQL) filter using LOCAL files.
    Only includes forms that actually exist in pokedex.json (no artificial form 0).
    Example: bulbasaur_normal (form 163) and bulbasaur_fall_2019 (form 897)
    Sorted by Pokemon ID for consistent ordering.
    """
    global _HEATMAP_VALID_FORMS
    if _HEATMAP_VALID_FORMS is None:
        species_map = _get_species_map()
        form_map = _get_form_map()

        master_list = []
        # Build name to PID lookup
        name_to_pid = {v.upper(): k for k, v in species_map.items()}

        # Only add forms that exist in pokedex.json - no form 0
        for fid, fname in form_map.items():
            fname_upper = fname.upper()
            matched_pid = None
            # Match form name to species BULBASAUR_NORMAL - pid 1
            for sname in sorted(name_to_pid.keys(), key=len, reverse=True):
                if fname_upper.startswith(sname):
                    matched_pid = name_to_pid[sname]
                    break
            if matched_pid:
                master_list.append({'pid': matched_pid, 'form': fid, 'name': fname, 'key': f"{matched_pid}:{fid}"})

        # Sort by PID for ordering
        master_list.sort(key=lambda x: (x['pid'], x['form']))
        _HEATMAP_VALID_FORMS = master_list
        logger.info(f"Generated {len(_HEATMAP_VALID_FORMS)} total Pokemon forms from local files")

    return _HEATMAP_VALID_FORMS

def resolve_pokemon_name(pid, form_id):
    species_map = _get_species_map()
    form_map = _get_form_map()
    pid, form_id = safe_int(pid), safe_int(form_id)
    base_name = species_map.get(pid, f"Pokemon {pid}")
    if form_id <= 0: return base_name
    form_name_full = form_map.get(form_id)
    if form_name_full:
        if "Normal" in form_name_full and not any(x in form_name_full for x in ["Alola", "Galar", "Hisui"]):
             return base_name
        return form_name_full
    return base_name

# Layout

def generate_area_cards(geofences, selected_area_name):
    cards = []
    for idx, geo in enumerate(geofences):
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', geo['name'])
        is_selected = (selected_area_name == geo['name'])
        map_children = [html.Div("✓ Selected", style={'position': 'absolute', 'top': '10px', 'right': '10px', 'backgroundColor': '#28a745', 'color': 'white', 'padding': '4px 8px', 'borderRadius': '4px', 'fontWeight': 'bold', 'zIndex': '1000'})] if is_selected else []

        card = dbc.Card([
            html.Div(map_children, id=f"poke-area-map-{safe_name}", **{'data-map-geofence': json.dumps(geo)}, style={'height': '150px', 'backgroundColor': '#1a1a1a', 'position': 'relative'}),
            dbc.CardBody([
                html.H5(geo['name'], className="card-title text-truncate", style={'color': '#28a745' if is_selected else 'inherit'}),
                dbc.Button("✓ Selected" if is_selected else "Select", href=f"/pokemon?area={geo['name']}", color="success" if is_selected else "primary", size="sm", className="w-100", disabled=is_selected)
            ])
        ], style={"width": "14rem", "margin": "10px", "border": f"3px solid {'#28a745' if is_selected else 'transparent'}"}, className="shadow-sm")

        if is_selected: card.id = "selected-area-card"
        cards.append(card)
    return cards if cards else html.Div("No areas match your search.", className="text-center text-muted my-4")

def layout(area=None, **kwargs):
    geofences = get_cached_geofences() or []
    initial_cards = generate_area_cards(geofences, area)
    area_options = [{"label": g["name"], "value": g["name"]} for g in geofences]
    area_label = area if area else "No Area Selected"

    return dbc.Container([
        dcc.Store(id="raw-data-store"),
        dcc.Store(id="whitelist-store", data=[]),
        dcc.Store(id="table-sort-store", data={"col": "total", "dir": "desc"}),
        dcc.Store(id="table-page-store", data={"current_page": 1, "rows_per_page": 25}),
        dcc.Store(id="total-pages-store", data=1),
        dcc.Store(id="clientside-dummy-store"),
        dcc.Store(id="heatmap-data-store", data=[]),
        dcc.Store(id="heatmap-mode-store", data="markers"),
        dcc.Dropdown(id="area-selector", options=area_options, value=area, style={'display': 'none'}),
        dcc.Store(id="mode-persistence-store", storage_type="local"),

        dbc.Row([
            dbc.Col(html.H2("Pokémon Analytics", className="text-white"), width=12, className="my-4"),
        ]),
        html.Div(id="notification-area"),

        # Main Control Card
        dbc.Card([
            dbc.CardHeader("⚙️ Analysis Settings", className="fw-bold"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dbc.Label("Selected Area", className="fw-bold"),
                        dbc.InputGroup([
                            dbc.InputGroupText("🗺️"),
                            dbc.Input(value=area_label, disabled=True, style={"backgroundColor": "#fff", "color": "#333", "fontWeight": "bold"}),
                            dbc.Button("Change", id="open-area-modal", color="primary")
                        ], className="mb-3")
                    ], width=12, md=6),
                    dbc.Col([
                        dbc.Label("Data Source", className="fw-bold"),
                        html.Div(
                            dbc.RadioItems(
                                id="data-source-selector",
                                options=[
                                    {"label": "Live (Stats)", "value": "live"},
                                    {"label": "Live (TTH)", "value": "live_tth"},
                                    {"label": "Historical (Stats)", "value": "historical"},
                                    {"label": "Historical (TTH)", "value": "historical_tth"},
                                    {"label": "Heatmap (SQL)", "value": "sql_heatmap"},
                                ],
                                value="live", inline=True, inputClassName="btn-check", labelClassName="btn btn-outline-secondary", labelCheckedClassName="active"
                            ), className="mb-3"
                        )
                    ], width=12, md=6)
                ], className="g-3"),

                html.Hr(className="my-3"),

                dbc.Row([
                    dbc.Col([
                        html.Div(id="live-controls", children=[
                            dbc.Label(f"📅 Time Window (Hours)"),
                            dbc.InputGroup([
                                dbc.Input(id="live-time-input", type="number", min=1, max=MAX_RETENTION_HOURS, value=1),
                                dbc.InputGroupText("hours")
                            ])
                        ]),
                        html.Div(id="historical-controls", style={"display": "none"}, children=[
                            dbc.Label("📅 Date Range"),
                            dcc.DatePickerRange(id="historical-date-picker", start_date=date.today(), end_date=date.today(), className="d-block w-100")
                        ])
                    ], width=6, md=3),
                    dbc.Col([
                        html.Div(id="interval-control-container", style={"display": "none"}, children=[
                            dbc.Label("⏱️ Interval"),
                            dcc.Dropdown(id="interval-selector", options=[{"label": "Hourly", "value": "hourly"}], value="hourly", clearable=False, className="text-dark")
                        ])
                    ], width=6, md=3),
                    dbc.Col([
                        dbc.Label("📊 View Mode"),
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
                        dbc.Label("Actions", style={"visibility": "hidden"}),
                        html.Div([
                            dbc.Button("Run Analysis", id="submit-btn", color="success", className="w-100 fw-bold mb-2"),
                            dbc.Button("Select Pokémon", id="open-filter-btn", color="secondary", className="w-100", style={"display": "none"})
                        ])
                    ], width=6, md=3)
                ], className="align-items-end g-3"),

                # HEATMAP FILTERS - Layout
                html.Div(id="heatmap-filters-container", style={"display": "none"}, children=[
                    html.Hr(className="my-3"),
                    dbc.Card([
                        dbc.CardBody([
                            dbc.Row([
                                dbc.Col([
                                    html.Div([
                                        dbc.Label("IV Filter", className="fw-bold text-white mb-2"),
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
                                        dbc.Label("Level Filter", className="fw-bold text-white mb-2"),
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
                    ], className="border-secondary mb-3", style={"backgroundColor": "rgba(0,0,0,0.3)"})
                ])
            ])
        ], className="shadow-sm border-0 mb-4"),

        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Select an Area")),
            dbc.ModalBody([
                html.Div(
                    dbc.Input(id="area-filter-input", placeholder="Filter areas...", className="mb-3", autoFocus=True),
                    style={"position": "sticky", "top": "-16px", "zIndex": "1020", "backgroundColor": "var(--bs-modal-bg, #fff)", "paddingTop": "16px", "paddingBottom": "10px", "marginBottom": "10px", "borderBottom": "1px solid #dee2e6"}
                ),
                html.Div(initial_cards, id="area-cards-container", className="d-flex flex-wrap justify-content-center")
            ]),
            dbc.ModalFooter(dbc.Button("Close", id="close-area-modal", className="ms-auto"))
        ], id="area-modal", size="xl", scrollable=True),

        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Select Pokémon for Heatmap")),
            dbc.ModalBody([
                dbc.Input(id="pokemon-filter-search", placeholder="Search by name (e.g. 'Pikachu')...", className="mb-3", autoFocus=True),
                html.P("Click on a Pokémon to include it in the heatmap. Red = Excluded, Green = Included.", className="text-muted small"),
                dbc.Row([
                    dbc.Col(dbc.Button("Select All", id="filter-select-all", color="success", size="sm", style={"display": "none"}), width="auto"),
                    dbc.Col(dbc.Button("Clear Selection", id="filter-clear-all", color="danger", size="sm"), width="auto"),
                    dbc.Col(html.Div(id="select-all-info", className="text-muted small align-self-center"), width="auto")
                ], className="mb-3"),
                dcc.Loading(html.Div(id="pokemon-filter-grid", className="d-flex flex-wrap justify-content-center gap-2"))
            ]),
            dbc.ModalFooter(dbc.Button("Apply", id="apply-filter-btn", color="primary", className="ms-auto"))
        ], id="filter-modal", size="lg", scrollable=True),

        dcc.Loading(html.Div(id="stats-container", style={"display": "none"}, children=[
            dbc.Row([
                dbc.Col(dbc.Card([
                    dbc.CardHeader("📈 Total Counts"),
                    dbc.CardBody(html.Div(id="total-counts-display"))
                ], className="shadow-sm border-0 h-100"), width=12, lg=4, className="mb-4"),

                dbc.Col(dbc.Card([
                    dbc.CardHeader([
                        html.Div("📋 Activity Data", className="d-inline-block me-auto"),
                        html.Div(
                            dbc.RadioItems(
                                id="heatmap-display-mode",
                                options=[{"label": "Markers", "value": "markers"}, {"label": "Heatmap", "value": "density"}],
                                value="markers", inline=True, className="ms-2"
                            ), id="heatmap-toggle-container", style={"display": "none", "float": "right"}
                        )
                    ]),
                    dbc.CardBody([
                        dcc.Input(id="table-search-input", type="text", placeholder="🔍 Search Table...", debounce=True, className="form-control mb-3", style={"display": "none"}),
                        html.Div(id="main-visual-container"),
                        html.Div(id="heatmap-map-container", style={"height": "600px", "width": "100%", "display": "none"})
                    ])
                ], className="shadow-sm border-0 h-100"), width=12, lg=8, className="mb-4"),
            ]),
            dbc.Row([dbc.Col(dbc.Card([
                dbc.CardHeader("🛠️ Raw Data Inspector"),
                dbc.CardBody(html.Pre(id="raw-data-display", style={"maxHeight": "300px", "overflow": "scroll"}))
            ], className="shadow-sm border-0"), width=12)])
        ]))
    ])

# Parsing Logic

def parse_data_to_df(data, mode, source):
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

    elif mode == "grouped":
        if isinstance(data, dict) and data:
            first_val = next(iter(data.values()))
            if isinstance(first_val, (int, float)):
                for key_str, count in data.items():
                    parts = key_str.split(":")
                    if len(parts) >= 3:
                        pid, form, metric = parts[0], parts[1], parts[2]
                        name = resolve_pokemon_name(pid, form)
                        records.append({"metric": metric, "pid": int(pid) if pid.isdigit() else 0, "form": int(form) if form.isdigit() else 0, "key": name, "count": count, "time_bucket": "Total"})
            elif isinstance(first_val, dict):
                for metric, items in data.items():
                    if isinstance(items, dict):
                        for key_str, count in items.items():
                            parts = key_str.split(":")
                            pid, form = (parts[0], parts[1]) if len(parts) >= 2 else (0, 0)
                            name = resolve_pokemon_name(pid, form)
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
                                name = resolve_pokemon_name(pid, form)
                                records.append({"metric": metric, "pid": int(pid) if pid.isdigit() else 0, "form": int(form) if form.isdigit() else 0, "key": name, "count": count, "time_bucket": h_val})
                        else:
                            records.append({"metric": key_str, "pid": "All", "form": "All", "key": "All", "count": count, "time_bucket": h_val})

    df = pd.DataFrame(records)
    if df.empty: return pd.DataFrame(columns=["metric", "count", "pid", "form", "key", "time_bucket"])
    return df

# Callbacks

@callback(
    [Output("live-controls", "style"), Output("historical-controls", "style"),
     Output("interval-control-container", "style"), Output("heatmap-filters-container", "style"),
     Output("open-filter-btn", "style")],
    Input("data-source-selector", "value")
)
def toggle_source_controls(source):
    live_s, hist_s, int_s, heat_s, filt_btn_s = {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "none"}
    if "live" in source:
        live_s = {"display": "block"}
    elif source == "sql_heatmap":
        hist_s = {"display": "block", "position": "relative", "zIndex": 1002}
        heat_s = {"display": "block"}
        filt_btn_s = {"display": "block"}
    else:
        hist_s = {"display": "block", "position": "relative", "zIndex": 1002}
        int_s = {"display": "block"}
    return live_s, hist_s, int_s, heat_s, filt_btn_s

@callback(
    [Output("mode-selector", "options"), Output("mode-selector", "value")],
    Input("data-source-selector", "value"),
    [State("mode-persistence-store", "data"), State("mode-selector", "value")]
)
def restrict_modes(source, stored_mode, current_ui_mode):
    full_options = [{"label": "Surged (Hourly)", "value": "surged"}, {"label": "Grouped (Table)", "value": "grouped"}, {"label": "Sum (Totals)", "value": "sum"}]
    tth_options = [{"label": "Surged (Hourly)", "value": "surged"}, {"label": "Sum (Totals)", "value": "sum"}]
    heatmap_options = [{"label": "Map View", "value": "map"}]

    if "tth" in source: allowed = tth_options
    elif source == "sql_heatmap": allowed = heatmap_options
    else: allowed = full_options
    allowed_vals = [o['value'] for o in allowed]
    final_val = current_ui_mode if current_ui_mode in allowed_vals else (stored_mode if stored_mode in allowed_vals else allowed_vals[0])
    return allowed, final_val

@callback(Output("mode-persistence-store", "data"), Input("mode-selector", "value"), prevent_initial_call=True)
def save_mode(val): return val

@callback(Output("heatmap-mode-store", "data"), Input("heatmap-display-mode", "value"))
def update_heatmap_mode_store(val): return val

@callback(Output("iv-range-display", "children"), Input("iv-slider", "value"))
def update_iv_display(value):
    if not value or value == [0, 100]:
        return "All IVs (0-100%)"
    elif value[0] == value[1]:
        return f"Exactly {value[0]}% IV"
    elif value[1] == 100:
        return f"IV ≥ {value[0]}%"
    else:
        return f"IV: {value[0]}% - {value[1]}%"

@callback(Output("level-range-display", "children"), Input("level-slider", "value"))
def update_level_display(value):
    if not value or value == [1, 50]:
        return "All Levels (1-50)"
    elif value[0] == value[1]:
        return f"Exactly Level {value[0]}"
    elif value[1] == 50:
        return f"Level ≥ {value[0]}"
    else:
        return f"Level: {value[0]} - {value[1]}"

@callback(
    [Output("filter-select-all", "style"), Output("select-all-info", "children")],
    Input("iv-slider", "value")
)
def toggle_select_all_button(iv_range):
    """Show Select All button only if IV filter is exact or range < 25%"""
    if not iv_range:
        return {"display": "none"}, ""

    min_iv, max_iv = iv_range[0], iv_range[1]
    iv_range_size = max_iv - min_iv

    # Allow Select All if exact value or range < 25%
    if min_iv == max_iv:
        return {"display": "inline-block"}, f"Select All allowed (exact IV: {min_iv}%)"
    elif iv_range_size < 25:
        return {"display": "inline-block"}, f"Select All allowed (range: {iv_range_size}%)"
    else:
        return {"display": "none"}, f"Select All disabled (range too wide: {iv_range_size}%)"

@callback(
    Output("whitelist-store", "data", allow_duplicate=True),
    Input("iv-slider", "value"),
    prevent_initial_call=True
)
def clear_selection_on_iv_change(iv_range):
    """Clear Pokemon selection when IV filter changes"""
    return []

@callback(
    [Output("area-modal", "is_open"), Output("filter-modal", "is_open"), Output("table-search-input", "style")],
    [Input("open-area-modal", "n_clicks"), Input("close-area-modal", "n_clicks"),
     Input("open-filter-btn", "n_clicks"), Input("apply-filter-btn", "n_clicks"),
     Input("mode-selector", "value")],
    [State("area-modal", "is_open"), State("filter-modal", "is_open")]
)
def handle_modals_and_search(ao, ac, fo, fa, mode, is_area, is_filter):
    search_style = {"display": "block", "width": "100%"} if mode == "grouped" else {"display": "none"}
    trigger = ctx.triggered_id
    if trigger in ["open-area-modal", "close-area-modal"]: return not is_area, is_filter, search_style
    if trigger in ["open-filter-btn", "apply-filter-btn"]: return is_area, not is_filter, search_style
    return is_area, is_filter, search_style

@callback(Output("area-cards-container", "children"), [Input("area-filter-input", "value")], [State("area-selector", "value")])
def filter_area_cards(search, area):
    geos = get_cached_geofences() or []
    if search: geos = [g for g in geos if search.lower() in g['name'].lower()]
    return generate_area_cards(geos, area)

dash.clientside_callback(
    ClientsideFunction(namespace='clientside', function_name='scrollToSelected'),
    Output("clientside-dummy-store", "data"), Input("area-modal", "is_open")
)

# FILTER GRID POPULATOR
@callback(
    Output("pokemon-filter-grid", "children"),
    [Input("pokemon-filter-search", "value"), Input("whitelist-store", "data"), Input("data-source-selector", "value")]
)
def populate_filter_grid(search_term, whitelist, data_source):
    if whitelist is None:
        whitelist = []

    # Convert whitelist to set for faster lookups
    whitelist_set = set(whitelist)

    # Use local heatmap data for Heatmap(SQL), regular data for everything else
    if data_source == "sql_heatmap":
        master_list = _get_heatmap_valid_forms()
    else:
        master_list = _get_all_valid_forms()

    results = []
    s_term = search_term.lower() if search_term else ""

    # Filter and collect matching entries first
    filtered_entries = []
    for entry in master_list:
        key, name, pid, form = entry['key'], entry['name'], entry['pid'], entry['form']
        # Match search against Name or ID
        if not s_term or (s_term in name.lower() or s_term == str(pid)):
            filtered_entries.append(entry)

    # Limit display for performance - but keep all filtered if searching
    limit = 200 if not s_term else len(filtered_entries)
    display_entries = filtered_entries[:limit]

    # Create UI elements with consistent key-based selection
    for entry in display_entries:
        key, name, pid, form = entry['key'], entry['name'], entry['pid'], entry['form']
        is_selected = key in whitelist_set

        # Red if NOT selected, Green if selected
        style = {
            "border": "3px solid #28a745" if is_selected else "3px solid #dc3545",
            "opacity": "1" if is_selected else "0.5",
            "cursor": "pointer",
            "borderRadius": "10px",
            "padding": "5px",
            "backgroundColor": "#333",
            "margin": "2px"
        }

        results.append(html.Div(
            html.Img(src=get_pokemon_icon_url(pid, form), style={"width": "48px", "height": "48px"}),
            id={"type": "poke-filter-icon", "index": key},
            n_clicks=0,
            style=style,
            title=f"{name} ({key})"
        ))

    # Add show more message if truncated
    if len(filtered_entries) > limit:
        results.append(html.P(
            f"... and {len(filtered_entries) - limit} more. Use search to find specific Pokemon.",
            className="text-muted text-center w-100 mt-2"
        ))

    return results

@callback(
    Output("whitelist-store", "data", allow_duplicate=True),
    [Input({"type": "poke-filter-icon", "index": ALL}, "n_clicks"), Input("filter-clear-all", "n_clicks"), Input("filter-select-all", "n_clicks")],
    [State("whitelist-store", "data"), State({"type": "poke-filter-icon", "index": ALL}, "id"), State("data-source-selector", "value")],
    prevent_initial_call=True
)
def update_whitelist(icon_clicks, clear_click, select_all_click, current_list, icon_ids, data_source):
    trigger = ctx.triggered_id
    if current_list is None: current_list = []

    # Clear all selections
    if trigger == "filter-clear-all":
        return []

    # Select all visible Pokemon
    if trigger == "filter-select-all":
        if data_source == "sql_heatmap":
            all_forms = _get_heatmap_valid_forms()
        else:
            all_forms = _get_all_valid_forms()
        return [entry['key'] for entry in all_forms]

    # Individual Click Logic - Toggle inclusion
    if isinstance(trigger, dict) and trigger.get('type') == 'poke-filter-icon':
        clicked_key = trigger['index']
        if clicked_key in current_list:
            return [x for x in current_list if x != clicked_key]
        else:
            return current_list + [clicked_key]

    return current_list

@callback(
    [Output("raw-data-store", "data"), Output("stats-container", "style"), Output("heatmap-data-store", "data"), Output("notification-area", "children")],
    [Input("submit-btn", "n_clicks"), Input("data-source-selector", "value")],
    [State("area-selector", "value"), State("live-time-input", "value"), State("historical-date-picker", "start_date"),
     State("historical-date-picker", "end_date"), State("interval-selector", "value"),
     State("mode-selector", "value"), State("iv-slider", "value"), State("level-slider", "value"),
     State("whitelist-store", "data")]
)
def fetch_data(n, source, area, live_h, start, end, interval, mode, iv_range, level_range, whitelist):
    if not n: return {}, {"display": "none"}, [], None
    if not area: return {}, {"display": "none"}, [], dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), "Please select an Area first."], color="warning", dismissable=True, duration=4000)

    try:
        if source == "sql_heatmap":
            # BLOCK if whitelist is empty
            if not whitelist:
                return {}, {"display": "none"}, [], dbc.Alert([html.I(className="bi bi-hand-index-thumb-fill me-2"), "Please select at least one Pokémon in the filter."], color="danger", dismissable=True, duration=5000)

            # Check if all Pokemon are selected. If so, use "all" for efficiency
            all_available = _get_heatmap_valid_forms()
            all_keys = {entry['key'] for entry in all_available}
            whitelist_set = set(whitelist)

            if whitelist_set == all_keys:
                # All Pokemon selected - use "all" for API
                pids = "all"
                forms = "all"
            else:
                # Specific Pokemon selected - send list
                p_list = [x.split(":")[0] for x in whitelist]
                f_list = [x.split(":")[1] for x in whitelist]
                pids = ",".join(p_list)
                forms = ",".join(f_list)

            # IV filter logic: Use == for exact, >= when max is 100, otherwise >=min,<=max
            if iv_range[0] == iv_range[1]:
                iv_str = f"=={iv_range[0]}"
            elif iv_range == [0, 100]:
                iv_str = "all"
            elif iv_range[1] == 100:
                iv_str = f">={iv_range[0]}"  # Max is 100, so just >=
            else:
                iv_str = f">={iv_range[0]},<={iv_range[1]}"

            # Level filter logic: Use == for exact, >= when max is 50, otherwise >=min,<=max
            if level_range[0] == level_range[1]:
                level_str = f"=={level_range[0]}"
            elif level_range == [1, 50]:
                level_str = "all"
            elif level_range[1] == 50:
                level_str = f">={level_range[0]}"  # Max is 50, so just >=
            else:
                level_str = f">={level_range[0]},<={level_range[1]}"

            params = {"start_time": f"{start}T00:00:00", "end_time": f"{end}T23:59:59", "area": area, "response_format": "json", "iv": iv_str, "level": level_str, "pokemon_id": pids, "form": forms}
            raw_data = get_pokemon_stats("sql_heatmap", params)

            safe_data = []
            if raw_data and isinstance(raw_data, list):
                df = pd.DataFrame(raw_data)
                if "pokemon_id" in df.columns:
                    if "form" not in df.columns: df["form"] = 0
                    grouped = df.groupby(['latitude', 'longitude', 'pokemon_id', 'form'])['count'].sum().reset_index()
                    safe_data = grouped.to_dict('records')
            if not safe_data: return {}, {"display": "block"}, [], dbc.Alert("No Heatmap data found for criteria.", color="info", dismissable=True, duration=4000)
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
            params = {"start_time": f"{hours} hours", "end_time": "now", "mode": mode, "area": area, "response_format": "json"}
            data = get_pokemon_stats("timeseries", params)
        else:
            params = {"counter_type": "totals", "interval": interval, "start_time": f"{start}T00:00:00", "end_time": f"{end}T23:59:59", "mode": mode, "area": area, "response_format": "json"}
            data = get_pokemon_stats("counter", params)

        if not data: return {}, {"display": "block"}, [], dbc.Alert("No data found for this period.", color="warning", dismissable=True, duration=4000)
        return data, {"display": "block"}, [], None
    except Exception as e:
        logger.error(f"Fetch error: {e}")
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

@callback(
    [Output("total-counts-display", "children"), Output("main-visual-container", "children"), Output("raw-data-display", "children"), Output("total-pages-store", "data"),
     Output("heatmap-map-container", "style"), Output("main-visual-container", "style"), Output("heatmap-toggle-container", "style")],
    [Input("raw-data-store", "data"), Input("table-search-input", "value"), Input("table-sort-store", "data"), Input("table-page-store", "data"), Input("heatmap-data-store", "data")],
    [State("mode-selector", "value"), State("data-source-selector", "value")]
)
def update_visuals(data, search_term, sort, page, heatmap_data, mode, source):
    if source == "sql_heatmap":
        count = len(heatmap_data) if heatmap_data else 0
        raw_text = json.dumps(heatmap_data, indent=2)
        total_div = [html.H1(f"{count:,} Spawns", className="text-primary")]
        return total_div, html.Div(), raw_text, 1, {"height": "600px", "width": "100%", "display": "block"}, {"display": "none"}, {"display": "block", "float": "right"}

    if not data: return [], html.Div(), "", 1, {"display": "none"}, {"display": "block"}, {"display": "none"}
    df = parse_data_to_df(data, mode, source)
    if df.empty: return "No Data", html.Div(), json.dumps(data, indent=2), 1, {"display": "none"}, {"display": "block"}, {"display": "none"}

    if mode == "grouped" and search_term:
        df = df[df['key'].str.lower().str.contains(search_term.lower(), na=False)]

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

        header_cells = [html.Th("Image", style={"backgroundColor": "#1a1a1a", "width": "60px", "position": "sticky", "top": "0", "zIndex": "10"})]
        header_cells.append(html.Th(html.Span(["Pokémon", html.Span(" ▲" if col == 'key' and ascending else (" ▼" if col == 'key' else ""), style={"color": "#aaa", "marginLeft": "5px"})], id={"type": "sort-header", "index": "key"}, style={"cursor": "pointer"}), style={"backgroundColor": "#1a1a1a", "position": "sticky", "top": "0", "zIndex": "10"}))

        for c in [x for x in pivot.columns if x not in ['pid', 'form', 'key']]:
             label = html.Img(src=metric_icons[c.lower()], style={"width":"24px", "verticalAlign":"middle"}) if c.lower() in metric_icons else c
             arrow = " ▲" if col == c and ascending else (" ▼" if col == c else "")
             header_cells.append(html.Th(html.Span([label, html.Span(arrow, style={"color": "#aaa"})], id={"type": "sort-header", "index": c}, style={"cursor": "pointer", "display":"inline-flex", "alignItems":"center"}), style={"backgroundColor": "#1a1a1a", "position": "sticky", "top": "0", "zIndex": "10", "textAlign": "center"}))

        header_row = html.Tr(header_cells)

        rows = []
        for i, r in enumerate(page_df.iterrows()):
            _, r = r
            bg = "#1a1a1a" if i % 2 == 0 else "#242424"
            cells = [
                html.Td(html.Img(src=get_pokemon_icon_url(r['pid'], r['form']), style={"width":"40px", "display":"block", "margin":"auto"}), style={"backgroundColor":bg, "textAlign": "center"}),
                html.Td(f"{r['key']}", style={"backgroundColor":bg})
            ]
            for m in [x for x in pivot.columns if x not in ['pid', 'form', 'key']]:
                cells.append(html.Td(f"{int(r[m]):,}", style={"backgroundColor":bg, "textAlign": "center"}))
            rows.append(html.Tr(cells))

        controls = html.Div([
            dbc.Row([
                dbc.Col([html.Span(f"Total: {total_rows} | Rows: ", className="me-2 align-middle"), dcc.Dropdown(id="rows-per-page-selector", options=[{'label': str(x), 'value': x} for x in [10, 25, 50, 100]] + [{'label': 'All', 'value': total_rows}], value=rows_per_page, clearable=False, className="rows-per-page-selector", style={"width":"80px", "display":"inline-block", "color":"black", "verticalAlign": "middle"})], width="auto", className="d-flex align-items-center"),
                dbc.Col([
                    dbc.ButtonGroup([dbc.Button("<<", id="first-page-btn", size="sm", disabled=current_page <= 1), dbc.Button("<", id="prev-page-btn", size="sm", disabled=current_page <= 1)], className="me-2"),
                    html.Span("Page ", className="align-middle me-1"), dcc.Input(id="goto-page-input", type="number", min=1, max=total_pages_val, value=current_page, debounce=True, style={"width": "60px", "textAlign": "center", "display": "inline-block", "color": "black"}), html.Span(f" of {total_pages_val}", className="align-middle ms-1 me-2"),
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

        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', title=f"{mode.title()} Data")
        visual_content = dcc.Graph(figure=fig, id="main-graph")

    return total_div, visual_content, json.dumps(data, indent=2), total_pages_val, {"display": "none"}, {"display": "block"}, {"display": "none"}

dash.clientside_callback(
    """
    function(data, whitelist, mode) {
        if (!window.renderPokemonHeatmap) return window.dash_clientside.no_update;
        // renderPokemonHeatmap expects (data, blocklist, renderMode)
        // Data is already filtered by SQL query, so pass empty blocklist
        window.renderPokemonHeatmap(data, [], mode);
        return window.dash_clientside.no_update;
    }
    """,
    Output("heatmap-map-container", "children"),
    Input("heatmap-data-store", "data"),
    Input("whitelist-store", "data"),
    Input("heatmap-mode-store", "data")
)
