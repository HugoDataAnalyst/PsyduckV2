import dash
from dash import html, dcc, callback, Input, Output, State, ALL, ctx, MATCH, ClientsideFunction
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, date
from dashboard.utils import get_cached_geofences, get_quests_stats
from utils.logger import logger
import config as AppConfig
import json
import re
import os

dash.register_page(__name__, path='/quests', title='Quest Analytics')

ICON_BASE_URL = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main"

REWARD_TYPES = {
    0: "Unset", 1: "XP", 2: "Item", 3: "Stardust", 4: "Candy", 5: "Avatar",
    6: "Quest", 7: "Pokémon", 8: "Pokecoin", 9: "XL Candy", 10: "Level Cap",
    11: "Sticker", 12: "Mega Energy", 13: "Incident", 14: "Attribute",
    15: "Badge", 16: "Egg", 17: "Stat", 18: "Loot", 19: "Friendship"
}

QUEST_TYPES = {
    0: "Unset", 1: "First Catch", 2: "First Spin", 3: "Multi-Part", 4: "Catch Pokémon",
    5: "Spin Pokestop", 6: "Hatch Egg", 7: "Gym Battle", 8: "Raid Battle", 9: "Complete Quest",
    10: "Transfer", 11: "Favorite", 12: "Autocomplete", 13: "Use Berry", 14: "Upgrade",
    15: "Evolve", 16: "Throw", 17: "Buddy Candy", 18: "Badge Rank", 19: "Level Up",
    20: "Join Raid", 21: "Battle", 22: "Add Friend", 23: "Trade", 24: "Send Gift",
    25: "Evolve Into", 27: "Combat", 28: "Snapshot", 29: "Battle Rocket", 30: "Purify",
    31: "Find Rocket", 32: "First Grunt", 33: "Feed Buddy", 34: "Buddy Affection",
    35: "Pet Buddy", 36: "Buddy Level", 37: "Buddy Walk", 38: "Buddy Yatta", 39: "Incense",
    40: "Buddy Souvenir", 41: "Collect Reward", 42: "Walk", 43: "Mega Evolve", 44: "Stardust",
    45: "Collection", 46: "AR Scan", 50: "Evo Walk", 51: "GBL Rank", 53: "Charge Move",
    54: "Change Form", 55: "Battle NPC", 56: "Power Up", 57: "Wild Snapshot", 58: "Use Item",
    59: "Open Gift", 60: "Earn XP", 61: "Battle Leader", 62: "First Route", 63: "Sleep Data",
    64: "Route Travel", 65: "Route Complete", 66: "Collect Tappable", 67: "Ability",
    68: "NPC Gift Send", 69: "NPC Gift Open", 70: "OAuth", 71: "Fight Mon", 72: "Non-Combat Move",
    73: "Fuse", 74: "Unfuse", 75: "Walk Meters", 76: "Change Into Form", 77: "Fuse Into",
    78: "Unfuse Into", 82: "Collect MP", 83: "Loot Station", 84: "Bread Battle", 85: "Bread Move",
    86: "Unlock Bread", 87: "Enhance Bread", 88: "Collect Stamp", 89: "Dough Battle",
    90: "Visit Page", 91: "Incubator", 92: "Choose Buddy", 93: "Lure", 94: "Lucky Egg",
    95: "Pin Postcard", 96: "Feed Gym", 97: "Star Piece", 98: "Reach CP", 99: "Spend Dust"
}

_SPECIES_MAP = None
_FORM_MAP = None
_ITEMS_MAP = None

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

def _get_items_map():
    global _ITEMS_MAP
    if _ITEMS_MAP is None:
        try:
            path = os.path.join(os.path.dirname(__file__), '..', 'assets', 'items.json')
            if not os.path.exists(path): path = os.path.join(os.getcwd(), 'assets', 'items.json')
            with open(path, 'r') as f:
                data = json.load(f)
                _ITEMS_MAP = {v: k.replace("_", " ").title() for k, v in data.items()}
        except: _ITEMS_MAP = {}
    return _ITEMS_MAP

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

def get_quest_icon_url(reward_type, item_id=None, poke_id=None, form=0):
    rt = safe_int(reward_type)
    item_id, poke_id, form = safe_int(item_id), safe_int(poke_id), safe_int(form)

    # Specific Lookups
    if rt == 7 and poke_id > 0:
        return f"{ICON_BASE_URL}/pokemon/{poke_id}_f{form}.webp" if form > 0 else f"{ICON_BASE_URL}/pokemon/{poke_id}.webp"
    if rt == 2 and item_id > 0: return f"{ICON_BASE_URL}/reward/item/{item_id}.webp"
    if rt == 4 and item_id > 0: return f"{ICON_BASE_URL}/reward/candy/{item_id}.webp"
    if rt == 12 and item_id > 0: return f"{ICON_BASE_URL}/reward/mega_resource/{item_id}.webp"
    if rt == 9 and item_id > 0: return f"{ICON_BASE_URL}/reward/xl_candy/{item_id}.webp"

    # Defaults
    defaults = {
        0: "reward/unset/0.webp", 1: "reward/experience/0.webp", 2: "reward/item/0.webp",
        3: "reward/stardust/0.webp", 4: "reward/candy/0.webp", 5: "reward/avatar_clothing/0.webp",
        6: "reward/quest/0.webp", 7: "misc/pokemon.webp", 8: "reward/pokecoin/0.webp",
        9: "reward/xl_candy/0.webp", 10: "reward/level_cap/0.webp", 11: "reward/sticker/0.webp",
        12: "reward/mega_resource/0.webp", 13: "reward/incident/0.webp", 14: "reward/player_attribute/0.webp",
        15: "misc/badge_3.webp", 16: "misc/egg.webp", 17: "station/0.webp", 18: "reward/unset/0.webp",
        19: "misc/bestbuddy.webp"
    }
    path = defaults.get(rt, "misc/0.webp")
    return f"{ICON_BASE_URL}/{path}"

# Layout

def generate_area_cards(geofences, selected_area_name):
    cards = []
    for idx, geo in enumerate(geofences):
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', geo['name'])
        is_selected = (selected_area_name == geo['name'])

        # Visual indicator for selected card
        map_children = [html.Div("✓ Selected", style={'position': 'absolute', 'top': '10px', 'right': '10px', 'backgroundColor': '#28a745', 'color': 'white', 'padding': '4px 8px', 'borderRadius': '4px', 'fontWeight': 'bold', 'zIndex': '1000'})] if is_selected else []

        card = dbc.Card([
            html.Div(map_children, id=f"quests-map-{safe_name}", **{'data-map-geofence': json.dumps(geo)}, style={'height': '150px', 'backgroundColor': '#1a1a1a', 'position': 'relative'}),
            dbc.CardBody([
                html.H5(geo['name'], className="card-title text-truncate", style={'color': '#28a745' if is_selected else 'inherit'}),
                dbc.Button("✓ Selected" if is_selected else "Select", href=f"/quests?area={geo['name']}", color="success" if is_selected else "primary", size="sm", className="w-100", disabled=is_selected)
            ])
        ], style={"width": "14rem", "margin": "10px", "border": f"3px solid {'#28a745' if is_selected else 'transparent'}"}, className="shadow-sm")

        # IMPORTANT: Assign this ID so custom_callbacks.js can scroll to it
        if is_selected:
            card.id = "selected-area-card"

        cards.append(card)
    return cards if cards else html.Div("No areas match your search.", className="text-center text-muted my-4")

def layout(area=None, **kwargs):
    geofences = get_cached_geofences() or []
    initial_cards = generate_area_cards(geofences, area)
    area_options = [{"label": g["name"], "value": g["name"]} for g in geofences]
    area_label = area if area else "No Area Selected"

    return dbc.Container([
        dcc.Store(id="quests-raw-data-store"),
        dcc.Store(id="quests-table-sort-store", data={"col": "count", "dir": "desc"}),
        dcc.Store(id="quests-table-page-store", data={"current_page": 1, "rows_per_page": 25}),
        dcc.Store(id="quests-total-pages-store", data=1),
        dcc.Store(id="quests-clientside-dummy-store"),
        dcc.Dropdown(id="quests-area-selector", options=area_options, value=area, style={'display': 'none'}),
        dcc.Store(id="quests-mode-persistence-store", storage_type="local"),
        dcc.Store(id="quests-source-persistence-store", storage_type="local"),

        # Header
        dbc.Row([
            dbc.Col(html.H2("Quest Analytics", className="text-white"), width=12, className="my-4"),
        ]),

        # Notification Area
        html.Div(id="quests-notification-area"),

        # Main Control Card
        dbc.Card([
            dbc.CardHeader("⚙️ Analysis Settings", className="fw-bold"),
            dbc.CardBody([
                dbc.Row([
                    # Area Selection
                    dbc.Col([
                        dbc.Label("Selected Area", className="fw-bold"),
                        dbc.InputGroup([
                            dbc.InputGroupText("🗺️"),
                            dbc.Input(value=area_label, disabled=True, style={"backgroundColor": "#fff", "color": "#333", "fontWeight": "bold"}),
                            dbc.Button("Change", id="quests-open-area-modal", color="primary")
                        ], className="mb-3")
                    ], width=12, md=6),

                    # Data Source
                    dbc.Col([
                        dbc.Label("Data Source", className="fw-bold"),
                        html.Div([
                            # Stats Live & Historical
                            html.Div([
                                html.Span("Stats: ", className="text-muted small me-2", style={"minWidth": "45px"}),
                                dbc.RadioItems(
                                    id="quests-data-source-selector",
                                    options=[
                                        {"label": "Live", "value": "live"},
                                        {"label": "Historical", "value": "historical"},
                                    ],
                                    value="live", inline=True, inputClassName="btn-check",
                                    labelClassName="btn btn-outline-info btn-sm",
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
                        html.Div(id="quests-live-controls", children=[
                            dbc.Label("📅 Time Window (Hours)"),
                            dbc.InputGroup([
                                dbc.Input(id="quests-live-time-input", type="number", min=1, max=72, value=24),
                                dbc.InputGroupText("hours")
                            ])
                        ]),
                        html.Div(id="quests-historical-controls", style={"display": "none"}, children=[
                            dbc.Label("📅 Date Range"),
                            dcc.DatePickerRange(
                                id="quests-historical-date-picker",
                                start_date=date.today(), end_date=date.today(),
                                className="d-block w-100",
                                persistence=True, persistence_type="local"
                            )
                        ])
                    ], width=6, md=3),

                    # Interval
                    dbc.Col([
                        html.Div(id="quests-interval-control-container", style={"display": "none"}, children=[
                            dbc.Label("⏱️ Interval"),
                            dcc.Dropdown(id="quests-interval-selector", options=[{"label": "Hourly", "value": "hourly"}], value="hourly", clearable=False, className="text-dark")
                        ])
                    ], width=6, md=3),

                    # Mode
                    dbc.Col([
                        dbc.Label("📊 View Mode"),
                        dcc.Dropdown(
                            id="quests-mode-selector",
                            options=[],
                            value=None,
                            clearable=False,
                            className="text-dark"
                        )
                    ], width=6, md=3),

                    # Actions
                    dbc.Col([
                        dbc.Label("Actions", style={"visibility": "hidden"}),
                        dbc.Button("Run Analysis", id="quests-submit-btn", color="success", className="w-100 fw-bold")
                    ], width=6, md=3)
                ], className="align-items-end g-3")
            ])
        ], className="shadow-sm border-0 mb-4"),

        # Area Selection Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Select an Area")),
            dbc.ModalBody([
                # STICKY SEARCH BAR
                html.Div(
                    dbc.Input(id="quests-area-filter-input", placeholder="Filter areas by name...", className="mb-3", autoFocus=True),
                    style={
                        "position": "sticky",
                        "top": "-16px",
                        "zIndex": "1020",
                        "backgroundColor": "var(--bs-modal-bg, #fff)",
                        "paddingTop": "16px",
                        "paddingBottom": "10px",
                        "marginBottom": "10px",
                        "borderBottom": "1px solid #dee2e6"
                    }
                ),
                html.Div(initial_cards, id="quests-area-cards-container", className="d-flex flex-wrap justify-content-center")
            ]),
            dbc.ModalFooter(dbc.Button("Close", id="quests-close-area-modal", className="ms-auto"))
        ], id="quests-area-modal", size="xl", scrollable=True),

        # Results Container
        dcc.Loading(html.Div(id="quests-stats-container", style={"display": "none"}, children=[
            dbc.Row([
                # Sidebar
                dbc.Col(dbc.Card([
                    dbc.CardHeader("📈 Total Counts"),
                    dbc.CardBody(html.Div(id="quests-total-counts-display"))
                ], className="shadow-sm border-0 h-100"), width=12, lg=4, className="mb-4"),

                # Activity Data
                dbc.Col(dbc.Card([
                    dbc.CardHeader("📋 Activity Data"),
                    dbc.CardBody([
                        # Embedded Search Input
                        dcc.Input(
                            id="quests-search-input",
                            type="text",
                            placeholder="🔍 Search Quest, Reward or Item...",
                            debounce=True,
                            className="form-control mb-3",
                            style={"display": "none"}
                        ),
                        html.Div(id="quests-main-visual-container")
                    ])
                ], className="shadow-sm border-0 h-100"), width=12, lg=8, className="mb-4"),
            ]),

            # Debugger
            dbc.Row([dbc.Col(dbc.Card([
                dbc.CardHeader("🛠️ Raw Data Inspector"),
                dbc.CardBody(html.Pre(id="quests-raw-data-display", style={"maxHeight": "300px", "overflow": "scroll"}))
            ], className="shadow-sm border-0"), width=12)])
        ]))
    ])

# Parsing Logic

def parse_data_to_df(data, mode, source):
    records = []
    working_data = data.get('data', data) if isinstance(data, dict) else data
    if not isinstance(working_data, dict): return pd.DataFrame()

    items_map = _get_items_map()
    species_map = _get_species_map()

    if mode == "sum":
        if "total" in working_data:
            records.append({"type": "Total", "name": "Total Quests", "count": working_data["total"], "key": "total", "category": "General", "time_bucket": "Total"})
        if "quest_mode" in working_data:
            for k, v in working_data["quest_mode"].items():
                records.append({"type": "Mode", "name": f"{k.upper()} Mode", "count": v, "key": f"mode_{k}", "category": "General", "time_bucket": "Total"})

    ts_keys = [k for k in working_data.keys() if str(k).startswith("ts:")]

    if ts_keys:
        for key_str in ts_keys:
            parts = key_str.split(':')
            if len(parts) < 10: continue

            q_mode = parts[2]
            q_type_id = safe_int(parts[4])
            r_type_id = safe_int(parts[5])
            item_id = safe_int(parts[6])
            amount = safe_int(parts[7])
            pid = safe_int(parts[8])
            form = safe_int(parts[9])

            time_data = working_data[key_str]

            q_name = QUEST_TYPES.get(q_type_id, f"Type {q_type_id}")
            r_name, category, filter_key, icon_url = "Unknown", "Other", "other", None
            amt_display = f" x{amount}" if amount > 1 else ""

            if r_type_id == 2:
                category = "Item"
                i_name = items_map.get(item_id, f"Item {item_id}")
                r_name = f"{i_name}{amt_display}"
                filter_key = f"item_{item_id}"
                icon_url = get_quest_icon_url(r_type_id, item_id=item_id)
            elif r_type_id == 7:
                category = "Pokemon"
                p_name = resolve_pokemon_name(pid, form)
                r_name = f"{p_name}"
                filter_key = f"poke_{pid}_{form}"
                icon_url = get_quest_icon_url(r_type_id, poke_id=pid, form=form)
            elif r_type_id == 3:
                category = "Stardust"
                r_name = f"Stardust{amt_display}"
                filter_key = "stardust"
                icon_url = get_quest_icon_url(3)
            elif r_type_id == 12:
                category = "Mega Energy"
                target_id = pid if pid > 0 else item_id
                p_name = resolve_pokemon_name(target_id, 0)
                r_name = f"{p_name} Mega Energy{amt_display}"
                filter_key = f"mega_{target_id}"
                icon_url = get_quest_icon_url(7, poke_id=target_id) if target_id > 0 else get_quest_icon_url(12)
            elif r_type_id == 9:
                category = "XL Candy"
                p_name = resolve_pokemon_name(item_id, 0)
                r_name = f"{p_name} XL Candy{amt_display}"
                filter_key = f"xl_{item_id}"
                icon_url = get_quest_icon_url(9, item_id=item_id)
            elif r_type_id == 4:
                category = "Candy"
                p_name = resolve_pokemon_name(item_id, 0)
                r_name = f"{p_name} Candy{amt_display}"
                filter_key = f"candy_{item_id}"
                icon_url = get_quest_icon_url(4, item_id=item_id)
            elif r_type_id == 1:
                category = "XP"
                r_name = f"XP{amt_display}"
                filter_key = "xp"
                icon_url = get_quest_icon_url(1)
            else:
                category = REWARD_TYPES.get(r_type_id, "Unknown")
                r_name = f"{category}{amt_display}"
                filter_key = f"other_{r_type_id}"
                icon_url = get_quest_icon_url(r_type_id)

            if isinstance(time_data, dict):
                for time_k, count in time_data.items():
                    h_val = safe_int(time_k)
                    if "hour" in str(time_k):
                        try: h_val = int(str(time_k).replace("hour ", ""))
                        except: pass

                    records.append({
                        "type": q_name,
                        "name": r_name,
                        "count": count,
                        "key": filter_key,
                        "category": category,
                        "time_bucket": h_val,
                        "icon": icon_url,
                        "q_mode": q_mode
                    })

    elif mode == "grouped":
        for ts_bucket, details in working_data.items():
            if not isinstance(details, dict): continue

            if "reward_item" in details:
                for i_id_str, count in details["reward_item"].items():
                    i_id = safe_int(i_id_str)
                    if i_id == 0: continue
                    i_name = items_map.get(i_id, f"Item {i_id}")
                    records.append({
                        "type": "Item Reward", "name": i_name, "count": count,
                        "key": f"item_{i_id}", "category": "Item", "time_bucket": "Total",
                        "icon": get_quest_icon_url(2, item_id=i_id)
                    })

            if "reward_poke" in details:
                for p_id_str, count in details["reward_poke"].items():
                    p_id = safe_int(p_id_str)
                    if p_id == 0: continue
                    p_name = resolve_pokemon_name(p_id, 0)
                    records.append({
                        "type": "Pokemon Encounter", "name": p_name, "count": count,
                        "key": f"poke_{p_id}_0", "category": "Pokemon", "time_bucket": "Total",
                        "icon": get_quest_icon_url(7, poke_id=p_id)
                    })

            if "reward_type" in details:
                if "3" in details["reward_type"]:
                    records.append({
                        "type": "Stardust", "name": "Stardust", "count": details["reward_type"]["3"],
                        "key": "stardust", "category": "Stardust", "time_bucket": "Total",
                        "icon": get_quest_icon_url(3)
                    })
                if "12" in details["reward_type"]:
                     records.append({
                        "type": "Mega Energy", "name": "Mega Energy (Total)", "count": details["reward_type"]["12"],
                        "key": "mega_total", "category": "Mega Energy", "time_bucket": "Total",
                        "icon": get_quest_icon_url(12, item_id=0)
                    })

    df = pd.DataFrame(records)
    if df.empty: return pd.DataFrame(columns=["name", "count", "category", "time_bucket", "key"])
    if mode == "grouped":
        df = df.groupby(['type', 'name', 'key', 'category', 'icon', 'time_bucket'])['count'].sum().reset_index()

    return df

# Callbacks

@callback(
    [Output("quests-live-controls", "style"), Output("quests-historical-controls", "style"), Output("quests-interval-control-container", "style")],
    Input("quests-data-source-selector", "value")
)
def toggle_source(source):
    if source == "live": return {"display": "block"}, {"display": "none"}, {"display": "none"}
    return {"display": "none"}, {"display": "block", "zIndex": 1002, "position": "relative"}, {"display": "block"}

@callback(
    [Output("quests-mode-selector", "options"), Output("quests-mode-selector", "value")],
    Input("quests-data-source-selector", "value"),
    [State("quests-mode-persistence-store", "data"), State("quests-mode-selector", "value")]
)
def restrict_modes(source, stored_mode, current_ui_mode):
    opts = [
        # {"label": "Surged (Hourly)", "value": "surged"}, # Disabled per user request
        {"label": "Grouped (Table)", "value": "grouped"},
        {"label": "Sum (Totals)", "value": "sum"}
    ]
    allowed_values = [o['value'] for o in opts]

    if current_ui_mode in allowed_values: return opts, current_ui_mode
    if stored_mode in allowed_values: return opts, stored_mode
    return opts, "grouped" # Default to grouped

@callback(Output("quests-mode-persistence-store", "data"), Input("quests-mode-selector", "value"), prevent_initial_call=True)
def save_mode(val): return val

@callback(Output("quests-source-persistence-store", "data"), Input("quests-data-source-selector", "value"), prevent_initial_call=True)
def save_source(val): return val

@callback(
    Output("quests-data-source-selector", "value"),
    Input("quests-source-persistence-store", "modified_timestamp"),
    State("quests-source-persistence-store", "data"),
    prevent_initial_call=False
)
def load_persisted_source(ts, stored_source):
    """Load persisted data source on page load."""
    if ts is not None and ts > 0:
        raise dash.exceptions.PreventUpdate
    valid_sources = ["live", "historical"]
    if stored_source in valid_sources:
        return stored_source
    return "live"

@callback(
    [Output("quests-area-modal", "is_open"), Output("quests-search-input", "style")],
    [Input("quests-open-area-modal", "n_clicks"), Input("quests-close-area-modal", "n_clicks"), Input("quests-mode-selector", "value")],
    [State("quests-area-modal", "is_open")]
)
def handle_modals_and_search(ao, ac, mode, isa):
    search_style = {"display": "block", "width": "100%"} if mode == "grouped" else {"display": "none"}
    tid = ctx.triggered_id
    if tid in ["quests-open-area-modal", "quests-close-area-modal"]: return not isa, search_style
    return isa, search_style

# Area Cards Filter Callback
@callback(
    Output("quests-area-cards-container", "children"),
    [Input("quests-area-filter-input", "value")],
    [State("quests-area-selector", "value")]
)
def filter_area_cards(search_term, selected_area):
    geofences = get_cached_geofences() or []
    if search_term:
        geofences = [g for g in geofences if search_term.lower() in g['name'].lower()]
    return generate_area_cards(geofences, selected_area)

# Clientside Scroll Callback
dash.clientside_callback(
    ClientsideFunction(namespace='clientside', function_name='scrollToSelected'),
    Output("quests-clientside-dummy-store", "data"),
    Input("quests-area-modal", "is_open")
)

@callback(
    [Output("quests-raw-data-store", "data"),
     Output("quests-stats-container", "style"),
     Output("quests-notification-area", "children")],
    [Input("quests-submit-btn", "n_clicks"), Input("quests-data-source-selector", "value")],
    [State("quests-area-selector", "value"), State("quests-live-time-input", "value"), State("quests-historical-date-picker", "start_date"), State("quests-historical-date-picker", "end_date"), State("quests-interval-selector", "value"), State("quests-mode-selector", "value")]
)
def fetch_data(n, source, area, live_h, start, end, interval, mode):
    if not n: return {}, {"display": "none"}, None

    if not area:
        alert = dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), "Please select an Area first."], color="warning", dismissable=True, duration=4000)
        return {}, {"display": "none"}, alert

    try:
        if source == "live":
            params = {"start_time": f"{live_h} hours", "end_time": "now", "mode": mode, "area": area, "response_format": "json"}
            data = get_quests_stats("timeseries", params)
        else:
            params = {"counter_type": "totals", "interval": interval, "start_time": f"{start}T00:00:00", "end_time": f"{end}T23:59:59", "mode": mode, "area": area, "response_format": "json"}
            data = get_quests_stats("counter", params)

        return data, {"display": "block"}, None
    except Exception as e:
        logger.error(f"Quest Fetch Error: {e}")
        err_alert = dbc.Alert(f"Error fetching data: {str(e)}", color="danger", dismissable=True)
        return {}, {"display": "none"}, err_alert

# Sorting Callback
@callback(
    Output("quests-table-sort-store", "data"), Input({"type": "quests-sort-header", "index": ALL}, "n_clicks"), State("quests-table-sort-store", "data"), prevent_initial_call=True
)
def update_sort_order(n_clicks, current_sort):
    if not ctx.triggered_id or not any(n_clicks): return dash.no_update
    col = ctx.triggered_id['index']
    return {"col": col, "dir": "asc" if current_sort['col'] == col and current_sort['dir'] == "desc" else "desc"}

# Pagination Callback
@callback(
    Output("quests-table-page-store", "data"),
    [Input("quests-first-page-btn", "n_clicks"), Input("quests-prev-page-btn", "n_clicks"), Input("quests-next-page-btn", "n_clicks"), Input("quests-last-page-btn", "n_clicks"), Input("quests-rows-per-page-selector", "value"), Input("quests-goto-page-input", "value")],
    [State("quests-table-page-store", "data"), State("quests-total-pages-store", "data")],
    prevent_initial_call=True
)
def update_pagination(first, prev, next, last, rows, goto, state, total_pages):
    trigger = ctx.triggered_id
    if not trigger: return dash.no_update
    current = state.get('current_page', 1)
    total_pages = total_pages or 1
    new_page = current
    if trigger == "quests-first-page-btn": new_page = 1
    elif trigger == "quests-last-page-btn": new_page = total_pages
    elif trigger == "quests-prev-page-btn": new_page = max(1, current - 1)
    elif trigger == "quests-next-page-btn": new_page = min(total_pages, current + 1)
    elif trigger == "quests-goto-page-input":
        if goto is not None: new_page = min(total_pages, max(1, goto))
    elif trigger == "quests-rows-per-page-selector": return {"current_page": 1, "rows_per_page": rows}
    return {**state, "current_page": new_page, "rows_per_page": state.get('rows_per_page', 25)}

# Visuals Update
@callback(
    [Output("quests-total-counts-display", "children"),
     Output("quests-main-visual-container", "children"),
     Output("quests-raw-data-display", "children"),
     Output("quests-total-pages-store", "data")],
    [Input("quests-raw-data-store", "data"), Input("quests-search-input", "value"),
     Input("quests-table-sort-store", "data"), Input("quests-table-page-store", "data")],
    [State("quests-mode-selector", "value"), State("quests-data-source-selector", "value")]
)
def update_visuals(data, search_term, sort, page, mode, source):
    if not data: return "No Data", html.Div(), "", 1

    df = parse_data_to_df(data, mode, source)
    if df.empty: return "No Data", html.Div(), json.dumps(data, indent=2), 1

    if mode == "grouped" and search_term:
        s = search_term.lower()
        df = df[
            df['name'].str.lower().str.contains(s, na=False) |
            df['type'].str.lower().str.contains(s, na=False) |
            df['category'].str.lower().str.contains(s, na=False)
        ]

    # Sidebar
    total_count = df[df['time_bucket'] == 'Total']['count'].sum() if mode == 'sum' else df['count'].sum()
    cat_gb = df.groupby('category')['count'].sum().reset_index().sort_values('count', ascending=False)

    sidebar = [html.H1(f"{total_count:,}", className="text-primary")]

    cat_to_icon_path = {
        "Pokemon": "misc/pokemon.webp", "Item": "reward/item/0.webp", "Stardust": "reward/stardust/0.webp",
        "Candy": "reward/candy/0.webp", "XP": "reward/experience/0.webp", "Mega Energy": "reward/mega_resource/0.webp",
        "XL Candy": "reward/xl_candy/0.webp", "Pokecoin": "reward/pokecoin/0.webp", "Sticker": "reward/sticker/0.webp",
        "Incident": "reward/incident/0.webp", "Badge": "misc/badge_3.webp", "Egg": "misc/egg.webp",
        "Friendship": "misc/bestbuddy.webp"
    }

    for _, r in cat_gb.iterrows():
        cat_name = r['category']
        path = cat_to_icon_path.get(cat_name)
        if not path:
            if "Item" in cat_name: path = "reward/item/0.webp"
            elif "Pokemon" in cat_name: path = "misc/pokemon.webp"
            else: path = "misc/0.webp"

        icon_src = f"{ICON_BASE_URL}/{path}"
        row_content = [html.Strong(f"{r['count']:,}", style={'fontSize': '1.2em'})]
        row_content.insert(0, html.Img(src=icon_src, style={"width": "32px", "marginRight": "10px", "verticalAlign": "middle"}))
        sidebar.append(html.Div(row_content, className="d-flex align-items-center mb-2"))

    visual_content = html.Div()
    total_pages_val = 1

    if mode == "sum":
        fig = go.Figure()
        top = df.sort_values('count', ascending=False).head(20)
        fig.add_trace(go.Bar(x=top['name'], y=top['count'], marker_color='#17a2b8'))
        max_y = top['count'].max()
        for i, (idx, row) in enumerate(top.iterrows()):
            if row.get('icon'):
                fig.add_layout_image(dict(source=row['icon'], x=row['name'], y=row['count'], xref="x", yref="y", sizex=0.5, sizey=max_y*0.1, xanchor="center", yanchor="bottom"))
        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', title="Quest Activity")
        visual_content = dcc.Graph(figure=fig)

    elif mode == "surged":
        fig = go.Figure()
        df_time = df[pd.to_numeric(df['time_bucket'], errors='coerce').notnull()].copy()
        df_time['time_bucket'] = df_time['time_bucket'].astype(int)
        agg = df_time.groupby(['time_bucket', 'name', 'category'])['count'].sum().reset_index()
        top_names = agg.groupby('name')['count'].sum().nlargest(10).index
        agg = agg[agg['name'].isin(top_names)]

        for name in agg['name'].unique():
            d = agg[agg['name'] == name].sort_values('time_bucket')
            fig.add_trace(go.Scatter(x=d['time_bucket'], y=d['count'], mode='lines+markers', name=name))

        fig.update_xaxes(dtick=1)
        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', title="Quest Activity")
        visual_content = dcc.Graph(figure=fig)

    elif mode == "grouped":
        table_df = df.groupby(['name', 'type', 'category', 'icon'])['count'].sum().reset_index()
        col, ascending = sort['col'], sort['dir'] == "asc"
        if col in table_df.columns:
            table_df = table_df.sort_values(col, ascending=ascending)
        else:
            table_df = table_df.sort_values('count', ascending=False)

        rows_per_page = page['rows_per_page']
        total_rows = len(table_df)
        total_pages_val = max(1, (total_rows + rows_per_page - 1) // rows_per_page)
        current_page = min(max(1, page['current_page']), total_pages_val)
        page_df = table_df.iloc[(current_page - 1) * rows_per_page : current_page * rows_per_page]

        header_cells = [html.Th("Image", style={"backgroundColor": "#1a1a1a", "width": "60px"})]
        for c in ["type", "name", "count"]:
            label = c.title()
            arrow = " ▲" if col == c and ascending else (" ▼" if col == c else "")
            header_cells.append(html.Th(
                html.Span([label, html.Span(arrow, style={"color": "#aaa"})], id={"type": "quests-sort-header", "index": c}, style={"cursor": "pointer"}),
                style={"backgroundColor": "#1a1a1a"}
            ))

        rows = []
        for _, r in page_df.iterrows():
            rows.append(html.Tr([
                html.Td(html.Img(src=r['icon'], style={"width": "40px"})),
                html.Td(r['type']),
                html.Td(r['name']),
                html.Td(f"{r['count']:,}", className="text-end")
            ]))

        controls = html.Div([
            dbc.Row([
                dbc.Col([html.Span(f"Total: {total_rows} | Rows: ", className="me-2 align-middle"), dcc.Dropdown(id="quests-rows-per-page-selector", options=[{'label': str(x), 'value': x} for x in [10, 25, 50, 100]] + [{'label': 'All', 'value': total_rows}], value=rows_per_page, clearable=False, className="rows-per-page-selector", style={"width":"80px", "display":"inline-block", "color":"black", "verticalAlign": "middle"})], width="auto", className="d-flex align-items-center"),
                dbc.Col([
                    dbc.ButtonGroup([dbc.Button("<<", id="quests-first-page-btn", size="sm", disabled=current_page <= 1), dbc.Button("<", id="quests-prev-page-btn", size="sm", disabled=current_page <= 1)], className="me-2"),
                    html.Span("Page ", className="align-middle me-1"), dcc.Input(id="quests-goto-page-input", type="number", min=1, max=total_pages_val, value=current_page, debounce=True, style={"width": "60px", "textAlign": "center", "display": "inline-block", "color": "black"}), html.Span(f" of {total_pages_val}", className="align-middle ms-1 me-2"),
                    dbc.ButtonGroup([dbc.Button(">", id="quests-next-page-btn", size="sm", disabled=current_page >= total_pages_val), dbc.Button(">>", id="quests-last-page-btn", size="sm", disabled=current_page >= total_pages_val)]),
                ], width="auto", className="d-flex align-items-center justify-content-end ms-auto")
            ], className="g-0")
        ], className="p-2 bg-dark rounded mb-2 border border-secondary")

        visual_content = html.Div([
            controls,
            dbc.Table([html.Thead(html.Tr(header_cells)), html.Tbody(rows)], striped=True, hover=True, class_name="table-dark")
        ])

    return sidebar, visual_content, json.dumps(data, indent=2), total_pages_val
