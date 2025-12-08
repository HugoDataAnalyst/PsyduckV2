import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import json
import os
import time

def load_dashboard_config():
    """Load dashboard configuration from dashboard_config.json"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'dashboard_config.json')
    default_config = {"map_name": "", "icon_url": ""}
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return {**default_config, **config}
    except Exception as e:
        print(f"Warning: Could not load dashboard_config.json: {e}")
    return default_config

dashboard_config = load_dashboard_config()
MAP_NAME = dashboard_config.get("map_name", "")
ICON_URL = dashboard_config.get("icon_url", "")
page_title = f"PsyduckV2 {MAP_NAME}".strip() if MAP_NAME else "PsyduckV2"

def build_title_with_icon():
    parts = [html.Span("PsyduckV2")]
    if ICON_URL:
        parts.append(html.Img(
            src=ICON_URL,
            alt="Map Icon",
            style={"height": "50px", "width": "50px", "marginLeft": "15px", "marginRight": "15px", "verticalAlign": "middle", "objectFit": "contain", "display": "inline-block"},
            crossOrigin="anonymous"
        ))
    if MAP_NAME:
        parts.append(html.Span(MAP_NAME, style={"marginLeft": "15px" if not ICON_URL else "0"}))
    parts.append(html.Span(" Dashboard"))
    return parts

dash.register_page(__name__, path='/', title=page_title)

# CONFIG & PATHS
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
POKE_FILE = os.path.join(DATA_DIR, 'global_pokes.json')
RAID_FILE = os.path.join(DATA_DIR, 'global_raids.json')
INVASION_FILE = os.path.join(DATA_DIR, 'global_invasions.json')
QUEST_FILE = os.path.join(DATA_DIR, 'global_quests.json')

POKE_FILE_ALL = os.path.join(DATA_DIR, 'global_pokes_alltime.json')
RAID_FILE_ALL = os.path.join(DATA_DIR, 'global_raids_alltime.json')
INVASION_FILE_ALL = os.path.join(DATA_DIR, 'global_invasions_alltime.json')
QUEST_FILE_ALL = os.path.join(DATA_DIR, 'global_quests_alltime.json')

icon_base_url = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main"

ICONS = {
    "iv0": "/assets/images/0iv.png",
    "iv100": "/assets/images/100iv.png",
    "pvp_little": f"{icon_base_url}/misc/500.webp",
    "pvp_great": f"{icon_base_url}/misc/1500.webp",
    "pvp_ultra": f"{icon_base_url}/misc/2500.webp",
    "shiny": f"{icon_base_url}/misc/sparkles.webp",
    "ar_quest": f"{icon_base_url}/misc/ar.webp",
    "pokemon": f"{icon_base_url}/misc/pokemon.webp",
    "raid": f"{icon_base_url}/misc/raid2.webp",
    "invasion": f"{icon_base_url}/misc/invasion.webp",
    "quest": f"{icon_base_url}/misc/quest.webp"
}

# HELPERS

def create_time_toggle(id_name):
    return dbc.RadioItems(
        id=id_name,
        options=[{"label": "24h", "value": "24h"}, {"label": "All", "value": "all"}],
        value="24h",
        inline=True,
        className="btn-group",
        inputClassName="btn-check",
        labelClassName="btn btn-outline-light btn-sm cursor-pointer",
        labelCheckedClassName="active",
        style={"marginLeft": "auto"}
    )

def create_mini_stat(count, label, color, icon_url=None, icon_class=None):
    if icon_url:
        icon = html.Img(src=icon_url, style={"height": "24px", "width": "24px", "marginRight": "8px"})
    elif icon_class:
        icon = html.I(className=f"{icon_class} me-2", style={"fontSize": "1.3rem", "color": color})
    else:
        icon = None

    return html.Div([
        html.Div(icon, className="d-flex align-items-center"),
        html.Div([
            html.Div(f"{count:,}", className="fw-bold", style={"color": color, "fontSize": "1.1rem"}),
            html.Div(label, className="text-muted", style={"fontSize": "0.7rem", "textTransform": "uppercase"})
        ])
    ], className="d-flex align-items-center bg-dark rounded p-2 px-3", style={"border": "1px solid #333", "flex": "1 1 40%"})

def get_total_header(count, title):
    return html.Div([
        html.H3(f"{count:,}", className="text-white fw-bold mb-0"),
        html.Small(title, className="text-muted text-uppercase")
    ], className="w-100 text-center mb-3 pb-3 border-bottom border-secondary")

def wrap_anim(content):
    """Wraps content in a div with the animate-flip class.
    The key=time.time() forces React to rebuild the element, triggering the CSS animation."""
    return html.Div(content, className="animate-flip d-flex flex-wrap justify-content-around gap-3 w-100", key=f"{time.time()}")

# LAYOUT

def layout():
    return dbc.Container([
        dcc.Interval(id="home-interval", interval=60*1000, n_intervals=0),

        # Header
        dbc.Row([
            dbc.Col(html.Div([
                html.H1(build_title_with_icon(), className="display-4 text-center mb-4 text-primary"),
                html.P("Real-time Pokémon Go Analytics & Monitoring", className="text-center lead text-muted"),
            ]), width=12)
        ], className="my-4"),

        # Grid
        dbc.Row([
            # Pokemon
            dbc.Col(dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.Div(
                            [
                                html.Img(src=ICONS["pokemon"], style={"height": "1.5em", "width": "auto"}, className="me-2"),
                                html.Span("Pokémon Stats", className="fw-bold fs-5")
                            ],
                        className="d-flex align-items-center me-3"),
                        create_time_toggle("poke-time-toggle")
                    ], className="d-flex justify-content-between align-items-center w-100 flex-wrap")
                ], className="bg-primary text-white"),
                dbc.CardBody([
                    html.P(id="poke-desc", children="Global activity over the last 24 hours.", className="card-text text-muted mb-4 small"),
                    html.Div(id="global-pokemon-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                    html.Div(dbc.Button("View Pokémons", href="/pokemon", color="outline-primary", size="sm", className="w-100"), className="mt-auto")
                ]),
            ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}), width=12, md=6, xl=3, className="mb-4"),

            # Raids
            dbc.Col(dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.Div(
                            [
                                html.Img(src=ICONS["raid"], style={"height": "1.5em", "width": "auto"}, className="me-2"),
                                html.Span("Raid Stats", className="fw-bold fs-5"),
                            ],
                        className="d-flex align-items-center me-3"),
                        create_time_toggle("raid-time-toggle")
                    ], className="d-flex justify-content-between align-items-center w-100 flex-wrap")
                ], className="bg-danger text-white"),
                dbc.CardBody([
                    html.P(id="raid-desc", children="Global Raid Battles (24h).", className="card-text text-muted mb-4 small"),
                    html.Div(id="global-raid-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                    html.Div(dbc.Button("View Raids", href="/raids", color="outline-danger", size="sm", className="w-100"), className="mt-auto")
                ]),
            ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}), width=12, md=6, xl=3, className="mb-4"),

            # Invasions
            dbc.Col(dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.Div(
                            [
                                html.Img(src=ICONS["invasion"], style={"height": "1.5em", "width": "auto"}, className="me-2"),
                                html.Span("Invasion Stats", className="fw-bold fs-5")
                            ],
                        className="d-flex align-items-center me-3"),
                        create_time_toggle("invasion-time-toggle")
                    ], className="d-flex justify-content-between align-items-center w-100 flex-wrap")
                ], className="bg-dark text-white", style={"borderBottom": "2px solid #555"}),
                dbc.CardBody([
                    html.P(id="inv-desc", children="Global Team Rocket Activity (24h).", className="card-text text-muted mb-4 small"),
                    html.Div(id="global-invasion-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                    html.Div(dbc.Button("View Invasions", href="/invasions", color="outline-secondary", size="sm", className="w-100"), className="mt-auto")
                ]),
            ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}), width=12, md=6, xl=3, className="mb-4"),

            # Quests
            dbc.Col(dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.Div(
                            [
                                html.Img(src=ICONS["quest"], style={"height": "1.5em", "width": "auto"}, className="me-2"),
                                html.Span("Quest Stats", className="fw-bold fs-5")
                            ],
                            className="d-flex align-items-center me-3"),
                        create_time_toggle("quest-time-toggle")
                    ], className="d-flex justify-content-between align-items-center w-100 flex-wrap")
                ], className="bg-info text-white"),
                dbc.CardBody([
                    html.P(id="quest-desc", children="Global Field Research & Stops (24h).", className="card-text text-muted mb-4 small"),
                    html.Div(id="global-quest-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                    html.Div(dbc.Button("View Quests", href="/quests", color="outline-info", size="sm", className="w-100"), className="mt-auto")
                ]),
            ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}), width=12, md=6, xl=3, className="mb-4"),

        ], className="g-4")
    ], fluid=True)

# CALLBACKS

# 1. Pokemon Callback
@callback(
    [Output("global-pokemon-stats-container", "children"), Output("poke-desc", "children")],
    [Input("home-interval", "n_intervals"), Input("poke-time-toggle", "value")]
)
def update_pokemon(n, toggle_val):
    file_path = POKE_FILE if toggle_val == "24h" else POKE_FILE_ALL
    label = "Global activity over the last 24 hours." if toggle_val == "24h" else "Global activity (All Time)."

    content = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            content = [
                get_total_header(data.get('total', 0), "Total Spawns"),
                create_mini_stat(data.get('shiny', 0), "Shiny", "#FFD700", icon_url=ICONS['shiny']),
                create_mini_stat(data.get('iv100', 0), "100 IV", "#dc3545", icon_url=ICONS['iv100']),
                create_mini_stat(data.get('iv0', 0), "0 IV", "#28a745", icon_url=ICONS['iv0']),
                create_mini_stat(data.get('pvp_little', 0), "PvP Lit", "#e0e0e0", icon_url=ICONS['pvp_little']),
                create_mini_stat(data.get('pvp_great', 0), "PvP Grt", "#007bff", icon_url=ICONS['pvp_great']),
                create_mini_stat(data.get('pvp_ultra', 0), "PvP Ult", "#FFD700", icon_url=ICONS['pvp_ultra']),
            ]
        except Exception as e:
            print(f"Error pokemon: {e}")

    return wrap_anim(content), label

# 2. Raid Callback
@callback(
    [Output("global-raid-stats-container", "children"), Output("raid-desc", "children")],
    [Input("home-interval", "n_intervals"), Input("raid-time-toggle", "value")]
)
def update_raids(n, toggle_val):
    file_path = RAID_FILE if toggle_val == "24h" else RAID_FILE_ALL
    label = "Global Raid Battles (24h)." if toggle_val == "24h" else "Global Raid Battles (All Time)."

    content = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            total = data.get('total', 0)
            levels = data.get('raid_level', {})
            content = [get_total_header(total, "Total Raids")]

            priority_levels = ["1", "3", "5", "6", "11", "13", "15"]
            for lvl in priority_levels:
                count = levels.get(lvl, 0)
                if count > 0:
                    l_color = "#e0e0e0"
                    l_label = f"Level {lvl}"
                    if lvl == "3": l_color = "#f0ad4e"
                    elif lvl == "5": l_color = "#dc3545"
                    elif lvl == "6": l_label = "Mega"; l_color = "#a020f0"
                    elif lvl == "7": l_label = "Mega 5"; l_color = "#7fce83"
                    elif lvl == "8": l_label = "Ultra Beast"; l_color = "#e881f1"
                    elif lvl == "10": l_label = "Primal"; l_color = "#ad5b2c"
                    elif lvl in ["11","12","13","14","15"]: l_label = f"Shadow L{int(lvl)-10}"; l_color = "#0a0a0a"

                    content.append(create_mini_stat(count, l_label, l_color, icon_url=f"{icon_base_url}/raid/egg/{lvl}.webp"))
        except Exception as e:
            print(f"Error raids: {e}")

    return wrap_anim(content), label

# 3. Invasion Callback
@callback(
    [Output("global-invasion-stats-container", "children"), Output("inv-desc", "children")],
    [Input("home-interval", "n_intervals"), Input("invasion-time-toggle", "value")]
)
def update_invasions(n, toggle_val):
    file_path = INVASION_FILE if toggle_val == "24h" else INVASION_FILE_ALL
    label = "Global Team Rocket Activity (24h)." if toggle_val == "24h" else "Global Team Rocket Activity (All Time)."

    content = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            stats = data.get('stats', {})
            content = [
                get_total_header(data.get('total', 0), "Total Invasions"),
                create_mini_stat(stats.get('confirmed', 0), "Confirmed", "#28a745", icon_class="bi bi-check-circle-fill"),
                create_mini_stat(stats.get('unconfirmed', 0), "Unconfirmed", "#dc3545", icon_class="bi bi-x-circle-fill"),
            ]
        except Exception as e:
            print(f"Error invasions: {e}")

    return wrap_anim(content), label

# 4. Quest Callback
@callback(
    [Output("global-quest-stats-container", "children"), Output("quest-desc", "children")],
    [Input("home-interval", "n_intervals"), Input("quest-time-toggle", "value")]
)
def update_quests(n, toggle_val):
    file_path = QUEST_FILE if toggle_val == "24h" else QUEST_FILE_ALL
    label = "Global Field Research & Stops (24h)." if toggle_val == "24h" else "Global Field Research & Stops (All Time)."

    content = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            quests = data.get('quests', {})
            content = [
                get_total_header(data.get('total_pokestops', 0), "Total PokéStops"),
                create_mini_stat(quests.get('ar', 0), "AR Quests", "#17a2b8", icon_url=ICONS['ar_quest']),
                create_mini_stat(quests.get('normal', 0), "Normal", "#e0e0e0", icon_class="bi bi-vinyl"),
            ]
        except Exception as e:
            print(f"Error quests: {e}")

    return wrap_anim(content), label
