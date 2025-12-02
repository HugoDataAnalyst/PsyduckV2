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
    """Build the page title with optional icon"""
    parts = [html.Span("PsyduckV2")]

    if ICON_URL:
        parts.append(html.Img(
            src=ICON_URL,
            alt="Map Icon",
            style={
                "height": "50px",
                "width": "50px",
                "marginLeft": "15px",
                "marginRight": "15px",
                "verticalAlign": "middle",
                "objectFit": "contain",
                "display": "inline-block"
            },
            crossOrigin="anonymous"
        ))

    if MAP_NAME:
        parts.append(html.Span(MAP_NAME, style={"marginLeft": "15px" if not ICON_URL else "0"}))

    parts.append(html.Span(" Dashboard"))
    return parts

dash.register_page(__name__, path='/', title=page_title)

# Data Files
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
POKE_FILE = os.path.join(DATA_DIR, 'global_pokes.json')
RAID_FILE = os.path.join(DATA_DIR, 'global_raids.json')
INVASION_FILE = os.path.join(DATA_DIR, 'global_invasions.json')
QUEST_FILE = os.path.join(DATA_DIR, 'global_quests.json')

icon_base_url = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main"

# Icon Mapping
ICONS = {
    "iv0": "/assets/images/0iv.png",
    "iv100": "/assets/images/100iv.png",
    "pvp_little": f"{icon_base_url}/misc/500.webp",
    "pvp_great": f"{icon_base_url}/misc/1500.webp",
    "pvp_ultra": f"{icon_base_url}/misc/2500.webp",
    "shiny": f"{icon_base_url}/misc/sparkles.webp",
    "ar_quest": f"{icon_base_url}/misc/ar.webp"
}

def layout():
    return dbc.Container([
        # Refresh interval: check every 60 seconds
        dcc.Interval(id="home-interval", interval=60*1000, n_intervals=0),

        # Header
        dbc.Row([
            dbc.Col(
                html.Div([
                    html.H1(build_title_with_icon(), className="display-4 text-center mb-4 text-primary"),
                    html.P("Real-time Pokémon Go Analytics & Monitoring", className="text-center lead text-muted"),
                ]),
                width=12
            )
        ], className="my-4"),

        # Main Content Grid
        dbc.Row([
            # Pokémon Stats Module
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader([
                        html.Div([
                            html.I(className="bi bi-vinyl-fill me-2"),
                            html.Span("Pokémon Stats", className="fw-bold fs-5")
                        ], className="d-flex align-items-center")
                    ], className="bg-primary text-white"),

                    dbc.CardBody([
                        html.P("Global activity over the last 24 hours.", className="card-text text-muted mb-4 small"),
                        html.Div(id="global-pokemon-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                        html.Div(
                            dbc.Button("View Pokémons", href="/pokemon", color="outline-primary", size="sm", className="w-100"),
                            className="mt-auto"
                        )
                    ]),
                ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}),
                # CHANGED: xl=3 allows 4 cards to fit on one row (12/4 = 3)
                width=12, md=6, xl=3, className="mb-4"
            ),

            # Raids Stats Module
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader([
                        html.Div([
                            html.I(className="bi bi-lightning-fill me-2"),
                            html.Span("Raid Stats", className="fw-bold fs-5")
                        ], className="d-flex align-items-center")
                    ], className="bg-danger text-white"),

                    dbc.CardBody([
                        html.P("Global Raid Battles (24h).", className="card-text text-muted mb-4 small"),
                        html.Div(id="global-raid-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                        html.Div(
                            dbc.Button("View Raids", href="/raids", color="outline-danger", size="sm", className="w-100"),
                            className="mt-auto"
                        )
                    ]),
                ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}),
                # CHANGED: xl=3
                width=12, md=6, xl=3, className="mb-4"
            ),

            # Invasion Stats Module
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader([
                        html.Div([
                            html.I(className="bi bi-robot me-2"),
                            html.Span("Invasion Stats", className="fw-bold fs-5")
                        ], className="d-flex align-items-center")
                    ], className="bg-dark text-white", style={"borderBottom": "2px solid #555"}),

                    dbc.CardBody([
                        html.P("Global Team Rocket Activity (24h).", className="card-text text-muted mb-4 small"),
                        html.Div(id="global-invasion-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                        html.Div(dbc.Button("View Invasions", href="/invasions", color="outline-secondary", size="sm", className="w-100"), className="mt-auto")
                    ]),
                ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}),
                # CHANGED: xl=3
                width=12, md=6, xl=3, className="mb-4"
            ),

            # Quest Stats Module
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader([
                        html.Div([
                            html.I(className="bi bi-signpost-2-fill me-2"),
                            html.Span("Quest Stats", className="fw-bold fs-5")
                        ], className="d-flex align-items-center")
                    ], className="bg-info text-white"),

                    dbc.CardBody([
                        html.P("Global Field Research & Stops (24h).", className="card-text text-muted mb-4 small"),
                        html.Div(id="global-quest-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                        html.Div(dbc.Button("View Quests", href="/quests", color="outline-info", size="sm", className="w-100"), className="mt-auto")
                    ]),
                ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}),
                # CHANGED: xl=3
                width=12, md=6, xl=3, className="mb-4"
            ),

        ], className="g-4") # Grid gap

    ], fluid=True)


@callback(
    [Output("global-pokemon-stats-container", "children"),
     Output("global-raid-stats-container", "children"),
     Output("global-invasion-stats-container", "children"),
     Output("global-quest-stats-container", "children")],
    Input("home-interval", "n_intervals")
)
def update_all_stats(n):
    # Helper
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

    # Pokemon Logic
    poke_list = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(POKE_FILE):
        try:
            with open(POKE_FILE, 'r') as f:
                p_data = json.load(f)
            poke_list = [
                get_total_header(p_data.get('total', 0), "Total Spawns"),
                create_mini_stat(p_data.get('shiny', 0), "Shiny", "#FFD700", icon_url=ICONS['shiny']),
                create_mini_stat(p_data.get('iv100', 0), "100 IV", "#dc3545", icon_url=ICONS['iv100']),
                create_mini_stat(p_data.get('iv0', 0), "0 IV", "#28a745", icon_url=ICONS['iv0']),
                create_mini_stat(p_data.get('pvp_little', 0), "PvP Lit", "#e0e0e0", icon_url=ICONS['pvp_little']),
                create_mini_stat(p_data.get('pvp_great', 0), "PvP Grt", "#007bff", icon_url=ICONS['pvp_great']),
                create_mini_stat(p_data.get('pvp_ultra', 0), "PvP Ult", "#FFD700", icon_url=ICONS['pvp_ultra']),
            ]
        except Exception as e:
            print(f"Error loading poke stats: {e}")

    # Raid Logic
    raid_list = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(RAID_FILE):
        try:
            with open(RAID_FILE, 'r') as f:
                r_data = json.load(f)

            total_raids = r_data.get('total', 0)
            levels = r_data.get('raid_level', {})
            raid_list = [get_total_header(total_raids, "Total Raids")]

            priority_levels = ["1", "3", "5", "6", "11", "13", "15"]

            for lvl in priority_levels:
                count = levels.get(lvl, 0)
                if count > 0:
                    file_suffix = lvl
                    label = f"Level {lvl}"
                    color = "#e0e0e0"

                    if lvl == "1": color = "#e0e0e0"
                    elif lvl == "3": color = "#f0ad4e"
                    elif lvl == "5": color = "#dc3545"
                    elif lvl == "6": label = "Mega"; color = "#a020f0"
                    elif lvl == "7": label = "Mega 5"; color = "#7fce83"
                    elif lvl == "8": label = "Ultra Beast"; color = "#e881f1"
                    elif lvl == "9": label = "Extended Egg"; color = "#ce2c2c"
                    elif lvl == "10": label = "Primal"; color = "#ad5b2c"
                    elif lvl == "11": label = "Shadow L1"; color = "#0a0a0a"
                    elif lvl == "12": label = "Shadow L2"; color = "#0a0a0a"
                    elif lvl == "13": label = "Shadow L3"; color = "#0a0a0a"
                    elif lvl == "14": label = "Shadow L4"; color = "#0a0a0a"
                    elif lvl == "15": label = "Shadow L5"; color = "#0a0a0a"

                    icon_url = f"{icon_base_url}/raid/egg/{file_suffix}.webp"
                    raid_list.append(create_mini_stat(count, label, color, icon_url=icon_url))

        except Exception as e:
            print(f"Error loading raid stats: {e}")

    # Invasion Logic
    invasion_list = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(INVASION_FILE):
        try:
            with open(INVASION_FILE, 'r') as f:
                i_data = json.load(f)

            total_inv = i_data.get('total', 0)
            stats = i_data.get('stats', {})

            invasion_list = [
                get_total_header(total_inv, "Total Invasions"),
                create_mini_stat(stats.get('confirmed', 0), "Confirmed", "#28a745", icon_class="bi bi-check-circle-fill"),
                create_mini_stat(stats.get('unconfirmed', 0), "Unconfirmed", "#dc3545", icon_class="bi bi-x-circle-fill"),
            ]
        except Exception as e:
            print(f"Error loading invasion stats: {e}")

    # Quest Logic
    quest_list = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(QUEST_FILE):
        try:
            with open(QUEST_FILE, 'r') as f:
                q_data = json.load(f)

            total_stops = q_data.get('total_stops', 0)
            quests = q_data.get('quests', {})

            quest_list = [
                get_total_header(total_stops, "Total PokéStops"),
                create_mini_stat(quests.get('ar', 0), "AR Quests", "#17a2b8", icon_url=ICONS['ar_quest']),
                create_mini_stat(quests.get('normal', 0), "Normal", "#e0e0e0", icon_class="bi bi-vinyl"),
            ]
        except Exception as e:
            print(f"Error loading quest stats: {e}")

    return poke_list, raid_list, invasion_list, quest_list
