import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import json
import os
import time

# --- IMPORT TRANSLATION MANAGER ---
try:
    from dashboard.translations.manager import translate
except ImportError:
    from translations.manager import translate

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
    "ar_quest": f"{icon_base_url}/pokestop/0_ar.webp",
    "normal_quest": f"{icon_base_url}/pokestop/0.webp",
    "pokemon": f"{icon_base_url}/misc/pokemon.webp",
    "raid": f"{icon_base_url}/misc/raid2.webp",
    "invasion": f"{icon_base_url}/misc/invasion.webp",
    "quest": f"{icon_base_url}/misc/quest.webp",
    # Invasion character icons
    "grunt_male": f"{icon_base_url}/invasion/4.webp",
    "grunt_female": f"{icon_base_url}/invasion/5.webp",
    "cliff": f"{icon_base_url}/invasion/41.webp",
    "arlo": f"{icon_base_url}/invasion/42.webp",
    "sierra": f"{icon_base_url}/invasion/43.webp",
    "giovanni": f"{icon_base_url}/invasion/44.webp",
    "unset": f"{icon_base_url}/invasion/0.webp",
    # Quest reward icons
    "stardust": f"{icon_base_url}/reward/stardust/0.webp",
    "item": f"{icon_base_url}/reward/item/0.webp",
    "mega": f"{icon_base_url}/reward/mega_resource/0.webp",
    "candy": f"{icon_base_url}/reward/candy/1.webp",
    "xp": f"{icon_base_url}/reward/experience/0.webp",
    "encounter": f"{icon_base_url}/misc/pokemon.webp"
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
        labelCheckedClassName="active"
        #style={"marginLeft": "auto"}
    )

def create_mini_stat(count, label, color, icon_url=None, icon_class=None, sub_items=None):
    """
    Create a mini stat card with optional sub-items breakdown.

    Args:
        count: Main count to display
        label: Label for the stat
        color: Color for the count text
        icon_url: URL for icon image
        icon_class: Bootstrap icon class
        sub_items: Optional list of dicts with {'icon', 'count', 'label'} for breakdown
    """
    if icon_url:
        icon = html.Img(src=icon_url, style={"height": "24px", "width": "24px", "marginRight": "8px"})
    elif icon_class:
        icon = html.I(className=f"{icon_class} me-2", style={"fontSize": "1.3rem", "color": color})
    else:
        icon = None

    main_content = [
        html.Div(icon, className="d-flex align-items-center"),
        html.Div([
            html.Div(f"{count:,}", className="fw-bold", style={"color": color, "fontSize": "1.1rem"}),
            html.Div(label, className="text-muted", style={"fontSize": "0.7rem", "textTransform": "uppercase"})
        ])
    ]

    # Add sub-items if provided
    if sub_items:
        sub_content = []
        for item in sub_items:
            if item.get('count', 0) > 0:
                sub_content.append(
                    html.Span([
                        html.Img(src=item['icon'], style={"height": "14px", "width": "14px", "marginRight": "2px", "verticalAlign": "middle"}) if item.get('icon') else None,
                        html.Span(f"{item['count']:,}", style={"fontSize": "0.65rem", "color": "#aaa"})
                    ], style={"marginRight": "6px"})
                )
        if sub_content:
            main_content.append(html.Div(sub_content, className="d-flex flex-wrap mt-1", style={"marginLeft": "32px"}))

    return html.Div(main_content, className="d-flex flex-column bg-dark rounded p-2 px-3", style={"border": "1px solid #333", "flex": "1 1 40%"})

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
                html.P("Real-time Pokémon Go Analytics & Monitoring", id="home-subtitle", className="text-center lead text-muted"),
            ]), width=12)
        ], className="my-4"),

        # Grid
        dbc.Row([
            # Pokemon
            dbc.Col(dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.Div([
                            html.Img(src=ICONS["pokemon"], style={"height": "1.5em", "width": "auto"}, className="me-2"),
                            html.Span("Pokémon Stats", id="poke-header-text", className="fw-bold fs-5")
                        ], className="d-flex align-items-center justify-content-center mb-2"),
                        html.Div(
                            create_time_toggle("poke-time-toggle"),
                            className="d-flex justify-content-center w-100"
                        )
                    ], className="d-flex flex-column w-100")
                ], className="bg-primary text-white"),
                dbc.CardBody([
                    html.P(id="poke-desc", children="Global activity over the last 24 hours.", className="card-text text-muted mb-4 small text-center"),
                    html.Div(id="global-pokemon-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                    html.Div(dbc.Button("View Pokémons", id="btn-view-poke", href="/pokemon", color="outline-primary", size="sm", className="w-100"), className="mt-auto")
                ]),
            ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}), width=12, md=6, xl=3, className="mb-4"),

            # Raids
            dbc.Col(dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.Div([
                            html.Img(src=ICONS["raid"], style={"height": "1.5em", "width": "auto"}, className="me-2"),
                            html.Span("Raid Stats", id="raid-header-text", className="fw-bold fs-5"),
                        ], className="d-flex align-items-center justify-content-center mb-2"),
                        html.Div(
                            create_time_toggle("raid-time-toggle"),
                            className="d-flex justify-content-center w-100"
                        )
                    ], className="d-flex flex-column w-100")
                ], className="bg-danger text-white"),
                dbc.CardBody([
                    html.P(id="raid-desc", children="Global Raid Battles (24h).", className="card-text text-muted mb-4 small text-center"),
                    html.Div(id="global-raid-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                    html.Div(dbc.Button("View Raids", id="btn-view-raid", href="/raids", color="outline-danger", size="sm", className="w-100"), className="mt-auto")
                ]),
            ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}), width=12, md=6, xl=3, className="mb-4"),

            # Invasions
            dbc.Col(dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.Div([
                            html.Img(src=ICONS["invasion"], style={"height": "1.5em", "width": "auto"}, className="me-2"),
                            html.Span("Invasion Stats", id="inv-header-text", className="fw-bold fs-5")
                        ], className="d-flex align-items-center justify-content-center mb-2"),
                        html.Div(
                            create_time_toggle("invasion-time-toggle"),
                            className="d-flex justify-content-center w-100"
                        )
                    ], className="d-flex flex-column w-100")
                ], className="bg-dark text-white", style={"borderBottom": "2px solid #555"}),
                dbc.CardBody([
                    html.P(id="inv-desc", children="Global Team Rocket Activity (24h).", className="card-text text-muted mb-4 small text-center"),
                    html.Div(id="global-invasion-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                    html.Div(dbc.Button("View Invasions", id="btn-view-inv", href="/invasions", color="outline-secondary", size="sm", className="w-100"), className="mt-auto")
                ]),
            ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}), width=12, md=6, xl=3, className="mb-4"),

            # Quests
            dbc.Col(dbc.Card([
                dbc.CardHeader([
                    html.Div([
                        html.Div([
                            html.Img(src=ICONS["quest"], style={"height": "1.5em", "width": "auto"}, className="me-2"),
                            html.Span("Quest Stats", id="quest-header-text", className="fw-bold fs-5")
                        ], className="d-flex align-items-center justify-content-center mb-2"),
                        html.Div(
                            create_time_toggle("quest-time-toggle"),
                            className="d-flex justify-content-center w-100"
                        )
                    ], className="d-flex flex-column w-100")
                ], className="bg-info text-white"),
                dbc.CardBody([
                    html.P(id="quest-desc", children="Global Field Research & Stops (24h).", className="card-text text-muted mb-4 small text-center"),
                    html.Div(id="global-quest-stats-container", className="d-flex flex-wrap justify-content-around gap-3 mb-4"),
                    html.Div(dbc.Button("View Quests", id="btn-view-quest", href="/quests", color="outline-info", size="sm", className="w-100"), className="mt-auto")
                ]),
            ], className="h-100 shadow-sm border-0", style={"backgroundColor": "#222"}), width=12, md=6, xl=3, className="mb-4"),

        ], className="g-4")
    ], fluid=True)

# CALLBACKS

# 0. Static Translation Callback
@callback(
    [Output("home-subtitle", "children"),
     Output("poke-header-text", "children"), Output("btn-view-poke", "children"),
     Output("raid-header-text", "children"), Output("btn-view-raid", "children"),
     Output("inv-header-text", "children"), Output("btn-view-inv", "children"),
     Output("quest-header-text", "children"), Output("btn-view-quest", "children")],
    Input("language-store", "data")
)
def update_static_translations(lang):
    lang = lang or "en"
    return (
        translate("Real-time Pokémon Go Analytics & Monitoring", lang),
        translate("Pokémon Stats", lang), translate("View Pokémons", lang),
        translate("Raid Stats", lang), translate("View Raids", lang),
        translate("Invasion Stats", lang), translate("View Invasions", lang),
        translate("Quest Stats", lang), translate("View Quests", lang)
    )

# 1. Pokemon Callback
@callback(
    [Output("global-pokemon-stats-container", "children"), Output("poke-desc", "children")],
    [Input("home-interval", "n_intervals"), Input("poke-time-toggle", "value"), Input("language-store", "data")]
)
def update_pokemon(n, toggle_val, lang):
    lang = lang or "en"
    file_path = POKE_FILE if toggle_val == "24h" else POKE_FILE_ALL

    # Translate label based on toggle
    if toggle_val == "24h":
        label = translate("Global activity over the last 24 hours.", lang)
    else:
        label = translate("Global activity (All Time).", lang)

    content = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            content = [
                get_total_header(data.get('total', 0), translate("Total Spawns", lang)),
                create_mini_stat(data.get('shiny', 0), translate("Shiny", lang), "#FFD700", icon_url=ICONS['shiny']),
                create_mini_stat(data.get('iv100', 0), translate("100 IV", lang), "#dc3545", icon_url=ICONS['iv100']),
                create_mini_stat(data.get('iv0', 0), translate("0 IV", lang), "#28a745", icon_url=ICONS['iv0']),
                create_mini_stat(data.get('pvp_little', 0), translate("PvP Lit", lang), "#e0e0e0", icon_url=ICONS['pvp_little']),
                create_mini_stat(data.get('pvp_great', 0), translate("PvP Grt", lang), "#007bff", icon_url=ICONS['pvp_great']),
                create_mini_stat(data.get('pvp_ultra', 0), translate("PvP Ult", lang), "#FFD700", icon_url=ICONS['pvp_ultra']),
            ]
        except Exception as e:
            print(f"Error pokemon: {e}")

    return wrap_anim(content), label

# 2. Raid Callback
@callback(
    [Output("global-raid-stats-container", "children"), Output("raid-desc", "children")],
    [Input("home-interval", "n_intervals"), Input("raid-time-toggle", "value"), Input("language-store", "data")]
)
def update_raids(n, toggle_val, lang):
    lang = lang or "en"
    file_path = RAID_FILE if toggle_val == "24h" else RAID_FILE_ALL

    if toggle_val == "24h":
        label = translate("Global Raid Battles (24h).", lang)
    else:
        label = translate("Global Raid Battles (All Time).", lang)

    content = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            total = data.get('total', 0)
            levels = data.get('raid_level', {})
            content = [get_total_header(total, translate("Total Raids", lang))]

            priority_levels = ["1", "3", "5", "6", "11", "13", "15"]
            for lvl in priority_levels:
                count = levels.get(lvl, 0)
                if count > 0:
                    l_color = "#e0e0e0"
                    l_label = f"{translate('Level', lang)} {lvl}"
                    if lvl == "3": l_color = "#f0ad4e"
                    elif lvl == "5": l_color = "#dc3545"
                    elif lvl == "6": l_label = translate("Mega", lang); l_color = "#a020f0"
                    elif lvl == "7": l_label = translate("Mega 5", lang); l_color = "#7fce83"
                    elif lvl == "8": l_label = translate("Ultra Beast", lang); l_color = "#e881f1"
                    elif lvl == "10": l_label = translate("Primal", lang); l_color = "#ad5b2c"
                    elif lvl in ["11","12","13","14","15"]: l_label = f"{translate('Shadow', lang)} L{int(lvl)-10}"; l_color = "#0a0a0a"

                    content.append(create_mini_stat(count, l_label, l_color, icon_url=f"{icon_base_url}/raid/egg/{lvl}.webp"))
        except Exception as e:
            print(f"Error raids: {e}")

    return wrap_anim(content), label

# 3. Invasion Callback
@callback(
    [Output("global-invasion-stats-container", "children"), Output("inv-desc", "children")],
    [Input("home-interval", "n_intervals"), Input("invasion-time-toggle", "value"), Input("language-store", "data")]
)
def update_invasions(n, toggle_val, lang):
    lang = lang or "en"
    file_path = INVASION_FILE if toggle_val == "24h" else INVASION_FILE_ALL

    if toggle_val == "24h":
        label = translate("Global Team Rocket Activity (24h).", lang)
    else:
        label = translate("Global Team Rocket Activity (All Time).", lang)

    content = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            stats = data.get('stats', {})

            # Build content with new breakdown categories
            content = [get_total_header(data.get('total', 0), translate("Total Invasions", lang))]

            # Male & Female Grunts
            male_count = stats.get('male', 0)
            female_count = stats.get('female', 0)
            if male_count > 0:
                content.append(create_mini_stat(male_count, translate("Male", lang), "#3498db", icon_url=ICONS['grunt_male']))
            if female_count > 0:
                content.append(create_mini_stat(female_count, translate("Female", lang), "#e91e63", icon_url=ICONS['grunt_female']))

            # Leaders (Cliff, Arlo, Sierra)
            cliff_count = stats.get('cliff', 0)
            arlo_count = stats.get('arlo', 0)
            sierra_count = stats.get('sierra', 0)
            if cliff_count > 0:
                content.append(create_mini_stat(cliff_count, "Cliff", "#d32f2f", icon_url=ICONS['cliff']))
            if arlo_count > 0:
                content.append(create_mini_stat(arlo_count, "Arlo", "#f57c00", icon_url=ICONS['arlo']))
            if sierra_count > 0:
                content.append(create_mini_stat(sierra_count, "Sierra", "#7b1fa2", icon_url=ICONS['sierra']))

            # Giovanni
            giovanni_count = stats.get('giovanni', 0)
            if giovanni_count > 0:
                content.append(create_mini_stat(giovanni_count, "Giovanni", "#212121", icon_url=ICONS['giovanni']))

            # Unset (if any)
            unset_count = stats.get('unset', 0)
            if unset_count > 0:
                content.append(create_mini_stat(unset_count, translate("Unknown", lang), "#6c757d", icon_url=ICONS['unset']))

            # Fallback for old data format (confirmed/unconfirmed)
            if not any([male_count, female_count, cliff_count, arlo_count, sierra_count, giovanni_count]):
                confirmed = stats.get('confirmed', 0)
                unconfirmed = stats.get('unconfirmed', 0)
                if confirmed > 0 or unconfirmed > 0:
                    content.append(create_mini_stat(confirmed, translate("Confirmed", lang), "#28a745", icon_class="bi bi-check-circle-fill"))
                    content.append(create_mini_stat(unconfirmed, translate("Unconfirmed", lang), "#dc3545", icon_class="bi bi-x-circle-fill"))

        except Exception as e:
            print(f"Error invasions: {e}")

    return wrap_anim(content), label

# 4. Quest Callback
@callback(
    [Output("global-quest-stats-container", "children"), Output("quest-desc", "children")],
    [Input("home-interval", "n_intervals"), Input("quest-time-toggle", "value"), Input("language-store", "data")]
)
def update_quests(n, toggle_val, lang):
    lang = lang or "en"
    file_path = QUEST_FILE if toggle_val == "24h" else QUEST_FILE_ALL

    if toggle_val == "24h":
        label = translate("Global Field Research & Stops (24h).", lang)
    else:
        label = translate("Global Field Research & Stops (All Time).", lang)

    content = [html.Div("Loading...", className="text-muted small")]
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            quests = data.get('quests', {})

            # New format: quests.ar and quests.normal are dicts with 'total' and 'rewards'
            ar_data = quests.get('ar', {})
            normal_data = quests.get('normal', {})

            if isinstance(ar_data, dict):
                ar_total = ar_data.get('total', 0)
                ar_rewards = ar_data.get('rewards', {})
            else:
                ar_total = int(ar_data) if ar_data else 0
                ar_rewards = {}

            if isinstance(normal_data, dict):
                normal_total = normal_data.get('total', 0)
                normal_rewards = normal_data.get('rewards', {})
            else:
                normal_total = int(normal_data) if normal_data else 0
                normal_rewards = {}

            total_stops = data.get('total_stops', data.get('total_pokestops', ar_total + normal_total))

            content = [get_total_header(total_stops, translate("Total PokéStops", lang))]

            # Helper to build reward list items
            def build_reward_rows(rewards):
                rows = []
                reward_order = ['pokemon', 'item', 'stardust', 'mega', 'candy', 'xp']
                reward_icons = {
                    'pokemon': ICONS['encounter'],
                    'item': ICONS['item'],
                    'stardust': ICONS['stardust'],
                    'mega': ICONS['mega'],
                    'candy': ICONS['candy'],
                    'xp': ICONS['xp']
                }
                reward_labels = {
                    'pokemon': 'Pokemon',
                    'item': 'Item',
                    'stardust': 'Stardust',
                    'mega': 'Mega',
                    'candy': 'Candy',
                    'xp': 'XP'
                }
                for key in reward_order:
                    count = rewards.get(key, 0)
                    if count > 0:
                        rows.append(html.Div([
                            html.Img(src=reward_icons[key], style={"height": "16px", "width": "16px", "marginRight": "6px"}),
                            html.Span(f"{count:,}", style={"fontWeight": "bold", "marginRight": "4px"}),
                            html.Span(reward_labels[key], className="text-muted", style={"fontSize": "0.75rem"})
                        ], className="d-flex align-items-center", style={"marginBottom": "2px"}))
                return rows

            # Build AR Quest Section
            ar_reward_rows = build_reward_rows(ar_rewards)
            ar_section = html.Div([
                # Header with icon, total, and label
                html.Div([
                    html.Img(src=ICONS['ar_quest'], style={"height": "24px", "width": "24px", "marginRight": "8px"}),
                    html.Div([
                        html.Div(f"{ar_total:,}", className="fw-bold", style={"color": "#17a2b8", "fontSize": "1.1rem"}),
                        html.Div(translate("AR Quests", lang), className="text-muted", style={"fontSize": "0.7rem", "textTransform": "uppercase"})
                    ])
                ], className="d-flex align-items-center mb-2"),
                # Reward breakdown list
                html.Div(ar_reward_rows, style={"marginLeft": "32px"}) if ar_reward_rows else None
            ], className="bg-dark rounded p-2 px-3", style={"border": "1px solid #333", "flex": "1 1 45%", "minWidth": "140px"})

            # Build Normal Quest Section
            normal_reward_rows = build_reward_rows(normal_rewards)
            normal_section = html.Div([
                # Header with icon, total, and label
                html.Div([
                    html.Img(src=ICONS['normal_quest'], style={"height": "24px", "width": "24px", "marginRight": "8px"}),
                    html.Div([
                        html.Div(f"{normal_total:,}", className="fw-bold", style={"color": "#e0e0e0", "fontSize": "1.1rem"}),
                        html.Div(translate("Normal", lang), className="text-muted", style={"fontSize": "0.7rem", "textTransform": "uppercase"})
                    ])
                ], className="d-flex align-items-center mb-2"),
                # Reward breakdown list
                html.Div(normal_reward_rows, style={"marginLeft": "32px"}) if normal_reward_rows else None
            ], className="bg-dark rounded p-2 px-3", style={"border": "1px solid #333", "flex": "1 1 45%", "minWidth": "140px"})

            content.append(ar_section)
            content.append(normal_section)

        except Exception as e:
            print(f"Error quests: {e}")

    return wrap_anim(content), label
