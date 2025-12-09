import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State, ctx
import json
import os

# --- IMPORT TRANSLATION MANAGER ---
try:
    from dashboard.translations.manager import translate
except ImportError:
    from translations.manager import translate

GITHUB_URL = "https://github.com/HugoDataAnalyst/PsyduckV2"
icon_base_url = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main"

# Flag Icons (Using a reliable CDN for round country flags)
FLAG_ICONS = {
    "en": "https://cdnjs.cloudflare.com/ajax/libs/flag-icon-css/3.5.0/flags/1x1/gb.svg",
    "pt": "https://cdnjs.cloudflare.com/ajax/libs/flag-icon-css/3.5.0/flags/1x1/pt.svg",
    "de": "https://cdnjs.cloudflare.com/ajax/libs/flag-icon-css/3.5.0/flags/1x1/de.svg",
    "fr": "https://cdnjs.cloudflare.com/ajax/libs/flag-icon-css/3.5.0/flags/1x1/fr.svg",
}

ICONS = {
    "pokemon": f"{icon_base_url}/misc/pokemon.webp",
    "raid": f"{icon_base_url}/misc/raid2.webp",
    "invasion": f"{icon_base_url}/misc/invasion.webp",
    "quest": f"{icon_base_url}/misc/quest.webp"
}

# --- CONFIG & PATHS ---
def load_dashboard_config():
    """Load dashboard configuration from dashboard_config.json"""
    config_path = os.path.join(os.path.dirname(__file__), 'dashboard_config.json')
    default_config = {"map_name": "", "icon_url": "", "custom_navbar_links": []}
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # Merge with defaults to ensure all keys exist
                return {**default_config, **config}
    except Exception as e:
        print(f"Warning: Could not load dashboard_config.json: {e}")

    return default_config

# Load config
dashboard_config = load_dashboard_config()
MAP_NAME = dashboard_config.get("map_name", "")
ICON_URL = dashboard_config.get("icon_url", "")
CUSTOM_NAVBAR_LINKS = dashboard_config.get("custom_navbar_links", [])

# Build brand element with optional icon
def build_brand():
    """Build the navbar brand with optional icon between PsyduckV2 and map name"""
    parts = [html.Span("PsyduckV2")]

    if ICON_URL:
        parts.append(
            html.Img(
                src=ICON_URL,
                style={
                    "height":"30px",
                    "width":"30px",
                    "margin":"0 8px",
                    "verticalAlign":"middle"
                },
                crossOrigin="anonymous"
            )
        )
    if MAP_NAME:
        parts.append(html.Span(MAP_NAME, style={"marginLeft": "8px" if not ICON_URL else "0"}))

    return parts

brand_content = build_brand()

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[
        dbc.themes.DARKLY, dbc.icons.BOOTSTRAP,
        "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css",
        "https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css",
        "https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"
    ],
    external_scripts=[
        "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js",
        "https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js",
        "https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"
    ],
    suppress_callback_exceptions=True
)

# DYNAMIC CONTENT

# Build navbar items
def generate_navbar_items(lang="en"):
    """Generates navbar items using Image Flags for Windows compatibility."""
    items = [
        dbc.NavItem(dbc.NavLink([html.I(className="bi bi-house-fill me-2"), translate("Home", lang)], href="/", active="exact")),
        dbc.NavItem(dbc.NavLink([html.Img(src=ICONS["pokemon"], style={"height": "1.5em"}, className="me-2"), translate("Pokémon", lang)], href="/pokemon", active="exact")),
        dbc.NavItem(dbc.NavLink([html.Img(src=ICONS["raid"], style={"height": "1.5em"}, className="me-2"), translate("Raids", lang)], href="/raids", active="exact")),
        dbc.NavItem(dbc.NavLink([html.Img(src=ICONS["invasion"], style={"height": "1.5em"}, className="me-2"), translate("Invasions", lang)], href="/invasions", active="exact")),
        dbc.NavItem(dbc.NavLink([html.Img(src=ICONS["quest"], style={"height": "1.5em"}, className="me-2"), translate("Quests", lang)], href="/quests", active="exact")),
    ]

    # Add custom navbar links from config
    for link in CUSTOM_NAVBAR_LINKS:
        icon_class = link.get("icon", "bi bi-link-45deg")
        nav_link = dbc.NavLink(
            [html.I(className=f"{icon_class} me-2"), translate(link.get("name", "Custom Link"), lang)],
            href=link.get("url", "#"),
            target="_blank" if link.get("external", True) else "_self",
            external_link=link.get("external", True)
        )
        items.append(dbc.NavItem(nav_link))

    items.append(
        dbc.NavItem(dbc.NavLink(html.I(className="bi bi-github", style={"fontSize": "1.5rem"}, title=translate("Github", lang)), href=GITHUB_URL, target="_blank", external_link=True))
    )

    # IMAGE BASED LANGUAGE TOGGLE
    current_flag_url = FLAG_ICONS.get(lang, FLAG_ICONS["en"])

    # Define style for the flag images
    flag_style = {"height": "20px", "width": "20px", "borderRadius": "50%", "objectFit": "cover"}

    lang_dropdown = dbc.DropdownMenu(
        # The main toggle button shows just the current flag
        label=html.Img(src=current_flag_url, style=flag_style),
        children=[
            dbc.DropdownMenuItem([
                html.Img(src=FLAG_ICONS["en"], style={**flag_style, "marginRight": "10px"}),
                "English"
            ], id="lang-switch-en", n_clicks=0),
            dbc.DropdownMenuItem([
                html.Img(src=FLAG_ICONS["pt"], style={**flag_style, "marginRight": "10px"}),
                "Português"
            ], id="lang-switch-pt", n_clicks=0),
            dbc.DropdownMenuItem([
                html.Img(src=FLAG_ICONS["de"], style={**flag_style, "marginRight": "10px"}),
                "Deutsch"
            ], id="lang-switch-de", n_clicks=0),
            dbc.DropdownMenuItem([
                html.Img(src=FLAG_ICONS["fr"], style={**flag_style, "marginRight": "10px"}),
                "Français"
            ], id="lang-switch-fr", n_clicks=0),
        ],
        nav=True, in_navbar=True, align_end=True
    )
    items.append(lang_dropdown)

    return items

def generate_footer_content(lang="en"):
    return dbc.Row([
        dbc.Col([
            html.P([
                html.Span(f"{translate('Made with', lang)} ", className="text-muted"),
                html.Span("❤️", style={"color": "#e74c3c"}),
                html.Span(f" {translate('by', lang)} ", className="text-muted"),
                html.A([
                    html.Img(src="https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png", style={"height": "20px", "width": "20px", "verticalAlign": "middle", "marginRight": "5px", "filter": "invert(1)"}),
                    html.Span("HugoDataAnalyst/PsyduckV2", style={"verticalAlign": "middle"})
                ], href=GITHUB_URL, target="_blank", className="text-decoration-none text-muted", style={"fontWeight": "500"})
            ], className="text-center mb-3 small")
        ], width=12)
    ])

# LAYOUT COMPONENTS
navbar = dbc.NavbarSimple(
    children=[], brand=brand_content, brand_href="/", color="primary", dark=True, className="mb-3", id="main-navbar"
)

footer = dbc.Container([
    html.Hr(className="my-4"),
    html.Div(id="main-footer")
], fluid=True)

# App Layout
app.layout = dbc.Container([
    dcc.Store(id="language-store", storage_type="local", data="en"),
    navbar,
    dash.page_container,
    footer
], fluid=True, className="min-vh-100 d-flex flex-column")

# CALLBACKS
@app.callback(
    Output("language-store", "data"),
    [
        Input("lang-switch-en", "n_clicks"),
        Input("lang-switch-pt", "n_clicks"),
        Input("lang-switch-de", "n_clicks"),
        Input("lang-switch-fr", "n_clicks")
    ],
    State("language-store", "data"),
    prevent_initial_call=True
)
def update_language(n_en, n_pt, n_de, n_fr, current_lang):
    if not ctx.triggered: return current_lang
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "lang-switch-en": return "en"
    if button_id == "lang-switch-pt": return "pt"
    if button_id == "lang-switch-de": return "de"
    if button_id == "lang-switch-fr": return "fr"

    return current_lang

@app.callback(
    [Output("main-navbar", "children"), Output("main-footer", "children")],
    Input("language-store", "data")
)
def update_dynamic_content(lang):
    lang = lang or "en"
    return generate_navbar_items(lang), generate_footer_content(lang)

if __name__ == "__main__":
    app.run_server(debug=True)
