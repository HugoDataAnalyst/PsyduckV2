import dash
import dash_bootstrap_components as dbc
from dash import html
import json
import os

GITHUB_URL = "https://github.com/HugoDataAnalyst/PsyduckV2"

# Load Dashboard Configuration
def load_dashboard_config():
    """Load dashboard configuration from dashboard_config.json"""
    config_path = os.path.join(os.path.dirname(__file__), 'dashboard_config.json')
    default_config = {
        "map_name": "",
        "icon_url": "",
        "custom_navbar_links": []
    }

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
        parts.append(html.Img(
            src=ICON_URL,
            alt="Map Icon",
            style={
                "height": "30px",
                "width": "30px",
                "marginLeft": "8px",
                "marginRight": "8px",
                "verticalAlign": "middle",
                "objectFit": "contain",
                "display": "inline-block"
            },
            # Add crossorigin attribute for CORS
            crossOrigin="anonymous"
        ))

    if MAP_NAME:
        parts.append(html.Span(MAP_NAME, style={"marginLeft": "8px" if not ICON_URL else "0"}))

    return parts

brand_content = build_brand()

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[
        dbc.themes.DARKLY,
        dbc.icons.BOOTSTRAP,
        "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css",  # Leaflet CSS for maps
        "https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css",  # Leaflet MarkerCluster CSS
        "https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"  # Leaflet MarkerCluster default styles
    ],
    external_scripts=[
        "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js",  # Leaflet JS for maps
        "https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js",  # Leaflet Heatmap Plugin
        "https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"  # Leaflet MarkerCluster Plugin
    ],
    suppress_callback_exceptions=True
)

# Build navbar items
navbar_items = [
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-house-fill me-2"), "Home"], href="/", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-vinyl-fill me-2"), "Pokémon"], href="/pokemon", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-lightning-fill me-2"), "Raids"], href="/raids", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-robot me-2"), "Invasions"], href="/invasions", active="exact")),
    dbc.NavItem(dbc.NavLink([html.I(className="bi bi-signpost-2-fill me-2"), "Quests"], href="/quests", active="exact")),
]

# Add custom navbar links from config
for link in CUSTOM_NAVBAR_LINKS:
    icon_class = link.get("icon", "bi bi-link-45deg")
    link_name = link.get("name", "Custom Link")
    link_url = link.get("url", "#")
    is_external = link.get("external", True)

    nav_link = dbc.NavLink(
        [html.I(className=f"{icon_class} me-2"), link_name],
        href=link_url,
        target="_blank" if is_external else "_self",
        external_link=is_external
    )
    navbar_items.append(dbc.NavItem(nav_link))

# Add GitHub link at the end
navbar_items.append(
    dbc.NavItem(
        dbc.NavLink(
            html.I(className="bi bi-github", style={"fontSize": "1.5rem"}, title="View on GitHub"),
            href=GITHUB_URL,
            target="_blank",
            external_link=True
        )
    )
)

# Define the Navbar
navbar = dbc.NavbarSimple(
    children=navbar_items,
    brand=brand_content,
    brand_href="/",
    color="primary",
    dark=True,
    className="mb-3"
)

# Define the Footer
footer = dbc.Container([
    html.Hr(className="my-4"),
    dbc.Row([
        dbc.Col([
            html.P([
                html.Span("Made with ", className="text-muted"),
                html.Span("❤️", style={"color": "#e74c3c"}),
                html.Span(" by ", className="text-muted"),
                html.A([
                    html.Img(
                        src="https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png",
                        style={"height": "20px", "width": "20px", "verticalAlign": "middle", "marginRight": "5px", "filter": "invert(1)"}
                    ),
                    html.Span("HugoDataAnalyst/PsyduckV2", style={"verticalAlign": "middle"})
                ], href=GITHUB_URL, target="_blank", className="text-decoration-none text-muted", style={"fontWeight": "500"})
            ], className="text-center mb-3 small")
        ], width=12)
    ])
], fluid=True)

# App Layout
app.layout = dbc.Container([
    navbar,
    dash.page_container,
    footer
], fluid=True, className="min-vh-100 d-flex flex-column")
