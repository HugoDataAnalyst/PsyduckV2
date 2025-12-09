import dash
from dash import html, dcc, callback, Input, Output, State, ALL, ctx, MATCH, ClientsideFunction
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, date
from dashboard.utils import get_cached_geofences, get_invasions_stats
from utils.logger import logger
import config as AppConfig
import json
import re
import os
from pathlib import Path
from dashboard.translations.manager import translate, translate_invader, translate_incident_display

dash.register_page(__name__, path='/invasions', title='Invasion Analytics')

ICON_BASE_URL = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main"

# Define Cache Paths
ASSETS_PATH = Path(__file__).parent / ".." / "assets"
INVASION_ICONS_PATH = ASSETS_PATH / "invasion_icons"


# Cached wrapper for invasion icons with local fallback - we can uncomment the lru_cache later if we want to use it like this
#@lru_cache(maxsize=None)
def get_invasion_icon_url(character_id):
    filename = f"{character_id}.webp"

    # Check local cache
    if (INVASION_ICONS_PATH / filename).exists():
        return f"/assets/invasion_icons/{filename}"

    return f"{ICON_BASE_URL}/invasion/{filename}"

def parse_invasion_key(key_str, lang="en"):
    """
    Parses keys like '1:10' (Display:Character).
    Returns (display_type_id, character_id, label)
    """
    parts = str(key_str).split(':')

    if len(parts) >= 2:
        try:
            disp_id = int(parts[0])
            char_id = int(parts[1])

            # Try to get translated name for character
            label = translate_invader(char_id, lang)

            # If fallback occurred (contains #), use display type instead
            if f"#{char_id}" in label:
                disp_name = translate_incident_display(disp_id, lang)
                label = f"{disp_name} (ID: {char_id})"

            return disp_id, char_id, label
        except ValueError:
            pass
    return 0, 0, str(key_str)

# Layout

def generate_area_cards(geofences, selected_area_name, lang="en"):
    cards = []
    for idx, geo in enumerate(geofences):
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', geo['name'])
        is_selected = (selected_area_name == geo['name'])
        map_children = [html.Div("✓ " + translate("Selected", lang), style={'position': 'absolute', 'top': '10px', 'right': '10px', 'backgroundColor': '#28a745', 'color': 'white', 'padding': '4px 8px', 'borderRadius': '4px', 'fontWeight': 'bold', 'zIndex': '1000'})] if is_selected else []

        card = dbc.Card([
            html.Div(map_children, id=f"invasions-area-map-{safe_name}", **{'data-map-geofence': json.dumps(geo)}, style={'height': '150px', 'backgroundColor': '#1a1a1a', 'position': 'relative'}),
            dbc.CardBody([
                html.H5(geo['name'], className="card-title text-truncate", style={'color': '#28a745' if is_selected else 'inherit'}),
                dbc.Button("✓" + translate("Selected", lang) if is_selected else translate("Select", lang), href=f"/invasions?area={geo['name']}", color="success" if is_selected else "primary", size="sm", className="w-100", disabled=is_selected)
            ])
        ], style={"width": "14rem", "margin": "10px", "border": f"3px solid {'#28a745' if is_selected else 'transparent'}"}, className="shadow-sm")

        if is_selected: card.id = "selected-area-card"
        cards.append(card)
    return cards if cards else html.Div("No areas match your search.", className="text-center text-muted my-4")

def layout(area=None, **kwargs):
    geofences = get_cached_geofences() or []
    initial_cards = generate_area_cards(geofences, area, "en")
    area_options = [{"label": g["name"], "value": g["name"]} for g in geofences]
    area_label = area if area else "No Area Selected"

    return dbc.Container([
        dcc.Store(id="invasions-raw-data-store"),
        dcc.Store(id="invasions-table-sort-store", data={"col": "count", "dir": "desc"}),
        dcc.Store(id="invasions-table-page-store", data={"current_page": 1, "rows_per_page": 25}),
        dcc.Store(id="invasions-total-pages-store", data=1),
        dcc.Store(id="invasions-clientside-dummy-store"),
        dcc.Dropdown(id="invasions-area-selector", options=area_options, value=area, style={'display': 'none'}),
        dcc.Store(id="invasions-mode-persistence-store", storage_type="local"),
        dcc.Store(id="invasions-source-persistence-store", storage_type="local"),
        dcc.Store(id="invasions-combined-source-store", data="live"),
        dcc.Store(id="invasions-heatmap-data-store", data=[]),
        dcc.Store(id="invasions-heatmap-mode-store", data="markers"),
        dcc.Store(id="invasions-heatmap-hidden-grunts", data=[]),

        # Header
        dbc.Row([
            dbc.Col(html.H2("Invasion Analytics", id="invasions-page-title", className="text-white"), width=12, className="my-4"),
        ]),

        # Notification Area
        html.Div(id="invasions-notification-area"),

        # Main Control Card
        dbc.Card([
            dbc.CardHeader("⚙️ Analysis Settings", id="invasions-settings-header", className="fw-bold"),
            dbc.CardBody([
                dbc.Row([
                    # Area Selection
                    dbc.Col([
                        dbc.Label("Selected Area", id="invasions-label-selected-area", className="fw-bold"),
                        dbc.InputGroup([
                            dbc.InputGroupText("🗺️"),
                            dbc.Input(id="invasions-selected-area-display", value=area_label, disabled=True, style={"backgroundColor": "#fff", "color": "#333", "fontWeight": "bold"}),
                            dbc.Button("Change", id="invasions-open-area-modal", color="primary")
                        ], className="mb-3")
                    ], width=12, md=6),

                    # Data Source
                    dbc.Col([
                        dbc.Label("Data Source", id="invasions-label-data-source", className="fw-bold"),
                        html.Div([
                            # Row 1: Stats (Live & Historical)
                            html.Div([
                                html.Span("Stats: ", id="invasions-label-stats", className="text-muted small me-2", style={"minWidth": "45px"}),
                                dbc.RadioItems(
                                    id="invasions-stats-source-selector",
                                    options=[
                                        {"label": "Live", "value": "live"},
                                        {"label": "Historical", "value": "historical"},
                                    ],
                                    value="live", inline=True, inputClassName="btn-check",
                                    labelClassName="btn btn-outline-info btn-sm",
                                    labelCheckedClassName="active"
                                ),
                            ], className="d-flex align-items-center mb-1"),
                            # Row 2: SQL (Heatmap)
                            html.Div([
                                html.Span("SQL: ", id="invasions-label-sql", className="text-muted small me-2", style={"minWidth": "45px"}),
                                dbc.RadioItems(
                                    id="invasions-sql-source-selector",
                                    options=[{"label": "Heatmap", "value": "sql_heatmap"}],
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
                        html.Div(id="invasions-live-controls", children=[
                            dbc.Label("📅 Time Window (Hours)", id="invasions-label-time-window"),
                            dbc.InputGroup([
                                dbc.Input(id="invasions-live-time-input", type="number", min=1, max=72, value=1),
                                dbc.InputGroupText("hours")
                            ])
                        ]),
                        html.Div(id="invasions-historical-controls", style={"display": "none"}, children=[
                            dbc.Label("📅 Date Range", id="invasions-label-date-range"),
                            dcc.DatePickerRange(id="invasions-historical-date-picker", min_date_allowed=date(2023, 1, 1), max_date_allowed=date.today(), start_date=date.today(), end_date=date.today(), className="d-block w-100", persistence=True, persistence_type="local")
                        ]),
                        html.Div(id="invasions-heatmap-controls", style={"display": "none"}, children=[
                            dbc.Label("📅 Date Range", id="invasions-label-date-range-2"),
                            dcc.DatePickerRange(id="invasions-heatmap-date-picker", min_date_allowed=date(2023, 1, 1), max_date_allowed=date.today(), start_date=date.today(), end_date=date.today(), className="d-block w-100", persistence=True, persistence_type="local")
                        ])
                    ], width=6, md=3),

                    # Interval / Display Mode
                    dbc.Col([
                        html.Div(id="invasions-interval-control-container", style={"display": "none"}, children=[
                            dbc.Label("⏱️ Interval", id="invasions-label-interval"),
                            dcc.Dropdown(id="invasions-interval-selector", options=[{"label": "Hourly", "value": "hourly"}], value="hourly", clearable=False, className="text-dark")
                        ]),
                    ], width=6, md=3),

                    # Mode
                    dbc.Col([
                        dbc.Label("📊 View Mode", id="invasions-label-view-mode"),
                        dcc.Dropdown(id="invasions-mode-selector", options=[], value=None, clearable=False, className="text-dark")
                    ], width=6, md=3),

                    # Actions
                    dbc.Col([
                        dbc.Label("Actions", id="invasions-label-actions", style={"visibility": "hidden"}),
                        dbc.Button("Run Analysis", id="invasions-submit-btn", color="success", className="w-100 fw-bold")
                    ], width=6, md=3)
                ], className="align-items-end g-3"),
                # Heatmap Display Mode (only visible for heatmap)
                html.Div(id="invasions-heatmap-display-container", style={"display": "none"}, children=[
                    html.Hr(className="my-3"),
                    dbc.Row([
                        dbc.Col([
                            dbc.Label("🗺️ Heatmap Display Mode", id="invasions-label-heatmap-display-mode", className="fw-bold"),
                            dbc.RadioItems(
                                id="invasions-heatmap-display-mode",
                                options=[
                                    {"label": "Markers (PokéStops)", "value": "markers"},
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
            dbc.ModalHeader(dbc.ModalTitle("Select an Area", id="invasions-modal-title-area")),
            dbc.ModalBody([
                html.Div(
                    dbc.Input(id="invasions-area-filter-input", placeholder="Filter areas by name...", className="mb-3", autoFocus=True),
                    style={"position": "sticky", "top": "-16px", "zIndex": "1020", "backgroundColor": "var(--bs-modal-bg, #fff)", "paddingTop": "16px", "paddingBottom": "10px", "marginBottom": "10px", "borderBottom": "1px solid #dee2e6"}
                ),
                html.Div(initial_cards, id="invasions-area-cards-container", className="d-flex flex-wrap justify-content-center")
            ]),
            dbc.ModalFooter(dbc.Button("Close", id="invasions-close-area-modal", className="ms-auto"))
        ], id="invasions-area-modal", size="xl", scrollable=True),

        # Results Container
        # Removed global dcc.Loading wrapper to prevent search focus loss
        html.Div(id="invasions-stats-container", style={"display": "none"}, children=[
            dbc.Row([
                # Sidebar
                dbc.Col(dbc.Card([
                    dbc.CardHeader("📈 Total Counts", id="invasions-card-header-total-counts"),
                    dbc.CardBody(
                        # Wrapped inner content for loading spinner
                        dcc.Loading(html.Div(id="invasions-total-counts-display"))
                    )
                ], className="shadow-sm border-0 h-100"), width=12, lg=4, className="mb-4"),

                # Activity Data
                dbc.Col(dbc.Card([
                    dbc.CardHeader("📋 Activity Data", id="invasions-card-header-activity"),
                    dbc.CardBody([
                         # Embedded Search Input (Visible only in Grouped mode)
                         # debounce=False for fluid search
                        dcc.Input(
                            id="invasions-search-input",
                            type="text",
                            placeholder="🔍 Search Invasions...",
                            debounce=False,
                            className="form-control mb-3",
                            style={"display": "none"}
                        ),
                        # Wrapped inner content for loading spinner
                        dcc.Loading(html.Div(id="invasions-main-visual-container"))
                    ])
                ], className="shadow-sm border-0 h-100"), width=12, lg=8, className="mb-4"),
            ]),

            dbc.Row([dbc.Col(dbc.Card([
                dbc.CardHeader("🛠️ Raw Data Inspector", id="invasions-card-header-raw"),
                dbc.CardBody(html.Pre(id="invasions-raw-data-display", style={"maxHeight": "300px", "overflow": "scroll"}))
            ], className="shadow-sm border-0"), width=12)])
        ]),

        # Heatmap Container (separate from stats container)
        html.Div(id="invasions-heatmap-container", style={"display": "none"}, children=[
            dbc.Row([
                # Left Column - Quick Filter
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            dbc.Row([
                                dbc.Col([
                                    html.Span("🎯 Grunt Filter", id="invasions-card-header-quick-filter", className="me-2"),
                                    html.Span(id="invasions-quick-filter-count", className="text-muted small")
                                ], width="auto", className="d-flex align-items-center"),
                                dbc.Col([
                                    dbc.ButtonGroup([
                                        dbc.Button("All", id="invasions-quick-filter-show-all", title="Show All", size="sm", color="success", outline=True),
                                        dbc.Button("None", id="invasions-quick-filter-hide-all", title="Hide All", size="sm", color="danger", outline=True),
                                    ], size="sm")
                                ], width="auto")
                            ], className="align-items-center justify-content-between g-0")
                        ]),
                        dbc.CardBody([
                            dbc.Input(id="invasions-quick-filter-search", placeholder="Search grunts...", size="sm", className="mb-2"),
                            html.P(id="invasions-quick-filter-instructions", children="Click to hide/show grunts from map", className="text-muted small mb-2"),
                            html.Div(id="invasions-quick-filter-grid",
                                     style={"display": "flex", "flexWrap": "wrap", "gap": "4px", "justifyContent": "center", "maxHeight": "500px", "overflowY": "auto"})
                        ])
                    ], className="shadow-sm border-0 h-100")
                ], width=12, lg=3, className="mb-4"),

                # Right Column - Map
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([
                            html.Span("🗺️ Invasion Heatmap", id="invasions-card-header-heatmap", className="fw-bold"),
                            html.Span(id="invasions-heatmap-stats", className="ms-3 text-muted small")
                        ]),
                        dbc.CardBody([
                            html.Div(id="invasions-heatmap-map-container", style={"height": "600px", "backgroundColor": "#1a1a1a"})
                        ])
                    ], className="shadow-sm border-0 h-100")
                ], width=12, lg=9, className="mb-4")
            ])
        ])
    ])

# 0. Static Translation Callback
@callback(
    [Output("invasions-page-title", "children"), Output("invasions-settings-header", "children"),
     Output("invasions-label-selected-area", "children"), Output("invasions-open-area-modal", "children"),
     Output("invasions-label-data-source", "children"), Output("invasions-label-stats", "children"),
     Output("invasions-label-sql", "children"), Output("invasions-label-time-window", "children"),
     Output("invasions-label-date-range", "children"), Output("invasions-label-date-range-2", "children"),
     Output("invasions-label-interval", "children"), Output("invasions-label-view-mode", "children"),
     Output("invasions-label-actions", "children"), Output("invasions-submit-btn", "children"),
     Output("invasions-label-heatmap-display-mode", "children"), Output("invasions-modal-title-area", "children"),
     Output("invasions-close-area-modal", "children"), Output("invasions-card-header-total-counts", "children"),
     Output("invasions-card-header-activity", "children"), Output("invasions-card-header-raw", "children"),
     Output("invasions-card-header-quick-filter", "children"), Output("invasions-card-header-heatmap", "children"),
     Output("invasions-quick-filter-show-all", "children"), Output("invasions-quick-filter-show-all", "title"),
     Output("invasions-quick-filter-hide-all", "children"), Output("invasions-quick-filter-hide-all", "title"),
     Output("invasions-area-filter-input", "placeholder"), Output("invasions-search-input", "placeholder"),
     Output("invasions-selected-area-display", "value"), Output("invasions-quick-filter-search", "placeholder"),
     Output("invasions-quick-filter-instructions", "children"),
    ],
    [Input("language-store", "data"), Input("invasions-area-selector", "value")],
)
def update_static_translations(lang, current_area):
    lang = lang or "en"

    if current_area:
        area_text = current_area
    else:
        area_text = translate("No Area Selected", lang)

    return (
        translate("Invasion Analytics", lang),
        translate("Analysis Settings", lang),
        translate("Selected Area", lang), translate("Change", lang),
        translate("Data Source", lang), translate("Stats", lang),
        translate("SQL", lang),
        translate("Time Window", lang), translate("Date Range", lang),
        translate("Date Range", lang),
        translate("Interval", lang), translate("View Mode", lang),
        translate("Actions", lang), translate("Run Analysis", lang),
        translate("Heatmap Display Mode", lang),
        translate("Select an Area", lang), translate("Close", lang),
        translate("Total Counts", lang), translate("Activity Data", lang),
        translate("Raw Data Inspector", lang), translate("Grunt Filter", lang), translate("Invasion Heatmap", lang),
        translate("All", lang), translate("Show All", lang),
        translate("None", lang), translate("Hide All", lang),
        translate("Filter areas by name...", lang),
        f"🔍 {translate('Search Invasions...', lang)}",
        area_text,
        translate("Search grunts...", lang),
        translate("Click to hide/show grunts from map", lang)
    )

# Parsing Logic

def parse_data_to_df(data, mode, source, lang="en"):
    records = []
    working_data = data.get('data', data) if isinstance(data, dict) else data

    if mode == "sum":
        if isinstance(working_data, dict):
            if "total" in working_data:
                records.append({"metric": "total", "count": working_data["total"], "key": "Total", "char_id": -1, "time_bucket": "Total"})

            if "confirmed" in working_data and isinstance(working_data["confirmed"], dict):
                for k, v in working_data["confirmed"].items():
                    lbl = translate("Confirmed", lang) if str(k) == "1" else translate("Unconfirmed", lang)
                    records.append({"metric": lbl, "count": v, "key": lbl, "char_id": -1, "time_bucket": "Total"})

            items_dict = {}
            if "display_type+character" in working_data:
                items_dict = working_data["display_type+character"]
            elif "grunt" in working_data:
                items_dict = {f"1:{k}": v for k,v in working_data["grunt"].items()}

            for key_str, count in items_dict.items():
                if not isinstance(count, (int, float)): continue
                disp_id, char_id, label = parse_invasion_key(key_str, lang)
                records.append({"metric": label, "count": count, "key": f"{disp_id}:{char_id}", "char_id": char_id, "time_bucket": "Total"})

    elif mode == "grouped":
        if isinstance(working_data, dict):
            source_dict = working_data.get("display_type+character", working_data)
            for key_str, content in source_dict.items():
                if source_dict is working_data and key_str in ["total", "confirmed", "grunt", "display_type+character"]: continue
                if ":" in str(key_str):
                    disp_id, char_id, label = parse_invasion_key(key_str, lang)
                    final_count = 0
                    if isinstance(content, (int, float)): final_count = content
                    elif isinstance(content, dict): final_count = sum(v for v in content.values() if isinstance(v, (int, float)))

                    if final_count > 0:
                        records.append({"metric": label, "count": final_count, "key": f"{disp_id}:{char_id}", "char_id": char_id, "time_bucket": "Total"})

    elif mode == "surged":
        if isinstance(working_data, dict):
            first_key = next(iter(working_data)) if working_data else ""
            is_hour_outer = "hour" in str(first_key).lower() or (str(first_key).isdigit() and len(str(first_key)) > 6)

            if is_hour_outer:
                for time_key, content in working_data.items():
                    h_val = time_key
                    if "hour" in str(time_key):
                        try: h_val = int(str(time_key).replace("hour ", ""))
                        except: pass

                    if isinstance(content, dict):
                        items_dict = {}
                        if "display_type+character" in content: items_dict = content["display_type+character"]
                        elif "grunt" in content: items_dict = {f"1:{k}": v for k,v in content["grunt"].items()}

                        for k, v in items_dict.items():
                            disp_id, char_id, label = parse_invasion_key(k, lang)
                            records.append({"metric": label, "count": v, "key": f"{disp_id}:{char_id}", "char_id": char_id, "time_bucket": h_val})

                        if "confirmed" in content and isinstance(content["confirmed"], dict):
                             for k, v in content["confirmed"].items():
                                lbl = translate("Confirmed", lang) if str(k) == "1" else translate("Unconfirmed", lang)
                                records.append({"metric": lbl, "count": v, "key": lbl, "char_id": -1, "time_bucket": h_val})
            else:
                for key_str, hourly_data in working_data.items():
                    if ":" in key_str:
                        disp_id, char_id, label = parse_invasion_key(key_str, lang)
                        if isinstance(hourly_data, dict):
                            for hour_key, count in hourly_data.items():
                                h_val = hour_key
                                if "hour" in str(hour_key):
                                    try: h_val = int(str(hour_key).replace("hour ", ""))
                                    except: pass
                                records.append({"metric": label, "count": count, "key": f"{disp_id}:{char_id}", "char_id": char_id, "time_bucket": h_val})
                    elif key_str == "confirmed" and isinstance(hourly_data, dict):
                         for status_key, time_dict in hourly_data.items():
                             lbl = translate("Confirmed", lang) if str(status_key) == "1" else translate("Unconfirmed", lang)
                             if isinstance(time_dict, dict):
                                 for hour_key, count in time_dict.items():
                                    h_val = hour_key
                                    if "hour" in str(hour_key):
                                        try: h_val = int(str(hour_key).replace("hour ", ""))
                                        except: pass
                                    records.append({"metric": lbl, "count": count, "key": lbl, "char_id": -1, "time_bucket": h_val})

    df = pd.DataFrame(records)
    if df.empty: return pd.DataFrame(columns=["metric", "count", "key", "char_id", "time_bucket"])
    return df

# Callbacks

# Combine data sources into single store
@callback(
    [Output("invasions-combined-source-store", "data", allow_duplicate=True),
     Output("invasions-stats-source-selector", "value", allow_duplicate=True),
     Output("invasions-sql-source-selector", "value", allow_duplicate=True)],
    [Input("invasions-stats-source-selector", "value"),
     Input("invasions-sql-source-selector", "value")],
    prevent_initial_call=True
)
def combine_data_sources(stats_val, sql_val):
    trigger = ctx.triggered_id
    if trigger == "invasions-stats-source-selector" and stats_val:
        return stats_val, stats_val, None
    elif trigger == "invasions-sql-source-selector" and sql_val:
        return sql_val, None, sql_val
    return "live", None, "live"

@callback(
    [Output("invasions-live-controls", "style"), Output("invasions-historical-controls", "style"),
     Output("invasions-interval-control-container", "style"), Output("invasions-heatmap-controls", "style"),
     Output("invasions-heatmap-display-container", "style")],
    Input("invasions-combined-source-store", "data")
)
def toggle_source_controls(source):
    if source == "live":
        return {"display": "block"}, {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "none"}
    elif source == "historical":
        return {"display": "none"}, {"display": "block", "position": "relative", "zIndex": 1002}, {"display": "block"}, {"display": "none"}, {"display": "none"}
    elif source == "sql_heatmap":
        return {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "block", "position": "relative", "zIndex": 1002}, {"display": "block"}
    return {"display": "block"}, {"display": "none"}, {"display": "none"}, {"display": "none"}, {"display": "none"}

@callback(
    [Output("invasions-mode-selector", "options"), Output("invasions-mode-selector", "value"),
     Output("invasions-stats-source-selector", "options"), Output("invasions-sql-source-selector", "options"),
     Output("invasions-heatmap-display-mode", "options"), Output("invasions-interval-selector", "options")],
    [Input("invasions-combined-source-store", "data"), Input("language-store", "data")],
    [State("invasions-mode-persistence-store", "data"), State("invasions-mode-selector", "value")]
)
def restrict_modes(source, lang, stored_mode, current_ui_mode):
    lang = lang or "en"
    if source == "sql_heatmap":
        heatmap_opts = [{"label": translate("Map View", lang), "value": "map"}]
        return heatmap_opts, "map", dash.no_update, dash.no_update, dash.no_update, dash.no_update

    full_options = [
        {"label": translate("Surged (Hourly)", lang), "value": "surged"},
        {"label": translate("Grouped (Table)", lang), "value": "grouped"},
        {"label": translate("Sum (Totals)", lang), "value": "sum"}
    ]
    allowed_values = [o['value'] for o in full_options]
    if current_ui_mode in allowed_values: final_value = current_ui_mode
    elif stored_mode in allowed_values: final_value = stored_mode
    else: final_value = allowed_values[0]

    # Source Options
    stats_opts = [{"label": translate("Live", lang), "value": "live"}, {"label": translate("Historical", lang), "value": "historical"}]
    sql_opts = [{"label": translate("Heatmap", lang), "value": "sql_heatmap"}]

    # Heatmap Display Options
    heatmap_mode_opts = [
        {"label": translate("Markers (PokéStops)", lang), "value": "markers"},
        {"label": translate("Density Heatmap", lang), "value": "density"},
        {"label": translate("Grid Overlay", lang), "value": "grid"}
    ]

    # Interval Options
    interval_opts = [
        {"label": translate("Hourly", lang), "value": "hourly"}
    ]

    return full_options, final_value, stats_opts, sql_opts, heatmap_mode_opts, interval_opts

@callback(Output("invasions-mode-persistence-store", "data"), Input("invasions-mode-selector", "value"), prevent_initial_call=True)
def save_mode(val): return val

@callback(Output("invasions-source-persistence-store", "data"), Input("invasions-combined-source-store", "data"), prevent_initial_call=True)
def save_source(val): return val

@callback(
    [Output("invasions-combined-source-store", "data"),
     Output("invasions-stats-source-selector", "value"),
     Output("invasions-sql-source-selector", "value")],
    Input("invasions-source-persistence-store", "modified_timestamp"),
    State("invasions-source-persistence-store", "data"),
    prevent_initial_call=False
)
def load_persisted_source(ts, stored_source):
    """Load persisted data source on page load."""
    if ts is not None and ts > 0:
        raise dash.exceptions.PreventUpdate
    valid_sources = ["live", "historical", "sql_heatmap"]
    if stored_source in valid_sources:
        if stored_source == "sql_heatmap":
            return stored_source, None, stored_source
        return stored_source, stored_source, None
    return "live", "live", None

@callback(Output("invasions-heatmap-mode-store", "data"), Input("invasions-heatmap-display-mode", "value"))
def update_heatmap_mode_store(val): return val

@callback(
    [Output("invasions-area-modal", "is_open"), Output("invasions-search-input", "style")],
    [Input("invasions-open-area-modal", "n_clicks"), Input("invasions-close-area-modal", "n_clicks"), Input("invasions-mode-selector", "value")],
    [State("invasions-area-modal", "is_open")]
)
def handle_modals_and_search(ao, ac, mode, isa):
    search_style = {"display": "block", "width": "100%"} if mode == "grouped" else {"display": "none"}
    tid = ctx.triggered_id
    if tid in ["invasions-open-area-modal", "invasions-close-area-modal"]: return not isa, search_style
    return isa, search_style

# Area Cards Filter & Scroll
@callback(Output("invasions-area-cards-container", "children"),
          [Input("invasions-area-filter-input", "value"), Input("language-store", "data")],
          [State("invasions-area-selector", "value")])
def filter_area_cards(search_term, lang, selected_area):
    geofences = get_cached_geofences() or []
    if search_term: geofences = [g for g in geofences if search_term.lower() in g['name'].lower()]
    return generate_area_cards(geofences, selected_area, lang or "en")

# Quick Filter Callbacks
@callback(
    [Output("invasions-quick-filter-grid", "children"), Output("invasions-quick-filter-count", "children")],
    [Input("invasions-heatmap-data-store", "data"),
     Input("invasions-quick-filter-search", "value"),
     Input("language-store", "data")],
    [State("invasions-combined-source-store", "data"),
     State("invasions-heatmap-hidden-grunts", "data")]
)
def populate_invasions_quick_filter(heatmap_data, search_term, lang, source, hidden_grunts):
    """Populate grunt image grid for quick filtering - fluid search"""
    lang = lang or "en"
    if source != "sql_heatmap" or not heatmap_data:
        return [], ""

    # 1. Process Data - aggregate by character
    grunt_set = {}
    for record in heatmap_data:
        char_id = record.get('character') or 0
        key = str(char_id)
        if key not in grunt_set:
            grunt_name = translate_invader(char_id, lang)
            grunt_set[key] = {
                'char_id': int(char_id),
                'name': grunt_name,
                'count': record.get('count', 0),
                'icon_url': get_invasion_icon_url(char_id)
            }
        else:
            grunt_set[key]['count'] += record.get('count', 0)

    # 2. Sort (by count descending, then ID)
    sorted_grunts = sorted(grunt_set.items(), key=lambda x: (-x[1]['count'], x[1]['char_id']))

    # 3. Filter (Search)
    search_lower = search_term.lower() if search_term else ""
    filtered_list = []

    for key, data in sorted_grunts:
        if search_lower:
            if search_lower not in data['name'].lower():
                continue
        filtered_list.append((key, data))

    # 4. Generate UI
    hidden_set = set(hidden_grunts or [])
    grunt_images = []

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

        grunt_images.append(html.Div([
            html.Img(src=data['icon_url'],
                    style={"width": "40px", "height": "40px", "display": "block"}),
            html.Div(f"{data['count']}",
                    style={"fontSize": "10px", "textAlign": "center", "marginTop": "2px", "color": "#aaa"})
        ], id={"type": "invasions-quick-filter-icon", "index": key}, style=style,
           title=f"{data['name']}: {data['count']} invasions"))

    count_text = f"({len(filtered_list)}/{len(sorted_grunts)})" if search_lower else f"({len(sorted_grunts)})"

    return grunt_images, count_text

# Clientside callback to update icon opacity without rebuilding the grid
dash.clientside_callback(
    """
    function(hiddenGrunts) {
        if (!hiddenGrunts) hiddenGrunts = [];
        var hiddenSet = new Set(hiddenGrunts);

        // Find the grid container and iterate its children
        var grid = document.getElementById('invasions-quick-filter-grid');
        if (!grid) return window.dash_clientside.no_update;

        var icons = grid.children;
        for (var i = 0; i < icons.length; i++) {
            var icon = icons[i];
            try {
                var idObj = JSON.parse(icon.id);
                if (idObj.type === 'invasions-quick-filter-icon') {
                    var key = idObj.index;
                    icon.style.opacity = hiddenSet.has(key) ? '0.3' : '1';
                }
            } catch(e) {}
        }

        return window.dash_clientside.no_update;
    }
    """,
    Output("invasions-quick-filter-grid", "className"),  # Dummy output
    Input("invasions-heatmap-hidden-grunts", "data"),
    prevent_initial_call=True
)

@callback(
    Output("invasions-heatmap-hidden-grunts", "data", allow_duplicate=True),
    [Input({"type": "invasions-quick-filter-icon", "index": ALL}, "n_clicks"),
     Input("invasions-quick-filter-show-all", "n_clicks"),
     Input("invasions-quick-filter-hide-all", "n_clicks")],
    [State("invasions-heatmap-hidden-grunts", "data"),
     State("invasions-heatmap-data-store", "data")],
    prevent_initial_call=True
)
def toggle_invasions_grunt_visibility(icon_clicks, show_clicks, hide_clicks, hidden_list, heatmap_data):
    """Toggle grunt visibility in quick filter"""
    trigger = ctx.triggered_id
    if not trigger:
        return dash.no_update

    # Button Logic
    if trigger == "invasions-quick-filter-show-all":
        return []

    if trigger == "invasions-quick-filter-hide-all":
        if not heatmap_data: return []
        all_keys = set()
        for record in heatmap_data:
            char_id = record.get('character') or 0
            all_keys.add(str(char_id))
        return list(all_keys)

    # Icon Click Logic - must verify an actual click occurred
    if isinstance(trigger, dict) and trigger.get('type') == 'invasions-quick-filter-icon':
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
    Output("invasions-heatmap-hidden-grunts", "data", allow_duplicate=True),
    Input("invasions-heatmap-data-store", "data"),
    prevent_initial_call=True
)
def reset_invasions_hidden_grunts_on_new_data(heatmap_data):
    """Reset hidden grunts list when new heatmap data arrives"""
    if heatmap_data:
        return []
    return dash.no_update

@callback(
    [Output("invasions-raw-data-store", "data"), Output("invasions-stats-container", "style"), Output("invasions-notification-area", "children"),
     Output("invasions-heatmap-data-store", "data"), Output("invasions-heatmap-container", "style"), Output("invasions-heatmap-stats", "children")],
    [Input("invasions-submit-btn", "n_clicks"), Input("invasions-combined-source-store", "data")],
    [State("invasions-area-selector", "value"), State("invasions-live-time-input", "value"),
     State("invasions-historical-date-picker", "start_date"), State("invasions-historical-date-picker", "end_date"),
     State("invasions-heatmap-date-picker", "start_date"), State("invasions-heatmap-date-picker", "end_date"),
     State("invasions-interval-selector", "value"), State("invasions-mode-selector", "value"),
     State("language-store", "data")]
)
def fetch_data(n, source, area, live_h, hist_start, hist_end, heatmap_start, heatmap_end, interval, mode, lang):
    lang = lang or "en"
    # Default outputs: raw_data, stats_style, notification, heatmap_data, heatmap_style, heatmap_stats
    if not n:
        return {}, {"display": "none"}, None, [], {"display": "none"}, ""
    if not area:
        return {}, {"display": "none"}, dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), translate("Please select an Area first.", lang)], color="warning", dismissable=True, duration=4000), [], {"display": "none"}, ""

    try:
        if source == "sql_heatmap":
            # SQL Heatmap Mode
            logger.info(f"🔍 Starting Invasion Heatmap Fetch for area: {area}")
            params = {
                "start_time": f"{heatmap_start}T00:00:00",
                "end_time": f"{heatmap_end}T23:59:59",
                "area": area,
                "response_format": "json"
            }
            data = get_invasions_stats("invasion_sql_data", params)

            heatmap_data = []
            if isinstance(data, dict) and "data" in data:
                heatmap_data = data["data"]
            elif isinstance(data, list):
                heatmap_data = data

            # Add icon URLs to each point
            for point in heatmap_data:
                char_id = point.get('character') or 0
                point['icon_url'] = get_invasion_icon_url(char_id)

            # Calculate stats
            total_invasions = sum(p.get('count', 0) for p in heatmap_data)
            unique_stops = len(set(p.get('pokestop_name', '') for p in heatmap_data))

            stops_word = translate("pokestops", lang)
            invasions_word = translate("invasions", lang)
            stats_text = f"{unique_stops} {stops_word} • {total_invasions} {invasions_word}"

            logger.info(f"✅ Invasion Heatmap: {len(heatmap_data)} data points, {unique_stops} pokestops, {total_invasions} invasions")

            return {}, {"display": "none"}, None, heatmap_data, {"display": "block"}, stats_text

        elif source == "live":
            hours = max(1, min(int(live_h or 1), 72))
            params = {"start_time": f"{hours} hours", "end_time": "now", "mode": mode, "area": area, "response_format": "json"}
            data = get_invasions_stats("timeseries", params)
        else:
            params = {"counter_type": "totals", "interval": interval, "start_time": f"{hist_start}T00:00:00", "end_time": f"{hist_end}T23:59:59", "mode": mode, "area": area, "response_format": "json"}
            data = get_invasions_stats("counter", params)

        return data, {"display": "block"}, None, [], {"display": "none"}, ""
    except Exception as e:
        logger.info(f"Fetch error: {e}")
        return {}, {"display": "none"}, dbc.Alert(f"Error: {str(e)}", color="danger", dismissable=True), [], {"display": "none"}, ""

# Sorting
@callback(
    Output("invasions-table-sort-store", "data"), Input({"type": "invasions-sort-header", "index": ALL}, "n_clicks"), State("invasions-table-sort-store", "data"), prevent_initial_call=True
)
def update_sort_order(n_clicks, current_sort):
    if not ctx.triggered_id or not any(n_clicks): return dash.no_update
    col = ctx.triggered_id['index']
    return {"col": col, "dir": "asc" if current_sort['col'] == col and current_sort['dir'] == "desc" else "desc"}

# Pagination
@callback(
    Output("invasions-table-page-store", "data"),
    [Input("invasions-first-page-btn", "n_clicks"), Input("invasions-prev-page-btn", "n_clicks"), Input("invasions-next-page-btn", "n_clicks"), Input("invasions-last-page-btn", "n_clicks"), Input("invasions-rows-per-page-selector", "value"), Input("invasions-goto-page-input", "value")],
    [State("invasions-table-page-store", "data"), State("invasions-total-pages-store", "data")],
    prevent_initial_call=True
)
def update_pagination(first, prev, next, last, rows, goto, state, total_pages):
    trigger = ctx.triggered_id
    if not trigger: return dash.no_update
    current = state.get('current_page', 1)
    total_pages = total_pages or 1
    new_page = current
    if trigger == "invasions-first-page-btn": new_page = 1
    elif trigger == "invasions-last-page-btn": new_page = total_pages
    elif trigger == "invasions-prev-page-btn": new_page = max(1, current - 1)
    elif trigger == "invasions-next-page-btn": new_page = min(total_pages, current + 1)
    elif trigger == "invasions-goto-page-input":
        if goto is not None: new_page = min(total_pages, max(1, goto))
    elif trigger == "invasions-rows-per-page-selector": return {"current_page": 1, "rows_per_page": rows}
    return {**state, "current_page": new_page, "rows_per_page": state.get('rows_per_page', 25)}

# Visuals Update
@callback(
    [Output("invasions-total-counts-display", "children"), Output("invasions-main-visual-container", "children"), Output("invasions-raw-data-display", "children"), Output("invasions-total-pages-store", "data"), Output("invasions-main-visual-container", "style")],
    [Input("invasions-raw-data-store", "data"), Input("invasions-search-input", "value"), Input("invasions-table-sort-store", "data"), Input("invasions-table-page-store", "data"), Input("language-store", "data")],
    [State("invasions-mode-selector", "value"), State("invasions-combined-source-store", "data")]
)
def update_visuals(data, search_term, sort, page, lang, mode, source):
    lang = lang or "en"
    if source == "sql_heatmap":
        return [], html.Div(translate("View heatmap below", lang)), "", 1, {"display": "none"}

    if not data: return [], html.Div(), "", 1, {"display": "block"}

    df = parse_data_to_df(data, mode, source, lang)
    if df.empty: return "No Data", html.Div(), json.dumps(data, indent=2), 1, {"display": "block"}

    # Search Logic - Grouped Mode
    if mode == "grouped" and search_term:
        df = df[df['metric'].str.lower().str.contains(search_term.lower(), na=False)]

    total_div = html.P("No data.")

    # Sidebar
    sidebar_metrics = []
    totals_df = df[df['metric'].isin(['total', 'Confirmed', 'Unconfirmed'])]
    if not totals_df.empty:
        totals_agg = totals_df.groupby('metric')['count'].sum().reset_index()
        for _, r in totals_agg.iterrows(): sidebar_metrics.append({"metric": r['metric'], "count": r['count'], "type": "summary"})

    grunts_df = df[~df['metric'].isin(['total', 'Confirmed', 'Unconfirmed'])]
    if not grunts_df.empty:
        top_grunts = grunts_df.groupby(['metric', 'char_id'])['count'].sum().reset_index().sort_values('count', ascending=False).head(15)
        for _, r in top_grunts.iterrows(): sidebar_metrics.append({"metric": r['metric'], "count": r['count'], "char_id": r['char_id'], "type": "grunt"})

    if sidebar_metrics:
        total_val = grunts_df['count'].sum() if not grunts_df.empty else sum(item['count'] for item in sidebar_metrics if item['metric'] in ['Confirmed', 'Unconfirmed'])
        total_div = [html.H1(f"{total_val:,}", className="text-primary")]

        for m in ["Confirmed", "Unconfirmed"]:
            val = next((item['count'] for item in sidebar_metrics if item['metric'] == m), None)
            if val is not None:
                color = "#28a745" if m == "Confirmed" else "#dc3545"
                icon = "bi bi-check-circle-fill" if m == "Confirmed" else "bi bi-x-circle-fill"
                total_div.append(html.Div([html.I(className=f"{icon} me-2", style={"fontSize": "1.2rem", "color": color}), html.Span(f"{val:,}", style={"fontSize": "1.1em", "fontWeight": "bold"})], className="d-flex align-items-center mb-1"))

        total_div.append(html.Hr(style={"borderColor": "#555"}))

        for item in sidebar_metrics:
            if item.get('type') == 'grunt':
                total_div.append(html.Div([
                    html.Img(src=get_invasion_icon_url(item['char_id']), style={"width": "28px", "marginRight": "8px", "verticalAlign": "middle"}),
                    html.Span(f"{item['count']:,}", style={"fontSize": "1.1em", "fontWeight": "bold"}),
                    html.Span(f" {item['metric']}", style={"fontSize": "0.8em", "color": "#aaa", "marginLeft": "5px"})
                ], className="d-flex align-items-center mb-1"))

    visual_content = html.Div("No data")
    total_pages_val = 1

    # Grouped Table
    if mode == "grouped" and not df.empty:
        table_df = df[~df['metric'].isin(['total', 'Confirmed', 'Unconfirmed'])]
        col, ascending = sort['col'], sort['dir'] == "asc"
        if col in table_df.columns: table_df = table_df.sort_values(col, ascending=ascending)
        else: table_df = table_df.sort_values('count', ascending=False)

        rows_per_page = page['rows_per_page']
        total_rows = len(table_df)
        total_pages_val = max(1, (total_rows + rows_per_page - 1) // rows_per_page)
        current_page = min(max(1, page['current_page']), total_pages_val)
        page_df = table_df.iloc[(current_page - 1) * rows_per_page : current_page * rows_per_page]

        header_row = html.Tr([
            html.Th(translate("Image", lang), style={"backgroundColor": "#1a1a1a", "zIndex": "10", "position": "sticky", "top": "0", "textAlign": "center", "width": "60px"}),
            html.Th(html.Span([translate("Type", lang), html.Span(" ▲" if col == 'metric' and ascending else (" ▼" if col == 'metric' else ""), style={"color": "#aaa", "marginLeft": "5px"})], id={"type": "invasions-sort-header", "index": "metric"}, style={"cursor": "pointer"}), style={"backgroundColor": "#1a1a1a", "zIndex": "10", "position": "sticky", "top": "0", "textAlign": "center"}),
            html.Th(html.Span([translate("Count", lang), html.Span(" ▲" if col == 'count' and ascending else (" ▼" if col == 'count' else ""), style={"color": "#aaa", "marginLeft": "5px"})], id={"type": "invasions-sort-header", "index": "count"}, style={"cursor": "pointer"}), style={"backgroundColor": "#1a1a1a", "zIndex": "10", "position": "sticky", "top": "0", "textAlign": "center"})
        ])

        rows = []
        for i, r in enumerate(page_df.iterrows()):
            _, r = r
            bg = "#1a1a1a" if i % 2 == 0 else "#242424"
            rows.append(html.Tr([
                html.Td(html.Img(src=get_invasion_icon_url(r['char_id']), style={"width":"40px", "height":"40px", "display":"block", "margin":"auto"}), style={"backgroundColor":bg, "verticalAlign": "middle", "textAlign": "center"}),
                html.Td(f"{r['metric']}", style={"backgroundColor":bg, "verticalAlign": "middle", "textAlign": "center"}),
                html.Td(f"{int(r['count']):,}", style={"textAlign":"center", "backgroundColor":bg, "verticalAlign": "middle"})
            ]))

        controls = html.Div([
            dbc.Row([
                dbc.Col([html.Span(f"{translate('Total', lang)}: {total_rows} | {translate('Rows', lang)}: ", className="me-2 align-middle"), dcc.Dropdown(id="invasions-rows-per-page-selector", options=[{'label': str(x), 'value': x} for x in [10, 25, 50, 100]] + [{'label': translate('All', lang), 'value': total_rows}], value=rows_per_page, clearable=False, className="rows-per-page-selector", style={"width":"80px", "display":"inline-block", "color":"black", "verticalAlign": "middle"})], width="auto", className="d-flex align-items-center"),
                dbc.Col([
                    dbc.ButtonGroup([dbc.Button("<<", id="invasions-first-page-btn", size="sm", disabled=current_page <= 1), dbc.Button("<", id="invasions-prev-page-btn", size="sm", disabled=current_page <= 1)], className="me-2"),
                    html.Span(f"{translate('Page', lang)} ", className="align-middle me-1"), dcc.Input(id="invasions-goto-page-input", type="number", min=1, max=total_pages_val, value=current_page, debounce=True, style={"width": "60px", "textAlign": "center", "display": "inline-block", "color": "black"}), html.Span(f" {translate('of', lang)} {total_pages_val}", className="align-middle ms-1 me-2"),
                    dbc.ButtonGroup([dbc.Button(">", id="invasions-next-page-btn", size="sm", disabled=current_page >= total_pages_val), dbc.Button(">>", id="invasions-last-page-btn", size="sm", disabled=current_page >= total_pages_val)]),
                ], width="auto", className="d-flex align-items-center justify-content-end ms-auto")
            ], className="g-0")
        ], className="p-2 bg-dark rounded mb-2 border border-secondary")

        visual_content = html.Div([controls, html.Div(html.Table([html.Thead(header_row), html.Tbody(rows)], style={"width":"100%", "color":"#fff"}), style={"overflowX":"auto", "maxHeight":"600px"})])

    # Charts Sum / Surged
    elif mode in ["surged", "sum"]:
        granular_df = df[~df['metric'].isin(['total', 'Confirmed', 'Unconfirmed'])]

        if granular_df.empty:
             graph_df = df[df['metric'].isin(['Confirmed', 'Unconfirmed'])].copy()
        else:
             graph_df = granular_df.copy()

        fig = go.Figure()

        if mode == "sum":
            d = graph_df.sort_values('count', ascending=True)
            colors = ["#28a745" if m == "Confirmed" else ("#dc3545" if m == "Unconfirmed" else "#dc3545") for m in d['metric']]

            fig.add_trace(go.Bar(x=d['metric'], y=d['count'], marker_color=colors))

            max_y = d['count'].max() if not d.empty else 10
            icon_size_y = max_y * 0.15
            for i, (idx, row) in enumerate(d.iterrows()):
                if row['char_id'] >= 0:
                    fig.add_layout_image(
                        dict(
                            source=get_invasion_icon_url(row['char_id']),
                            x=row['metric'],
                            y=row['count'],
                            xref="x", yref="y",
                            sizex=0.6, sizey=icon_size_y,
                            xanchor="center", yanchor="bottom"
                        )
                    )
            fig.update_layout(margin=dict(t=50))
            fig.update_yaxes(range=[0, max_y * 1.25])

        else:
            # Surged Line Chart
            graph_df['time_bucket'] = pd.to_numeric(graph_df['time_bucket'], errors='coerce').fillna(0).astype(int)

            agg = graph_df.groupby(["time_bucket", "metric"])["count"].sum().reset_index()
            for m in agg['metric'].unique():
                d = agg[agg['metric'] == m].sort_values("time_bucket")
                color = "#28a745" if m == "Confirmed" else ("#dc3545" if m == "Unconfirmed" else None)
                fig.add_trace(go.Scatter(x=d['time_bucket'], y=d['count'], mode='lines+markers', name=str(m), line=dict(color=color)))
            fig.update_xaxes(range=[-0.5, 23.5], dtick=1)

        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', title=f"{translate(mode.title(), lang)} {translate('Data', lang)}")
        visual_content = dcc.Graph(figure=fig, id="invasions-main-graph")

    return total_div, visual_content, json.dumps(data, indent=2), total_pages_val, {"display": "block"}

# Clientside callback for invasion heatmap rendering
dash.clientside_callback(
    ClientsideFunction(namespace='clientside', function_name='triggerInvasionHeatmapRenderer'),
    Output("invasions-clientside-dummy-store", "data", allow_duplicate=True),
    [Input("invasions-heatmap-data-store", "data"),
     Input("invasions-heatmap-hidden-grunts", "data"),
     Input("invasions-heatmap-mode-store", "data")],
    prevent_initial_call=True
)
