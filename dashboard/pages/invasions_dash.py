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

dash.register_page(__name__, path='/invasions', title='Invasion Analytics')

ICON_BASE_URL = "https://raw.githubusercontent.com/WatWowMap/wwm-uicons-webp/main"

INCIDENT_DISPLAY_TYPES = {
    0: "None", 1: "Grunt", 2: "Leader", 3: "Giovanni", 4: "Grunt B",
    5: "Event NPC", 6: "Route NPC", 7: "Generic", 8: "Stop Encounter",
    9: "Contest", 10: "Natural Art A", 11: "Natural Art B"
}

_GRUNT_MAP = None

def _get_grunt_map():
    """Loads grunts.json: ID -> Name (e.g. 52 -> Balloon Grunt Male)"""
    global _GRUNT_MAP
    if _GRUNT_MAP is None:
        try:
            path = os.path.join(os.path.dirname(__file__), '..', 'assets', 'grunts.json')
            if not os.path.exists(path): path = os.path.join(os.getcwd(), 'assets', 'grunts.json')
            with open(path, 'r') as f:
                data = json.load(f)
                _GRUNT_MAP = {v: k.replace("_", " ").title().replace("Npc", "NPC") for k, v in data.items()}
        except Exception as e:
            logger.error(f"Error loading grunts.json: {e}")
            _GRUNT_MAP = {}
    return _GRUNT_MAP

def get_invasion_icon_url(character_id):
    return f"{ICON_BASE_URL}/invasion/{character_id}.webp"

def parse_invasion_key(key_str):
    """
    Parses keys like '1:10' (Display:Character).
    Returns (display_type_id, character_id, label)
    """
    grunt_map = _get_grunt_map()
    parts = str(key_str).split(':')

    if len(parts) >= 2:
        try:
            disp_id = int(parts[0])
            char_id = int(parts[1])

            # Try to get specific name from grunts.json
            specific_name = grunt_map.get(char_id)

            if specific_name:
                label = specific_name
            else:
                # Fallback to Display Type
                disp_name = INCIDENT_DISPLAY_TYPES.get(disp_id, f"Type {disp_id}")
                label = f"{disp_name} (ID: {char_id})"

            return disp_id, char_id, label
        except ValueError:
            pass
    return 0, 0, str(key_str)

# Layout

def generate_area_cards(geofences, selected_area_name):
    cards = []
    for idx, geo in enumerate(geofences):
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', geo['name'])
        is_selected = (selected_area_name == geo['name'])
        map_children = [html.Div("✓ Selected", style={'position': 'absolute', 'top': '10px', 'right': '10px', 'backgroundColor': '#28a745', 'color': 'white', 'padding': '4px 8px', 'borderRadius': '4px', 'fontWeight': 'bold', 'zIndex': '1000'})] if is_selected else []

        card = dbc.Card([
            html.Div(map_children, id=f"invasions-area-map-{safe_name}", **{'data-map-geofence': json.dumps(geo)}, style={'height': '150px', 'backgroundColor': '#1a1a1a', 'position': 'relative'}),
            dbc.CardBody([
                html.H5(geo['name'], className="card-title text-truncate", style={'color': '#28a745' if is_selected else 'inherit'}),
                dbc.Button("✓ Selected" if is_selected else "Select", href=f"/invasions?area={geo['name']}", color="success" if is_selected else "primary", size="sm", className="w-100", disabled=is_selected)
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
        dcc.Store(id="invasions-raw-data-store"),
        dcc.Store(id="invasions-table-sort-store", data={"col": "count", "dir": "desc"}),
        dcc.Store(id="invasions-table-page-store", data={"current_page": 1, "rows_per_page": 25}),
        dcc.Store(id="invasions-total-pages-store", data=1),
        dcc.Store(id="invasions-clientside-dummy-store"),
        dcc.Dropdown(id="invasions-area-selector", options=area_options, value=area, style={'display': 'none'}),
        dcc.Store(id="invasions-mode-persistence-store", storage_type="local"),
        dcc.Store(id="invasions-source-persistence-store", storage_type="local"),

        # Header
        dbc.Row([
            dbc.Col(html.H2("Invasion Analytics", className="text-white"), width=12, className="my-4"),
        ]),

        # Notification Area
        html.Div(id="invasions-notification-area"),

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
                            dbc.Button("Change", id="invasions-open-area-modal", color="primary")
                        ], className="mb-3")
                    ], width=12, md=6),

                    # Data Source
                    dbc.Col([
                        dbc.Label("Data Source", className="fw-bold"),
                        html.Div(
                            dbc.RadioItems(
                                id="invasions-data-source-selector",
                                options=[{"label": "Live (Real-time)", "value": "live"}, {"label": "Historical (Stats)", "value": "historical"}],
                                value="live", inline=True, inputClassName="btn-check", labelClassName="btn btn-outline-secondary", labelCheckedClassName="active"
                            ), className="mb-3"
                        )
                    ], width=12, md=6)
                ], className="g-3"),

                html.Hr(className="my-3"),

                # Controls Row
                dbc.Row([
                    # Time Control
                    dbc.Col([
                        html.Div(id="invasions-live-controls", children=[
                            dbc.Label("📅 Time Window (Hours)"),
                            dbc.InputGroup([
                                dbc.Input(id="invasions-live-time-input", type="number", min=1, max=72, value=1),
                                dbc.InputGroupText("hours")
                            ])
                        ]),
                        html.Div(id="invasions-historical-controls", style={"display": "none"}, children=[
                            dbc.Label("📅 Date Range"),
                            dcc.DatePickerRange(id="invasions-historical-date-picker", min_date_allowed=date(2023, 1, 1), max_date_allowed=date.today(), start_date=date.today(), end_date=date.today(), className="d-block w-100")
                        ])
                    ], width=6, md=3),

                    # Interval
                    dbc.Col([
                        html.Div(id="invasions-interval-control-container", style={"display": "none"}, children=[
                            dbc.Label("⏱️ Interval"),
                            dcc.Dropdown(id="invasions-interval-selector", options=[{"label": "Hourly", "value": "hourly"}], value="hourly", clearable=False, className="text-dark")
                        ])
                    ], width=6, md=3),

                    # Mode
                    dbc.Col([
                        dbc.Label("📊 View Mode"),
                        dcc.Dropdown(id="invasions-mode-selector", options=[], value=None, clearable=False, className="text-dark")
                    ], width=6, md=3),

                    # Actions
                    dbc.Col([
                        dbc.Label("Actions", style={"visibility": "hidden"}),
                        dbc.Button("Run Analysis", id="invasions-submit-btn", color="success", className="w-100 fw-bold")
                    ], width=6, md=3)
                ], className="align-items-end g-3")
            ])
        ], className="shadow-sm border-0 mb-4"),

        # Area Selection Modal
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("Select an Area")),
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
        dcc.Loading(html.Div(id="invasions-stats-container", style={"display": "none"}, children=[
            dbc.Row([
                # Sidebar
                dbc.Col(dbc.Card([
                    dbc.CardHeader("📈 Total Counts"),
                    dbc.CardBody(html.Div(id="invasions-total-counts-display"))
                ], className="shadow-sm border-0 h-100"), width=12, lg=4, className="mb-4"),

                # Activity Data
                dbc.Col(dbc.Card([
                    dbc.CardHeader("📋 Activity Data"),
                    dbc.CardBody([
                         # Embedded Search Input (Visible only in Grouped mode)
                        dcc.Input(
                            id="invasions-search-input",
                            type="text",
                            placeholder="🔍 Search Invasions...",
                            debounce=True,
                            className="form-control mb-3",
                            style={"display": "none"}
                        ),
                        html.Div(id="invasions-main-visual-container")
                    ])
                ], className="shadow-sm border-0 h-100"), width=12, lg=8, className="mb-4"),
            ]),

            dbc.Row([dbc.Col(dbc.Card([
                dbc.CardHeader("🛠️ Raw Data Inspector"),
                dbc.CardBody(html.Pre(id="invasions-raw-data-display", style={"maxHeight": "300px", "overflow": "scroll"}))
            ], className="shadow-sm border-0"), width=12)])
        ]))
    ])

# Parsing Logic

def parse_data_to_df(data, mode, source):
    records = []
    working_data = data.get('data', data) if isinstance(data, dict) else data

    if mode == "sum":
        if isinstance(working_data, dict):
            if "total" in working_data:
                records.append({"metric": "total", "count": working_data["total"], "key": "Total", "char_id": -1, "time_bucket": "Total"})

            if "confirmed" in working_data and isinstance(working_data["confirmed"], dict):
                for k, v in working_data["confirmed"].items():
                    lbl = "Confirmed" if str(k) == "1" else "Unconfirmed"
                    records.append({"metric": lbl, "count": v, "key": lbl, "char_id": -1, "time_bucket": "Total"})

            items_dict = {}
            if "display_type+character" in working_data:
                items_dict = working_data["display_type+character"]
            elif "grunt" in working_data:
                items_dict = {f"1:{k}": v for k,v in working_data["grunt"].items()}

            for key_str, count in items_dict.items():
                if not isinstance(count, (int, float)): continue
                disp_id, char_id, label = parse_invasion_key(key_str)
                records.append({"metric": label, "count": count, "key": f"{disp_id}:{char_id}", "char_id": char_id, "time_bucket": "Total"})

    elif mode == "grouped":
        if isinstance(working_data, dict):
            source_dict = working_data.get("display_type+character", working_data)
            for key_str, content in source_dict.items():
                if source_dict is working_data and key_str in ["total", "confirmed", "grunt", "display_type+character"]: continue
                if ":" in str(key_str):
                    disp_id, char_id, label = parse_invasion_key(key_str)
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
                            disp_id, char_id, label = parse_invasion_key(k)
                            records.append({"metric": label, "count": v, "key": f"{disp_id}:{char_id}", "char_id": char_id, "time_bucket": h_val})

                        if "confirmed" in content and isinstance(content["confirmed"], dict):
                             for k, v in content["confirmed"].items():
                                lbl = "Confirmed" if str(k) == "1" else "Unconfirmed"
                                records.append({"metric": lbl, "count": v, "key": lbl, "char_id": -1, "time_bucket": h_val})
            else:
                for key_str, hourly_data in working_data.items():
                    if ":" in key_str:
                        disp_id, char_id, label = parse_invasion_key(key_str)
                        if isinstance(hourly_data, dict):
                            for hour_key, count in hourly_data.items():
                                h_val = hour_key
                                if "hour" in str(hour_key):
                                    try: h_val = int(str(hour_key).replace("hour ", ""))
                                    except: pass
                                records.append({"metric": label, "count": count, "key": f"{disp_id}:{char_id}", "char_id": char_id, "time_bucket": h_val})
                    elif key_str == "confirmed" and isinstance(hourly_data, dict):
                         for status_key, time_dict in hourly_data.items():
                             lbl = "Confirmed" if str(status_key) == "1" else "Unconfirmed"
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

@callback(
    [Output("invasions-live-controls", "style"), Output("invasions-historical-controls", "style"), Output("invasions-interval-control-container", "style")],
    Input("invasions-data-source-selector", "value")
)
def toggle_source(source):
    if "live" in source: return {"display": "block"}, {"display": "none"}, {"display": "none"}
    return {"display": "none"}, {"display": "block", "position": "relative", "zIndex": 1002}, {"display": "block"}

@callback(
    [Output("invasions-mode-selector", "options"), Output("invasions-mode-selector", "value")],
    Input("invasions-data-source-selector", "value"),
    [State("invasions-mode-persistence-store", "data"), State("invasions-mode-selector", "value")]
)
def restrict_modes(source, stored_mode, current_ui_mode):
    full_options = [{"label": "Surged (Hourly)", "value": "surged"}, {"label": "Grouped (Table)", "value": "grouped"}, {"label": "Sum (Totals)", "value": "sum"}]
    allowed_values = [o['value'] for o in full_options]
    if current_ui_mode in allowed_values: final_value = current_ui_mode
    elif stored_mode in allowed_values: final_value = stored_mode
    else: final_value = allowed_values[0]
    return full_options, final_value

@callback(Output("invasions-mode-persistence-store", "data"), Input("invasions-mode-selector", "value"), prevent_initial_call=True)
def save_mode(val): return val

@callback(Output("invasions-source-persistence-store", "data"), Input("invasions-data-source-selector", "value"), prevent_initial_call=True)
def save_source(val): return val

@callback(
    Output("invasions-data-source-selector", "value"),
    Input("invasions-source-persistence-store", "modified_timestamp"),
    State("invasions-source-persistence-store", "data"),
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
    [Output("invasions-area-modal", "is_open"), Output("invasions-search-input", "style")],
    [Input("invasions-open-area-modal", "n_clicks"), Input("invasions-close-area-modal", "n_clicks"), Input("invasions-mode-selector", "value")],
    [State("invasions-area-modal", "is_open")]
)
def handle_modals_and_search(ao, ac, mode, isa):
    search_style = {"display": "block", "width": "100%"} if mode == "grouped" else {"display": "none"}
    tid = ctx.triggered_id
    if tid in ["invasions-open-area-modal", "invasions-close-area-modal"]: return not isa, search_style
    return isa, search_style

# --- Area Cards Filter & Scroll ---
@callback(Output("invasions-area-cards-container", "children"), [Input("invasions-area-filter-input", "value")], [State("invasions-area-selector", "value")])
def filter_area_cards(search_term, selected_area):
    geofences = get_cached_geofences() or []
    if search_term: geofences = [g for g in geofences if search_term.lower() in g['name'].lower()]
    return generate_area_cards(geofences, selected_area)

dash.clientside_callback(
    ClientsideFunction(namespace='clientside', function_name='scrollToSelected'),
    Output("invasions-clientside-dummy-store", "data"), Input("invasions-area-modal", "is_open")
)

@callback(
    [Output("invasions-raw-data-store", "data"), Output("invasions-stats-container", "style"), Output("invasions-notification-area", "children")],
    [Input("invasions-submit-btn", "n_clicks"), Input("invasions-data-source-selector", "value")],
    [State("invasions-area-selector", "value"), State("invasions-live-time-input", "value"), State("invasions-historical-date-picker", "start_date"), State("invasions-historical-date-picker", "end_date"), State("invasions-interval-selector", "value"), State("invasions-mode-selector", "value")]
)
def fetch_data(n, source, area, live_h, start, end, interval, mode):
    if not n: return {}, {"display": "none"}, None
    if not area:
        return {}, {"display": "none"}, dbc.Alert([html.I(className="bi bi-exclamation-triangle-fill me-2"), "Please select an Area first."], color="warning", dismissable=True, duration=4000)

    try:
        if source == "live":
            hours = max(1, min(int(live_h or 1), 72))
            params = {"start_time": f"{hours} hours", "end_time": "now", "mode": mode, "area": area, "response_format": "json"}
            data = get_invasions_stats("timeseries", params)
        else:
            params = {"counter_type": "totals", "interval": interval, "start_time": f"{start}T00:00:00", "end_time": f"{end}T23:59:59", "mode": mode, "area": area, "response_format": "json"}
            data = get_invasions_stats("counter", params)
        return data, {"display": "block"}, None
    except Exception as e:
        logger.info(f"Fetch error: {e}")
        return {}, {"display": "none"}, dbc.Alert(f"Error: {str(e)}", color="danger", dismissable=True)

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
    [Input("invasions-raw-data-store", "data"), Input("invasions-search-input", "value"), Input("invasions-table-sort-store", "data"), Input("invasions-table-page-store", "data")],
    [State("invasions-mode-selector", "value"), State("invasions-data-source-selector", "value")]
)
def update_visuals(data, search_term, sort, page, mode, source):
    if not data: return [], html.Div(), "", 1, {"display": "block"}

    df = parse_data_to_df(data, mode, source)
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
            html.Th("Image", style={"backgroundColor": "#1a1a1a", "zIndex": "10", "position": "sticky", "top": "0", "textAlign": "center", "width": "60px"}),
            html.Th(html.Span(["Type", html.Span(" ▲" if col == 'metric' and ascending else (" ▼" if col == 'metric' else ""), style={"color": "#aaa", "marginLeft": "5px"})], id={"type": "invasions-sort-header", "index": "metric"}, style={"cursor": "pointer"}), style={"backgroundColor": "#1a1a1a", "zIndex": "10", "position": "sticky", "top": "0", "textAlign": "center"}),
            html.Th(html.Span(["Count", html.Span(" ▲" if col == 'count' and ascending else (" ▼" if col == 'count' else ""), style={"color": "#aaa", "marginLeft": "5px"})], id={"type": "invasions-sort-header", "index": "count"}, style={"cursor": "pointer"}), style={"backgroundColor": "#1a1a1a", "zIndex": "10", "position": "sticky", "top": "0", "textAlign": "center"})
        ])

        rows = []
        for i, r in enumerate(page_df.iterrows()):
            _, r = r
            bg = "#1a1a1a" if i % 2 == 0 else "#242424"
            rows.append(html.Tr([
                html.Td(html.Img(src=get_invasion_icon_url(r['char_id']), style={"width":"40px", "display":"block", "margin":"auto"}), style={"backgroundColor":bg, "verticalAlign": "middle", "textAlign": "center"}),
                html.Td(f"{r['metric']}", style={"backgroundColor":bg, "verticalAlign": "middle", "textAlign": "center"}),
                html.Td(f"{int(r['count']):,}", style={"textAlign":"center", "backgroundColor":bg, "verticalAlign": "middle"})
            ]))

        controls = html.Div([
            dbc.Row([
                dbc.Col([html.Span(f"Total: {total_rows} | Rows: ", className="me-2 align-middle"), dcc.Dropdown(id="invasions-rows-per-page-selector", options=[{'label': str(x), 'value': x} for x in [10, 25, 50, 100]] + [{'label': 'All', 'value': total_rows}], value=rows_per_page, clearable=False, className="rows-per-page-selector", style={"width":"80px", "display":"inline-block", "color":"black", "verticalAlign": "middle"})], width="auto", className="d-flex align-items-center"),
                dbc.Col([
                    dbc.ButtonGroup([dbc.Button("<<", id="invasions-first-page-btn", size="sm", disabled=current_page <= 1), dbc.Button("<", id="invasions-prev-page-btn", size="sm", disabled=current_page <= 1)], className="me-2"),
                    html.Span("Page ", className="align-middle me-1"), dcc.Input(id="invasions-goto-page-input", type="number", min=1, max=total_pages_val, value=current_page, debounce=True, style={"width": "60px", "textAlign": "center", "display": "inline-block", "color": "black"}), html.Span(f" of {total_pages_val}", className="align-middle ms-1 me-2"),
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

        fig.update_layout(template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', title=f"{mode.title()} Data")
        visual_content = dcc.Graph(figure=fig, id="invasions-main-graph")

    return total_div, visual_content, json.dumps(data, indent=2), total_pages_val, {"display": "block"}
