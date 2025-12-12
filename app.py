import pandas as pd 
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import Dash, dcc, html, Input, Output, State, ctx
import dash_bootstrap_components as dbc
import glob
import os
import re
from datetime import datetime

# File utilities remain the same
def get_latest_file(path_pattern):
    files = glob.glob(path_pattern)
    if not files:
        raise FileNotFoundError(f"No files found for pattern: {path_pattern}")
    return max(files, key=os.path.getmtime)

def extract_datetime_from_filename(filename):
    match = re.search(r'(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})', filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d_%H-%M-%S")
    return None

# Load files
realtime_path = get_latest_file('data/realtime/crypto_data_*.csv')
historical_path = get_latest_file('data/historical/top_10_crypto_*.csv')

realtime_time = extract_datetime_from_filename(realtime_path)
historical_time = extract_datetime_from_filename(historical_path)
last_updated = max(filter(None, [realtime_time, historical_time]))

# Load CSVs
realtime_df = pd.read_csv(realtime_path)
historical_df = pd.read_csv(historical_path)
historical_df['timestamp'] = pd.to_datetime(historical_df['timestamp'])

# Initialize app
app = Dash(__name__, external_stylesheets=[dbc.themes.CYBORG], suppress_callback_exceptions=True)
app.title = "Crypto Dashboard"

# Custom HTML/CSS for font + ticker animation
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link href="https://fonts.googleapis.com/css2?family=OCR+A+Extended&display=swap" rel="stylesheet">
        <style>
            body {
                font-family: 'OCR A Extended', monospace !important;
            }
            .ticker-wrapper {
                overflow: hidden;
                white-space: nowrap;
                width: 100%;
            }
            .ticker-content {
                display: inline-block;
                padding-left: 100%;
                animation: scroll-left 18s linear infinite;
            }
            @keyframes scroll-left {
                0% { transform: translateX(0%); }
                100% { transform: translateX(-100%); }
            }
            .ticker-item {
                display: inline-block;
                margin-right: 50px;
                font-size: 1.5rem;
                color: white;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# KPIs
total_market_cap = realtime_df['market_cap'].sum()
total_volume = realtime_df['total_volume'].sum()
btc_dominance = realtime_df[realtime_df['id'] == 'bitcoin']['market_cap'].values[0] / total_market_cap * 100
btc_price = realtime_df[realtime_df['id'] == 'bitcoin']['current_price'].values[0]

# Proper HTML Ticker
kpi_cards = dbc.Card(
    dbc.CardBody(
        html.Div([
            html.Div([
                html.Div([
                    html.Span(f"Total Market Cap: ${total_market_cap:,.0f}", className="ticker-item"),
                    html.Span(f"Total Volume: ${total_volume:,.0f}", className="ticker-item"),
                    html.Span(f"BTC Dominance: {btc_dominance:.2f}%", className="ticker-item"),
                    html.Span(f"BTC Price: ${btc_price:,.0f}", className="ticker-item"),
                ], className="ticker-content")
            ], className="ticker-wrapper")
        ])
    ),
    style={"backgroundColor": "#526ca2"},
    className="mb-4"
)

dropdown = dcc.Dropdown(
    id='crypto-select',
    options=[{'label': coin, 'value': coin} for coin in historical_df['id'].unique()],
    value='bitcoin',
    clearable=False,
    style={'color': '#000'}
)

top10 = realtime_df.nlargest(10, 'market_cap')
top10_sum = top10['market_cap'].sum()
rest_sum = total_market_cap - top10_sum
pie_data = pd.DataFrame({'category': ['Top 10 Coins', 'Other Coins'], 'market_cap': [top10_sum, rest_sum]})

chart_items = [
    ("Market Cap Distribution", px.histogram(realtime_df, x='market_cap', nbins=50, template='plotly_dark')),
    ("Current Price Distribution", px.histogram(realtime_df, x='current_price', nbins=50, template='plotly_dark')),
    ("Market Cap vs Volume", px.scatter(realtime_df, x='market_cap', y='total_volume', color='id', template='plotly_dark')),
    ("Price Change % in 24h", px.histogram(realtime_df, x='price_change_percentage_24h', nbins=50, template='plotly_dark')),
    ("Market Cap vs Price Change %", px.scatter(realtime_df, x='market_cap', y='price_change_percentage_24h', color='id', template='plotly_dark')),
    ("Top 10 Most Traded", px.bar(realtime_df.nlargest(10, 'total_volume'), x='id', y='total_volume', template='plotly_dark')),
    ("Top 10 vs Rest", px.pie(pie_data, names='category', values='market_cap', template='plotly_dark')),
    ("Price Over Time", None),
    ("Market Cap Over Time", None),
    ("Volume Over Time", None),
    ("Market Cap vs Price", px.scatter(historical_df, x='price', y='market_cap', color='id', template='plotly_dark')),
    ("Correlation Between Crypto Prices", px.imshow(historical_df.pivot_table(values='price', index='timestamp', columns='id').corr(), template='plotly_dark')),
    ("How far are current price of coins from their ATH", px.bar(
        historical_df.groupby('id').last().reset_index().assign(current_vs_ath=lambda df: df['price'] / df['ath'] * 100),
        x='id', y='current_vs_ath', template='plotly_dark')),
    ("Market Cap of BTC vs Other Cryptos", px.line(
        historical_df[historical_df['id'] != 'bitcoin']
        .assign(btc_market_cap=historical_df[historical_df['id'] == 'bitcoin']['market_cap'].mean())
        .assign(mcap_pct_btc=lambda df: df['market_cap'] / df['btc_market_cap'] * 100),
        x='timestamp', y='mcap_pct_btc', color='id', template='plotly_dark')),
    ("Price of BTC vs Other Cryptos", px.line(
        historical_df[historical_df['id'] != 'bitcoin']
        .assign(btc_price=historical_df[historical_df['id'] == 'bitcoin']['price'].mean())
        .assign(price_pct_btc=lambda df: df['price'] / df['btc_price'] * 100),
        x='timestamp', y='price_pct_btc', color='id', template='plotly_dark')),
]

icon_list = ["📊", "💰", "🔄", "📈", "📉", "🔥", "🥧", "🕒", "🏛️", "🔊", "💹", "📊", "📏", "🔁", "⚖️"]

nav_link_style = {
    "background-color": "#FF8C00",
    "color": "white",
    "border-radius": "10px",
    "padding": "8px 12px",
    "margin-bottom": "10px",
    "text-align": "center",
    "display": "block",
    "text-decoration": "none"
}

realtime_links = [
    dbc.NavLink(f"{icon_list[i]} {name}", href="#", id={'type': 'nav-link', 'index': f"rt-{i}"}, style=nav_link_style)
    for i, (name, _) in enumerate(chart_items[:7])
]

historical_links = [
    dbc.NavLink(f"{icon_list[i+7]} {name}", href="#", id={'type': 'nav-link', 'index': f"his-{i}"}, style=nav_link_style)
    for i, (name, _) in enumerate(chart_items[7:])
]

sidebar = html.Div([
    html.H4("Slides", className="text-white mt-4"),
    html.Hr(),
    dbc.Accordion([
        dbc.AccordionItem(realtime_links, title="Real Time Insights"),
        dbc.AccordionItem(historical_links, title="Historical Insights")
    ], always_open=True, flush=True)
], id="sidebar", style={
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "250px",
    "padding": "20px",
    "background-color": "#1f1f1f",
    "overflowY": "auto",
    "maxHeight": "100vh",
    "zIndex": 1000,
    "transition": "margin-left 0.3s"
})

toggle_btn = html.Button("☰", id="toggle-sidebar", style={
    "position": "fixed",
    "top": "15px",
    "zIndex": 1100,
    "background": "white",
    "color": "black",
    "border": "none",
    "fontSize": "24px",
    "padding": "4px 10px",
    "cursor": "pointer",
    "borderRadius": "5px"
})

app.layout = html.Div([
    dcc.Store(id='sidebar-toggle', data=True),
    sidebar,
    toggle_btn,
    html.Div([
        html.H2("Ｃｒｙｐｔｏｃｕｒｒｅｎｃｙ　Ｄａｓｈｂｏａｒｄ", className="text-center my-4"),

        html.P(
            f"Last updated: {last_updated.strftime('%Y-%m-%d %H:%M:%S')}",
            className="text-center text-muted mb-4"
        ),
        kpi_cards,
        html.Div([
            html.Div(id='question-title', className="my-3 text-center h4"),
            html.Div(id='dynamic-dropdown'),
            dcc.Graph(id='main-graph'),
            dbc.Row([
                dbc.Col(dbc.Button("Previous", id="prev-btn", color="primary", className="me-2"), width="auto"),
                dbc.Col(dbc.Button("Next", id="next-btn", color="primary"), width="auto"),
            ], justify="center", className="mb-4 mt-3"),
            dcc.Store(id='slide-index', data=0),
            dcc.Store(id='selected-coin', data='bitcoin'),
        ])
    ], id="main-content", style={"marginLeft": "270px", "padding": "20px", "transition": "margin-left 0.3s"}),

    html.Hr(),

    html.Div(
        "© 2025 Sahil Nechwani. All rights reserved.",
        style={
            "textAlign": "center",
            "fontSize": "12px",
            "color": "#aaaaaa",
            "marginTop": "40px",
            "marginBottom": "10px"
        }
    )
])


@app.callback(
    Output('sidebar-toggle', 'data'),
    Input('toggle-sidebar', 'n_clicks'),
    State('sidebar-toggle', 'data'),
    prevent_initial_call=True
)
def toggle_sidebar(n, current):
    return not current

@app.callback(
    Output('sidebar', 'style'),
    Output('main-content', 'style'),
    Output('toggle-sidebar', 'style'),
    Input('sidebar-toggle', 'data')
)
def adjust_layout(show_sidebar):
    toggle_style = toggle_btn.style.copy()
    if show_sidebar:
        sidebar_style = sidebar.style.copy()
        content_style = {"marginLeft": "270px", "padding": "20px", "transition": "margin-left 0.3s"}
        toggle_style["left"] = "270px"
    else:
        sidebar_style = sidebar.style.copy()
        sidebar_style['marginLeft'] = "-270px"
        content_style = {"marginLeft": "0px", "padding": "20px", "transition": "margin-left 0.3s"}
        toggle_style["left"] = "10px"
    return sidebar_style, content_style, toggle_style

@app.callback(
    Output('slide-index', 'data'),
    Input({'type': 'nav-link', 'index': dash.ALL}, 'n_clicks'),
    prevent_initial_call=True
)
def set_slide(n_clicks_list):
    triggered = ctx.triggered_id
    if triggered and isinstance(triggered, dict):
        full_index = triggered['index']
        if full_index.startswith('rt-'):
            return int(full_index.split('-')[1])
        elif full_index.startswith('his-'):
            return 7 + int(full_index.split('-')[1])
    return dash.no_update

@app.callback(
    Output('slide-index', 'data', allow_duplicate=True),
    Input('prev-btn', 'n_clicks'),
    Input('next-btn', 'n_clicks'),
    State('slide-index', 'data'),
    prevent_initial_call=True
)
def change_slide(prev_clicks, next_clicks, current_idx):
    if ctx.triggered_id == 'prev-btn':
        return (current_idx - 1) % len(chart_items)
    elif ctx.triggered_id == 'next-btn':
        return (current_idx + 1) % len(chart_items)
    return current_idx

@app.callback(
    Output('question-title', 'children'),
    Output('main-graph', 'figure'),
    Output('dynamic-dropdown', 'children'),
    Input('slide-index', 'data'),
    Input('selected-coin', 'data')
)
def update_slide(idx, coin):
    question, fig = chart_items[idx]
    show_dropdown = idx in [7, 8, 9]

    if show_dropdown:
        df = historical_df[historical_df['id'] == coin]
        if idx == 7:
            fig = px.line(df, x='timestamp', y='price', template='plotly_dark', title=f"{coin} Price Over Time")
        elif idx == 8:
            fig = px.line(df, x='timestamp', y='market_cap', template='plotly_dark', title=f"{coin} Market Cap Over Time")
        elif idx == 9:
            fig = px.line(df, x='timestamp', y='total_volume', template='plotly_dark', title=f"{coin} Volume Over Time")

    return question, fig, dropdown if show_dropdown else None

@app.callback(
    Output('selected-coin', 'data'),
    Input('crypto-select', 'value'),
    prevent_initial_call=True
)
def update_coin(val):
    return val if val else dash.no_update

if __name__ == '__main__':
    # for hosting on Render and similar services
    import os
    port = int(os.environ.get("PORT", 8050))
    app.run(host="0.0.0.0", port=port, debug=False)



