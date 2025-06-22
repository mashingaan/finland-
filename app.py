import dash
from dash import dcc, html, Input, Output, callback
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import json

# Загрузка данных
with open("dashboard_data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Данные по ключевому партнёру Германии (за 5 лет)
df_partners = pd.DataFrame(data["top_partner_countries"])
germany_row = df_partners[df_partners["country_name"] == "Германия"].iloc[0]
turnover_bln = germany_row.get("turnover_bln", (germany_row["X"] + germany_row["M"]) / 1_000_000_000)
export_bln = germany_row.get("export_bln", germany_row["X"] / 1_000_000_000)
import_bln = germany_row.get("import_bln", germany_row["M"] / 1_000_000_000)

# Блок с показателями торговли с Германией
div_germany = html.Div([
    html.H3("Ключевой партнёр — Германия", style={"marginBottom": "10px"}),
    html.Div([
        html.Div([
            html.H4("Товарооборот"),
            html.P(f"{fmt_ru(turnover_bln)}")
        ], style={"width": "30%", "display": "inline-block"}),
        html.Div([
            html.H4("Экспорт"),
            html.P(f"{fmt_ru(export_bln)}")
        ], style={"width": "30%", "display": "inline-block"}),
        html.Div([
            html.H4("Импорт"),
            html.P(f"{fmt_ru(import_bln)}")
        ], style={"width": "30%", "display": "inline-block"}),
    ], style={"display": "flex", "justifyContent": "space-between"})
], style={"backgroundColor": "#ffffff", "padding": "20px", "borderRadius": "8px", "marginBottom": "20px"})

# Функция для форматирования чисел
def fmt_ru(v):
    if pd.isna(v):
        return ""
    abs_v = abs(v)
    sign = "-" if v < 0 else ""
    if abs_v >= 1:
        return f"{sign}{abs_v:,.1f} млрд USD".replace(",", " ")
    else:
        return f"{sign}{abs_v*1_000:,.1f} млн USD".replace(",", " ")

# Инициализация приложения Dash
app = dash.Dash(__name__)
server = app.server

# Стили CSS
app.layout = html.Div([
    html.Div([
        html.H1("Дашборд внешней торговли Финляндии", 
                style={"textAlign": "center", "color": "#2c3e50", "marginBottom": "10px"}),
        html.P("Интерактивный анализ данных международной торговли Финляндии (2000-2023)", 
               style={"textAlign": "center", "color": "#7f8c8d", "marginBottom": "30px"})
    ], style={"backgroundColor": "#ecf0f1", "padding": "20px", "marginBottom": "20px"}),
    
    # График динамики торговли
    html.Div([
        html.H3("Динамика торговли", style={"color": "#2c3e50", "marginBottom": "15px"}),
        dcc.Graph(id="trade-dynamics-chart")
    ], style={"backgroundColor": "#ffffff", "padding": "20px", "marginBottom": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"}),
    
    # Строка с двумя графиками - товарные группы
    html.Div([
        # ТОП-10 товарных групп по экспорту
        html.Div([
            html.H3("ТОП-10 товарных групп по экспорту", style={"color": "#2c3e50", "marginBottom": "15px"}),
            dcc.Graph(id="top-commodities-export-chart")
        ], style={"width": "48%", "display": "inline-block", "backgroundColor": "#ffffff", 
                  "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"}),
        
        # ТОП-10 товарных групп по импорту
        html.Div([
            html.H3("ТОП-10 товарных групп по импорту", style={"color": "#2c3e50", "marginBottom": "15px"}),
            dcc.Graph(id="top-commodities-import-chart")
        ], style={"width": "48%", "display": "inline-block", "marginLeft": "4%", "backgroundColor": "#ffffff", 
                  "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"})
    ], style={"marginBottom": "20px"}),
    
    # Строка с двумя графиками
    html.Div([
        # Экономические секторы
        html.Div([
            html.H3("Экономические секторы", style={"color": "#2c3e50", "marginBottom": "15px"}),
            dcc.Graph(id="economic-sectors-chart")
        ], style={"width": "48%", "display": "inline-block", "backgroundColor": "#ffffff", 
                  "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"}),
        
        # География торговли
        html.Div([
            html.H3("География торговли", style={"color": "#2c3e50", "marginBottom": "15px"}),
            dcc.Graph(id="trade-geography-chart")
        ], style={"width": "48%", "display": "inline-block", "marginLeft": "4%", "backgroundColor": "#ffffff", 
                  "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"})
    ], style={"marginBottom": "20px"}),
    
    # Строка с двумя графиками
    html.Div([
        # ТОП-10 стран-партнёров
        html.Div([
            html.H3("ТОП-10 стран-партнёров (5 лет)", style={"color": "#2c3e50", "marginBottom": "15px"}),
            div_germany,
            dcc.Graph(id="top-countries-chart")
        ], style={"width": "48%", "display": "inline-block", "backgroundColor": "#ffffff",
                  "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"}),
        
        # Торговля с Россией
        html.Div([
            html.H3("Торговля с Россией (5 лет)", style={"color": "#2c3e50", "marginBottom": "15px"}),
            dcc.Graph(id="russia-trade-chart")
        ], style={"width": "48%", "display": "inline-block", "marginLeft": "4%", "backgroundColor": "#ffffff", 
                  "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"})
    ], style={"marginBottom": "20px"}),
    
    # Топ-5 прироста товарных групп (3 года)
    html.Div([
        html.H3("Топ-5 прироста товарных групп (3 года)", style={"color": "#2c3e50", "marginBottom": "15px", "textAlign": "center"}),
        html.Div([
            # Прирост экспорта
            html.Div([
                html.H4("Топ-5 прироста по экспорту", style={"color": "#2c3e50", "marginBottom": "15px"}),
                dcc.Graph(id="top-growth-export-chart")
            ], style={"width": "48%", "display": "inline-block", "backgroundColor": "#ffffff", 
                      "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"}),
            
            # Прирост импорта
            html.Div([
                html.H4("Топ-5 прироста по импорту", style={"color": "#2c3e50", "marginBottom": "15px"}),
                dcc.Graph(id="top-growth-import-chart")
            ], style={"width": "48%", "display": "inline-block", "marginLeft": "4%", "backgroundColor": "#ffffff", 
                      "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"})
        ])
    ], style={"backgroundColor": "#ecf0f1", "padding": "20px", "marginBottom": "20px", "borderRadius": "8px"}),
    
    # Изменения структуры торговли
    html.Div([
        html.H3("Изменения структуры (10 лет)", style={"color": "#2c3e50", "marginBottom": "15px"}),
        dcc.Graph(id="structure-changes-chart")
    ], style={"backgroundColor": "#ffffff", "padding": "20px", "borderRadius": "8px", "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"})

], style={"fontFamily": "Arial, sans-serif", "margin": "0", "padding": "20px", "backgroundColor": "#f8f9fa"})

# Callback для динамики торговли
@app.callback(
    Output("trade-dynamics-chart", "figure"),
    Input("trade-dynamics-chart", "id") # Dummy input to trigger callback on load
)
def update_trade_dynamics(dummy_input):
    df = pd.DataFrame(data["trade_dynamics"])
    df["year"] = df["year"].astype(int) # Ensure years are integers
    df["X_bln"] = df["X"] / 1_000_000_000
    df["M_bln"] = df["M"] / 1_000_000_000
    df["balance_bln"] = df["balance"] / 1_000_000_000
    
    fig = go.Figure()
    
    # Экспорт
    fig.add_trace(go.Scatter(
        x=df["year"],
        y=df["X_bln"],
        mode="lines+markers",
        name="Экспорт",
        line=dict(color="#27ae60", width=3),
        marker=dict(size=6),
        hovertemplate="Год: %{x}<br>Экспорт: %{customdata}<extra></extra>",
        customdata=[fmt_ru(val) for val in df["X_bln"]]
    ))
    
    # Импорт
    fig.add_trace(go.Scatter(
        x=df["year"],
        y=df["M_bln"],
        mode="lines+markers",
        name="Импорт",
        line=dict(color="#e74c3c", width=3),
        marker=dict(size=6),
        hovertemplate="Год: %{x}<br>Импорт: %{customdata}<extra></extra>",
        customdata=[fmt_ru(val) for val in df["M_bln"]]
    ))
    
    # Сальдо на второй оси
    fig.add_trace(go.Scatter(
        x=df["year"],
        y=df["balance_bln"],
        mode="lines+markers",
        name="Сальдо",
        line=dict(color="#3498db", width=3),
        marker=dict(size=6),
        yaxis="y2",
        hovertemplate="Год: %{x}<br>Сальдо: %{customdata}<extra></extra>",
        customdata=[fmt_ru(val) for val in df["balance_bln"]]
    ))
    
    fig.update_layout(
        title="Динамика экспорта, импорта и торгового сальдо",
        xaxis_title="Год",
        yaxis=dict(
            title="Объём торговли (млрд USD)",
            side="left",
            tickformat=".1f", # Format for billion USD
            gridcolor="rgba(0,0,0,0.05)"
        ),
        yaxis2=dict(
            title="Торговое сальдо (млрд USD)",
            side="right",
            overlaying="y",
            tickformat=".1f", # Format for billion USD
            gridcolor="rgba(0,0,0,0.05)"
        ),
        hovermode="x unified",
        legend=dict(x=0.02, y=0.98),
        height=400,
        font=dict(family="Arial", size=12),
        plot_bgcolor="rgba(0,0,0,0)", # Transparent background
        paper_bgcolor="rgba(0,0,0,0)" # Transparent background
    )
    
    return fig

# Callback для ТОП-10 товарных групп по экспорту
@app.callback(
    Output("top-commodities-export-chart", "figure"),
    Input("top-commodities-export-chart", "id") # Dummy input
)
def update_top_commodities_export(dummy_input):
    df = pd.DataFrame(data["top_export_commodities"])
    df["value_bln"] = df["primaryValue"] / 1_000_000_000
    
    # Сокращаем длинные названия товарных групп
    df["short_name"] = df["commodity_name"].apply(lambda x: (x[:35] + "...") if len(x) > 35 else x)
    
    fig = go.Figure(go.Bar(
        x=df["value_bln"],
        y=df["short_name"],
        orientation="h",
        marker_color="#27ae60",
        text=[fmt_ru(val) for val in df["value_bln"]],
        textposition="inside",
        textfont=dict(color="white", size=10),
        hovertemplate="%{customdata}<br>Объём: %{text}<extra></extra>",
        customdata=df["commodity_name"]
    ))
    
    fig.update_layout(
        title="ТОП-10 товарных групп по экспорту",
        xaxis_title="Объём (млрд USD)",
        yaxis_title="Товарная группа",
        height=500,
        margin=dict(l=200),
        font=dict(family="Arial", size=10),
        plot_bgcolor="rgba(0,0,0,0)", # Transparent background
        paper_bgcolor="rgba(0,0,0,0)", # Transparent background
        xaxis=dict(gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(gridcolor="rgba(0,0,0,0.05)")
    )
    
    return fig

# Callback для ТОП-10 товарных групп по импорту
@app.callback(
    Output("top-commodities-import-chart", "figure"),
    Input("top-commodities-import-chart", "id") # Dummy input
)
def update_top_commodities_import(dummy_input):
    df = pd.DataFrame(data["top_import_commodities"])
    df["value_bln"] = df["primaryValue"] / 1_000_000_000
    
    # Сокращаем длинные названия товарных групп
    df["short_name"] = df["commodity_name"].apply(lambda x: (x[:35] + "...") if len(x) > 35 else x)
    
    fig = go.Figure(go.Bar(
        x=df["value_bln"],
        y=df["short_name"],
        orientation="h",
        marker_color="rgba(0,123,255,0.8)",  # Bootstrap primary blue
        text=[fmt_ru(val) for val in df["value_bln"]],
        textposition="inside",
        textfont=dict(color="white", size=10),
        hovertemplate="%{customdata}<br>Объём: %{text}<extra></extra>",
        customdata=df["commodity_name"]
    ))
    
    fig.update_layout(
        title="ТОП-10 товарных групп по импорту",
        xaxis_title="Объём (млрд USD)",
        yaxis_title="Товарная группа",
        height=500,
        margin=dict(l=200),
        font=dict(family="Arial", size=10),
        plot_bgcolor="rgba(0,0,0,0)", # Transparent background
        paper_bgcolor="rgba(0,0,0,0)", # Transparent background
        xaxis=dict(gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(gridcolor="rgba(0,0,0,0.05)")
    )
    
    return fig

# Callback для экономических секторов
@app.callback(
    Output("economic-sectors-chart", "figure"),
    Input("economic-sectors-chart", "id") # Dummy input
)
def update_economic_sectors(dummy_input):
    df = pd.DataFrame(data["economic_sectors"])
    
    # Фильтруем только секторы с экспортом > 0
    df_filtered = df[df["X"] > 0].copy()
    df_filtered["export_share_pct"] = df_filtered["export_share"] * 100
    df_filtered["X_bln"] = df_filtered["X"] / 1_000_000_000
    
    # Сокращаем названия секторов
    df_filtered["short_sector"] = df_filtered["sector"].apply(lambda x: (x[:25] + "...") if len(x) > 25 else x)
    
    fig = go.Figure(go.Pie(
        labels=df_filtered["short_sector"],
        values=df_filtered["export_share_pct"],
        hole=0.4,
        textinfo="label+percent",
        textposition="outside",
        textfont=dict(size=10),
        hovertemplate="%{label}<br>Доля: %{value:.1f}%<br>Объём: %{customdata}<extra></extra>",
        customdata=[fmt_ru(val) for val in df_filtered["X_bln"]]
    ))
    
    fig.update_layout(
        title="Распределение экспорта по секторам экономики",
        height=500,
        font=dict(family="Arial", size=10),
        showlegend=False,
        plot_bgcolor="rgba(0,0,0,0)", # Transparent background
        paper_bgcolor="rgba(0,0,0,0)" # Transparent background
    )
    
    return fig

# Callback для географии торговли
@app.callback(
    Output("trade-geography-chart", "figure"),
    Input("trade-geography-chart", "id") # Dummy input
)
def update_trade_geography(dummy_input):
    df = pd.DataFrame(data["trade_geography"])

    # Исключаем неизвестные регионы
    df_filtered = df[df["world_part"] != "Неизвестно"].copy()
    df_filtered["export_pct"] = df_filtered["export_share"] * 100
    df_filtered["import_pct"] = df_filtered["import_share"] * 100
    df_filtered["X_bln"] = df_filtered["X"] / 1_000_000_000
    df_filtered["M_bln"] = df_filtered["M"] / 1_000_000_000
    
    fig = go.Figure()
    
    # Экспорт
    fig.add_trace(go.Bar(
        x=df_filtered["world_part"],
        y=df_filtered["export_pct"],
        name="Экспорт",
        marker_color="#27ae60",
        text=[f"{val:.1f}%" for val in df_filtered["export_pct"]],
        textposition="inside",
        hovertemplate="%{x}<br>Экспорт: %{customdata}<br>Доля: %{text}<extra></extra>",
        customdata=[fmt_ru(val) for val in df_filtered["X_bln"]]
    ))
    
    # Импорт
    fig.add_trace(go.Bar(
        x=df_filtered["world_part"],
        y=df_filtered["import_pct"],
        name="Импорт",
        marker_color="#e74c3c",
        text=[f"{val:.1f}%" for val in df_filtered["import_pct"]],
        textposition="inside",
        hovertemplate="%{x}<br>Импорт: %{customdata}<br>Доля: %{text}<extra></extra>",
        customdata=[fmt_ru(val) for val in df_filtered["M_bln"]]
    ))
    
    fig.update_layout(
        title="Доля торговли по регионам мира",
        xaxis_title="Регион",
        yaxis_title="Доля в торговле (%)",
        barmode="group",
        height=400,
        font=dict(family="Arial", size=12),
        plot_bgcolor="rgba(0,0,0,0)", # Transparent background
        paper_bgcolor="rgba(0,0,0,0)", # Transparent background
        xaxis=dict(gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(gridcolor="rgba(0,0,0,0.05)")
    )
    
    return fig

# Callback для ТОП-10 стран-партнёров
@app.callback(
    Output("top-countries-chart", "figure"),
    Input("top-countries-chart", "id") # Dummy input
)
def update_top_countries(dummy_input):
    df_all = pd.DataFrame(data["top_partner_countries"])
    germany_row = df_all[df_all["country_name"] == "Германия"].iloc[0]
    turnover_bln = germany_row["turnover_bln"]
    export_bln = germany_row.get("export_bln", germany_row["X"] / 1_000_000_000)
    import_bln = germany_row.get("import_bln", germany_row["M"] / 1_000_000_000)
    df = df_all

    # Use the pre-calculated balance_bln and turnover_bln from data_preparation.py
    colors = ["#27ae60" if bal >= 0 else "#e74c3c" for bal in df["balance_bln"]]
    
    fig = go.Figure(go.Bar(
        x=df["country_name"],
        y=df["turnover_bln"],
        marker_color=colors,
        text=[fmt_ru(val) for val in df["turnover_bln"]],
        textposition="outside",
        hovertemplate="%{x}<br>Общий объём торговли: %{customdata}<extra></extra>",
        customdata=[fmt_ru(val) for val in df["turnover_bln"]]
    ))
    
    fig.update_layout(
        title="ТОП-10 стран-партнёров по общему объёму торговли",
        xaxis_title="Страна",
        yaxis_title="Объём торговли (млрд USD)",
        height=400,
        font=dict(family="Arial", size=10),
        xaxis_tickangle=-45,
        plot_bgcolor="rgba(0,0,0,0)", # Transparent background
        paper_bgcolor="rgba(0,0,0,0)", # Transparent background
        xaxis=dict(gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(gridcolor="rgba(0,0,0,0.05)")
    )
    
    return fig

# Callback для торговли с Россией
@app.callback(
    Output("russia-trade-chart", "figure"),
    Input("russia-trade-chart", "id") # Dummy input
)
def update_russia_trade(dummy_input):
    df = pd.DataFrame(data["russia_trade_dynamics"])
    
    if df.empty:
        # Если нет данных по России, показываем пустой график
        fig = go.Figure()
        fig.add_annotation(
            text="Нет данных по торговле с Россией",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Торговля с Россией (5 лет)",
            height=400,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor="rgba(0,0,0,0)", # Transparent background
            paper_bgcolor="rgba(0,0,0,0)" # Transparent background
        )
        return fig

    df["year"] = df["year"].astype(int) # Ensure years are integers
    df["X_mln"] = df["X"] / 1_000_000  # Convert to millions
    df["M_mln"] = df["M"] / 1_000_000
    df["balance_mln"] = df["balance"] / 1_000_000 if "balance" in df.columns else (df["X"] - df["M"]) / 1_000_000

    fig = go.Figure()
    
    # Экспорт
    fig.add_trace(go.Scatter(
        x=df["year"],
        y=df["X_mln"],
        mode="lines+markers",
        name="Экспорт",
        line=dict(color="#27ae60", width=3),
        marker=dict(size=6),
        hovertemplate="Год: %{x}<br>Экспорт: %{customdata}<extra></extra>",
        customdata=[fmt_ru(val / 1_000) for val in df["X_mln"]] # Convert to billions for fmt_ru
    ))
    
    # Импорт
    fig.add_trace(go.Scatter(
        x=df["year"],
        y=df["M_mln"],
        mode="lines+markers",
        name="Импорт",
        line=dict(color="#e74c3c", width=3),
        marker=dict(size=6),
        hovertemplate="Год: %{x}<br>Импорт: %{customdata}<extra></extra>",
        customdata=[fmt_ru(val / 1_000) for val in df["M_mln"]] # Convert to billions for fmt_ru
    ))
    
    # Сальдо
    fig.add_trace(go.Scatter(
        x=df["year"],
        y=df["balance_mln"],
        mode="lines+markers",
        name="Сальдо",
        line=dict(color="#3498db", width=3),
        marker=dict(size=6),
        hovertemplate="Год: %{x}<br>Сальдо: %{customdata}<extra></extra>",
        customdata=[fmt_ru(val / 1_000) for val in df["balance_mln"]]
    ))

    fig.update_layout(
        title="Торговля с Россией (5 лет)",
        xaxis_title="Год",
        yaxis_title="Объём торговли (млн USD)",
        hovermode="x unified",
        legend=dict(x=0.02, y=0.98),
        height=400,
        font=dict(family="Arial", size=12),
        plot_bgcolor="rgba(0,0,0,0)", # Transparent background
        paper_bgcolor="rgba(0,0,0,0)", # Transparent background
        xaxis=dict(gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(gridcolor="rgba(0,0,0,0.05)")
    )
    
    return fig

# Callback для изменений структуры торговли
@app.callback(
    Output("structure-changes-chart", "figure"),
    Input("structure-changes-chart", "id") # Dummy input
)
def update_structure_changes(dummy_input):
    df = pd.DataFrame(data["declining_commodities"])
    
    if df.empty:
        # Если нет данных, показываем пустой график
        fig = go.Figure()
        fig.add_annotation(
            text="Недостаточно данных для анализа изменений структуры",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Изменения структуры экспорта (10 лет)",
            height=400,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor="rgba(0,0,0,0)", # Transparent background
            paper_bgcolor="rgba(0,0,0,0)" # Transparent background
        )
        return fig
    
    df["change_bln"] = df["change"] / 1_000_000_000
    
    # Сокращаем названия товарных групп
    df["short_name"] = df["commodity_name"].apply(lambda x: (x[:25] + "...") if len(x) > 25 else x)
    
    fig = go.Figure(go.Bar(
        x=df["change_bln"],
        y=df["short_name"],
        orientation="h",
        marker_color="#e74c3c",
        text=[fmt_ru(val) for val in df["change_bln"]],
        textposition="inside",
        textfont=dict(color="white", size=10),
        hovertemplate="%{customdata}<br>Изменение: %{text}<extra></extra>",
        customdata=df["commodity_name"]
    ))
    
    fig.update_layout(
        title="Товарные группы с наибольшим снижением объёмов торговли",
        xaxis_title="Изменение объёма (млрд USD)",
        yaxis_title="Товарная группа",
        height=400,
        margin=dict(l=200),
        font=dict(family="Arial", size=10),
        plot_bgcolor="rgba(0,0,0,0)", # Transparent background
        paper_bgcolor="rgba(0,0,0,0)", # Transparent background
        xaxis=dict(gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(gridcolor="rgba(0,0,0,0.05)")
    )
    
    return fig

# Callback для топ-5 прироста экспорта
@app.callback(
    Output("top-growth-export-chart", "figure"),
    Input("top-growth-export-chart", "id") # Dummy input
)
def update_top_growth_export(dummy_input):
    df = pd.DataFrame(data["export_growth"])
    
    if df.empty:
        # Если нет данных, показываем пустой график
        fig = go.Figure()
        fig.add_annotation(
            text="Нет данных по приросту экспорта",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Топ-5 прироста по экспорту (2021→2023)",
            height=400,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)"
        )
        return fig
    
    # Сокращаем названия товарных групп
    df["short_name"] = df["commodity_name"].apply(lambda x: (x[:35] + "...") if len(x) > 35 else x)
    
    fig = go.Figure(go.Bar(
        x=df["delta"],
        y=df["short_name"],
        orientation="h",
        marker_color="#28a745",  # Зеленый цвет для экспорта
        text=[fmt_ru(val) for val in df["delta"]],
        textposition="inside",
        textfont=dict(color="white", size=10),
        hovertemplate="%{customdata}<br>Прирост: %{text}<extra></extra>",
        customdata=df["commodity_name"]
    ))
    
    fig.update_layout(
        title="Топ-5 прироста по экспорту (2021→2023)",
        xaxis_title="Прирост объёма (млрд USD)",
        yaxis_title="Товарная группа",
        height=400,
        margin=dict(l=200),
        font=dict(family="Arial", size=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(gridcolor="rgba(0,0,0,0.05)")
    )
    
    return fig

# Callback для топ-5 прироста импорта
@app.callback(
    Output("top-growth-import-chart", "figure"),
    Input("top-growth-import-chart", "id") # Dummy input
)
def update_top_growth_import(dummy_input):
    df = pd.DataFrame(data["import_growth"])
    
    if df.empty:
        # Если нет данных, показываем пустой график
        fig = go.Figure()
        fig.add_annotation(
            text="Нет данных по приросту импорта",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="gray")
        )
        fig.update_layout(
            title="Топ-5 прироста по импорту (2021→2023)",
            height=400,
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)"
        )
        return fig
    
    # Сокращаем названия товарных групп
    df["short_name"] = df["commodity_name"].apply(lambda x: (x[:35] + "...") if len(x) > 35 else x)
    
    fig = go.Figure(go.Bar(
        x=df["delta"],
        y=df["short_name"],
        orientation="h",
        marker_color="#ff5733",  # Красно-оранжевый цвет для импорта
        text=[fmt_ru(val) for val in df["delta"]],
        textposition="inside",
        textfont=dict(color="white", size=10),
        hovertemplate="%{customdata}<br>Прирост: %{text}<extra></extra>",
        customdata=df["commodity_name"]
    ))
    
    fig.update_layout(
        title="Топ-5 прироста по импорту (2021→2023)",
        xaxis_title="Прирост объёма (млрд USD)",
        yaxis_title="Товарная группа",
        height=400,
        margin=dict(l=200),
        font=dict(family="Arial", size=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(gridcolor="rgba(0,0,0,0.05)"),
        yaxis=dict(gridcolor="rgba(0,0,0,0.05)")
    )
    
    return fig

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)

