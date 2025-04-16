import dash
from dash import dcc, html, dash_table
import pandas as pd
import plotly.express as px

# Загружаем данные
df = pd.read_parquet("campaign_activity_first_4_hours.parquet")

# Инициализация Dash-приложения
app = dash.Dash(__name__, external_stylesheets=['styles.css'])
app.title = "Анализ активности кампаний"

# Топ-10 кампаний по количеству кликов
fig_top10_clicks = px.bar(
    df.nlargest(10, 'total_clicks').assign(campaign_id=lambda d: d['campaign_id'].astype(str)),
    x='campaign_id',
    y='total_clicks',
    color='total_clicks',
    title='Топ-10 кампаний по количеству кликов',
    labels={'campaign_id': 'ID кампании', 'total_clicks': 'Клики'},
    color_continuous_scale=['#ffe082', '#ffca28']
)

fig_top10_clicks.update_layout(
    bargap=0.2,
    width=900,
    height=400,
    coloraxis_showscale=False,
    margin=dict(l=20, r=20, t=40, b=20),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font={'color': '#f5f5dc', 'size': 16},
    title={'font': {'color': '#fff8dc', 'size': 20}}
)

# Топ-5 кампаний по уникальным пользователям (таблица)
top5_users = df.nlargest(5, 'unique_users')[['campaign_id', 'unique_users']]

# Данные по кликам по дням
clicks_by_day = pd.DataFrame({
    'click_date': ['2024-08-31', '2024-09-01', '2024-09-02', '2024-09-03', '2024-09-04'],
    'total_clicks': [22, 449, 502, 1252, 1455],
    'percentage': [0.016199, 0.330616, 0.369642, 0.921897, 1.071373]
})

# Данные по кликам по месяцам
clicks_by_month = pd.DataFrame({
    'click_month': ['2024-08', '2024-09', '2024-10', '2024-11', '2024-12'],
    'total_clicks': [22, 36491, 33999, 35552, 29743],
    'percentage': [0.016199, 26.869749, 25.034792, 26.178327, 21.900933]
})

# Топ-10 кампаний по количеству кликов
fig_top10_clicks = px.bar(
    df.nlargest(10, 'total_clicks').assign(campaign_id=lambda d: d['campaign_id'].astype(str)),
    x='campaign_id',
    y='total_clicks',
    color='total_clicks',
    title='Топ-10 кампаний по количеству кликов',
    labels={'campaign_id': 'ID кампании', 'total_clicks': 'Клики'},
    color_continuous_scale=['#ffe082', '#ffca28']
)

fig_top10_clicks.update_layout(
    bargap=0.2,
    width=900,
    height=400,
    coloraxis_showscale=False,
    margin=dict(l=20, r=20, t=40, b=20),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font={'color': '#f5f5dc', 'size': 16},
    title={'font': {'color': '#fff8dc', 'size': 20}},
    xaxis=dict(showgrid=True, gridcolor='rgba(255, 255, 255, 0.3)', zeroline=False),  # Меняем цвет и прозрачность сетки по оси X
    yaxis=dict(showgrid=True, gridcolor='rgba(255, 255, 255, 0.3)', zeroline=False)   # Меняем цвет и прозрачность сетки по оси Y
)

# График кликов по дням
fig_clicks_per_day = px.line(
    clicks_by_day,
    x='click_date',
    y='total_clicks',
    title='Клики по дням',
    labels={'click_date': 'Дата', 'total_clicks': 'Клики'},
    line_shape='linear'
)
fig_clicks_per_day.update_traces(line=dict(color='yellow'))

fig_clicks_per_day.update_layout(
    width=900,
    height=400,
    margin=dict(l=20, r=20, t=40, b=20),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font={'color': '#f5f5dc', 'size': 16},
    title={'font': {'color': '#fff8dc', 'size': 20}},
    xaxis=dict(showgrid=True, gridcolor='rgba(255, 255, 255, 0.3)', zeroline=False),  # Сетка для оси X
    yaxis=dict(showgrid=True, gridcolor='rgba(255, 255, 255, 0.3)', zeroline=False)   # Сетка для оси Y
)

# График кликов по месяцам
fig_clicks_per_month = px.line(
    clicks_by_month,
    x='click_month',
    y='total_clicks',
    title='Клики по месяцам',
    labels={'click_month': 'Месяц', 'total_clicks': 'Клики'},
    line_shape='linear'
)

fig_clicks_per_month.update_traces(line=dict(color='yellow'))

fig_clicks_per_month.update_layout(
    width=900,
    height=400,
    margin=dict(l=20, r=20, t=40, b=20),
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font={'color': '#f5f5dc', 'size': 16},
    title={'font': {'color': '#fff8dc', 'size': 20}},
    xaxis=dict(showgrid=True, gridcolor='rgba(255, 255, 255, 0.3)', zeroline=False),  # Сетка для оси X
    yaxis=dict(showgrid=True, gridcolor='rgba(255, 255, 255, 0.3)', zeroline=False)   # Сетка для оси Y
)

# Вёрстка дашборда
app.layout = html.Div([
    html.Div([
        html.H1("\ud83d\udcca Анализ активности пользователей", className="main-header"),
    ], className="header-container"),

    html.Div([
        html.Div([
            html.Div([
                html.Span("\u23f1", className="time-icon"),
                html.H2("Анализ первых 4 часов активности", className="time-header-0")
            ], className="time-container"),

            html.Div([
                html.P("Ключевые показатели кликов и уникальных пользователей",
                       className="description-text")
            ], className="description-container")
        ], className="info-section"),

        # График
        html.Div([
            dcc.Graph(
                figure=fig_top10_clicks,
                config={'displayModeBar': True},
                className="animated-graph"
            )
        ], className="graph-container"),


        # Таблица
        html.Div([
            html.H3("Топ кампаний по уникальным пользователям", className="table-header"),

            dash_table.DataTable(
                data=top5_users.to_dict('records'),
                columns=[{"name": i, "id": i} for i in top5_users.columns],
                style_table={
                    'margin': '0 auto',
                    'width': '90%'
                },
                style_cell={
                    'textAlign': 'center',
                    'padding': '15px',
                    'border': '1px solid rgba(255,255,255,0.1)',
                    'color': '#fff8dc',
                    'backgroundColor': 'rgba(60, 60, 30, 0.8)',
                    'fontSize': '16px',
                    'fontFamily': 'Segoe UI, sans-serif'
                },
                style_header={
                    'backgroundColor': 'rgba(100, 100, 50, 0.9)',
                    'fontWeight': '600',
                    'border': '1px solid rgba(255,255,255,0.1)',
                    'color': '#fffad1',
                    'fontSize': '17px'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgba(80, 80, 40, 0.7)'
                    }
                ]
            )
        ], className="table-container"),

        html.Div([
            html.Span("\u23f1", className="time-icon"),
            html.H2("Динамика кликов по дням и месяцам", className="time-header-0")
        ], className="time-container"),

        # График кликов по дням
        html.Div([
            dcc.Graph(
                figure=fig_clicks_per_day,
                config={'displayModeBar': True},
                className="animated-graph"
            )
        ], className="graph-container"),

        # Таблица по дням
        html.Div([
            html.H3("Клики по дням", className="table-header"),

            dash_table.DataTable(
                data=clicks_by_day.to_dict('records'),
                columns=[{"name": i, "id": i} for i in clicks_by_day.columns],
                style_table={'margin': '0 auto', 'width': '90%'},
                style_cell={
                    'textAlign': 'center',
                    'padding': '15px',
                    'border': '1px solid rgba(255,255,255,0.1)',
                    'color': '#fff8dc',
                    'backgroundColor': 'rgba(60, 60, 30, 0.8)',
                    'fontSize': '16px',
                    'fontFamily': 'Segoe UI, sans-serif'
                },
                style_header={
                    'backgroundColor': 'rgba(100, 100, 50, 0.9)',
                    'fontWeight': '600',
                    'border': '1px solid rgba(255,255,255,0.1)',
                    'color': '#fffad1',
                    'fontSize': '17px'
                },
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgba(80, 80, 40, 0.7)'}
                ]
            )
        ], className="table-container"),

        # График кликов по месяцам
        html.Div([
            dcc.Graph(
                figure=fig_clicks_per_month,
                config={'displayModeBar': True},
                className="animated-graph"
            )
        ], className="graph-container"),

        # Таблица по месяцам
        html.Div([
            html.H3("Клики по месяцам", className="table-header"),

            dash_table.DataTable(
                data=clicks_by_month.to_dict('records'),
                columns=[{"name": i, "id": i} for i in clicks_by_month.columns],
                style_table={'margin': '0 auto', 'width': '90%'},
                style_cell={
                    'textAlign': 'center',
                    'padding': '15px',
                    'border': '1px solid rgba(255,255,255,0.1)',
                    'color': '#fff8dc',
                    'backgroundColor': 'rgba(60, 60, 30, 0.8)',
                    'fontSize': '16px',
                    'fontFamily': 'Segoe UI, sans-serif'
                },
                style_header={
                    'backgroundColor': 'rgba(100, 100, 50, 0.9)',
                    'fontWeight': '600',
                    'border': '1px solid rgba(255,255,255,0.1)',
                    'color': '#fffad1',
                    'fontSize': '17px'
                },
                style_data_conditional=[
                    {'if': {'row_index': 'odd'}, 'backgroundColor': 'rgba(80, 80, 40, 0.7)'}
                ]
            )
        ], className="table-container")
    ], className="dashboard-container")
])

if __name__ == '__main__':
    app.run(debug=True)
