import dash
from dash import dcc, html
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import numpy as np

# Загружаем данные из разных файлов
PROJECT_ROOT = Path(__file__).parent.parent.parent

# 1. Данные за первые 4 часа
df_4hours = pd.read_parquet(
    PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'processed_data_first_4_hours.parquet')

# 2. Данные по дням
df_days = pd.read_parquet(
    PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'processed_data_per_day.parquet')

# 3. Данные по месяцам
df_months = pd.read_parquet(
    PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'processed_data_per_month.parquet')

# 4. Данные динамики кампаний
daily_dynamics = (pd.read_parquet
    (PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' /'processed_data' / 'campaign_dynamics_daily.parquet'))
monthly_dynamics = (pd.read_parquet
    (PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' /'processed_data' / 'campaign_dynamics_monthly.parquet'))

# Инициализация Dash-приложения
app = dash.Dash(__name__, external_stylesheets=['styles.css'])
app.title = "Анализ активности кампаний"

# Настройки для всплывающих подсказок
tooltip_style = {
    'bgcolor': 'rgba(30, 30, 30, 0.9)',
    'font': {'color': 'white', 'family': 'Segoe UI, sans-serif'},
    'bordercolor': 'rgba(255, 202, 40, 0.5)'
}

# 1. Топ-10 кампаний по кликам за первые 4 часа
fig_top_clicks = px.bar(
    df_4hours.nlargest(10, 'total_clicks').assign(campaign_id=lambda d: d['campaign_id'].astype(str)),
    x='campaign_id',
    y='total_clicks',
    title='Топ-10 кампаний по кликам (первые 4 часа)',
    color='total_clicks',
    color_continuous_scale=['#ffe082', '#ffca28'],
    labels={'campaign_id': 'ID кампании', 'total_clicks': 'Клики'}
)
fig_top_clicks.update_traces(
    hovertemplate="<b>Кампания:</b> %{x}<br><b>Клики:</b> %{y:,}<extra></extra>"
)

# 2. Клики по дням (с шагом 5 дней для лучшей читаемости)
df_days_sampled = df_days.sort_values('click_date').iloc[::1, :]

fig_daily = px.area(
    df_days_sampled,
    x='click_date',
    y='total_clicks',
    title='Клики по дням',
    line_shape='linear',
    labels={'click_date': 'Дата', 'total_clicks': 'Клики'}
)
fig_daily.update_traces(
    line=dict(color='#ffca28', width=2),
    fillcolor='rgba(255, 202, 40, 0.2)',
    hovertemplate="<b>Дата:</b> %{x}<br><b>Клики:</b> %{y:,}<extra></extra>"
)

# 3. Клики по месяцам (все данные)
fig_monthly = px.area(
    df_months,
    x='click_month',
    y='total_clicks',
    title='Клики по месяцам',
    line_shape='linear',
    labels={'click_month': 'Месяц', 'total_clicks': 'Клики'}
)
fig_monthly.update_traces(
    line=dict(color='#4caf50', width=2),
    fillcolor='rgba(76, 175, 80, 0.2)',
    hovertemplate="<b>Месяц:</b> %{x}<br><b>Клики:</b> %{y:,}<extra></extra>"
)

# 4. Динамика создания кампаний по дням недели
weekdays_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
russian_weekdays = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']

# Добавляем категории по уровню активности
daily_dynamics['activity_level'] = pd.cut(
    daily_dynamics['campaigns_count'],
    bins=[0, 20, 50, 100, 500, 1000, 2000, 3000, 5000, 10000, np.inf],
    labels=['0-20', '20-50', '50-100', '100-500', '500-1000', '1000-2000', '2000-3000', '3000-5000', '5000-10000', '10000+']
)

fig_weekdays = px.bar(
    daily_dynamics.groupby(['day_of_week', 'activity_level']).size().reset_index(name='count'),
    x='day_of_week',
    y='count',
    color='activity_level',
    category_orders={'day_of_week': weekdays_order},
    title='Количество созданных компаний по дням недели',
    labels={'day_of_week': 'День недели', 'count': 'Количество дней'}
)

# 5. Динамика создания кампаний по месяцам
months_order = ['January', 'February', 'March', 'April', 'May', 'June',
                'July', 'August', 'September', 'October', 'November', 'December']
russian_months = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн',
                  'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']

monthly_sum = monthly_dynamics.groupby('month')['campaigns_count'].sum().reset_index()

fig_months = px.bar(
    monthly_sum,
    x='month',
    y='campaigns_count',
    title='Количество созданных кампаний по месяцам',
    labels={'month': 'Месяц', 'campaigns_count': 'Количество кампаний'},
    color='campaigns_count',
    color_continuous_scale='Blues'
)
fig_months.update_layout(
    xaxis=dict(
        categoryorder='array',
        categoryarray=months_order,
        tickvals=months_order,
        ticktext=russian_months
    )
)

# 6. Тепловая карта создания кампаний
# 6. Тепловая карта создания кампаний (исправленная версия)
heatmap_data = daily_dynamics.pivot_table(
    index='week_of_year',
    columns='day_of_week',
    values='campaigns_count',
    aggfunc='mean'
).sort_index(ascending=False)  # Сортируем недели в обратном порядке для правильного отображения

# Убедимся, что все дни недели присутствуют
for day in weekdays_order:
    if day not in heatmap_data.columns:
        heatmap_data[day] = 0

# Переупорядочиваем столбцы по дням недели
heatmap_data = heatmap_data[weekdays_order]

fig_heatmap = go.Figure(data=go.Heatmap(
    z=heatmap_data.values,
    x=[russian_weekdays[weekdays_order.index(d)] for d in heatmap_data.columns],  # Русские сокращения дней
    y=heatmap_data.index.astype(str),  # Недели как строки
    colorscale='YlGnBu',
    hoverongaps=False,
    hovertemplate="<b>Неделя:</b> %{y}<br><b>День:</b> %{x}<br><b>Кампаний:</b> %{z:.1f}<extra></extra>",
    zmin=0  # Минимальное значение для цветовой шкалы
))

fig_heatmap.update_layout(
    title='Создано кампаний по дням недели и неделям года',
    xaxis_title='День недели',
    yaxis_title='Неделя года',
    height=400,  # Уменьшенная высота
    width=458,
    yaxis=dict(
        autorange=True,
        type='category',  # Обрабатываем недели как категории
        tickmode='array',
        tickvals=heatmap_data.index.astype(str),
        ticktext=heatmap_data.index.astype(str)
    ),
    xaxis=dict(
        tickmode='array',
        tickvals=[russian_weekdays[weekdays_order.index(d)] for d in weekdays_order],
        ticktext=russian_weekdays
    )
)

# Общие настройки графиков
for fig in [fig_top_clicks, fig_daily, fig_monthly, fig_weekdays, fig_months, fig_heatmap]:
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font={'color': '#f5f5dc', 'size': 12},
        title={'font': {'color': '#fff8dc', 'size': 16}, 'x': 0.5},
        margin=dict(l=40, r=40, t=60, b=40),
        height=350,
        xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)', linecolor='rgba(255,255,255,0.3)'),
        yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)', linecolor='rgba(255,255,255,0.3)'),
        hoverlabel=tooltip_style,
        coloraxis_showscale=False
    )

# Вёрстка дашборда
app.layout = html.Div([
    html.Div([
        html.H1("📊 Анализ активности кампаний", className="main-header"),
    ], className="header-container"),

    html.Div([
        html.Div([
            html.Div([dcc.Graph(figure=fig_top_clicks, config={'displayModeBar': False})], className="graph-cell"),
            html.Div([dcc.Graph(figure=fig_daily, config={'displayModeBar': False})], className="graph-cell"),
            html.Div([dcc.Graph(figure=fig_monthly, config={'displayModeBar': False})], className="graph-cell")
        ], className="graph-row"),

        html.Div([
            html.Div([dcc.Graph(figure=fig_weekdays, config={'displayModeBar': False})], className="graph-cell"),
            html.Div([dcc.Graph(figure=fig_months, config={'displayModeBar': False})], className="graph-cell"),
            html.Div([dcc.Graph(figure=fig_heatmap, config={'displayModeBar': False})], className="graph-cell-full")
        ], className="graph-row"),
    ], className="dashboard-container")
])

if __name__ == '__main__':
    app.run(debug=True)