import dash
from dash import dcc, html
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import numpy as np
import json
import base64
from io import BytesIO
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import seaborn as sns

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
daily_dynamics = pd.read_parquet(
    PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'campaign_dynamics_daily.parquet')
monthly_dynamics = pd.read_parquet(
    PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'campaign_dynamics_monthly.parquet')

# 5. Данные скорости реакции
response_stats = pd.read_parquet(
    PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'response_time_analysis_campaign_stats.parquet')

with open(
        PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'response_time_analysis_overall_stats.json') as f:
    overall_stats = json.load(f)

# Загружаем клики
clicks_df = pd.read_parquet(
    PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'clicks_processed.parquet')

# 1. Загрузка данных активности по часам
hour_activity = pd.read_parquet(
    PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'activity_by_timezone_by_hour.parquet')

# 2. Загрузка данных активности по регионам
region_activity = pd.read_parquet(
    PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'activity_by_timezone_by_region.parquet')

# Уникальные клиенты
unique_clients = clicks_df['uid'].nunique()
# или если нужно по member_id:
# unique_clients = clicks_df['member_id'].nunique()


# Инициализация Dash-приложения
app = dash.Dash(__name__, external_stylesheets=['styles.css'])
app.title = "Анализ активности кампаний"

# Настройки для всплывающих подсказок
tooltip_style = {
    'bgcolor': 'rgba(30, 30, 30, 0.9)',
    'font': {'color': 'white', 'family': 'Segoe UI, sans-serif'},
    'bordercolor': 'rgba(255, 202, 40, 0.5)'
}

# Словарь соответствия названий регионов и их координат
REGION_COORDINATES = {
    1: (44.6098, 40.1004),  # Адыгея
    2: (54.7351, 55.9587),  # Башкортостан
    3: (52.0495, 107.0847),  # Бурятия
    4: (50.7114, 86.8576),  # Алтай
    5: (42.9849, 47.5046),  # Дагестан
    6: (43.1151, 45.3397),  # Ингушетия
    7: (43.4846, 43.6071),  # Кабардино-Балкария
    8: (46.3080, 44.2557),  # Калмыкия
    9: (43.9200, 41.7831),  # Карачаево-Черкесия
    10: (61.7840, 34.3469),  # Карелия
    11: (61.6688, 50.8364),  # Коми
    12: (56.6344, 46.8654),  # Марий Эл
    13: (54.4412, 44.4666),  # Мордовия
    14: (66.9416, 129.6425),  # Якутия
    15: (43.0241, 44.6814),  # Северная Осетия
    16: (55.7963, 49.1084),  # Татарстан
    17: (51.7191, 94.4378),  # Тыва
    18: (57.0671, 53.0273),  # Удмуртия
    19: (53.7222, 91.4425),  # Хакасия
    20: (43.3178, 45.6949),  # Чечня
    21: (56.1439, 47.2489),  # Чувашия
    22: (52.6932, 82.6935),  # Алтайский край
    23: (45.0355, 38.9753),  # Краснодарский край
    24: (56.0184, 92.8672),  # Красноярский край
    25: (43.1736, 131.8955),  # Приморский край
    26: (45.0448, 41.9692),  # Ставропольский край
    27: (48.4802, 135.0719),  # Хабаровский край
    28: (50.2907, 127.5272),  # Амурская область
    29: (64.5393, 40.5169),  # Архангельская область
    30: (46.3479, 48.0336),  # Астраханская область
    31: (50.5956, 36.5873),  # Белгородская область
    32: (53.2436, 34.3634),  # Брянская область
    33: (56.1290, 40.4070),  # Владимирская область
    34: (48.7071, 44.5169),  # Волгоградская область
    35: (59.2181, 39.8886),  # Вологодская область
    36: (51.6720, 39.1843),  # Воронежская область
    37: (57.0004, 40.9739),  # Ивановская область
    38: (52.2864, 104.2807),  # Иркутская область
    39: (54.7104, 20.4522),  # Калининградская область
    40: (54.5138, 36.2612),  # Калужская область
    41: (53.0241, 158.6439),  # Камчатский край
    42: (55.3545, 86.0883),  # Кемеровская область
    43: (58.6036, 49.6680),  # Кировская область
    44: (58.5500, 43.6833),  # Костромская область
    45: (55.4408, 65.3411),  # Курганская область
    46: (51.7304, 36.1936),  # Курская область
    47: (59.9391, 30.3159),  # Ленинградская область
    48: (52.6088, 39.5992),  # Липецкая область
    49: (59.5612, 150.7989),  # Магаданская область
    50: (55.5043, 36.2712),  # Московская область
    51: (68.9585, 33.0827),  # Мурманская область
    52: (56.3269, 44.0065),  # Нижегородская область
    53: (58.5215, 31.2755),  # Новгородская область
    54: (55.0084, 82.9357),  # Новосибирская область
    55: (54.9893, 73.3682),  # Омская область
    56: (51.7686, 55.0974),  # Оренбургская область
    57: (52.9674, 36.0696),  # Орловская область
    58: (53.1959, 45.0183),  # Пензенская область
    59: (58.0105, 56.2502),  # Пермский край
    60: (57.8194, 28.3324),  # Псковская область
    61: (47.2224, 39.7187),  # Ростовская область
    62: (54.6296, 39.7419),  # Рязанская область
    63: (53.1959, 50.1002),  # Самарская область
    64: (51.5336, 46.0343),  # Саратовская область
    65: (46.9591, 142.7380),  # Сахалинская область
    66: (56.8389, 60.6057),  # Свердловская область
    67: (54.7826, 32.0453),  # Смоленская область
    68: (52.7212, 41.4523),  # Тамбовская область
    69: (56.8587, 35.9176),  # Тверская область
    70: (56.4846, 84.9476),  # Томская область
    71: (54.1931, 37.6173),  # Тульская область
    72: (57.1530, 65.5343),  # Тюменская область
    73: (54.3142, 48.4031),  # Ульяновская область
    74: (55.1644, 61.4368),  # Челябинская область
    76: (57.6261, 39.8845),  # Ярославская область
    77: (55.7558, 37.6173),  # Москва
    78: (59.9343, 30.3351),  # Санкт-Петербург
    79: (48.4808, 132.5067),  # Еврейская АО
    82: (45.0469, 34.1008),  # Крым
    83: (67.6381, 53.0069),  # Ненецкий АО
    86: (61.0032, 69.0189),  # Ханты-Мансийский АО
    87: (66.3167, 171.0167),  # Чукотский АО
    89: (66.5299, 66.6136),  # Ямало-Ненецкий АО
    92: (44.6167, 33.5254),  # Севастополь
    101: (52.0336, 113.5014)  # Забайкальский край
}

REGION_NAMES = {
    1: "Республика Адыгея",
    2: "Республика Башкортостан",
    3: "Республика Бурятия",
    4: "Республика Алтай",
    5: "Республика Дагестан",
    6: "Республика Ингушетия",
    7: "Кабардино-Балкарская Республика",
    8: "Республика Калмыкия",
    9: "Карачаево-Черкесская Республика",
    10: "Республика Карелия",
    11: "Республика Коми",
    12: "Республика Марий Эл",
    13: "Республика Мордовия",
    14: "Республика Саха (Якутия)",
    15: "Республика Северная Осетия - Алания",
    16: "Республика Татарстан",
    17: "Республика Тыва",
    18: "Удмуртская Республика",
    19: "Республика Хакасия",
    20: "Чеченская Республика",
    21: "Чувашская Республика",
    22: "Алтайский край",
    23: "Краснодарский край",
    24: "Красноярский край",
    25: "Приморский край",
    26: "Ставропольский край",
    27: "Хабаровский край",
    28: "Амурская область",
    29: "Архангельская область",
    30: "Астраханская область",
    31: "Белгородская область",
    32: "Брянская область",
    33: "Владимирская область",
    34: "Волгоградская область",
    35: "Вологодская область",
    36: "Воронежская область",
    37: "Ивановская область",
    38: "Иркутская область",
    39: "Калининградская область",
    40: "Калужская область",
    41: "Камчатский край",
    42: "Кемеровская область",
    43: "Кировская область",
    44: "Костромская область",
    45: "Курганская область",
    46: "Курская область",
    47: "Ленинградская область",
    48: "Липецкая область",
    49: "Магаданская область",
    50: "Московская область",
    51: "Мурманская область",
    52: "Нижегородская область",
    53: "Новгородская область",
    54: "Новосибирская область",
    55: "Омская область",
    56: "Оренбургская область",
    57: "Орловская область",
    58: "Пензенская область",
    59: "Пермский край",
    60: "Псковская область",
    61: "Ростовская область",
    62: "Рязанская область",
    63: "Самарская область",
    64: "Саратовская область",
    65: "Сахалинская область",
    66: "Свердловская область",
    67: "Смоленская область",
    68: "Тамбовская область",
    69: "Тверская область",
    70: "Томская область",
    71: "Тульская область",
    72: "Тюменская область",
    73: "Ульяновская область",
    74: "Челябинская область",
    75: "Забайкальский край",
    76: "Ярославская область",
    77: "Москва",
    78: "Санкт-Петербург",
    79: "Еврейская автономная область",
    82: "Республика Крым",
    83: "Ненецкий автономный округ",
    86: "Ханты-Мансийский автономный округ - Югра",
    87: "Чукотский автономный округ",
    89: "Ямало-Ненецкий автономный округ",
    92: "Севастополь",
    101: "Забайкальский край"
}

def load_and_prepare_geo_data():
    """Загрузка и подготовка данных для тепловой карты по регионам России"""

    clicks_file = PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'clicks_processed.parquet'
    clicks = pd.read_parquet(clicks_file)

    # Группируем по региону и считаем уникальных клиентов по uid
    region_stats = clicks.groupby('region').agg(
        clients_count=('uid', 'nunique')
    ).reset_index()

    # Добавляем координаты (широта и долгота) из словаря
    region_stats['latitude'] = region_stats['region'].map(lambda x: REGION_COORDINATES.get(x, (None, None))[0])
    region_stats['longitude'] = region_stats['region'].map(lambda x: REGION_COORDINATES.get(x, (None, None))[1])

    # Удаляем строки с отсутствующими координатами
    region_stats = region_stats.dropna(subset=['latitude', 'longitude'])

    # Логарифмируем количество клиентов для лучшего отображения цветовой шкалы (не используется в графике напрямую)
    region_stats['log_clients'] = np.log1p(region_stats['clients_count'])

    return region_stats


def create_plotly_heatmap():
    """Создание интерактивной тепловой карты с Plotly"""

    data = load_and_prepare_geo_data()

    # Человеко-понятные названия
    data['Название региона'] = data['region'].map(lambda x: REGION_NAMES.get(x, f"Регион {x}"))
    data['Количество клиентов'] = data['clients_count']
    data['Широта'] = data['latitude']
    data['Долгота'] = data['longitude']
    data['zcolor'] = np.log1p(data['Количество клиентов'])

    fig = px.density_mapbox(
        data,
        lat='Широта',
        lon='Долгота',
        z='Количество клиентов',
        radius=25,
        center=dict(lat=62, lon=94),
        zoom=3,
        mapbox_style="carto-positron",  # или "stamen-terrain", если предпочитаешь
        hover_name='Название региона',
        hover_data={
            'Количество клиентов': ':,',
            'region': False,
            'Широта': False,
            'Долгота': False
        },
        color_continuous_scale=[
            #[0.000, '#FFF59D'],  светло-жёлтый (мягкий, но яркий)
            [0.0000, '#FFEB3B'],  # чистый насыщенный жёлтый
            [0.010, '#FFC107'],  # тёплый янтарный
            [0.050, '#FF9800'],  # тёплый ярко-оранжевый
            [0.100, '#F4511E'],  # огненно-оранжевый
            [0.350, '#E53935'],  # насыщенный красно-оранжевый
            [1.000, '#B71C1C']  # глубокий красный (почти винный)
        ],

    range_color=[data['Количество клиентов'].min(), data['Количество клиентов'].max()],

        opacity=0.8
    )

    # Настройка ховеров
    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>Клиентов: %{z:,}<extra></extra>"
    )

    # Оформление легенды
    fig.update_layout(
        title='🗺️ Распределение клиентов по регионам России',
        margin=dict(l=0, r=0, t=50, b=0),
        height=600,
        mapbox=dict(
            zoom=3,
            center=dict(lat=62, lon=94)
        ),
        coloraxis_colorbar=dict(
            title='Активность клиентов',
            tickvals=[
                data['Количество клиентов'].min(),
                data['Количество клиентов'].median(),
                data['Количество клиентов'].max()
            ],
            ticktext=['Низкая', 'Средняя', 'Высокая']
        )
    )

    return fig


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

# Рассчитываем общее количество кликов
total_clicks_days = df_days_sampled['total_clicks'].sum()

# Добавляем столбец с процентом от общего количества
df_days_sampled['percentage'] = (df_days_sampled['total_clicks'] / total_clicks_days) * 100

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
    hovertemplate="<b>Дата:</b> %{x}<br><b>Клики:</b> %{y:,}<br><b>Доля от общего числа:</b> %{customdata:.2f}%<extra></extra>",
    customdata=df_days_sampled['percentage']
)

# 3. Клики по месяцам (все данные)
# Рассчитываем общее количество кликов по месяцам
total_clicks_months = df_months['total_clicks'].sum()

# Добавляем столбец с процентом от общего количества
df_months['percentage'] = (df_months['total_clicks'] / total_clicks_months) * 100

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
    hovertemplate="<b>Месяц:</b> %{x}<br><b>Клики:</b> %{y:,}<br><b>Доля от общего числа:</b> %{customdata:.2f}%<extra></extra>",
    customdata=df_months['percentage']
)

# 4. Динамика создания кампаний по дням недели
weekdays_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
russian_weekdays = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']

daily_dynamics['activity_level'] = pd.cut(
    daily_dynamics['campaigns_count'],
    bins=[0, 20, 50, 100, 500, 1000, 2000, 3000, 5000, 10000, np.inf],
    labels=['0-20', '20-50', '50-100', '100-500', '500-1000', '1000-2000', '2000-3000', '3000-5000', '5000-10000',
            '10000+']
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

# 6. Тепловая карта создания кампаний (по дням недели и неделям года)
heatmap_data = daily_dynamics.pivot_table(
    index='week_of_year',
    columns='day_of_week',
    values='campaigns_count',
    aggfunc='mean'
).sort_index(ascending=False)

for day in weekdays_order:
    if day not in heatmap_data.columns:
        heatmap_data[day] = 0

heatmap_data = heatmap_data[weekdays_order]

fig_heatmap_week = go.Figure(data=go.Heatmap(
    z=heatmap_data.values,
    x=[russian_weekdays[weekdays_order.index(d)] for d in heatmap_data.columns],
    y=heatmap_data.index.astype(str),
    colorscale='YlGnBu',
    hoverongaps=False,
    hovertemplate="<b>Неделя:</b> %{y}<br><b>День:</b> %{x}<br><b>Кампаний:</b> %{z:.1f}<extra></extra>",
    zmin=0
))

fig_heatmap_week.update_layout(
    title='Создано кампаний по дням недели и неделям года',
    xaxis_title='День недели',
    yaxis_title='Неделя года',
    height=400,
    width=458,
    yaxis=dict(
        autorange=True,
        type='category',
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

# 7. График скорости реакции клиентов
fig_response_time = px.histogram(
    response_stats,
    x='avg_response_time_hours',
    nbins=50,
    title='Распределение времени реакции клиентов',
    labels={'avg_response_time_hours': 'Среднее время реакции (часы)', 'count': 'Количество кампаний'},
    color_discrete_sequence=['#ff7043']
)
fig_response_time.update_traces(
    hovertemplate="<b>Время реакции:</b> %{x:.1f} ч<br><b>Кампаний:</b> %{y}<extra></extra>"
)


# Функция для создания таблицы статистики
# Функция для создания таблицы статистики
def create_stats_table(stats):
    # Конвертируем максимальное время из секунд в часы
    max_response_hours = stats['max_response_time_seconds'] / 3600

    return html.Div([
        html.Table(
            [
                html.Thead(
                    html.Tr([
                        html.Th("Метрика", style={
                            'text-align': 'left',
                            'padding': '12px 15px',
                            'background': 'rgba(255, 202, 40, 0.2)',
                            'border-bottom': '1px solid rgba(255, 202, 40, 0.3)'
                        }),
                        html.Th("Значение", style={
                            'text-align': 'right',
                            'padding': '12px 15px',
                            'background': 'rgba(255, 202, 40, 0.2)',
                            'border-bottom': '1px solid rgba(255, 202, 40, 0.3)'
                        })
                    ])
                ),
                html.Tbody([
                    html.Tr([
                        html.Td("Общее количество кликов", style={
                            'text-align': 'left',
                            'padding': '12px 15px',
                            'border-bottom': '1px solid rgba(255, 255, 255, 0.1)'
                        }),
                        html.Td(f"{stats['total_clicks']:,}", style={
                            'text-align': 'right',
                            'padding': '12px 15px',
                            'border-bottom': '1px solid rgba(255, 255, 255, 0.1)',
                            'font-weight': 'bold',
                            'color': '#FFCA28'
                        })
                    ]),
                    html.Tr([
                        html.Td("Среднее время реакции", style={
                            'text-align': 'left',
                            'padding': '12px 15px',
                            'border-bottom': '1px solid rgba(255, 255, 255, 0.1)'
                        }),
                        html.Td(f"{stats['avg_response_time_hours']:.2f} часов", style={
                            'text-align': 'right',
                            'padding': '12px 15px',
                            'border-bottom': '1px solid rgba(255, 255, 255, 0.1)',
                            'font-weight': 'bold',
                            'color': '#FFCA28'
                        })
                    ]),
                    html.Tr([
                        html.Td("Минимальное время реакции", style={
                            'text-align': 'left',
                            'padding': '12px 15px',
                            'border-bottom': '1px solid rgba(255, 255, 255, 0.1)'
                        }),
                        html.Td(f"{stats['min_response_time_seconds']:.0f} секунд", style={
                            'text-align': 'right',
                            'padding': '12px 15px',
                            'border-bottom': '1px solid rgba(255, 255, 255, 0.1)',
                            'font-weight': 'bold',
                            'color': '#FFCA28'
                        })
                    ]),
                    html.Tr([
                        html.Td("Максимальное время реакции", style={
                            'text-align': 'left',
                            'padding': '12px 15px'
                        }),
                        html.Td(f"{max_response_hours:.2f} часов", style={
                            'text-align': 'right',
                            'padding': '12px 15px',
                            'font-weight': 'bold',
                            'color': '#FFCA28'
                        })
                    ])
                ])
            ],
            style={
                'width': '100%',
                'border-collapse': 'collapse',
                'font-family': 'Segoe UI, sans-serif',
                'font-size': '14px',
                'background': 'rgba(30, 30, 30, 0.5)',
                'border-radius': '8px',
                'overflow': 'hidden',
                'box-shadow': '0 4px 6px rgba(0, 0, 0, 0.1)'
            }
        )
    ], style={
        'padding': '10px',
        'background': 'rgba(30, 30, 30, 0.3)',
        'border-radius': '10px',
        'border': '1px solid rgba(255, 202, 40, 0.2)'
    })


# Создаем географическую тепловую карту
geo_heatmap = create_plotly_heatmap()


def create_geo_pie_chart(top_n=5):
    """Создание круговой диаграммы географического распределения клиентов"""
    # Загружаем данные кликов
    clicks_file = PROJECT_ROOT / 'Хакатон' / 'Stream_telecom' / 'processed_data' / 'clicks_processed.parquet'
    clicks = pd.read_parquet(clicks_file)

    # Группируем по регионам и считаем уникальных клиентов
    region_stats = clicks.groupby('region', observed=False).agg(
        clients_count=('uid', 'nunique')
    ).reset_index()

    # Добавляем названия регионов (с заменой null на "Неопознанный регион")
    region_stats['region_name'] = region_stats['region'].map(
        lambda x: REGION_NAMES.get(x, "Неопознанный регион")
    )

    # Сортируем по убыванию активности (исключая неопознанные регионы)
    region_stats = region_stats.sort_values('clients_count', ascending=False)

    # Разделяем на опознанные и неопознанные регионы
    known_regions = region_stats[region_stats['region'] != 0]
    unknown_regions = region_stats[region_stats['region'] == 0]

    # Выбираем топ-N опознанных регионов
    top_regions = known_regions.head(top_n)

    # Суммируем остальные опознанные регионы в "Другие"
    other_known_count = known_regions['clients_count'].iloc[top_n:].sum()
    other_known_row = pd.DataFrame({'region': [-1],
                                    'region_name': ['Другие регионы'],
                                    'clients_count': [other_known_count]})

    # Объединяем топ регионы, "Другие" и неопознанные регионы
    pie_data = pd.concat([top_regions, other_known_row, unknown_regions], ignore_index=True)

    # Рассчитываем проценты для легенды
    total = pie_data['clients_count'].sum()
    pie_data['percentage'] = pie_data['clients_count'] / total * 100

    # Создаем диаграмму
    fig = px.pie(
        pie_data,
        values='clients_count',
        names='region_name',
        title=f'<b>Географические зоны с наибольшей активностью</b>',
        color_discrete_sequence=px.colors.qualitative.Pastel + ['#cccccc', '#ff9999'],
        hole=0.3,
        labels={'clients_count': 'Количество клиентов'}
    )

    # Настраиваем внешний вид
    fig.update_traces(
        textposition='inside',
        textinfo='percent',
        textfont=dict(color='white', size=12),
        hovertemplate="<b>%{label}</b><br>Клиентов: %{value:,}<br>Доля: %{percent}<extra></extra>",
        marker=dict(line=dict(color='#444444', width=1)),
        pull=[0.1 if i < top_n else 0 for i in range(len(pie_data))]  # Выделяем топ регионы
    )

    # Настройки темного фона и легенды
    fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        title_font=dict(color='white', size=16),
        uniformtext_minsize=12,
        uniformtext_mode='hide',
        legend=dict(
            title=dict(text='<b>Регионы</b>', font=dict(color='white')),
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="right",
            x=1.3,
            font=dict(size=11, color='white'),
            bgcolor='rgba(30,30,30,0.7)',
            bordercolor='rgba(255,255,255,0.2)',
            borderwidth=1
        ),
        height=350,
        margin=dict(l=20, r=150, t=60, b=20),  # Увеличиваем правый отступ для легенды
        showlegend=True,
        hoverlabel=dict(
            bgcolor='rgba(30,30,30,0.9)',
            font=dict(color='white')
        )
    )

    # Добавляем проценты в названия легенды
    for i, trace in enumerate(fig.data):
        trace.legendgroup = trace.name
        trace.name = f"{trace.name} ({pie_data.iloc[i]['percentage']:.1f}%)"

    return fig

# Создаем круговую диаграмму
geo_pie_chart = create_geo_pie_chart(top_n=5)

# 1. График активности по часам
fig_hour_activity = px.bar(
    hour_activity,
    x='hour',
    y='percentage',
    title='Активность клиентов по часам (UTC)',
    labels={'hour': 'Час дня', 'percentage': 'Процент кликов'},
    color='percentage',
    color_continuous_scale=['#1e88e5', '#0d47a1']
)
fig_hour_activity.update_traces(
    hovertemplate="<b>Час:</b> %{x}:00<br><b>Доля кликов:</b> %{y:.1f}%<extra></extra>"
)

# Исправленная часть кода для создания графика активности по регионам

# 2. График активности по регионам (исключаем регион 0)
# Фильтрация (исключаем регион 0) и сортировка
region_activity_filtered = region_activity[region_activity['region'] != 0].sort_values('total_clicks', ascending=False).head(7)

# Добавляем названия регионов
region_activity_filtered['region_name'] = region_activity_filtered['region'].map(REGION_NAMES)

# Создаем график с номерами регионов на оси X и названиями в легенде
fig_region_activity = px.bar(
    region_activity_filtered,
    x='region',  # Используем номер региона на оси X
    y='total_clicks',
    title='Топ-7 регионов по активности (без неопознанных)',
    labels={'region_name': 'Код региона', 'total_clicks': 'Количество кликов'},
    color='region_name',  # Используем названия регионов для цвета
    color_discrete_sequence=px.colors.qualitative.Pastel
)

# Настраиваем отображение
fig_region_activity.update_layout(
    xaxis=dict(
        type='category',  # Чтобы номера регионов отображались как категории
        tickmode='array',
        tickvals=region_activity_filtered['region'],
        ticktext=region_activity_filtered['region']
    ),
    legend=dict(
        title='Регионы',
        orientation='v',
        yanchor='top',
        y=1,
        xanchor='right',
        x=1.2
    )
)

fig_region_activity.update_traces(
    hovertemplate="<b>Кликов:</b> %{y:,}<extra></extra>"
)

# Добавляем новый график активности по часам (красно-голубой)
fig_hour_activity_redblue = px.bar(
    hour_activity,
    x='hour',
    y='percentage',
    title='Оптимальное время для рассылок',
    labels={'hour': 'Час дня', 'percentage': 'Процент кликов'},
    color='percentage',
    color_continuous_scale=['#1e88e5', '#e53935']  # Голубой -> Красный
)
fig_hour_activity_redblue.update_traces(
    hovertemplate="<b>Час:</b> %{x}:00<br><b>Доля кликов:</b> %{y:.1f}%<extra></extra>"
)

# Обновляем настройки для нового графика
fig_hour_activity_redblue.update_layout(
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font={'color': '#f5f5dc', 'size': 12},
    title={'font': {'color': '#fff8dc', 'size': 16}, 'x': 0.5},
    margin=dict(l=40, r=40, t=60, b=40),
    height=350,
    xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)', linecolor='rgba(255,255,255,0.3)'),
    yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)', linecolor='rgba(255,255,255,0.3)'),
    hoverlabel=tooltip_style,
    coloraxis_colorbar=dict(
        title='Доля кликов',
        tickvals=[hour_activity['percentage'].min(), hour_activity['percentage'].max()],
        ticktext=['Низкая', 'Высокая'],
        yanchor="top",
        y=1,
        xanchor="left",
        x=1.02
    )
)

# Общие настройки для всех графиков
for fig in [fig_hour_activity, fig_region_activity, fig_top_clicks, fig_daily, fig_monthly,
            fig_weekdays, fig_months, fig_heatmap_week, fig_response_time, geo_heatmap]:
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

app.layout = html.Div([
    html.Div([
        html.H1("📊 Анализ активности кампаний", className="main-header"),
    ], className="header-container"),

    # 1 строка: клики по дням, месяцам и топ кампаний
    html.Div([
        html.Div([dcc.Graph(figure=fig_daily, config={'displayModeBar': False})],
                className="graph-cell", style={'width': '33%'}),
        html.Div([dcc.Graph(figure=fig_monthly, config={'displayModeBar': False})],
                className="graph-cell", style={'width': '33%'}),
        html.Div([dcc.Graph(figure=fig_top_clicks, config={'displayModeBar': False})],
                className="graph-cell", style={'width': '33%'}),
    ], className="graph-row"),

    # 2 строка: созданные кампании
    html.Div([
        html.Div([dcc.Graph(figure=fig_months, config={'displayModeBar': False})],
                className="graph-cell", style={'width': '33%'}),
        html.Div([dcc.Graph(figure=fig_weekdays, config={'displayModeBar': False})],
                className="graph-cell", style={'width': '33%'}),
        html.Div([dcc.Graph(figure=fig_heatmap_week, config={'displayModeBar': False})],
                className="graph-cell", style={'width': '33%'}),
    ], className="graph-row"),

    # 3 строка: активность и время реакции
    html.Div([
        html.Div([dcc.Graph(figure=fig_hour_activity, config={'displayModeBar': False})],
                 className="graph-cell", style={'width': '33%'}),
        html.Div([
            html.H3("Общая статистика времени реакции", style={
                'textAlign': 'center',
                'color': '#fff8dc',
                'marginBottom': '10px',
                'fontSize': '16px'
            }),
            create_stats_table(overall_stats)
        ], className="graph-cell", style={
            'width': '33%',
            'padding': '10px',
            'background': 'rgba(255,255,255,0.05)',
            'borderRadius': '8px'
        }),
        html.Div([dcc.Graph(figure=fig_response_time, config={'displayModeBar': False})],
                 className="graph-cell", style={'width': '34%'})
    ], className="graph-row"),

    # 4 строка: оптимальное время и распределение по регионам
    html.Div([
        html.Div([dcc.Graph(figure=fig_hour_activity_redblue, config={'displayModeBar': False})],
                className="graph-cell", style={'width': '33%'}),
        html.Div([dcc.Graph(figure=geo_heatmap, config={'displayModeBar': True})],
                className="graph-cell", style={'width': '67%'})
    ], className="graph-row"),

    # 5 строка: региональная аналитика
    html.Div([
        html.Div([dcc.Graph(figure=fig_region_activity, config={'displayModeBar': False})],
                className="graph-cell", style={'width': '50%'}),
        html.Div([dcc.Graph(figure=geo_pie_chart, config={'displayModeBar': False})],
                className="graph-cell", style={'width': '50%'}),
    ], className="graph-row")
], className="dashboard-container")

if __name__ == '__main__':
    app.run(debug=True)
