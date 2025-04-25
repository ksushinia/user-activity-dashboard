# geographic_heatmap_final_fixed.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Настройки
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Конфигурация
PROJECT_ROOT = Path(__file__).parent.parent.parent
CLICKS_FILE = PROJECT_ROOT / 'processed_data' / 'clicks_processed.parquet'
REGIONS_FILE = PROJECT_ROOT / 'processed_data' / 'regions_processed.parquet'
PLOTS_DIR = PROJECT_ROOT / 'plots' / 'geographic'
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# Словарь соответствия названий регионов и их координат
REGION_COORDINATES = {
    1: (44.6098, 40.1004),   # Адыгея
    2: (54.7351, 55.9587),   # Башкортостан
    3: (52.0495, 107.0847),  # Бурятия
    4: (50.7114, 86.8576),   # Алтай
    5: (42.9849, 47.5046),   # Дагестан
    6: (43.1151, 45.3397),   # Ингушетия
    7: (43.4846, 43.6071),   # Кабардино-Балкария
    8: (46.3080, 44.2557),   # Калмыкия
    9: (43.9200, 41.7831),   # Карачаево-Черкесия
    10: (61.7840, 34.3469),  # Карелия
    11: (61.6688, 50.8364),  # Коми
    12: (56.6344, 46.8654),  # Марий Эл
    13: (54.4412, 44.4666),  # Мордовия
    14: (66.9416, 129.6425), # Якутия
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
    25: (43.1736, 131.8955), # Приморский край
    26: (45.0448, 41.9692),  # Ставропольский край
    27: (48.4802, 135.0719), # Хабаровский край
    28: (50.2907, 127.5272), # Амурская область
    29: (64.5393, 40.5169),  # Архангельская область
    30: (46.3479, 48.0336),  # Астраханская область
    31: (50.5956, 36.5873),  # Белгородская область
    32: (53.2436, 34.3634),  # Брянская область
    33: (56.1290, 40.4070),  # Владимирская область
    34: (48.7071, 44.5169),  # Волгоградская область
    35: (59.2181, 39.8886),  # Вологодская область
    36: (51.6720, 39.1843),  # Воронежская область
    37: (57.0004, 40.9739),  # Ивановская область
    38: (52.2864, 104.2807), # Иркутская область
    39: (54.7104, 20.4522),  # Калининградская область
    40: (54.5138, 36.2612),  # Калужская область
    41: (53.0241, 158.6439), # Камчатский край
    42: (55.3545, 86.0883),  # Кемеровская область
    43: (58.6036, 49.6680),  # Кировская область
    44: (58.5500, 43.6833),  # Костромская область
    45: (55.4408, 65.3411),  # Курганская область
    46: (51.7304, 36.1936),  # Курская область
    47: (59.9391, 30.3159),  # Ленинградская область
    48: (52.6088, 39.5992),  # Липецкая область
    49: (59.5612, 150.7989), # Магаданская область
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
    65: (46.9591, 142.7380), # Сахалинская область
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
    79: (48.4808, 132.5067), # Еврейская АО
    82: (45.0469, 34.1008),  # Крым
    83: (67.6381, 53.0069),  # Ненецкий АО
    86: (61.0032, 69.0189),  # Ханты-Мансийский АО
    87: (66.3167, 171.0167), # Чукотский АО
    89: (66.5299, 66.6136),  # Ямало-Ненецкий АО
    92: (44.6167, 33.5254),  # Севастополь
    101: (52.0336, 113.5014) # Забайкальский край
}


def load_and_prepare_data():
    """Загрузка и подготовка данных"""
    print("Загрузка данных...")

    # Загружаем клики
    clicks = pd.read_parquet(CLICKS_FILE)

    # Проверяем доступные столбцы
    print("Доступные столбцы в clicks:", clicks.columns.tolist())

    # Группируем по регионам (используем столбец 'region' вместо 'region_id')
    region_stats = clicks.groupby('region').agg(
        clients_count=('uid', 'nunique')
    ).reset_index()

    # Добавляем координаты
    region_stats['latitude'] = region_stats['region'].map(lambda x: REGION_COORDINATES.get(x, (None, None))[0])
    region_stats['longitude'] = region_stats['region'].map(lambda x: REGION_COORDINATES.get(x, (None, None))[1])

    # Фильтруем регионы с координатами
    region_stats = region_stats.dropna(subset=['latitude', 'longitude'])

    if region_stats.empty:
        print("Нет данных с координатами для построения карты!")
        print("Доступные регионы в данных:", clicks['region'].unique())
        return None

    # Логарифмируем для лучшей визуализации
    region_stats['log_clients'] = np.log1p(region_stats['clients_count'])

    return region_stats


def create_heatmap(data):
    """Создание тепловой карты на географической карте России"""
    print("Создание тепловой карты на карте России...")

    # Создаем фигуру с картографической проекцией
    fig = plt.figure(figsize=(16, 12))
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())

    # Устанавливаем границы карты (оптимальные для России)
    ax.set_extent([10, 180, 40, 80], crs=ccrs.PlateCarree())

    # Добавляем географические элементы
    ax.add_feature(cfeature.LAND, facecolor='#f5f5f5')
    ax.add_feature(cfeature.OCEAN, facecolor='#e6f3ff')
    ax.add_feature(cfeature.COASTLINE, linewidth=0.7)
    ax.add_feature(cfeature.BORDERS, linestyle=':', linewidth=0.5)
    ax.add_feature(cfeature.LAKES, alpha=0.5, facecolor='#e6f3ff')
    ax.add_feature(cfeature.RIVERS, edgecolor='#e6f3ff', linewidth=0.5)

    # Тепловая карта
    sns.kdeplot(
        x=data['longitude'],
        y=data['latitude'],
        weights=data['log_clients'],
        cmap='YlOrRd',
        fill=True,
        alpha=0.6,
        levels=15,
        thresh=0.05,
        bw_adjust=0.5,
        transform=ccrs.PlateCarree(),
        ax=ax
    )

    # Точечные маркеры
    scatter = ax.scatter(
        x=data['longitude'],
        y=data['latitude'],
        s=data['clients_count'] / 10,
        c='darkred',
        alpha=0.7,
        edgecolors='white',
        linewidth=0.5,
        transform=ccrs.PlateCarree(),
        zorder=10
    )

    # Подписи для топ-5 регионов
    top_regions = data.nlargest(5, 'clients_count')
    for _, row in top_regions.iterrows():
        ax.text(
            row['longitude'] + 1,
            row['latitude'] + 0.5,
            f"{row['region']}\n{row['clients_count']}",
            transform=ccrs.PlateCarree(),
            fontsize=9,
            bbox=dict(boxstyle='round,pad=0.3', fc='white', alpha=0.8)
        )

    plt.title('Распределение клиентов по регионам России', pad=20, fontsize=16)

    # Сохранение
    heatmap_path = PLOTS_DIR / 'russia_regions_heatmap_on_map.png'
    plt.tight_layout()
    plt.savefig(heatmap_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Тепловая карта на карте России сохранена: {heatmap_path}")


if __name__ == '__main__':
    print("=== Анализ географического распределения ===")

    data = load_and_prepare_data()

    if data is not None:
        print(f"Данные для {len(data)} регионов загружены")
        create_heatmap(data)
    else:
        print("Не удалось подготовить данные для визуализации")

    print("Анализ завершен!")
