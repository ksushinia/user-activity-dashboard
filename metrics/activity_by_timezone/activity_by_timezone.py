import pandas as pd
from datetime import timedelta
import sys
from time import time
import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate
from pathlib import Path

# Настройка стиля графиков
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Словарь названий регионов
REGION_NAMES = {
    0: "Неопознанный регион",
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

# Конфигурация
PROJECT_ROOT = Path(__file__).parent.parent.parent
(PROJECT_ROOT / 'processed_data').mkdir(parents=True, exist_ok=True)
CLICKS_FILE = PROJECT_ROOT / 'processed_data' / 'clicks_processed.parquet'
CAMPAIGN_FILE = PROJECT_ROOT / 'processed_data' / 'campaign_processed.parquet'
REGIONS_FILE = PROJECT_ROOT / 'processed_data' / 'regions_processed.parquet'
PLOTS_DIR = PROJECT_ROOT / 'plots'
OUTPUT_FILE = PROJECT_ROOT / 'processed_data' / 'activity_by_timezone'


# Загрузка данных
def load_data():
    print("Загрузка данных для анализа активности...")
    start_time = time()

    try:
        clicks = pd.read_parquet(CLICKS_FILE)
        campaigns = pd.read_parquet(CAMPAIGN_FILE)
        regions = pd.read_parquet(REGIONS_FILE)

        campaigns['created_at'] = pd.to_datetime(campaigns['created_at'].astype('int64') // 10 ** 9, unit='s')

        print(f"\nДанные загружены за {time() - start_time:.1f} сек")
        print(f"Кликов: {len(clicks):,}")
        print(f"Кампаний: {len(campaigns):,}")
        print(f"Регионов: {len(regions):,}")

        return clicks, campaigns, regions

    except Exception as e:
        print(f"Ошибка загрузки данных: {str(e)}", file=sys.stderr)
        sys.exit(1)


# Анализ активности по часам
def analyze_activity_by_hour(clicks):
    print("\nАнализ активности по часам...")
    start_time = time()

    try:
        clicks['hour'] = clicks['click_time'].dt.hour
        activity = clicks.groupby('hour').agg(
            total_clicks=('uid', 'count'),
            unique_users=('uid', 'nunique')
        ).reset_index()

        total = activity['total_clicks'].sum()
        activity['percentage'] = (activity['total_clicks'] / total) * 100

        print(f"Анализ завершен за {time() - start_time:.1f} сек")
        return activity

    except Exception as e:
        print(f"Ошибка анализа: {str(e)}", file=sys.stderr)
        return None


# Анализ активности по регионам
def analyze_activity_by_region(clicks):
    print("\nАнализ активности по регионам...")
    start_time = time()

    try:
        # Исключаем регион с ID 0 (неопознанный)
        filtered_clicks = clicks[clicks['region'] != 0]

        # Добавляем названия регионов
        filtered_clicks['region_name'] = filtered_clicks['region'].map(REGION_NAMES)
        filtered_clicks['region_name'] = filtered_clicks['region_name'].fillna("Неизвестный регион")

        region_activity = filtered_clicks.groupby(['region', 'region_name']).agg(
            total_clicks=('uid', 'count'),
            unique_users=('uid', 'nunique')
        ).reset_index().sort_values('total_clicks', ascending=False)

        print(f"Анализ завершен за {time() - start_time:.1f} сек")
        return region_activity

    except Exception as e:
        print(f"Ошибка анализа: {str(e)}", file=sys.stderr)
        return None


# Визуализация данных
def visualize_data(hour_activity, region_activity):
    import os
    if not os.path.exists(PLOTS_DIR):
        os.makedirs(PLOTS_DIR)

    print("\nСоздание визуализаций...")

    # График активности по часам с подписями
    plt.figure(figsize=(14, 7))
    ax = sns.barplot(data=hour_activity, x='hour', y='percentage', color='blue')
    plt.title('Активность клиентов по часам (UTC)', fontsize=16)
    plt.xlabel('Час дня', fontsize=14)
    plt.ylabel('Процент кликов', fontsize=14)

    # Добавляем подписи значений
    for p in ax.patches:
        ax.annotate(f"{p.get_height():.1f}%",
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center',
                    xytext=(0, 10),
                    textcoords='offset points',
                    fontsize=10)

    plt.xticks(range(0, 24), rotation=45)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/activity_by_hour.png', dpi=300)
    plt.close()

    # График топ-10 регионов (без нулевого региона)
    plt.figure(figsize=(14, 7))
    top_regions = region_activity.head(10)
    ax = sns.barplot(data=top_regions, x='region_name', y='total_clicks', palette='viridis')
    plt.title('Топ-10 регионов по активности (без неопознанных)', fontsize=16)
    plt.xlabel('Регион', fontsize=14)
    plt.ylabel('Количество кликов', fontsize=14)

    # Добавляем подписи значений
    for p in ax.patches:
        ax.annotate(f"{int(p.get_height()):,}",
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center',
                    xytext=(0, 10),
                    textcoords='offset points',
                    fontsize=10)

    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/activity_by_region.png', dpi=300)
    plt.close()

    print(f"Графики сохранены в {PLOTS_DIR}")


if __name__ == '__main__':
    clicks, campaigns, regions = load_data()
    hour_activity = analyze_activity_by_hour(clicks)
    region_activity = analyze_activity_by_region(clicks)

    if hour_activity is not None and region_activity is not None:
        visualize_data(hour_activity, region_activity)

        # Сохраняем результаты анализа в файлы Parquet
        try:
            hour_activity.to_parquet(PROJECT_ROOT / 'processed_data' / 'activity_by_timezone_by_hour.parquet')
            region_activity.to_parquet(PROJECT_ROOT / 'processed_data' / 'activity_by_timezone_by_region.parquet')
            print("\nРезультаты анализа сохранены в файлы:")
            print(f"- {PROJECT_ROOT / 'processed_data' / 'activity_by_timezone_by_hour.parquet'}")
            print(f"- {PROJECT_ROOT / 'processed_data' / 'activity_by_timezone_by_region.parquet'}")
        except Exception as e:
            print(f"\nОшибка при сохранении результатов анализа: {str(e)}", file=sys.stderr)

        print("\nТоп-5 часов активности:")
        print(tabulate(hour_activity.nlargest(5, 'total_clicks'),
                       headers='keys',
                       tablefmt='pretty',
                       floatfmt=".1f"))

        print("\nТоп-5 регионов по активности:")
        print(tabulate(region_activity[['region_name', 'total_clicks', 'unique_users']].head(5),
                       headers=['Регион', 'Клики', 'Уникальные пользователи'],
                       tablefmt='pretty'))

    print("\nАнализ завершен!")
