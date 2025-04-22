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

# ========================================
# Конфигурация
# ========================================
PROJECT_ROOT = Path(__file__).parent.parent.parent  # Поднимаемся на уровень выше metrics/
(PROJECT_ROOT / 'processed_data').mkdir(parents=True, exist_ok=True)
CLICKS_FILE = PROJECT_ROOT / 'processed_data' / 'clicks_processed.parquet'
CAMPAIGN_FILE = PROJECT_ROOT / 'processed_data' / 'campaign_processed.parquet'
PLOTS_DIR = PROJECT_ROOT / 'plots'
OUTPUT_FILE = PROJECT_ROOT / 'processed_data' / 'response_time_analysis'


# ========================================
# Загрузка данных
# ========================================
def load_data():
    print("Загрузка данных для анализа скорости реакции...")
    start_time = time()

    try:
        # Загружаем обработанные данные
        clicks = pd.read_parquet(CLICKS_FILE)
        campaigns = pd.read_parquet(CAMPAIGN_FILE)

        # Преобразуем created_at из Unix timestamp в наносекундах
        campaigns['created_at'] = pd.to_datetime(campaigns['created_at'].astype('int64') // 10 ** 9, unit='s')

        print(f"Данные загружены за {time() - start_time:.1f} сек")
        print(f"Кликов: {len(clicks):,}")
        print(f"Кампаний: {len(campaigns):,}")

        return clicks, campaigns

    except Exception as e:
        print(f"Ошибка загрузки данных: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Расчет скорости реакции клиентов
# ========================================
def calculate_response_time(clicks, campaigns):
    print("\nРасчет скорости реакции клиентов...")
    start_time = time()

    # Объединяем клики с информацией о кампаниях
    merged = pd.merge(clicks, campaigns, left_on='campaign_id', right_on='id', how='left')

    # Вычисляем время реакции (разница между кликом и созданием кампании)
    merged['response_time'] = (merged['click_time'] - merged['created_at']).dt.total_seconds()

    # Фильтруем некорректные значения (если клик был до создания кампании)
    merged = merged[merged['response_time'] >= 0]

    # Добавляем время реакции в часах
    merged['response_time_hours'] = merged['response_time'] / 3600

    # Группируем по кампаниям для анализа
    campaign_response = merged.groupby('campaign_id').agg(
        total_clicks=('uid', 'count'),
        avg_response_time_seconds=('response_time', 'mean'),
        median_response_time_seconds=('response_time', 'median'),
        min_response_time_seconds=('response_time', 'min'),
        max_response_time_seconds=('response_time', 'max'),
        avg_response_time_hours=('response_time_hours', 'mean'),
        median_response_time_hours=('response_time_hours', 'median')
    ).reset_index()

    # Добавляем информацию о кампании
    campaign_response = pd.merge(campaign_response,
                                 campaigns[['id', 'created_at']],
                                 left_on='campaign_id',
                                 right_on='id',
                                 how='left')

    # Общая статистика по всем кликам
    overall_stats = {
        'total_clicks': len(merged),
        'avg_response_time_seconds': merged['response_time'].mean(),
        'median_response_time_seconds': merged['response_time'].median(),
        'min_response_time_seconds': merged['response_time'].min(),
        'max_response_time_seconds': merged['response_time'].max(),
        'avg_response_time_hours': merged['response_time_hours'].mean(),
        'median_response_time_hours': merged['response_time_hours'].median()
    }

    print(f"Анализ завершен за {time() - start_time:.1f} сек")
    print(f"Проанализировано {len(merged)} кликов")
    print(f"Среднее время реакции: {overall_stats['avg_response_time_hours']:.2f} часов")
    print(f"Медианное время реакции: {overall_stats['median_response_time_hours']:.2f} часов")

    return campaign_response, overall_stats


# ========================================
# Визуализация данных
# ========================================
def visualize_response_data(campaign_response, overall_stats):
    import os
    if not os.path.exists(PLOTS_DIR):
        os.makedirs(PLOTS_DIR)

    print("\nСоздание визуализаций скорости реакции...")

    # 1. Распределение времени реакции
    plt.figure(figsize=(12, 6))
    sns.histplot(data=campaign_response, x='avg_response_time_hours', bins=50, kde=True)
    plt.title('Распределение среднего времени реакции по кампаниям (часы)')
    plt.xlabel('Среднее время реакции (часы)')
    plt.ylabel('Количество кампаний')
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/response_time_distribution.png')
    plt.close()

    # 2. Топ-10 кампаний по скорости реакции
    top10_fastest = campaign_response.nsmallest(10, 'avg_response_time_hours')[
        ['campaign_id', 'avg_response_time_hours']]
    plt.figure(figsize=(12, 6))
    sns.barplot(data=top10_fastest, x='campaign_id', y='avg_response_time_hours', palette='viridis')
    plt.title('Топ-10 кампаний с самым быстрым временем реакции')
    plt.xlabel('ID кампании')
    plt.ylabel('Среднее время реакции (часы)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/top10_fastest_campaigns.png')
    plt.close()

    print(f"Графики сохранены в папку {PLOTS_DIR}")


# ========================================
# Сохранение результатов
# ========================================
def save_response_results(campaign_response, overall_stats):
    print("\nСохранение результатов анализа скорости реакции...")

    try:
        # Сохраняем статистику по кампаниям
        campaign_response.to_parquet(f"{OUTPUT_FILE}_campaign_stats.parquet", engine='pyarrow')

        # Сохраняем общую статистику в JSON
        import json
        with open(f"{OUTPUT_FILE}_overall_stats.json", 'w') as f:
            json.dump(overall_stats, f, indent=4)

        print(f"Результаты сохранены в {OUTPUT_FILE}_campaign_stats.parquet и {OUTPUT_FILE}_overall_stats.json")
    except Exception as e:
        print(f"Ошибка при сохранении: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Вывод красивых таблиц
# ========================================
def print_response_tables(campaign_response, overall_stats):
    print("\nСводная статистика по скорости реакции:")

    # Общая статистика
    print("\nОбщая статистика времени реакции:")
    overall_table = [
        ["Общее количество кликов", f"{overall_stats['total_clicks']:,}"],
        ["Среднее время реакции", f"{overall_stats['avg_response_time_hours']:.2f} часов"],
        ["Медианное время реакции", f"{overall_stats['median_response_time_hours']:.2f} часов"],
        ["Минимальное время реакции", f"{overall_stats['min_response_time_seconds']:.0f} секунд"],
        ["Максимальное время реакции", f"{overall_stats['max_response_time_seconds']:.0f} секунд"]
    ]
    print(tabulate(overall_table, headers=["Метрика", "Значение"], tablefmt='pretty'))

    # Топ-10 самых быстрых кампаний
    top10_fastest = campaign_response.nsmallest(10, 'avg_response_time_hours')[
        ['campaign_id', 'avg_response_time_hours']]
    print("\nТоп-10 кампаний с самым быстрым временем реакции:")
    print(tabulate(top10_fastest, headers='keys', tablefmt='pretty', floatfmt=".2f"))


# ========================================
# Главная функция
# ========================================
if __name__ == '__main__':
    # Загрузка данных
    clicks, campaigns = load_data()

    # Расчет скорости реакции
    campaign_response, overall_stats = calculate_response_time(clicks, campaigns)

    # Визуализация данных
    visualize_response_data(campaign_response, overall_stats)

    # Вывод таблиц
    print_response_tables(campaign_response, overall_stats)

    # Сохранение результатов
    save_response_results(campaign_response, overall_stats)

    print("\nГотово! Анализ скорости реакции завершен.")
