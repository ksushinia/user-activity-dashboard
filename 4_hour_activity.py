import pandas as pd
from datetime import timedelta, datetime
import sys
from time import time
import matplotlib.pyplot as plt
import seaborn as sns
from tabulate import tabulate

# Настройка стиля графиков
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# ========================================
# Конфигурация
# ========================================
CLICKS_FILE = 'C:/Users/79508\Desktop/4 семестри/Хакатон/Stream_telecom/processed_data/clicks_processed.parquet'
CAMPAIGN_FILE = 'C:/Users/79508\Desktop/4 семестри/Хакатон/Stream_telecom/processed_data/campaign_processed.parquet'
REGIONS_FILE = 'C:/Users/79508\Desktop/4 семестри/Хакатон/Stream_telecom/processed_data/regions_processed.parquet'
OUTPUT_FILE = 'campaign_activity_first_4_hours'
PLOTS_DIR = 'plots'


# ========================================
# Загрузка данных
# ========================================
def load_data():
    print("Загрузка данных...")
    start_time = time()

    try:
        # Загружаем обработанные данные
        clicks = pd.read_parquet(CLICKS_FILE)
        campaigns = pd.read_parquet(CAMPAIGN_FILE)
        regions = pd.read_parquet(REGIONS_FILE)

        # Преобразуем created_at из Unix timestamp в наносекундах
        campaigns['created_at'] = pd.to_datetime(campaigns['created_at'].astype('int64') // 10 ** 9, unit='s')

        print(f"Данные загружены за {time() - start_time:.1f} сек")
        print(f"Кликов: {len(clicks):,}")
        print(f"Кампаний: {len(campaigns):,}")
        print(f"Регионов: {len(regions):,}")

        return clicks, campaigns, regions

    except Exception as e:
        print(f"Ошибка загрузки данных: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Визуализация данных
# ========================================
def visualize_data(activity_stats):
    import os
    if not os.path.exists(PLOTS_DIR):
        os.makedirs(PLOTS_DIR)

    print("\nСоздание визуализаций...")

    # 1. Топ-10 кампаний по количеству кликов (график + таблица рядом)
    top_clicks = activity_stats.nlargest(10, 'total_clicks')

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5), gridspec_kw={'width_ratios': [2, 1]})

    # Левая часть: barplot
    sns.barplot(x='campaign_id', y='total_clicks', data=top_clicks, ax=ax1)
    ax1.set_title('Топ-10 кампаний по кликам (первые 4 часа)')
    ax1.set_xlabel('ID кампании')
    ax1.set_ylabel('Количество кликов')
    ax1.tick_params(axis='x', rotation=45)

    # Правая часть: таблица
    table_data = top_clicks[['campaign_id', 'total_clicks']]
    ax2.axis('off')  # Убираем оси
    table = ax2.table(cellText=table_data.values,
                      colLabels=table_data.columns,
                      cellLoc='center',
                      loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)  # Масштаб таблицы

    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/top10_clicks_with_table.png')
    plt.close()

    print(f"Графики сохранены в папку {PLOTS_DIR}")


# ========================================
# Анализ активности
# ========================================
def analyze_activity(clicks, campaigns):
    print("\nАнализ активности в первые 4 часа кампании...")
    start_time = time()

    # Объединяем клики с информацией о кампаниях
    merged = pd.merge(clicks, campaigns, left_on='campaign_id', right_on='id', how='left')

    # Вычисляем разницу между временем клика и созданием кампании
    merged['time_since_campaign_start'] = merged['click_time'] - merged['created_at']

    # Фильтруем только клики в первые 4 часа
    first_4_hours = merged[merged['time_since_campaign_start'] <= timedelta(hours=4)]

    # Группируем по кампании и считаем метрики
    activity_stats = first_4_hours.groupby('campaign_id').agg(
        total_clicks=('uid', 'count'),
        unique_users=('uid', 'nunique'),
        regions_count=('region', 'nunique'),
        devices=('device', lambda x: x.value_counts().to_dict()),
        first_click_time=('click_time', 'min'),
        last_click_time=('click_time', 'max'),
        campaign_created=('created_at', 'first')
    ).reset_index()

    # Вычисляем продолжительность активности
    activity_stats['activity_duration'] = activity_stats['last_click_time'] - activity_stats['first_click_time']

    # Добавляем процент активности от 4 часов
    activity_stats['activity_percentage'] = (
            activity_stats['activity_duration'].dt.total_seconds() / (4 * 3600) * 100
    ).round(1)

    print(f"Анализ завершен за {time() - start_time:.1f} сек")
    print(f"Проанализировано {len(activity_stats)} кампаний")

    return activity_stats


# ========================================
# Сохранение результатов
# ========================================
def save_results(df):
    print("\nСохранение результатов...")
    try:
        # Пробуем сохранить в Parquet
        try:
            df.to_parquet(f"{OUTPUT_FILE}.parquet", engine='pyarrow')
            print(f"Результаты сохранены в {OUTPUT_FILE}.parquet")
        except:
            try:
                df.to_parquet(f"{OUTPUT_FILE}.parquet", engine='fastparquet')
                print(f"Результаты сохранены в {OUTPUT_FILE}.parquet (использован fastparquet)")
            except:
                # Если не получилось сохранить в Parquet, сохраняем в CSV
                df.to_csv(f"{OUTPUT_FILE}.csv.gz", compression='gzip', index=False)
                print(f"Результаты сохранены в {OUTPUT_FILE}.csv.gz")
    except Exception as e:
        print(f"Ошибка при сохранении: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Вывод красивых таблиц
# ========================================
def print_tables(activity_stats):
    print("\nСводная статистика по активности кампаний:")

    # Общая статистика
    summary = activity_stats.describe(include='all').transpose()
    print(tabulate(summary, headers='keys', tablefmt='pretty', floatfmt=".1f"))

    # Топ-5 кампаний по разным метрикам
    metrics = ['total_clicks', 'unique_users', 'regions_count', 'activity_percentage']
    for metric in metrics:
        top5 = activity_stats.nlargest(5, metric)[['campaign_id', metric]]
        print(f"\nТоп-5 кампаний по {metric.replace('_', ' ')}:")
        print(tabulate(top5, headers='keys', tablefmt='pretty', showindex=False))


# ========================================
# Главная функция
# ========================================
if __name__ == '__main__':
    # Загрузка данных
    clicks, campaigns, regions = load_data()

    # Анализ активности
    activity_stats = analyze_activity(clicks, campaigns)

    # Визуализация данных
    visualize_data(activity_stats)

    # Вывод таблиц
    print_tables(activity_stats)

    # Сохранение результатов
    save_results(activity_stats)

    print("\nГотово! Анализ завершен.")