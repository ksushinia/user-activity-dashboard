import pandas as pd
from datetime import timedelta
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
CLICKS_FILE = 'C:/Users/79508/Desktop/4 семестри/Хакатон/Stream_telecom/processed_data/clicks_processed.parquet'
CAMPAIGN_FILE = 'C:/Users/79508/Desktop/4 семестри/Хакатон/Stream_telecom/processed_data/campaign_processed.parquet'
OUTPUT_FILE = 'clicks_per_day_and_month_activity'
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
# Анализ кликов по дням и месяцам
# ========================================
def analyze_clicks_per_day_and_month(clicks, campaigns):
    print("\nАнализ кликов по дням и месяцам кампаний...")
    start_time = time()

    # Объединяем клики с информацией о кампаниях
    merged = pd.merge(clicks, campaigns, left_on='campaign_id', right_on='id', how='left')

    # Вычисляем день и месяц клика
    merged['click_date'] = merged['click_time'].dt.date
    merged['click_month'] = merged['click_time'].dt.to_period('M')

    # Преобразуем click_month в строку для правильной визуализации
    merged['click_month'] = merged['click_month'].astype(str)

    # Группируем по дням и месяцам
    clicks_per_day = merged.groupby('click_date').agg(
        total_clicks=('uid', 'count')
    ).reset_index()

    clicks_per_month = merged.groupby('click_month').agg(
        total_clicks=('uid', 'count')
    ).reset_index()

    # Получаем общее количество кликов
    total_clicks_all_time = len(merged)

    # Добавляем процентное соотношение кликов по дням и месяцам
    clicks_per_day['percentage'] = (clicks_per_day['total_clicks'] / total_clicks_all_time) * 100
    clicks_per_month['percentage'] = (clicks_per_month['total_clicks'] / total_clicks_all_time) * 100

    # Выводим результаты
    print(f"Анализ завершен за {time() - start_time:.1f} сек")
    print(f"Проанализировано {len(clicks_per_day)} дней и {len(clicks_per_month)} месяцев")
    print(f"Общее количество кликов: {total_clicks_all_time}")

    # Покажем примеры
    print("\nКлики по дням:")
    print(clicks_per_day.head())

    print("\nКлики по месяцам:")
    print(clicks_per_month.head())

    return clicks_per_day, clicks_per_month


# ========================================
# Визуализация данных
# ========================================
def visualize_data(clicks_per_day, clicks_per_month):
    import os
    if not os.path.exists(PLOTS_DIR):
        os.makedirs(PLOTS_DIR)

    print("\nСоздание визуализаций...")

    # 1. График кликов по дням
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=clicks_per_day, x='click_date', y='total_clicks', marker='o', color='yellow')
    plt.title('Динамика кликов по дням')
    plt.xlabel('Дата')
    plt.ylabel('Количество кликов')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/clicks_per_day.png')
    plt.close()

    # 2. График кликов по месяцам
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=clicks_per_month, x='click_month', y='total_clicks', marker='o', color='green')
    plt.title('Динамика кликов по месяцам')
    plt.xlabel('Месяц')
    plt.ylabel('Количество кликов')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/clicks_per_month.png')
    plt.close()

    print(f"Графики сохранены в папку {PLOTS_DIR}")


# ========================================
# Сохранение результатов
# ========================================
def save_results(clicks_per_day, clicks_per_month):
    print("\nСохранение результатов...")

    try:
        # Пробуем сохранить в Parquet
        try:
            clicks_per_day.to_parquet(f"{OUTPUT_FILE}_per_day.parquet", engine='pyarrow')
            clicks_per_month.to_parquet(f"{OUTPUT_FILE}_per_month.parquet", engine='pyarrow')
            print(f"Результаты сохранены в {OUTPUT_FILE}_per_day.parquet и {OUTPUT_FILE}_per_month.parquet")
        except:
            # Если не получилось сохранить в Parquet, сохраняем в CSV
            clicks_per_day.to_csv(f"{OUTPUT_FILE}_per_day.csv.gz", compression='gzip', index=False)
            clicks_per_month.to_csv(f"{OUTPUT_FILE}_per_month.csv.gz", compression='gzip', index=False)
            print(f"Результаты сохранены в {OUTPUT_FILE}_per_day.csv.gz и {OUTPUT_FILE}_per_month.csv.gz")
    except Exception as e:
        print(f"Ошибка при сохранении: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Вывод красивых таблиц
# ========================================
def print_tables(clicks_per_day, clicks_per_month):
    print("\nСводная статистика по кликам:")

    # Топ-5 по дням
    top5_days = clicks_per_day.nlargest(5, 'total_clicks')[['click_date', 'total_clicks', 'percentage']]
    print("\nТоп-5 дней по количеству кликов:")
    print(tabulate(top5_days, headers='keys', tablefmt='pretty', floatfmt=".1f"))

    # Топ-5 по месяцам
    top5_months = clicks_per_month.nlargest(5, 'total_clicks')[['click_month', 'total_clicks', 'percentage']]
    print("\nТоп-5 месяцев по количеству кликов:")
    print(tabulate(top5_months, headers='keys', tablefmt='pretty', floatfmt=".1f"))


# ========================================
# Главная функция
# ========================================
if __name__ == '__main__':
    # Загрузка данных
    clicks, campaigns = load_data()

    # Анализ кликов по дням и месяцам
    clicks_per_day, clicks_per_month = analyze_clicks_per_day_and_month(clicks, campaigns)

    # Визуализация данных
    visualize_data(clicks_per_day, clicks_per_month)

    # Вывод таблиц
    print_tables(clicks_per_day, clicks_per_month)

    # Сохранение результатов
    save_results(clicks_per_day, clicks_per_month)

    print("\nГотово! Анализ завершен.")
