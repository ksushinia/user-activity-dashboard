import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate
from pathlib import Path
import sys
from time import time

# Конфигурация
PROJECT_ROOT = Path(__file__).parent.parent.parent
CLICKS_FILE = PROJECT_ROOT / 'processed_data' / 'clicks_processed.parquet'
PLOTS_DIR = PROJECT_ROOT / 'plots'
PLOTS_DIR.mkdir(exist_ok=True)


# Загрузка данных
def load_clicks_data():
    print("Загрузка данных о кликах...")
    try:
        clicks = pd.read_parquet(CLICKS_FILE)
        print(f"Загружено {len(clicks):,} записей о кликах")
        return clicks
    except Exception as e:
        print(f"Ошибка загрузки данных: {str(e)}", file=sys.stderr)
        sys.exit(1)


# Анализ активности по часам
def analyze_activity(clicks):
    print("\nАнализ активности пользователей...")
    start_time = time()

    # Анализ по UTC
    clicks['hour'] = clicks['click_time'].dt.hour
    hour_activity = clicks['hour'].value_counts().sort_index().reset_index()
    hour_activity.columns = ['hour', 'clicks_count']
    hour_activity['pct'] = (hour_activity['clicks_count'] / hour_activity['clicks_count'].sum()) * 100

    # Топ-3 лучших часа
    best_hours = hour_activity.nlargest(3, 'clicks_count')['hour'].tolist()

    print(f"Анализ завершен за {time() - start_time:.1f} сек")
    return hour_activity, best_hours


# Визуализация результатов
def visualize_results(hour_activity, best_hours):
    print("\nСоздание визуализаций...")

    plt.figure(figsize=(12, 6))
    bars = plt.bar(hour_activity['hour'], hour_activity['clicks_count'], color='skyblue')

    # Подсветка лучших часов
    for hour in best_hours:
        bars[hour].set_color('red')

    plt.title('Активность пользователей по часам (UTC)', fontsize=14)
    plt.xlabel('Час дня (UTC)', fontsize=12)
    plt.ylabel('Количество кликов', fontsize=12)
    plt.xticks(range(0, 24))
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'user_activity_by_hour.png', dpi=120)
    plt.close()


# Формирование рекомендаций
def generate_recommendations(hour_activity, best_hours):
    print("\nРекомендации по времени отправки кампаний:")

    print("\nТоп-5 часов активности:")
    print(tabulate(hour_activity.nlargest(5, 'clicks_count'),
                   headers=['Час (UTC)', 'Кликов', 'Доля,%'],
                   tablefmt='pretty',
                   floatfmt=".1f"))

    print("\nЛучшее время для отправки (UTC):")
    for i, hour in enumerate(best_hours, 1):
        print(f"{i}. {hour:02d}:00 - {hour + 1:02d}:00")

    # Пример конвертации в московское время (UTC+3)
    print("\nПример для Москвы (UTC+3):")
    for hour in best_hours:
        msk_hour = (hour + 3) % 24
        print(f"- UTC {hour:02d}:00 → МСК {msk_hour:02d}:00")


# Главная функция
def main():
    # Загрузка данных
    clicks = load_clicks_data()

    # Анализ активности
    hour_activity, best_hours = analyze_activity(clicks)

    # Визуализация
    visualize_results(hour_activity, best_hours)

    # Рекомендации
    generate_recommendations(hour_activity, best_hours)

    print("\nАнализ завершен. Результаты сохранены в:", PLOTS_DIR)


if __name__ == '__main__':
    main()
