# response_analysis.py
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import sys
from time import time
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Настройка стиля графиков
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# ========================================
# Конфигурация
# ========================================
PROJECT_ROOT = Path(__file__).parent.parent.parent
PLOTS_DIR = PROJECT_ROOT / 'plots' / 'response_analysis'
PLOTS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = PROJECT_ROOT / 'processed_data' / 'response_analysis'


# ========================================
# Загрузка данных
# ========================================
def load_and_prepare_data():
    print("Загрузка и подготовка данных...")
    start_time = time()

    try:
        # 1. Загружаем данные кампаний
        campaigns = pd.read_parquet(PROJECT_ROOT / 'processed_data' / 'campaign_processed.parquet')

        # 2. Загружаем данные кликов
        clicks = pd.read_parquet(PROJECT_ROOT / 'processed_data' / 'clicks_processed.parquet')

        # 3. Преобразуем временные метки
        def convert_timestamp(ts):
            try:
                # Если это числовой timestamp (наносекунды)
                if pd.api.types.is_number(ts):
                    return pd.to_datetime(ts, unit='ns')
                # Если это строка
                elif isinstance(ts, str):
                    # Попробуем преобразовать как число (наносекунды)
                    try:
                        return pd.to_datetime(float(ts), unit='ns')
                    except:
                        # Если не число, пробуем как строку даты
                        return pd.to_datetime(ts)
                return pd.NaT
            except:
                return pd.NaT

        campaigns['created_at'] = campaigns['created_at'].apply(convert_timestamp)
        clicks['click_time'] = clicks['click_time'].apply(convert_timestamp)

        # 4. Находим время первого клика для каждой кампании
        first_clicks = clicks.groupby('campaign_id')['click_time'].min().reset_index()
        first_clicks.rename(columns={'click_time': 'first_click_time'}, inplace=True)

        # 5. Объединяем с данными кампаний
        campaigns = campaigns.merge(first_clicks, on='campaign_id', how='left')

        # 6. Добавляем количество кликов по кампаниям
        click_counts = clicks.groupby('campaign_id').size().reset_index(name='clicks_count')
        campaigns = campaigns.merge(click_counts, on='campaign_id', how='left')

        # Заполняем пропуски для кампаний без кликов
        campaigns['clicks_count'] = campaigns['clicks_count'].fillna(0)

        # Удаляем строки с некорректными датами
        campaigns = campaigns.dropna(subset=['created_at', 'first_click_time'])

        print(f"Данные загружены и подготовлены за {time() - start_time:.1f} сек")
        print(f"Кампаний: {len(campaigns):,}")
        print(f"Из них с кликами: {len(campaigns[campaigns['clicks_count'] > 0]):,}")

        return campaigns

    except Exception as e:
        print(f"Ошибка загрузки данных: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Расчет скорости реакции клиентов
# ========================================
def calculate_response_stats(campaigns):
    print("\nРасчет скорости реакции клиентов...")
    start_time = time()

    # Рассчитываем время реакции (в секундах)
    campaigns['response_time_sec'] = (campaigns['first_click_time'] - campaigns['created_at']).dt.total_seconds()

    # Фильтруем только кампании с кликами
    responded_campaigns = campaigns[campaigns['clicks_count'] > 0].copy()

    # Основные метрики
    stats = {
        'total_campaigns': len(campaigns),
        'responded_campaigns': len(responded_campaigns),
        'response_rate': len(responded_campaigns) / len(campaigns) * 100,
        'avg_response_sec': responded_campaigns['response_time_sec'].mean(),
        'median_response_sec': responded_campaigns['response_time_sec'].median(),
        'min_response_sec': responded_campaigns['response_time_sec'].min(),
        'max_response_sec': responded_campaigns['response_time_sec'].max(),
        'std_response_sec': responded_campaigns['response_time_sec'].std(),
        'percentile_90': np.percentile(responded_campaigns['response_time_sec'], 90),
        'percentile_95': np.percentile(responded_campaigns['response_time_sec'], 95)
    }

    # Добавляем читаемые временные форматы
    stats.update({
        'avg_response': str(timedelta(seconds=stats['avg_response_sec'])),
        'median_response': str(timedelta(seconds=stats['median_response_sec'])),
        'min_response': str(timedelta(seconds=stats['min_response_sec'])),
        'max_response': str(timedelta(seconds=stats['max_response_sec'])),
        'percentile_90_response': str(timedelta(seconds=stats['percentile_90'])),
        'percentile_95_response': str(timedelta(seconds=stats['percentile_95']))
    })

    print(f"Расчет завершен за {time() - start_time:.1f} сек")
    return stats, responded_campaigns


def visualize_response_stats(responded_campaigns, stats):
    print("\nСоздание визуализаций скорости реакции...")

    # 1. Распределение времени реакции
    plt.figure(figsize=(12, 6))
    sns.histplot(responded_campaigns['response_time_sec'] / 60,  # в минутах
                 bins=50,
                 kde=True,
                 color='skyblue')
    plt.title('Распределение времени реакции клиентов')
    plt.xlabel('Время реакции (минуты)')
    plt.ylabel('Количество кампаний')
    plt.axvline(stats['median_response_sec'] / 60, color='red', linestyle='--',
                label=f'Медиана: {stats["median_response"]}')
    plt.legend()
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/response_time_distribution.png', dpi=300)
    plt.close()

    # 2. Скорость реакции по дням недели
    responded_campaigns['day_of_week'] = responded_campaigns['created_at'].dt.day_name()
    weekdays_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    russian_weekdays = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']

    plt.figure(figsize=(12, 6))
    ax = sns.boxplot(data=responded_campaigns,
                     x='day_of_week',
                     y='response_time_sec',
                     order=weekdays_order,
                     showfliers=False,
                     color='lightgreen')

    # Заменяем английские названия на русские
    ax.set_xticklabels(russian_weekdays)

    plt.title('Скорость реакции клиентов по дням недели')
    plt.xlabel('День недели')
    plt.ylabel('Время реакции (секунды)')
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/response_time_by_weekday.png', dpi=300)
    plt.close()

    # 3. Скорость реакции по часам дня
    responded_campaigns['hour_of_day'] = responded_campaigns['created_at'].dt.hour

    plt.figure(figsize=(12, 6))
    sns.boxplot(data=responded_campaigns,
                x='hour_of_day',
                y='response_time_sec',
                showfliers=False,
                color='salmon')
    plt.title('Скорость реакции клиентов по часам дня')
    plt.xlabel('Час дня')
    plt.ylabel('Время реакции (секунды)')
    plt.yscale('log')
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/response_time_by_hour.png', dpi=300)
    plt.close()

    print(f"Графики сохранены в папку {PLOTS_DIR}")


# ========================================
# Сохранение результатов
# ========================================
def save_response_stats(stats, responded_campaigns):
    print("\nСохранение результатов анализа...")

    try:
        # Сохраняем статистику в JSON
        pd.DataFrame([stats]).to_json(f"{OUTPUT_FILE}_stats.json", orient='records', indent=2)

        # Сохраняем данные по кампаниям с кликами
        responded_campaigns.to_parquet(f"{OUTPUT_FILE}_data.parquet")

        print(f"Результаты сохранены в {OUTPUT_FILE}_stats.json и {OUTPUT_FILE}_data.parquet")
    except Exception as e:
        print(f"Ошибка при сохранении: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Главная функция
# ========================================
if __name__ == '__main__':
    # Загрузка и подготовка данных
    campaigns = load_and_prepare_data()

    # Расчет статистики
    stats, responded_campaigns = calculate_response_stats(campaigns)

    # Вывод результатов
    print("\nСтатистика скорости реакции клиентов:")
    print(f"Всего кампаний: {stats['total_campaigns']:,}")
    print(f"Кампании с кликами: {stats['responded_campaigns']:,} ({stats['response_rate']:.1f}%)")
    print(f"\nСреднее время реакции: {stats['avg_response']}")
    print(f"Медианное время реакции: {stats['median_response']}")
    print(f"Минимальное время: {stats['min_response']}")
    print(f"Максимальное время: {stats['max_response']}")
    print(f"90-й процентиль: {stats['percentile_90_response']}")
    print(f"95-й процентиль: {stats['percentile_95_response']}")

    # Визуализация
    visualize_response_stats(responded_campaigns, stats)

    # Сохранение
    save_response_stats(stats, responded_campaigns)

    print("\nАнализ скорости реакции клиентов завершен!")