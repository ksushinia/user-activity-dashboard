# campaign_dynamics.py
import numpy as np
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
CAMPAIGN_FILE = PROJECT_ROOT / 'processed_data' / 'campaign_processed.parquet'
PLOTS_DIR = PROJECT_ROOT / 'plots' / 'campaign_dynamics'
PLOTS_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = PROJECT_ROOT / 'processed_data' / 'campaign_dynamics'


# ========================================
# Загрузка данных
# ========================================
def load_campaign_data():
    print("Загрузка данных кампаний...")
    start_time = time()

    try:
        # Загружаем обработанные данные
        campaigns = pd.read_parquet(CAMPAIGN_FILE)

        # Преобразуем created_at из Unix timestamp в наносекундах
        campaigns['created_at'] = pd.to_datetime(campaigns['created_at'].astype('int64') // 10 ** 9, unit='s')

        print(f"Данные загружены за {time() - start_time:.1f} сек")
        print(f"Кампаний: {len(campaigns):,}")

        return campaigns

    except Exception as e:
        print(f"Ошибка загрузки данных: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Анализ динамики создания кампаний
# ========================================
def analyze_campaign_dynamics(campaigns):
    print("\nАнализ динамики создания кампаний...")
    start_time = time()

    # Создаем копию данных
    df = campaigns.copy()

    # Извлекаем временные компоненты
    df['date'] = df['created_at'].dt.date
    df['day_of_week'] = df['created_at'].dt.day_name()
    df['week_of_year'] = df['created_at'].dt.isocalendar().week
    df['month'] = df['created_at'].dt.month_name()
    df['year'] = df['created_at'].dt.year
    df['year_month'] = df['created_at'].dt.to_period('M')

    # 1. Динамика по дням в разрезе недели
    daily_dynamics = df.groupby(['date', 'day_of_week', 'week_of_year']).agg(
        campaigns_count=('id', 'count')
    ).reset_index()

    # 2. Динамика по месяцам в разрезе года
    monthly_dynamics = df.groupby(['year', 'month', 'year_month']).agg(
        campaigns_count=('id', 'count')
    ).reset_index()

    # Упорядочивание дней недели и месяцев
    weekdays_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    months_order = ['January', 'February', 'March', 'April', 'May', 'June',
                    'July', 'August', 'September', 'October', 'November', 'December']

    daily_dynamics['day_of_week'] = pd.Categorical(
        daily_dynamics['day_of_week'],
        categories=weekdays_order,
        ordered=True
    )

    monthly_dynamics['month'] = pd.Categorical(
        monthly_dynamics['month'],
        categories=months_order,
        ordered=True
    )

    print(f"Анализ завершен за {time() - start_time:.1f} сек")
    return daily_dynamics, monthly_dynamics


# ========================================
# Визуализация динамики
# ========================================
def visualize_campaign_dynamics(daily_dynamics, monthly_dynamics):
    print("\nСоздание визуализаций динамики кампаний...")

    months_order = ['January', 'February', 'March', 'April', 'May', 'June',
                    'July', 'August', 'September', 'October', 'November', 'December']
    weekdays_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    russian_weekdays = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']

    # 1. График по дням недели
    plt.figure(figsize=(12, 6))
    sns.boxplot(
        data=daily_dynamics,
        x='day_of_week',
        y='campaigns_count',
        showfliers=False,
        color='lightgreen',
        order=weekdays_order
    )
    plt.title('Распределение количества создаваемых кампаний по дням недели')
    plt.xlabel('День недели')
    plt.ylabel('Количество кампаний')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'{PLOTS_DIR}/campaigns_per_weekday.png', dpi=300)
    plt.close()

    # 2. График по месяцам
    fig, ax = plt.subplots(figsize=(12, 6))
    monthly_sum = monthly_dynamics.groupby('month')['campaigns_count'].sum().reset_index()

    sns.barplot(
        data=monthly_sum,
        x='month',
        y='campaigns_count',
        color='skyblue',
        order=months_order,
        ax=ax
    )

    for p in ax.patches:
        ax.annotate(f"{int(p.get_height()):,}",
                    (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center',
                    xytext=(0, 10),
                    textcoords='offset points')

    ax.set_title('Количество созданных кампаний по месяцам', pad=20)
    ax.set_xlabel('Месяц')
    ax.set_ylabel('Количество кампаний')
    ax.tick_params(axis='x', rotation=45)
    fig.tight_layout()
    fig.savefig(f'{PLOTS_DIR}/campaigns_per_month_fixed.png', dpi=300)
    plt.close(fig)

    # 3. Тепловая карта
    fig, ax = plt.subplots(figsize=(12, 8))
    heatmap_data = daily_dynamics.pivot_table(
        index='week_of_year',
        columns='day_of_week',
        values='campaigns_count',
        aggfunc='mean'
    )

    sns.heatmap(
        heatmap_data,
        cmap='YlGnBu',
        annot=True,
        fmt='.1f',
        linewidths=.5,
        ax=ax
    )

    ax.set_title('Среднее количество создаваемых кампаний по дням недели и неделям года')
    ax.set_xlabel('День недели')
    ax.set_ylabel('Неделя года')
    fig.tight_layout()
    fig.savefig(f'{PLOTS_DIR}/campaigns_heatmap_weekday_week.png', dpi=300)
    plt.close(fig)

    print(f"Графики сохранены в папку {PLOTS_DIR}")


# ========================================
# Сохранение результатов
# ========================================
def save_campaign_dynamics(daily_dynamics, monthly_dynamics):
    print("\nСохранение результатов анализа динамики...")

    try:
        daily_dynamics.to_parquet(f"{OUTPUT_FILE}_daily.parquet", engine='pyarrow')
        monthly_dynamics.to_parquet(f"{OUTPUT_FILE}_monthly.parquet", engine='pyarrow')
        print(f"Результаты сохранены в {OUTPUT_FILE}_daily.parquet и {OUTPUT_FILE}_monthly.parquet")
    except Exception as e:
        print(f"Ошибка при сохранении: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Главная функция
# ========================================
if __name__ == '__main__':
    # Загрузка данных
    campaigns = load_campaign_data()

    # Анализ динамики
    daily_dynamics, monthly_dynamics = analyze_campaign_dynamics(campaigns)

    # Визуализация
    visualize_campaign_dynamics(daily_dynamics, monthly_dynamics)

    # Сохранение результатов
    save_campaign_dynamics(daily_dynamics, monthly_dynamics)

    print("\nАнализ динамики создания кампаний завершен!")