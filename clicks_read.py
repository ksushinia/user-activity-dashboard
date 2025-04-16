import pandas as pd
from time import time
import sys

# ========================================
# Конфигурация
# ========================================
INPUT_FILE = 'C:/Users/79508\Desktop/4 семестри/Хакатон/Stream_telecom/data/clicks.csv'
OUTPUT_FILE = 'clicks_processed'  # Без расширения, добавим позже
CHUNK_SIZE = 50_000
LOG_EVERY = 5
SAMPLE_SIZE = 50  # Количество строк для вывода

DTYPES = {
    'uid': 'str',
    'member_id': 'int32',
    'campaign_id': 'int32',
    'region': 'int8',
    'OS': 'category',
    'browser': 'category',
    'device': 'category',
    'language': 'str'
}

BOT_KEYWORDS = ['bot', 'axios', 'spider', 'crawler']
VALID_DEVICES = ['Android', 'iPhone', 'Generic_Android', 'Samsung']


# ========================================
# Обработка
# ========================================
def process_chunks():
    start_time = time()

    result_chunks = []
    stats = {
        'total_rows': 0,
        'filtered_rows': 0,
        'regions': pd.Series(dtype='int64'),
        'campaigns': {}
    }

    try:
        for i, chunk in enumerate(pd.read_csv(
                INPUT_FILE,
                chunksize=CHUNK_SIZE,
                dtype=DTYPES,
                parse_dates=['click_time', 'click_date'],
                engine='c',
                memory_map=True
        )):
            is_bot = chunk['browser'].str.contains('|'.join(BOT_KEYWORDS), case=False, na=False)
            is_valid_device = chunk['device'].isin(VALID_DEVICES)
            filtered_chunk = chunk[~is_bot & is_valid_device]

            result_chunks.append(filtered_chunk)
            stats['total_rows'] += len(chunk)
            stats['filtered_rows'] += len(filtered_chunk)
            stats['regions'] = stats['regions'].add(
                filtered_chunk['region'].value_counts(),
                fill_value=0
            )

            if (i + 1) % LOG_EVERY == 0:
                elapsed = time() - start_time
                print(
                    f"[Чанк {i + 1}] "
                    f"Обработано: {stats['total_rows']:,} строк, "
                    f"Фильтр: {stats['filtered_rows']:,} ({stats['filtered_rows'] / stats['total_rows']:.1%}), "
                    f"Время: {elapsed:.1f} сек"
                )

        df_final = pd.concat(result_chunks, ignore_index=True)

        print("\n" + "=" * 50)
        print(f"ОБРАБОТКА ЗАВЕРШЕНА")
        print(f"Всего строк:    {stats['total_rows']:,}")
        print(f"После фильтра:  {stats['filtered_rows']:,} ({stats['filtered_rows'] / stats['total_rows']:.1%})")
        print(f"Топ-5 регионов:\n{stats['regions'].nlargest(5)}")
        print(f"Общее время:    {time() - start_time:.1f} сек")
        print("=" * 50 + "\n")

        # Вывод первых 20 строк для проверки
        print("Первые 20 строк обработанных данных:")
        print(df_final.head(SAMPLE_SIZE).to_string())
        print("\n" + "=" * 50)

        return df_final

    except Exception as e:
        print(f"Ошибка при обработке: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Сохранение с проверкой доступных форматов
# ========================================
def save_data(df, base_filename):
    try:
        # Пробуем сохранить в Parquet (предпочтительно)
        try:
            import pyarrow
            parquet_file = f"{base_filename}.parquet"
            df.to_parquet(parquet_file, engine='pyarrow')
            print(f"Данные сохранены в {parquet_file} (формат Parquet)")
            return
        except ImportError:
            pass

        # Если PyArrow не установлен, пробуем fastparquet
        try:
            import fastparquet
            parquet_file = f"{base_filename}.parquet"
            df.to_parquet(parquet_file, engine='fastparquet')
            print(f"Данные сохранены в {parquet_file} (формат Parquet через fastparquet)")
            return
        except ImportError:
            pass

        # Если оба варианта с Parquet не сработали, сохраняем в сжатый CSV
        csv_file = f"{base_filename}.csv.gz"
        df.to_csv(csv_file, compression='gzip', index=False)
        print(f"Данные сохранены в {csv_file} (формат CSV с gzip-сжатием)")

    except Exception as e:
        print(f"Критическая ошибка при сохранении: {str(e)}", file=sys.stderr)
        sys.exit(1)


# ========================================
# Главная функция
# ========================================
if __name__ == '__main__':
    print("Начало обработки...")
    df = process_chunks()

    print("\nСохранение результатов...")
    save_data(df, OUTPUT_FILE)

    print("\nГотово! Скрипт завершил работу.")