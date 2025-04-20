from time import time
import pandas as pd
from pathlib import Path
import sys

# ================== ИСПРАВЛЕННЫЙ БЛОК ==================
# Получаем абсолютный путь к корню проекта
project_root = Path(__file__).parent.parent
(project_root / 'processed_data').mkdir(parents=True, exist_ok=True)
INPUT_FILE = project_root / 'data' / 'regions.csv'  # Абсолютный путь
OUTPUT_FILE = project_root / 'processed_data' / 'regions_processed'  # Без расширения
# ========================================================

CHUNK_SIZE = 50_000
LOG_EVERY = 5
SAMPLE_SIZE = 50

DTYPES = {
    'region_id': 'int32',
    'name': 'str'
}


# ========================================
# Обработка
# ========================================
def process_chunks():
    start_time = time()

    result_chunks = []
    stats = {
        'total_rows': 0,
        'filtered_rows': 0,
        'unique_regions': 0
    }

    try:
        # Проверка существования файла
        if not INPUT_FILE.exists():
            raise FileNotFoundError(f"Файл {INPUT_FILE} не найден")

        for i, chunk in enumerate(pd.read_csv(
                INPUT_FILE,
                chunksize=CHUNK_SIZE,
                dtype=DTYPES,
                engine='c',
                memory_map=True,
                encoding='utf-8',
                on_bad_lines='warn'
        )):
            # Обработка чанка
            result_chunks.append(chunk)
            stats['total_rows'] += len(chunk)
            stats['filtered_rows'] += len(chunk)

            if (i + 1) % LOG_EVERY == 0:
                elapsed = time() - start_time
                print(
                    f"[Чанк {i + 1}] "
                    f"Обработано: {stats['total_rows']:,} строк, "
                    f"Время: {elapsed:.1f} сек"
                )

        df_final = pd.concat(result_chunks, ignore_index=True)
        stats['unique_regions'] = df_final['region_id'].nunique()

        print("\n" + "=" * 50)
        print(f"ОБРАБОТКА ЗАВЕРШЕНА")
        print(f"Всего строк:       {stats['total_rows']:,}")
        print(f"Уникальных регионов: {stats['unique_regions']:,}")
        print(f"Общее время:       {time() - start_time:.1f} сек")
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
    print("Начало обработки regions.csv...")
    df = process_chunks()

    print("\nСохранение результатов...")
    save_data(df, OUTPUT_FILE)

    print("\nГотово! Скрипт завершил работу.")