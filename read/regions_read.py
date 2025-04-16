import pandas as pd
import os
from pathlib import Path
import sys
import logging
from time import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================== КОНФИГУРАЦИЯ С АБСОЛЮТНЫМИ ПУТЯМИ ==================
# Получаем корень проекта (2 уровня вверх от расположения скрипта)
project_root = Path(__file__).parent.parent

# Абсолютные пути к файлам
INPUT_FILE = project_root / 'data' / 'regions.csv'
OUTPUT_FILE = project_root / 'processed_data' / 'regions_processed'  # Без расширения
# =======================================================================

CHUNK_SIZE = 50_000
LOG_EVERY = 5
SAMPLE_SIZE = 50

DTYPES = {
    'region_id': 'int32',
    'name': 'str'
}


def check_file_exists(file_path):
    """Проверяет существование файла"""
    if not file_path.exists():
        logger.error(f"Файл не найден: {file_path}")
        logger.info(f"Текущая рабочая директория: {os.getcwd()}")
        logger.info("Убедитесь, что файл находится в папке data/")
        return False
    return True


def process_chunks():
    """Обработка данных по чанкам"""
    start_time = time()

    # Проверяем наличие файла
    if not check_file_exists(INPUT_FILE):
        sys.exit(1)

    result_chunks = []
    stats = {
        'total_rows': 0,
        'filtered_rows': 0,
        'unique_regions': 0
    }

    try:
        logger.info(f"Начало обработки файла: {INPUT_FILE}")

        for i, chunk in enumerate(pd.read_csv(
                INPUT_FILE,
                chunksize=CHUNK_SIZE,
                dtype=DTYPES,
                engine='c',
                memory_map=True
        )):
            # В данном случае фильтрация не требуется, просто сохраняем все данные
            result_chunks.append(chunk)
            stats['total_rows'] += len(chunk)
            stats['filtered_rows'] += len(chunk)  # Все строки сохраняются

            if (i + 1) % LOG_EVERY == 0:
                elapsed = time() - start_time
                logger.info(
                    f"[Чанк {i + 1}] "
                    f"Обработано: {stats['total_rows']:,} строк, "
                    f"Время: {elapsed:.1f} сек"
                )

        df_final = pd.concat(result_chunks, ignore_index=True)
        stats['unique_regions'] = df_final['region_id'].nunique()

        logger.info("\n" + "=" * 50)
        logger.info(f"ОБРАБОТКА ЗАВЕРШЕНА")
        logger.info(f"Всего строк:       {stats['total_rows']:,}")
        logger.info(f"Уникальных регионов: {stats['unique_regions']:,}")
        logger.info(f"Общее время:       {time() - start_time:.1f} сек")
        logger.info("=" * 50)

        # Вывод первых строк для проверки
        logger.info("\nПервые 20 строк обработанных данных:")
        logger.info(df_final.head(SAMPLE_SIZE).to_string())

        return df_final

    except Exception as e:
        logger.error(f"Ошибка при обработке: {str(e)}", exc_info=True)
        sys.exit(1)


def save_data(df, base_filename):
    """Сохранение данных с проверкой доступных форматов"""
    try:
        # Создаем папку processed_data, если её нет
        output_dir = project_root / 'processed_data'
        output_dir.mkdir(parents=True, exist_ok=True)

        # Пробуем сохранить в Parquet (предпочтительно)
        try:
            import pyarrow
            parquet_file = f"{base_filename}.parquet"
            df.to_parquet(parquet_file, engine='pyarrow')
            logger.info(f"Данные сохранены в {parquet_file} (формат Parquet)")
            return
        except ImportError:
            pass

        # Если PyArrow не установлен, пробуем fastparquet
        try:
            import fastparquet
            parquet_file = f"{base_filename}.parquet"
            df.to_parquet(parquet_file, engine='fastparquet')
            logger.info(f"Данные сохранены в {parquet_file} (формат Parquet через fastparquet)")
            return
        except ImportError:
            pass

        # Если оба варианта с Parquet не сработали, сохраняем в сжатый CSV
        csv_file = f"{base_filename}.csv.gz"
        df.to_csv(csv_file, compression='gzip', index=False)
        logger.info(f"Данные сохранены в {csv_file} (формат CSV с gzip-сжатием)")

    except Exception as e:
        logger.error(f"Критическая ошибка при сохранении: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    logger.info(f"Запуск обработки файла: {INPUT_FILE}")
    logger.info(f"Проверка существования файла: {INPUT_FILE.exists()}")

    df = process_chunks()

    logger.info("\nСохранение результатов...")
    save_data(df, OUTPUT_FILE)

    logger.info("\nГотово! Скрипт завершил работу.")