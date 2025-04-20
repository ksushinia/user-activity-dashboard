import pandas as pd
import os
import pyarrow
import fastparquet
from pathlib import Path
import logging
from time import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
CONFIG = {
    'input_files': {
        'clicks': {
            'path': 'C:/Users/79508/Desktop/4 семестри/Хакатон/Stream_telecom/data/clicks.csv',
            'dtypes': {
                'uid': 'string',
                'member_id': 'int32',
                'campaign_id': 'int32',
                'region': 'int8',
                'OS': 'category',
                'browser': 'category',
                'device': 'category',
                'language': 'string'
            },
            'parse_dates': ['click_time', 'click_date'],
            'filters': {
                'bot_keywords': ['bot', 'axios', 'spider', 'crawler'],
                'valid_devices': ['Android', 'iPhone', 'Generic_Android', 'Samsung']
            },
            'chunk_size': 50000
        },
        'regions': {
            'path': 'C:/Users/79508/Desktop/4 семестри/Хакатон/Stream_telecom/data/regions.csv',
            'dtypes': {
                'region_id': 'int8',
                'region_name': 'string'
            },
            'chunk_size': None
        },
        'campaign': {
            'path': 'C:/Users/79508/Desktop/4 семестри/Хакатон/Stream_telecom/data/campaign.csv',
            'dtypes': {
                'id': 'int32',
                'name': 'string',
                'created_at': 'string'  # Изменено с int64 на string
            },
            'parse_dates': ['created_at'],  # Добавлено для парсинга даты
            'chunk_size': None
        }
    },
    'output_dir': 'processed_data',
    'parquet_engine': 'pyarrow',
    'compression': 'snappy',
    'log_every': 5
}


def process_file(file_name, file_config):
    """Обработка одного файла"""
    logger.info(f"Начало обработки файла {file_name}...")
    start_time = time()

    output_path = Path(CONFIG['output_dir']) / f"{file_name}_processed.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        logger.info(f"Файл {output_path} уже существует, пропускаем обработку")
        return pd.read_parquet(output_path)

    # Параметры чтения
    read_params = {
        'dtype': file_config['dtypes'],
        'engine': 'c'
    }

    if 'parse_dates' in file_config:
        read_params['parse_dates'] = file_config['parse_dates']

    # Обработка по чанкам или целиком
    if file_config.get('chunk_size'):
        result_chunks = []
        total_rows = 0

        for i, chunk in enumerate(pd.read_csv(
                file_config['path'],
                chunksize=file_config['chunk_size'],
                **read_params
        )):
            if file_name == 'clicks':
                is_bot = chunk['browser'].str.contains(
                    '|'.join(file_config['filters']['bot_keywords']),
                    case=False,
                    na=False
                )
                is_valid_device = chunk['device'].isin(file_config['filters']['valid_devices'])
                chunk = chunk[~is_bot & is_valid_device].copy()

            for col, dtype in file_config['dtypes'].items():
                if col in chunk.columns:
                    chunk[col] = chunk[col].astype(dtype)

            result_chunks.append(chunk)
            total_rows += len(chunk)

            if (i + 1) % CONFIG['log_every'] == 0:
                logger.info(f"Обработано {i + 1} чанков | Строк: {total_rows:,}")

        df = pd.concat(result_chunks, ignore_index=True)
    else:
        df = pd.read_csv(file_config['path'], **read_params)

        # Для campaign преобразуем created_at в datetime, если он еще не преобразован
        if file_name == 'campaign' and 'created_at' in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df['created_at']):
                df['created_at'] = pd.to_datetime(df['created_at'])

        logger.info(f"Файл {file_name} загружен полностью, строк: {len(df):,}")

    # Сохранение в Parquet
    df.to_parquet(
        output_path,
        engine=CONFIG['parquet_engine'],
        compression=CONFIG['compression']
    )

    elapsed = time() - start_time
    logger.info(
        f"Обработка {file_name} завершена\n"
        f"Строк: {len(df):,}\n"
        f"Время: {elapsed:.1f} сек\n"
        f"Размер файла: {output_path.stat().st_size / (1024 ** 2):.2f} MB"
    )

    return df


def process_all_data():
    """Обработка всех файлов"""
    start_time = time()
    results = {}

    for file_name, file_config in CONFIG['input_files'].items():
        try:
            results[file_name] = process_file(file_name, file_config)
        except Exception as e:
            logger.error(f"Ошибка при обработке файла {file_name}: {str(e)}", exc_info=True)
            raise

    elapsed = time() - start_time
    logger.info(f"\nВсе файлы обработаны за {elapsed:.1f} сек")

    return results


if __name__ == '__main__':
    process_all_data()