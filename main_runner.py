# main_runner.py
import logging
import os
import subprocess
import sys
import time
import socket
from pathlib import Path
import webbrowser

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log', encoding='utf-8')  # Добавьте encoding
    ]
)
logger = logging.getLogger('PipelineRunner')


def find_free_port(start_port=8050, max_attempts=100):
    """Находит свободный порт для дашборда"""
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', port))
                return port
        except socket.error:
            continue
    raise RuntimeError(f"Не удалось найти свободный порт в диапазоне {start_port}-{start_port + max_attempts}")


def run_script(script_path):
    """Запускает скрипт с правильными путями"""
    script_name = Path(script_path).stem
    logger.info(f"Запуск скрипта: {script_name}")

    try:
        project_root = Path(__file__).parent
        os.chdir(project_root)

        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            env={**os.environ, 'PYTHONPATH': str(project_root)}
        )

        if result.stdout:
            logger.info(result.stdout)
        if result.stderr:
            logger.error(result.stderr)

        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Ошибка в скрипте {script_name}:\n{e.stderr}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {str(e)}")
        return False


def run_dashboard(dashboard_path):
    """Запускает дашборд с автоматическим определением порта"""
    logger.info("Запуск дашборда...")
    try:
        project_root = Path(__file__).parent
        os.chdir(project_root)

        port = find_free_port()
        dashboard_url = f'http://127.0.0.1:{port}'

        # Запускаем процесс с явным указанием UTF-8
        process = subprocess.Popen(
            [sys.executable, dashboard_path, '--port', str(port)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            bufsize=1,
            universal_newlines=True
        )

        # Даем больше времени на запуск (до 30 секунд)
        for _ in range(30):
            if is_port_open(port):
                webbrowser.open(dashboard_url)
                logger.info(f"Дашборд запущен по адресу: {dashboard_url}")

                # Чтение вывода в реальном времени
                def log_stream(stream, logger_func):
                    while True:
                        line = stream.readline()
                        if not line:
                            break
                        logger_func(line.strip())

                import threading
                threading.Thread(target=log_stream, args=(process.stdout, logger.info), daemon=True).start()
                threading.Thread(target=log_stream, args=(process.stderr, logger.error), daemon=True).start()

                return True
            time.sleep(1)

        logger.error(f"Дашборд не запустился на порту {port} после 30 секунд ожидания")
        process.terminate()
        return False

    except Exception as e:
        logger.error(f"Ошибка при запуске дашборда: {str(e)}", exc_info=True)
        if 'process' in locals():
            process.terminate()
        return False


def is_port_open(port):
    """Проверяет доступность порта"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0


def main():
    logger.info("=" * 50)
    logger.info("ЗАПУСК ДАННЫХ ПАЙПЛАЙНА")
    logger.info("=" * 50)

    # Определяем корень проекта
    project_root = Path(__file__).parent

    # Порядок выполнения скриптов
    scripts = [
        project_root / 'read' / 'clicks_read.py',
        project_root / 'read' / 'campaign_read.py',
        project_root / 'read' / 'regions_read.py',
        project_root / 'data_processor.py',
        project_root / 'metrics' / 'activity_by_timezone' / 'activity_by_timezone.py',
        project_root / 'metrics' / 'campaign dinamics' / 'campaign_dinamics.py',
        project_root / 'metrics' / 'campaign_activity_first_4_hours' / '4_hour_activity.py',
        project_root / 'metrics' / 'clicks_per_day_and_month_activity' / 'clicks_per_day_and_month.py',
        project_root / 'metrics' / 'geographic_pie_chart' / 'geographic_pie_chart.py',
        project_root / 'metrics' / 'geography distribution' / 'geography distribution.py',
        project_root / 'metrics' / 'response_analysis' / 'response analysis.py',
        project_root / 'metrics' / 'time_optimizer' / 'time_optimizer.py',
        project_root / 'dashboard.py'
    ]

    # Запускаем все скрипты кроме дашборда
    for script in scripts[:-1]:
        if not run_script(script):
            logger.error("Прерывание выполнения из-за ошибки")
            return

    # Запускаем дашборд последним
    run_dashboard(scripts[-1])

if __name__ == '__main__':
    main()
