# main_runner.py
import logging
import os
import subprocess
import sys
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log')
    ]
)
logger = logging.getLogger('PipelineRunner')


def run_script(script_path):
    """Запускает скрипт с правильными путями"""
    script_name = Path(script_path).stem
    logger.info(f"Запуск скрипта: {script_name}")

    try:
        # Устанавливаем рабочую директорию - корень проекта
        project_root = Path(__file__).parent
        os.chdir(project_root)

        # Запускаем скрипт
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            capture_output=True,
            text=True
        )

        # Выводим логи
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
    """Запускает дашборд"""
    logger.info("Запуск дашборда...")
    try:
        project_root = Path(__file__).parent
        os.chdir(project_root)

        # Запускаем в отдельном процессе
        process = subprocess.Popen(
            [sys.executable, dashboard_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Открываем браузер
        import webbrowser
        webbrowser.open('http://127.0.0.1:8050')

        logger.info("Дашборд запущен. Остановите процесс Ctrl+C в консоли дашборда")
        return True
    except Exception as e:
        logger.error(f"Ошибка при запуске дашборда: {str(e)}")
        return False


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
        project_root / 'metrics' / 'campaign_activity_first_4_hours' / '4_hour_activity.py',
        project_root / 'metrics' / 'clicks_per_day_and_month_activity' / 'clicks_per_day_and_month.py',
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