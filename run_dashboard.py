import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from utils.logger import setup_logging, logger
from dashboard.app import app
from dashboard.tasks.global_pokemons import start_background_task as start_pokemon_task
from dashboard.tasks.global_raids import start_background_task_raids as start_raids_task
from dashboard.tasks.global_invasions import start_background_task_invasions as start_invasions_task
from dashboard.tasks.global_quests import start_background_task_quests as start_quests_task
from dashboard.tasks.global_tasks import start_background_tasks
from dashboard.utils import precache_pokemon_icons
import config as AppConfig

if __name__ == "__main__":
    setup_logging(
        AppConfig.log_level_dashboard,
        {
            "to_file": AppConfig.log_file_dashboard,
            "file_path": "logs/dashboard_psyduckv2.log",
            "rotation": "5 MB",
            "keep_total": 5,
            "compression": "gz",
            "show_file": True,
            "show_function": True,
        },
    )

    DEBUG_MODE = AppConfig.dashboard_debug_mode
    logger.info(f"🚀 Starting Dash Dashboard on http://{AppConfig.dashboard_ip}:{AppConfig.dashboard_port} with debug mode: {DEBUG_MODE}")

    logger.info(f"🔗 Connecting to API at {AppConfig.api_base_url}")

    if not DEBUG_MODE or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        logger.info("📦 Pre-caching Pokemon icons...")
        precache_pokemon_icons(max_workers=20)

        logger.info("✅ Starting Background Tasks...")
        start_background_tasks()
        #start_pokemon_task()
        #start_raids_task()
        #start_invasions_task()
        #start_quests_task()

    app.run(debug=DEBUG_MODE, port=f"{AppConfig.dashboard_port}", host=f"{AppConfig.dashboard_ip}")
