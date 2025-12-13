import sys
import os
import atexit
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from waitress import serve
from utils.logger import setup_logging, logger
from dashboard.app import app
from dashboard.tasks.global_tasks import start_background_tasks, stop_background_tasks
from dashboard.utils import precache_pokemon_icons, precache_reward_icons
import config as AppConfig

# Force Dash to complete page discovery and callback registration before Waitress starts
# This prevents "Callback function not found" errors on startup
import dash
_page_count = len(dash.page_registry) if dash.page_registry else 0

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

def start_dashboard():
    """
    Start the Dash Dashboard.
    """
    DEBUG_MODE = AppConfig.dashboard_debug_mode
    logger.info(f"🚀 Starting Dash Dashboard on http://{AppConfig.dashboard_ip}:{AppConfig.dashboard_port} with debug mode: {DEBUG_MODE}")

    logger.info(f"🔗 Connecting to API at {AppConfig.api_base_url}")
    # Ensure tasks only run in the main process (or the reloader's active process)
    should_run_tasks = (not DEBUG_MODE) or (os.environ.get("WERKZEUG_RUN_MAIN") == "true")

    if should_run_tasks:
        logger.info("📦 Pre-caching Pokemon icons...")
        precache_pokemon_icons(max_workers=20)
        precache_reward_icons(max_workers=20)

        logger.info("✅ Starting Background Tasks...")
        start_background_tasks()
        atexit.register(stop_background_tasks)

    if DEBUG_MODE:
        logger.info(f"🚀 Serving with Flask on {AppConfig.dashboard_ip}:{AppConfig.dashboard_port}")
        app.run(
            debug=DEBUG_MODE,
            port=f"{AppConfig.dashboard_port}",
            host=f"{AppConfig.dashboard_ip}"
        )
    else:
        logger.info(f"🚀 Serving with Waitress on {AppConfig.dashboard_ip}:{AppConfig.dashboard_port}")
        serve(
            app.server,
            host=AppConfig.dashboard_ip,
            port=AppConfig.dashboard_port,
            threads=AppConfig.dashboard_workers,
            backlog=2048
        )

if __name__ == "__main__":
    try:
        start_dashboard()
    except KeyboardInterrupt:
        stop_background_tasks()
        logger.info("Exiting due to key board interrupt.")




