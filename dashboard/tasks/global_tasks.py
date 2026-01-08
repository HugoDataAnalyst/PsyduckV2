import threading
import time
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from utils.logger import logger
from dashboard.utils import (
    get_global_pokemon_task,
    get_global_raids_task,
    get_global_invasions_task,
    get_global_quests_task,
    get_global_pokestops_task,
    get_global_areas_task,
)

# Relative path to data/ folder
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

# Default intervals (in seconds)
DEFAULT_INTERVAL = 3600        # 1 hour for most tasks
DAILY_INTERVAL = 86400         # 24 hours for alltime/historical tasks

# Task categories for startup ordering
DAILY_TASKS = ["areas", "pokestops", "pokemons_daily", "raids_daily", "invasions_daily", "quests_daily"]
ALLTIME_TASKS = ["pokemon_alltime", "raids_alltime", "invasions_alltime", "quests_alltime"]

# Delay between daily and alltime tasks on startup (seconds)
ALLTIME_STARTUP_DELAY = 30

# Map
TASK_CONFIG = {
    # Fast refresh tasks
    "areas": {
        "func": get_global_areas_task,
        "file": os.path.join(DATA_DIR, 'global_areas.json'),
        "task_interval": DEFAULT_INTERVAL
    },
    "pokestops": {
        "func": get_global_pokestops_task,
        "file": os.path.join(DATA_DIR, 'global_pokestops.json'),
        "task_interval": DEFAULT_INTERVAL
    },
    "pokemons_daily": {
        "func": get_global_pokemon_task,
        "file": os.path.join(DATA_DIR, 'global_pokes.json'),
        "task_interval": DEFAULT_INTERVAL
    },
    "raids_daily": {
        "func": get_global_raids_task,
        "file": os.path.join(DATA_DIR, 'global_raids.json'),
        "task_interval": DEFAULT_INTERVAL
    },
    "invasions_daily": {
        "func": get_global_invasions_task,
        "file": os.path.join(DATA_DIR, 'global_invasions.json'),
        "params": {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "24 hours",
            "end_time": "now",
            "mode": "grouped",
            "response_format": "json",
            "area": "global",
            "display_type": "all",
            "character": "all",
            "grunt": "all",
            "confirmed": "all"
        },
        "task_interval": DEFAULT_INTERVAL
    },
    "quests_daily": {
        "func": get_global_quests_task,
        "file": os.path.join(DATA_DIR, 'global_quests.json'),
        "params": {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "24 hours",
            "end_time": "now",
            "mode": "grouped",
            "response_format": "json",
            "area": "global",
            "with_ar": "all", "ar_type": "all", "reward_ar_type": "all",
            "reward_ar_item_id": "all", "reward_ar_item_amount": "all",
            "reward_ar_poke_id": "all", "reward_ar_poke_form": "all",
            "normal_type": "all", "reward_normal_type": "all",
            "reward_normal_item_id": "all", "reward_normal_item_amount": "all",
            "reward_normal_poke_id": "all", "reward_normal_poke_form": "all"
        },
        "task_interval": DEFAULT_INTERVAL
    },
    # Slow refresh tasks
    "pokemon_alltime": {
        "func": get_global_pokemon_task,
        "file": os.path.join(DATA_DIR, 'global_pokes_alltime.json'),
        "params": {
            "counter_type": "totals",
            "area": "global",
            "start_time": "26280 hours",
            "end_time": "now",
            "mode": "sum",
            "interval": "hourly",
            "metric": "all",
            "pokemon_id": "all",
            "form_id": "all",
            "response_format": "json"
        },
        "task_interval": DAILY_INTERVAL
    },
    "raids_alltime": {
        "func": get_global_raids_task,
        "file": os.path.join(DATA_DIR, 'global_raids_alltime.json'),
        "params": {
            "counter_type": "totals",
            "area": "global",
            "start_time": "26280 hours",
            "end_time": "now",
            "mode": "sum",
            "interval": "hourly",
            "raid_pokemon": "all",
            "raid_form": "all",
            "raid_level": "all",
            "response_format": "json"
        },
        "task_interval": DAILY_INTERVAL
    },
    "invasions_alltime": {
        "func": get_global_invasions_task,
        "file": os.path.join(DATA_DIR, 'global_invasions_alltime.json'),
        "params": {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "26280 hours",
            "end_time": "now",
            "mode": "grouped",
            "response_format": "json",
            "area": "global",
            "display_type": "all",
            "character": "all",
            "grunt": "all",
            "confirmed": "all"
        },
        "task_interval": DAILY_INTERVAL
    },
    "quests_alltime": {
        "func": get_global_quests_task,
        "file": os.path.join(DATA_DIR, 'global_quests_alltime.json'),
        "params": {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "26280 hours",
            "end_time": "now",
            "mode": "grouped",
            "response_format": "json",
            "area": "global",
            "with_ar": "all", "ar_type": "all", "reward_ar_type": "all",
            "reward_ar_item_id": "all", "reward_ar_item_amount": "all",
            "reward_ar_poke_id": "all", "reward_ar_poke_form": "all",
            "normal_type": "all", "reward_normal_type": "all",
            "reward_normal_item_id": "all", "reward_normal_item_amount": "all",
            "reward_normal_poke_id": "all", "reward_normal_poke_form": "all"
        },
        "task_interval": DAILY_INTERVAL
    }
}

class BackgroundRunner:
    """
    Manages background tasks with per-task intervals and graceful termination.
    """
    def __init__(self, check_interval=60):
        """
        Args:
            check_interval: How often (seconds) to check if any tasks are due to run.
        """
        self.check_interval = check_interval
        self._stop_event = threading.Event()
        self._thread = None
        self._last_run_times = {}  # {task_name: timestamp}

    def _get_due_tasks(self, force_all=False):
        """
        Returns list of task names that are due to run based on their individual intervals.

        Args:
            force_all: If True, returns all tasks (used for initial run)
        """
        if force_all:
            return list(TASK_CONFIG.keys())

        current_time = time.time()
        due_tasks = []

        for name, config in TASK_CONFIG.items():
            task_interval = config.get("task_interval", DEFAULT_INTERVAL)
            last_run = self._last_run_times.get(name, 0)

            if (current_time - last_run) >= task_interval:
                due_tasks.append(name)

        return due_tasks

    def update_tasks_concurrently(self, task_names):
        """
        Executes specified tasks concurrently using a ThreadPoolExecutor.

        Args:
            task_names: List of task names to execute
        """
        if not task_names:
            return

        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        logger.info(f"[Task] Running {len(task_names)} task(s): {', '.join(task_names)}")
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=len(task_names)) as executor:
            future_to_name = {}

            for name in task_names:
                if self._stop_event.is_set():
                    logger.info("[Task] Stop signal received. Aborting remaining tasks.")
                    executor.shutdown(wait=False, cancel_futures=True)
                    return

                config = TASK_CONFIG[name]
                func = config["func"]
                task_params = config.get("params")

                if task_params:
                    future = executor.submit(func, params=task_params)
                else:
                    future = executor.submit(func)

                future_to_name[future] = name

            for future in as_completed(future_to_name):
                if self._stop_event.is_set():
                    break

                task_name = future_to_name[future]
                config = TASK_CONFIG[task_name]

                try:
                    final_data = future.result()
                    if final_data:
                        with open(config["file"], 'w', encoding='utf-8') as f:
                            json.dump(final_data, f, indent=2, ensure_ascii=False)

                        # Success Logging
                        count_val = final_data.get('total', 0)
                        if 'total_pokestops' in final_data:
                            count_val = final_data.get('total_pokestops', 0)
                        elif 'count' in final_data:
                            count_val = final_data.get('count', 0)

                        task_interval = config.get("task_interval", DEFAULT_INTERVAL)
                        logger.info(f"[Task] {task_name} updated. Count: {count_val} (next run in {task_interval}s)")

                        # Update last run time on success
                        self._last_run_times[task_name] = time.time()
                    else:
                        logger.info(f"[Task] {task_name} received no data.")
                        # Still update last run time to avoid hammering on failures
                        self._last_run_times[task_name] = time.time()

                except Exception as e:
                    logger.error(f"[Task] Error updating {task_name}: {e}")
                    # Update last run time even on error to avoid infinite retry loops
                    self._last_run_times[task_name] = time.time()

        logger.info(f"[Task] Batch finished in {time.time() - start_time:.2f}s.")

    def _run_schedule(self):
        """
        Main loop that checks for due tasks at regular intervals.
        On startup: runs daily tasks first, waits 1 minute, then runs alltime tasks.
        """
        # Startup: Run daily tasks first
        daily_tasks = [t for t in DAILY_TASKS if t in TASK_CONFIG]
        if daily_tasks:
            logger.info(f"[Task] Initial run: executing {len(daily_tasks)} daily tasks...")
            self.update_tasks_concurrently(daily_tasks)

        # Check if we should stop before waiting
        if self._stop_event.is_set():
            return

        # Wait before running alltime tasks
        alltime_tasks = [t for t in ALLTIME_TASKS if t in TASK_CONFIG]
        if alltime_tasks:
            logger.info(f"[Task] Waiting {ALLTIME_STARTUP_DELAY}s before running alltime tasks...")
            if self._stop_event.wait(ALLTIME_STARTUP_DELAY):
                return  # Stop event was set during wait

            logger.info(f"[Task] Running {len(alltime_tasks)} alltime tasks...")
            self.update_tasks_concurrently(alltime_tasks)

        while not self._stop_event.is_set():
            # Wait for check_interval OR until stop event is set
            if self._stop_event.wait(self.check_interval):
                break

            # Check which tasks are due and run them
            due_tasks = self._get_due_tasks()
            if due_tasks:
                self.update_tasks_concurrently(due_tasks)

        logger.info("[System] BackgroundRunner loop has exited cleanly.")

    def start(self):
        """Start the background thread."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning("[System] Background tasks are already running.")
            return

        self._stop_event.clear()
        self._last_run_times = {}  # Reset on start
        self._thread = threading.Thread(target=self._run_schedule, daemon=True)
        self._thread.start()
        logger.info("[System] Background task scheduler started.")

    def stop(self):
        """Signal the background thread to stop and wait for it to finish."""
        logger.info("[System] Stopping background tasks...")
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
            logger.info("[System] Background tasks stopped.")

# Create a singleton instance - checks every 60 seconds for due tasks
_runner = BackgroundRunner(check_interval=60)

def start_background_tasks():
    _runner.start()

def stop_background_tasks():
    _runner.stop()
