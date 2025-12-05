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
    get_global_quests_task
)

# Relative path to data/ folder
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

# Map
TASK_CONFIG = {
    "pokemons_daily": {
        "func": get_global_pokemon_task,
        "file": os.path.join(DATA_DIR, 'global_pokes.json')
    },
    "pokemon_alltime": {
        "func": get_global_pokemon_task,
        "file": os.path.join(DATA_DIR, 'global_pokes_alltime.json'),
        "params": {
            "counter_type": "totals",
            "area": "global",
            "start_time": "3 years", # This will now be honored
            "end_time": "now",
            "mode": "sum",
            "interval": "hourly",
            "metric": "all",
            "pokemon_id": "all",
            "form_id": "all",
            "response_format": "json"
        }
    },
    "raids_daily": {
        "func": get_global_raids_task,
        "file": os.path.join(DATA_DIR, 'global_raids.json')
    },
    "raids_alltime": {
        "func": get_global_raids_task,
        "file": os.path.join(DATA_DIR, 'global_raids_alltime.json'),
        "params": {
            "counter_type": "totals",
            "area": "global",
            "start_time": "3 years",
            "end_time": "now",
            "mode": "sum",
            "interval": "hourly",
            "raid_pokemon": "all",
            "raid_form": "all",
            "raid_level": "all",
            "response_format": "json"
        }
    },
    "invasions": {
        "func": get_global_invasions_task,
        "file": os.path.join(DATA_DIR, 'global_invasions.json')
    },
    "invasions_alltime": {
        "func": get_global_invasions_task,
        "file": os.path.join(DATA_DIR, 'global_invasions_alltime.json'),
        "params": {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "3 years",
            "end_time": "now",
            "mode": "sum",
            "response_format": "json",
            "area": "global",
            "display_type": "all",
            "character": "all",
            "grunt": "all",
            "confirmed": "all"
        }
    },
    "quests": {
        "func": get_global_quests_task,
        "file": os.path.join(DATA_DIR, 'global_quests.json')
    },
    "quests_alltime": {
        "func": get_global_quests_task,
        "file": os.path.join(DATA_DIR, 'global_quests_alltime.json'),
        "params": {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "3 years",
            "end_time": "now",
            "mode": "sum",
            "response_format": "json",
            "area": "global",
            "with_ar": "all", "ar_type": "all", "reward_ar_type": "all",
            "reward_ar_item_id": "all", "reward_ar_item_amount": "all",
            "reward_ar_poke_id": "all", "reward_ar_poke_form": "all",
            "normal_type": "all", "reward_normal_type": "all",
            "reward_normal_item_id": "all", "reward_normal_item_amount": "all",
            "reward_normal_poke_id": "all", "reward_normal_poke_form": "all"
        }
    }
}

def update_global_stats_concurrently():
    """
    Executes all global stats tasks concurrently using a ThreadPoolExecutor.
    If 'params' are provided in TASK_CONFIG, they are passed to the function.
    """
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    logger.info("[Task] Starting Concurrent Global Stats Update...")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=len(TASK_CONFIG)) as executor:
        future_to_name = {}

        # Iterate over config and submit tasks conditionally
        for name, config in TASK_CONFIG.items():
            func = config["func"]
            task_params = config.get("params") # Check if params exist

            if task_params:
                # Pass specific params (e.g., for pokemon_alltime)
                future = executor.submit(func, params=task_params)
            else:
                # Use defaults (for daily tasks)
                future = executor.submit(func)

            future_to_name[future] = name

        for future in as_completed(future_to_name):
            task_name = future_to_name[future]
            config = TASK_CONFIG[task_name]

            try:
                final_data = future.result()
                if final_data:
                    with open(config["file"], 'w') as f:
                        json.dump(final_data, f, indent=2)

                    # Success Logging
                    count_val = final_data.get('total', 0)
                    # Handle quests slightly differently as it uses 'total_stops' sometimes
                    if 'total_stops' in final_data:
                         count_val = final_data.get('total_stops', 0)

                    logger.info(f"[Task] Global {task_name} updated. Count: {count_val}")
                else:
                    logger.info(f"[Task] Global {task_name} received no data.")

            except Exception as e:
                logger.error(f"[Task] Error updating global {task_name}: {e}")

    logger.info(f"[Task] Concurrent update finished in {time.time() - start_time:.2f}s.")

def run_schedule():
    while True:
        update_global_stats_concurrently()
        time.sleep(300)

def start_background_tasks():
    thread = threading.Thread(target=run_schedule, daemon=True)
    thread.start()
    logger.info("[System] All Global Stats background tasks started.")
