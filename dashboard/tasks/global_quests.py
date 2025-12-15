import threading
import time
import json
import os
import sys
from collections import defaultdict
from utils.logger import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from dashboard.utils import get_quests_stats

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DATA_FILE = os.path.join(DATA_DIR, 'global_quests.json')

def update_global_quests():
    """
    Fetches stats for all areas (global) for the last 24 hours.
    Aggregates:
      - Total Scanned Stops
      - Total AR Quests
      - Total Normal Quests
    Saves to a JSON file.
    """
    logger.info("[Task] Fetching Global Quest Stats...")

    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    try:
        # Request parameters
        params = {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "24 hours",
            "end_time": "now",
            "mode": "sum",
            "response_format": "json",
            "area": "global",
            # Filtering parameters
            "with_ar": "all",
            "ar_type": "all",
            "reward_ar_type": "all",
            "reward_ar_item_id": "all",
            "reward_ar_item_amount": "all",
            "reward_ar_poke_id": "all",
            "reward_ar_poke_form": "all",
            "normal_type": "all",
            "reward_normal_type": "all",
            "reward_normal_item_id": "all",
            "reward_normal_item_amount": "all",
            "reward_normal_poke_id": "all",
            "reward_normal_poke_form": "all"
        }

        # Fetch data
        raw_data = get_quests_stats("counter", params)

        if not raw_data:
            logger.info("[Task] No data received for global quests.")
            return

        # Initialize counters
        total_scanned_stops = 0
        total_ar_quests = 0
        total_normal_quests = 0

        # Iterate over each area in the response
        for area_name, content in raw_data.items():
            if not isinstance(content, dict):
                continue

            stats = content.get('data', {})
            if not stats:
                continue

            # 1. Sum total
            total_scanned_stops += stats.get('total', 0)

            # 2. Analyze quest_mode dictionary
            quest_modes = stats.get('quest_mode', {})
            if isinstance(quest_modes, dict):
                # Sum AR Quests
                total_ar_quests += quest_modes.get('ar', 0)
                # Sum Normal Quests
                total_normal_quests += quest_modes.get('normal', 0)

        # Construct final structure
        final_data = {
            "total_stops": total_scanned_stops,
            "quests": {
                "ar": total_ar_quests,
                "normal": total_normal_quests
            },
            "last_updated": time.time()
        }

        # Save to file
        with open(DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=2)

        logger.info(f"[Task] Global quests updated. Stops: {total_scanned_stops}, AR: {total_ar_quests}, Normal: {total_normal_quests}")

    except Exception as e:
        logger.error(f"[Task] Error updating global quests: {e}")

def run_schedule():
    """Runs the update task immediately, then every 5 minutes."""
    while True:
        update_global_quests()
        time.sleep(300)

def start_background_task_quests():
    """Starts the background thread."""
    thread = threading.Thread(target=run_schedule, daemon=True)
    thread.start()
    logger.info("[System] Global Quest Stats background task started.")
