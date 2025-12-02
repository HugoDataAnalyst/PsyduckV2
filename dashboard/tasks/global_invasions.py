import threading
import time
import json
import os
import sys
from collections import defaultdict
from utils.logger import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from dashboard.utils import get_invasions_stats

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DATA_FILE = os.path.join(DATA_DIR, 'global_invasions.json')

def update_global_invasions():
    """
    Fetches stats for all areas (global) for the last 24 hours.
    Aggregates:
      - Total Invasions
      - Confirmed Invasions (key "1")
      - Unconfirmed Invasions (key "0")
    Saves to a JSON file.
    """
    logger.info("[Task] Fetching Global Invasion Stats...")

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
            "display_type": "all",
            "character": "all",
            "grunt": "all",
            "confirmed": "all"
        }

        # Fetch data
        raw_data = get_invasions_stats("counter", params)

        if not raw_data:
            logger.info("[Task] No data received for global invasions.")
            return

        # Initialize counters
        total_invasions = 0
        confirmed_count = 0
        unconfirmed_count = 0

        # Iterate over each area in the response
        for area_name, content in raw_data.items():
            if not isinstance(content, dict):
                continue

            stats = content.get('data', {})
            if not stats:
                continue

            # 1. Sum total
            total_invasions += stats.get('total', 0)

            # 2. Analyze confirmed dictionary
            # "0" = False (Unconfirmed), "1" = True (Confirmed)
            confirmed_data = stats.get('confirmed', {})
            if isinstance(confirmed_data, dict):
                confirmed_count += confirmed_data.get('1', 0)
                unconfirmed_count += confirmed_data.get('0', 0)

        # Construct final structure
        final_data = {
            "total": total_invasions,
            "stats": {
                "confirmed": confirmed_count,
                "unconfirmed": unconfirmed_count
            },
            "last_updated": time.time()
        }

        # Save to file
        with open(DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=2)

        logger.info(f"[Task] Global invasions updated. Total: {total_invasions}, Confirmed: {confirmed_count}")

    except Exception as e:
        logger.error(f"[Task] Error updating global invasions: {e}")

def run_schedule():
    """Runs the update task immediately, then every 5 minutes."""
    while True:
        update_global_invasions()
        time.sleep(300)

def start_background_task_invasions():
    """Starts the background thread."""
    thread = threading.Thread(target=run_schedule, daemon=True)
    thread.start()
    logger.info("[System] Global Invasion Stats background task started.")
