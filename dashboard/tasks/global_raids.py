import threading
import time
import json
import os
import sys
from collections import defaultdict
from utils.logger import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from dashboard.utils import get_raids_stats

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DATA_FILE = os.path.join(DATA_DIR, 'global_raids.json')

def update_global_raids():
    """
    Fetches stats for all areas (global) for the last 24 hours,
    aggregates them into a single total, and saves to a JSON file.
    """
    logger.info("[Task] Fetching Global Raid Stats...")

    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    try:
        # Request parameters
        params = {
            "counter_type": "totals",
            "area": "global",
            "start_time": "24 hours",
            "end_time": "now",
            "mode": "sum",
            "interval": "hourly",
            "raid_pokemon": "all",
            "raid_form": "all",
            "raid_level": "all",
            "response_format": "json"
        }

        # Fetch data
        raw_data = get_raids_stats("counter", params)

        if not raw_data:
            logger.info("[Task] No data received for global raids.")
            return

        # Aggregate data
        total_raids = 0
        raid_levels_agg = defaultdict(int)

        # Iterate over each area in the response
        for area_name, content in raw_data.items():
            if not isinstance(content, dict):
                continue

            stats = content.get('data', {})
            if not stats:
                continue

            # Sum total
            total_raids += stats.get('total', 0)

            # Sum raid_level breakdown
            raid_levels = stats.get('raid_level', {})
            if isinstance(raid_levels, dict):
                for level, count in raid_levels.items():
                    raid_levels_agg[str(level)] += count

        # Construct final structure
        final_data = {
            "total": total_raids,
            "raid_level": dict(raid_levels_agg),
            "last_updated": time.time()
        }

        # Save to file
        with open(DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=2)

        logger.info(f"[Task] Global raids updated successfully. Total: {final_data.get('total', 0)}")

    except Exception as e:
        logger.info(f"[Task] Error updating global raids: {e}")

def run_schedule():
    """Runs the update task immediately, then every 5 minutes."""
    while True:
        update_global_raids()
        time.sleep(300)

def start_background_task_raids():
    """Starts the background thread."""
    thread = threading.Thread(target=run_schedule, daemon=True)
    thread.start()
    logger.info("[System] Global Raid Stats background task started.")
