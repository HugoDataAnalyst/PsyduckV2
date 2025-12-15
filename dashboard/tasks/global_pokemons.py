import threading
import time
import json
import os
import sys
from collections import defaultdict
from utils.logger import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from dashboard.utils import get_pokemon_stats

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
DATA_FILE = os.path.join(DATA_DIR, 'global_pokes.json')

def update_global_stats():
    """
    Fetches stats for all areas (global) for the last 24 hours,
    aggregates them into a single total, and saves to a JSON file.
    """
    logger.info("[Task] Fetching Global Pokémon Stats...")

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
            "metric": "all",
            "pokemon_id": "all",
            "form_id": "all",
            "response_format": "json"
        }

        # Fetch data
        raw_data = get_pokemon_stats("counter", params)

        if not raw_data:
            logger.info("[Task] No data received for global stats.")
            return

        # Aggregate data
        aggregated = defaultdict(int)

        # Iterate over each area in the response
        for area_name, content in raw_data.items():
            if not isinstance(content, dict):
                continue

            stats = content.get('data', {})
            if not stats:
                continue

            for key, value in stats.items():
                if isinstance(value, (int, float)):
                    aggregated[key] += value

        # Add a timestamp
        final_data = dict(aggregated)
        final_data['last_updated'] = time.time()

        # Save to file
        with open(DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=2)

        logger.info(f"[Task] Global stats updated successfully. Total Spawns: {final_data.get('total', 0)}")

    except Exception as e:
        logger.info(f"[Task] Error updating global stats: {e}")

def run_schedule():
    """Runs the update task immediately, then every 5 minutes."""
    while True:
        update_global_stats()
        time.sleep(300)

def start_background_task():
    """Starts the background thread."""
    thread = threading.Thread(target=run_schedule, daemon=True)
    thread.start()
    logger.info("[System] Global Pokémon Stats background task started.")
