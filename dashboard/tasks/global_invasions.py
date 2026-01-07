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

# Character ID classification
# Leaders: Cliff=41, Arlo=42, Sierra=43, Giovanni=44
# Male grunts: IDs ending with (Male) in name - typically odd numbers for type grunts
# Female grunts: IDs ending with (Female) in name - typically even numbers for type grunts
LEADERS = {41: "Cliff", 42: "Arlo", 43: "Sierra"}
GIOVANNI_IDS = {44, 524}  # Regular Giovanni and Event Giovanni

# Female grunt IDs (even type-based grunts + specific IDs)
FEMALE_GRUNT_IDS = {5, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 46, 47, 49, 51, 53, 55, 57, 59, 61, 63, 65, 67, 69, 71, 73, 75, 77, 79, 81, 83, 85, 87, 89}
# Male grunt IDs (odd type-based grunts + specific IDs)
MALE_GRUNT_IDS = {4, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 45, 48, 50, 52, 54, 56, 58, 60, 62, 64, 66, 68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90}

def classify_character(char_id):
    """
    Classify a character ID into category.
    Returns one of: 'male', 'female', 'cliff', 'arlo', 'sierra', 'giovanni', 'unset'
    """
    char_id = int(char_id) if char_id else 0

    if char_id == 0:
        return 'unset'
    if char_id in GIOVANNI_IDS:
        return 'giovanni'
    if char_id == 41:
        return 'cliff'
    if char_id == 42:
        return 'arlo'
    if char_id == 43 or char_id == 525:  # Sierra and Event Sierra
        return 'sierra'
    if char_id == 526:  # Event Arlo
        return 'arlo'
    if char_id == 527:  # Event Cliff
        return 'cliff'
    if char_id in FEMALE_GRUNT_IDS:
        return 'female'
    if char_id in MALE_GRUNT_IDS:
        return 'male'

    # Fallback: use ID pattern for extended ranges
    # Many grunt IDs follow pattern where female is even, male is odd for type grunts
    return 'unset'


def update_global_invasions():
    """
    Fetches stats for all areas (global) for the last 24 hours.
    Aggregates by character type:
      - Total Invasions
      - Male Grunts
      - Female Grunts
      - Cliff, Arlo, Sierra (Leaders)
      - Giovanni
      - Unset (unknown character)
    Saves to a JSON file.
    """
    logger.info("[Task] Fetching Global Invasion Stats...")

    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    try:
        # Request parameters - use grouped mode to get character breakdown
        params = {
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
        }

        # Fetch data
        raw_data = get_invasions_stats("counter", params)

        if not raw_data:
            logger.info("[Task] No data received for global invasions.")
            return

        # Initialize counters for each category
        total_invasions = 0
        category_counts = {
            'male': 0,
            'female': 0,
            'cliff': 0,
            'arlo': 0,
            'sierra': 0,
            'giovanni': 0,
            'unset': 0
        }

        # Iterate over each area in the response
        for area_name, content in raw_data.items():
            if not isinstance(content, dict):
                continue

            stats = content.get('data', {})
            if not stats:
                continue

            # Get the display_type+character breakdown
            char_data = stats.get('display_type+character', {})
            if not char_data:
                # Fallback to grunt data if available
                char_data = stats.get('grunt', {})
                if char_data:
                    # Convert grunt format (just char_id: count) to display+char format
                    char_data = {f"1:{k}": v for k, v in char_data.items()}

            # Process each character entry
            for key_str, count in char_data.items():
                if not isinstance(count, (int, float)):
                    continue

                # Parse key format "display_type:character_id"
                parts = str(key_str).split(':')
                if len(parts) >= 2:
                    try:
                        char_id = int(parts[1])
                    except ValueError:
                        char_id = 0
                else:
                    try:
                        char_id = int(key_str)
                    except ValueError:
                        char_id = 0

                # Classify and accumulate
                category = classify_character(char_id)
                category_counts[category] += int(count)
                total_invasions += int(count)

        # Construct final structure
        final_data = {
            "total": total_invasions,
            "stats": {
                "male": category_counts['male'],
                "female": category_counts['female'],
                "cliff": category_counts['cliff'],
                "arlo": category_counts['arlo'],
                "sierra": category_counts['sierra'],
                "giovanni": category_counts['giovanni'],
                "unset": category_counts['unset']
            },
            "last_updated": time.time()
        }

        # Save to file
        with open(DATA_FILE, 'w') as f:
            json.dump(final_data, f, indent=2)

        logger.info(f"[Task] Global invasions updated. Total: {total_invasions}, Male: {category_counts['male']}, Female: {category_counts['female']}, Leaders: {category_counts['cliff']+category_counts['arlo']+category_counts['sierra']}, Giovanni: {category_counts['giovanni']}")

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
