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

# Quest reward type mappings (from PoGo data)
REWARD_TYPE_NAMES = {
    1: "XP",
    2: "Item",
    3: "Stardust",
    4: "Candy",
    7: "Pokémon",
    12: "Mega Energy"
}


def update_global_quests():
    """
    Fetches stats for all areas (global) for the last 24 hours.
    Aggregates:
      - Total Scanned Stops
      - AR Quests with reward type breakdown
      - Normal Quests with reward type breakdown
    Saves to a JSON file.
    """
    logger.info("[Task] Fetching Global Quest Stats...")

    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    try:
        # Request parameters - use grouped mode to get reward breakdown
        params = {
            "counter_type": "totals",
            "interval": "hourly",
            "start_time": "24 hours",
            "end_time": "now",
            "mode": "grouped",
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

        # Reward type breakdowns
        ar_rewards = defaultdict(int)
        normal_rewards = defaultdict(int)

        # Iterate over each area in the response
        for area_name, content in raw_data.items():
            if not isinstance(content, dict):
                continue

            stats = content.get('data', {})
            if not stats:
                continue

            # 1. Sum total from quest_mode
            quest_modes = stats.get('quest_mode', {})
            if isinstance(quest_modes, dict):
                ar_count = quest_modes.get('1', quest_modes.get('ar', 0))
                normal_count = quest_modes.get('0', quest_modes.get('normal', 0))
                total_ar_quests += ar_count
                total_normal_quests += normal_count
                total_scanned_stops += ar_count + normal_count

            # 2. Process reward breakdowns from ts: keys (live data format)
            for key, value in stats.items():
                if not str(key).startswith('ts:'):
                    continue

                # Parse key format: ts:quests_total:{quest_mode}:{reward_type}:{item_or_poke_id}:{form_or_amount}
                parts = str(key).split(':')
                if len(parts) < 4:
                    continue

                quest_mode = parts[2]  # 'ar' or 'normal'
                reward_type = parts[3]  # 'item', 'stardust', 'pokemon', 'mega', 'xp', 'candy'

                count = 0
                if isinstance(value, (int, float)):
                    count = int(value)
                elif isinstance(value, dict):
                    count = sum(v for v in value.values() if isinstance(v, (int, float)))

                if count > 0:
                    if quest_mode == 'ar':
                        ar_rewards[reward_type] += count
                    elif quest_mode == 'normal':
                        normal_rewards[reward_type] += count

            # 3. Process historical counter format (date bucket with nested reward data)
            if 'reward_item' in stats:
                # This is aggregated reward data - need to process differently
                pass

            if 'reward_type' in stats:
                # Process reward_type breakdown if available
                reward_types = stats.get('reward_type', {})
                for r_type, count in reward_types.items():
                    if isinstance(count, (int, float)):
                        # Map numeric type to name
                        r_type_int = int(r_type) if str(r_type).isdigit() else 0
                        r_name = REWARD_TYPE_NAMES.get(r_type_int, f"type_{r_type}")
                        # Can't determine AR vs Normal from this, so skip
                        pass

        # Normalize reward type names
        def normalize_rewards(rewards_dict):
            normalized = {}
            for key, count in rewards_dict.items():
                # Map various key formats to consistent names
                key_lower = str(key).lower()
                if 'pokemon' in key_lower or key_lower == '7':
                    normalized['pokemon'] = normalized.get('pokemon', 0) + count
                elif 'stardust' in key_lower or key_lower == '3':
                    normalized['stardust'] = normalized.get('stardust', 0) + count
                elif 'item' in key_lower or key_lower == '2':
                    normalized['item'] = normalized.get('item', 0) + count
                elif 'mega' in key_lower or key_lower == '12':
                    normalized['mega'] = normalized.get('mega', 0) + count
                elif 'candy' in key_lower or key_lower == '4':
                    normalized['candy'] = normalized.get('candy', 0) + count
                elif 'xp' in key_lower or key_lower == '1':
                    normalized['xp'] = normalized.get('xp', 0) + count
                else:
                    normalized['other'] = normalized.get('other', 0) + count
            return normalized

        ar_rewards_normalized = normalize_rewards(ar_rewards)
        normal_rewards_normalized = normalize_rewards(normal_rewards)

        # Construct final structure
        final_data = {
            "total_stops": total_scanned_stops,
            "quests": {
                "ar": {
                    "total": total_ar_quests,
                    "rewards": ar_rewards_normalized
                },
                "normal": {
                    "total": total_normal_quests,
                    "rewards": normal_rewards_normalized
                }
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
