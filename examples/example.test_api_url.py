import requests
import json
import time
from pathlib import Path
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config as AppConfig

# Mapping of endpoints to their parameter prompts
ENDPOINT_PROMPTS = {
    "/api/redis/get_pokemon_counterseries": {
        "description": "Pokémon Counter Series",
        "params": {
            "counter_type": ("Type of counter series: totals, tth, or weather", "totals"),
            "interval": ("Interval: hourly or weekly for totals and tth; monthly for weather", "hourly"),
            "start_time": ("Start time as ISO format or relative (e.g., '1 month', '10 days')", "2 days"),
            "end_time": ("End time as ISO format or relative (e.g., 'now')", "now"),
            "mode": ("Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'", "sum"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter counters", "global")
        }
    },
    "/api/redis/get_raids_counterseries": {
        "description": "Raid Counter Series",
        "params": {
            "counter_type": ("Type of counter series: totals", "totals"),
            "interval": ("Interval: hourly or weekly", "hourly"),
            "start_time": ("Start time as ISO format or relative (e.g., '1 month', '10 days')", "2 days"),
            "end_time": ("End time as ISO format or relative (e.g., 'now')", "now"),
            "mode": ("Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'", "sum"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter counters", "global")
        }
    },
    "/api/redis/get_invasions_counterseries": {
        "description": "Invasion Counter Series",
        "params": {
            "counter_type": ("Type of counter series: totals", "totals"),
            "interval": ("Interval: hourly or weekly", "hourly"),
            "start_time": ("Start time as ISO format or relative (e.g., '1 month', '10 days')", "2 days"),
            "end_time": ("End time as ISO format or relative (e.g., 'now')", "now"),
            "mode": ("Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'", "sum"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter counters", "global")
        }
    },
    "/api/redis/get_quest_counterseries": {
        "description": "Quest Counter Series",
        "params": {
            "counter_type": ("Type of counter series: totals", "totals"),
            "interval": ("Interval: hourly or weekly", "hourly"),
            "start_time": ("Start time as ISO format or relative (e.g., '1 month', '10 days')", "2 days"),
            "end_time": ("End time as ISO format or relative (e.g., 'now')", "now"),
            "mode": ("Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'", "sum"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter counters", "global")
        }
    },
    "/api/redis/get_pokemon_timeseries": {
        "description": "Pokémon TimeSeries",
        "params": {
            "start_time": ("Start time as ISO format or relative (e.g., '1 month', '10 days')", "2 days"),
            "end_time": ("End time as ISO format or relative (e.g., 'now')", "now"),
            "mode": ("Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'", "sum"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "pokemon_id": ("Pokémon ID", "all"),
            "form": ("Pokémon form", "all")
        }
    },
    "/api/redis/get_pokemon_tth_timeseries": {
        "description": "Pokémon TTH TimeSeries",
        "params": {
            "start_time": ("Start time as ISO format or relative (e.g., '1 month')", "2 days"),
            "end_time": ("End time as ISO format or relative (e.g., 'now')", "now"),
            "mode": ("Aggregation mode: 'sum', 'grouped', or 'surged'", "sum"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "tth_bucket": ("TTH bucket filter (e.g., '10_15'; use 'all' to match any)", "all")
        }
    },
    "/api/redis/get_raid_timeseries": {
        "description": "Raid TimeSeries",
        "params": {
            "start_time": ("Start time as ISO format or relative (e.g., '1 month', '10 days')", "2 days"),
            "end_time": ("End time as ISO format or relative (e.g., 'now')", "now"),
            "mode": ("Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'", "sum"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "raid_pokemon": ("all or Pokémon ID", "all"),
            "raid_form": ("all or Form ID", "all"),
            "raid_level": ("all or Raid Level", "all")
        }
    },
    "/api/redis/get_invasion_timeseries": {
        "description": "Invasion TimeSeries",
        "params": {
            "start_time": ("Start time as ISO format or relative (e.g., '1 month', '10 days')", "2 days"),
            "end_time": ("End time as ISO format or relative (e.g., 'now')", "now"),
            "mode": ("Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'", "sum"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "display": ("all or Invasion Display ID", "all"),
            "grunt": ("all or Grunt ID", "all"),
            "confirmed": ("0 or 1 (confirmed or not details)", "all")
        }
    },
    "/api/redis/get_quest_timeseries": {
        "description": "Quest TimeSeries",
        "params": {
            "start_time": ("Start time as ISO format or relative (e.g., '1 month', '10 days')", "2 days"),
            "end_time": ("End time as ISO format or relative (e.g., 'now')", "now"),
            "mode": ("Aggregation mode: 'sum' or 'grouped' or (for hourly only) 'surged'", "sum"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "quest_mode": ("all or AR or NORMAL", "all"),
            "quest_type": ("all or Quest Type ID", "all")
        }
    },
    "/api/sql/get_pokemon_heatmap_data": {
        "description": "Pokémon HeatMap Data",
        "params": {
            "start_time": ("Start time as 202503 (2025 year month 03)", "202503"),
            "end_time": ("End time as 202504 (2025 year month 04)", "202504"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "pokemon_id": ("all or Pokémon ID", "all"),
            "form": ("all or Pokémon Form ID", "all"),
            "iv_bucket": ("all or IV specific bucket(0, 25, 50, 75, 90, 100)", "all"),
            "limit": ("Optional row limit for preview in the UI, 1000 advised", "0")
        }
    },
    "/api/sql/get_shiny_rate_data": {
        "description": "Shiny Rate Data",
        "params": {
            "start_time": ("Start time as 202503 (2025 year month 03)", "202503"),
            "end_time": ("End time as 202504 (2025 year month 04)", "202504"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "username": ("all or specific username", "all"),
            "pokemon_id": ("all or Pokémon ID", "all"),
            "form": ("all or Pokémon Form ID", "all"),
            "shiny": ("all or shiny status (0=non-shiny, 1=shiny)", "all"),
            "limit": ("Optional row limit for preview in the UI, 1000 advised", "0")
        }
    },
    "/api/sql/get_raid_data": {
        "description": "Raid SQL Data",
        "params": {
            "start_time": ("Start time as 202503 (2025 year month 03)", "202503"),
            "end_time": ("End time as 202504 (2025 year month 04)", "202504"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "gym_id": ("all or specific gym ID", "all"),
            "raid_pokemon": ("all or raid boss Pokémon ID", "all"),
            "raid_level": ("all or raid level (1-5)", "all"),
            "raid_form": ("all or raid boss form", "all"),
            "raid_team": ("all or controlling team ID", "all"),
            "raid_costume": ("all or costume ID", "all"),
            "raid_is_exclusive": ("all or exclusive status (0 or 1)", "all"),
            "raid_ex_raid_eligible": ("all or EX eligibility (0 or 1)", "all"),
            "limit": ("Optional row limit for preview in the UI, 1000 advised", "0")
        }
    },
    "/api/sql/get_invasion_data": {
        "description": "Invasion SQL Data",
        "params": {
            "start_time": ("Start time as 202503 (2025 year month 03)", "202503"),
            "end_time": ("End time as 202504 (2025 year month 04)", "202504"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "pokestop_id": ("all or specific pokestop ID", "all"),
            "display_type": ("all or invasion display type", "all"),
            "character": ("all or invasion character", "all"),
            "grunt": ("all or grunt type", "all"),
            "confirmed": ("all or confirmed status (0 or 1)", "all"),
            "limit": ("Optional row limit for preview in the UI, 1000 advised", "0")
        }
    },
    "/api/sql/get_quest_data": {
        "description": "Quest SQL Data",
        "params": {
            "start_time": ("Start time as 202503 (2025 year month 03)", "202503"),
            "end_time": ("End time as 202504 (2025 year month 04)", "202504"),
            "response_format": ("Response format: json or text", "json"),
            "area": ("Area to filter", "global"),
            "pokestop_id": ("all or specific pokestop ID", "all"),
            "ar_type": ("all or AR quest type", "all"),
            "normal_type": ("all or normal quest type", "all"),
            "reward_ar_type": ("all or AR reward type", "all"),
            "reward_normal_type": ("all or normal reward type", "all"),
            "reward_ar_item_id": ("all or AR reward item ID", "all"),
            "reward_normal_item_id": ("all or normal reward item ID", "all"),
            "reward_ar_poke_id": ("all or AR reward Pokémon ID", "all"),
            "reward_normal_poke_id": ("all or normal reward Pokémon ID", "all"),
            "limit": ("Optional row limit for preview in the UI, 1000 advised", "0")
        }
    }
}

def prompt_for_endpoint():
    print("\nAvailable API Endpoints:")
    for i, endpoint in enumerate(ENDPOINT_PROMPTS.keys(), 1):
        print(f"{i}. {endpoint} - {ENDPOINT_PROMPTS[endpoint]['description']}")

    while True:
        try:
            choice = int(input("\nSelect an endpoint by number: "))
            if 1 <= choice <= len(ENDPOINT_PROMPTS):
                selected_endpoint = list(ENDPOINT_PROMPTS.keys())[choice - 1]
                return selected_endpoint
            else:
                print(f"Please enter a number between 1 and {len(ENDPOINT_PROMPTS)}")
        except ValueError:
            print("Please enter a valid number.")

def prompt_for_parameters(endpoint):
    params_config = ENDPOINT_PROMPTS[endpoint]["params"]
    params = {}

    print(f"\nConfigure API Request Parameters for {endpoint}")
    print("Press Enter to accept the default value shown in quotes.\n")

    for param, (prompt, default) in params_config.items():
        params[param] = input(f"{prompt} (default '{default}'): ") or default

    # Add secret key to all requests
    params["api_secret_key"] = AppConfig.api_secret_key
    return params

def process_response(endpoint, response):
    data = response.json()
    mode = data.get("mode", "sum")
    results = data.get("data", {})

    print(f"\n📊 Results for {endpoint} (Mode: {mode}):")

    if mode == "sum":
        for metric, value in results.items():
            print(f"{metric}: {value}")
    elif mode == "grouped":
        for metric, groups in results.items():
            print(f"\n{metric}:")
            for group, value in groups.items():
                print(f"  {group}: {value}")
    elif mode == "surged":
        for metric, hours in results.items():
            print(f"\n{metric}:")
            for hour, value in hours.items():
                print(f"  {hour}: {value}")
    else:
        # For endpoints that don't have a mode or have different response structures
        if isinstance(results, dict):
            for key, value in results.items():
                print(f"{key}: {value}")
        else:
            print(results)

def test_api_and_save():
    # Adjust webhook IP: if it's "0.0.0.0", use "127.0.0.1"
    webhook_ip = AppConfig.webhook_ip if AppConfig.webhook_ip != "0.0.0.0" else "127.0.0.1"
    base_url = f"http://{webhook_ip}:{AppConfig.golbat_webhook_port}"

    # Select endpoint
    endpoint = prompt_for_endpoint()
    url = base_url + endpoint

    # Security headers
    headers = {}
    if hasattr(AppConfig, "api_secret_key") and AppConfig.api_secret_key:
        headers["Authorization"] = f"Bearer {AppConfig.api_secret_key}"
    elif (hasattr(AppConfig, "api_header_name") and AppConfig.api_header_name and
        hasattr(AppConfig, "api_header_secret") and AppConfig.api_header_secret):
        headers[AppConfig.api_header_name] = AppConfig.api_header_secret

    # Prompt user for parameters
    params = prompt_for_parameters(endpoint)

    print(f"\n🚀 Starting API request to {endpoint}...")
    total_start = time.time()

    try:
        # --- 1. Request phase ---
        request_start = time.time()
        response = requests.get(url, headers=headers, params=params)
        request_duration = time.time() - request_start

        print(f"✅ API Request completed in {request_duration:.2f} seconds (Status: {response.status_code})")

        if response.status_code != 200:
            print(f"❌ Error: {response.text}")
            return

        # --- 2. Parse JSON phase ---
        parse_start = time.time()
        parse_duration = time.time() - parse_start
        print(f"🧮 JSON parsed in {parse_duration:.2f} seconds")

        # --- 3. Process results ---
        process_start = time.time()
        process_response(endpoint, response)
        process_duration = time.time() - process_start
        print(f"\n🔍 Processed results in {process_duration:.2f} seconds")

        # --- 4. Save to file phase ---
        save_start = time.time()

        # Generate filename with timestamp and parameters
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        endpoint_name = endpoint.replace("/api/", "").replace("/", "_")
        filename = f"{endpoint_name}_{timestamp}.json"
        output_dir = Path("data")
        output_dir.mkdir(parents=True, exist_ok=True)  # Create 'data' directory if it doesn't exist
        output_path = output_dir / filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(response.json(), f, indent=2, ensure_ascii=False)

        save_duration = time.time() - save_start
        print(f"💾 Saved file in {save_duration:.2f} seconds: {output_path.resolve()}")

    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return

    total_duration = time.time() - total_start
    print(f"\n⏱️ Total time taken: {total_duration:.2f} seconds")

if __name__ == "__main__":
    input("Press Enter to start the API request...")
    test_api_and_save()
