import requests
import json
import time
from pathlib import Path
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config as AppConfig

def prompt_for_parameters():
    print("Configure API Request Parameters. Press Enter to accept the default value shown in quotes.")
    start_time = input("Enter start time (default '2 days'): ") or "2 days"
    end_time = input("Enter end time (default 'now'): ") or "now"
    mode = input("Enter mode (sum/grouped/surged) (default 'surged'): ") or "surged"
    area = input("Enter area (default 'global'): ") or "global"
    pokemon_id = input("Enter pokemon_id (default 'all'): ") or "all"
    form = input("Enter form (default 'all'): ") or "all"

    return {
        "start_time": start_time,
        "end_time": end_time,
        "mode": mode,
        "response_format": "json",
        "area": area,
        "pokemon_id": pokemon_id,
        "form": form,
        "api_secret_key": AppConfig.api_secret_key
    }

def test_timeseries_api_and_save():
    # Adjust webhook IP: if it's "0.0.0.0", use "127.0.0.1"
    webhook_ip = AppConfig.webhook_ip if AppConfig.webhook_ip != "0.0.0.0" else "127.0.0.1"

    # API Configuration
    url = f"http://{webhook_ip}:{AppConfig.golbat_webhook_port}/api/redis/get_pokemon_timeseries"

    # Security headers
    secret_header_name = AppConfig.api_header_name
    secret_header_value = AppConfig.api_header_secret

    # Prompt user for parameters
    params = prompt_for_parameters()

    headers = {
        secret_header_name: secret_header_value
    }

    print("🚀 Starting Pokémon Timeseries API request...")
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
        data = response.json()
        parse_duration = time.time() - parse_start
        print(f"🧮 JSON parsed in {parse_duration:.2f} seconds")

        # --- 3. Process results based on mode ---
        process_start = time.time()
        mode = data.get("mode", "sum")
        results = data.get("data", {})

        if mode == "sum":
            print("📊 Sum Mode Results:")
            for metric, value in results.items():
                print(f"{metric}: {value}")

        elif mode == "grouped":
            print("📊 Grouped Mode Results:")
            for metric, groups in results.items():
                print(f"\n{metric}:")
                for group, value in groups.items():
                    print(f"  {group}: {value}")

        elif mode == "surged":
            print("📊 Surged Mode Results (Hourly):")
            for metric, hours in results.items():
                print(f"\n{metric}:")
                for hour, value in hours.items():
                    print(f"  {hour}: {value}")

        process_duration = time.time() - process_start
        print(f"\n🔍 Processed results in {process_duration:.2f} seconds")

        # --- 4. Save to file phase ---
        save_start = time.time()

        # Generate filename with timestamp and parameters
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"pokemon_timeseries_{mode}_{params['area']}_{timestamp}.json"
        output_dir = Path("data")
        output_dir.mkdir(parents=True, exist_ok=True)  # Create 'data' directory if it doesn't exist
        output_path = output_dir / filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        save_duration = time.time() - save_start
        print(f"💾 Saved file in {save_duration:.2f} seconds: {output_path.resolve()}")

    except Exception as e:
        print(f"❌ Exception occurred: {str(e)}")
        return

    total_duration = time.time() - total_start
    print(f"\n⏱️ Total time taken: {total_duration:.2f} seconds")

if __name__ == "__main__":
    input("Press Enter to start the Pokémon Timeseries API request...")
    test_timeseries_api_and_save()
