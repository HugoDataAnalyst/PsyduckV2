import os
import sys
import json
import dotenv
import urllib.parse
from utils.logger import logger
from typing import List, Optional, Dict

# load config.json

CONFIG_PATH = os.path.join(os.getcwd(), "config", "config.json")

def load_config() -> Dict[str, any]:
    try:
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
        logger.info(f"✅ Loaded config from {CONFIG_PATH}")
        return config
    except FileNotFoundError:
        logger.error(f"❌ Config file not found at {CONFIG_PATH}. Using default values.")
        return {}

config = load_config()

# Read environment variables from .env file
env_file = os.path.join(os.getcwd(), ".env")
dotenv.load_dotenv(env_file, override=True)

def get_env_var(name: str, default = None) -> Optional[str]:
    value = os.getenv(name, default)
    if value is None or value == '':
        logger.warning(f"⚠️ Missing environment variable: {name}. Using default: {default}")
        return default
    return value


def get_env_list(env_var_name: str, default = None) -> List[str]:
    if default is None:
        default = []
    value = os.getenv(env_var_name, '')
    if not value:
        logger.warning(f"⚠️ Missing environment variable: {env_var_name}. Using default: {default}")
        return default
    return [item.strip() for item in value.split(',') if item.strip()]


def get_env_int(name: str, default = None) -> Optional[int]:
    value = os.getenv(name)
    if value is None:
        logger.warning(f"⚠️ Missing environment variable: {name}. Using default: {default}")
        return default
    try:
        return int(value)
    except ValueError:
        logger.error(f"❌ Invalid value for environment variable {name}: {value}. Using default: {default}")
        return default

# Calculate Extraction Intervals
def retention_ms(hours: int) -> int:
    return hours * 3600 * 1000

# Database Settings
db_host = get_env_var('DB_HOST')
db_port = get_env_int('DB_PORT', 3306)
db_name = get_env_var('DB_NAME', "PsyduckV2")
db_user = get_env_var('DB_USER', "root")
# Url encode the password to handle special characters
db_password = get_env_var('DB_PASSWORD', "root_password")
db_retry_connection = 5
db_rest_betwen_connection = 5
store_sql_pokemon_aggregation = str(config.get('SQL', {}).get('store_sql_pokemon_aggregation', True)).upper() == "TRUE"
store_sql_pokemon_shiny = str(config.get('SQL', {}).get('store_sql_pokemon_shiny', True)).upper() == "TRUE"
store_sql_raid_aggregation = str(config.get('SQL', {}).get('store_sql_raid_aggregation', True)).upper() == "TRUE"
store_sql_quest_aggregation = str(config.get('SQL', {}).get('store_sql_quest_aggregation', True)).upper() == "TRUE"
store_sql_invasion_aggregation = str(config.get('SQL', {}).get('store_sql_invasion_aggregation', True)).upper() == "TRUE"

# Redis
redis_password = get_env_var("REDIS_PASSWORD", "myveryredisstrongpassword")
redis_encoded_password = urllib.parse.quote(redis_password)  # Ensures safe encoding
redis_host = get_env_var("REDIS_HOST", "localhost")
redis_server_port = get_env_int("REDIS_SERVER_PORT", 6379)
redis_gui_port = get_env_int("REDIS_GUI_PORT", 8001)
redis_db = get_env_int("REDIS_DB", 1)
# Build Redis url connection
redis_url = f"redis://:{redis_encoded_password}@{redis_host}:{redis_server_port}/{redis_db}"
# Redis max connections per pool
redis_pokemon_pool = 10
redis_quest_pool = 5
redis_raid_pool = 5
redis_invasion_pool = 5
redis_geofence_pool = 1
# Redis retention settings
timeseries_pokemon_retention_ms  = retention_ms(config.get("retention_hours", {}).get("timeseries_pokemon", 720))
tth_timeseries_retention_ms      = retention_ms(config.get("retention_hours", {}).get("tth_timeseries_pokemon", 720))
raid_timeseries_retention_ms     = retention_ms(config.get("retention_hours", {}).get("timeseries_raid", 720))
invasion_timeseries_retention_ms = retention_ms(config.get("retention_hours", {}).get("timeseries_invasion", 720))
quests_timeseries_retention_ms   = retention_ms(config.get("retention_hours", {}).get("timeseries_quest", 720))
# Log Level
log_level = get_env_var("LOG_LEVEL", "INFO").upper()
log_file = get_env_var("LOG_FILE", "FALSE").upper() == "TRUE"

# Koji
koji_bearer_token = get_env_var("KOJI_TOKEN")
koji_ip = get_env_var("KOJI_IP", "127.0.0.1")
koji_port = get_env_int("KOJI_PORT", 8080)
koji_project_name = get_env_var("KOJI_PROJECT_NAME")
koji_geofence_api_url = f"http://{koji_ip}:{koji_port}/api/v1/geofence/feature-collection/{koji_project_name}"
# Extract geofence settings
geofence_expire_cache_seconds = config.get("geofences", {}).get("expire_cache_seconds", 3600)
geofence_refresh_cache_seconds = config.get("geofences", {}).get("refresh_cache_seconds", 3500)


# Golbat
golbat_host = get_env_var("GOLBAT_HOST", "127.0.0.1")
golbat_webhook_ip = get_env_var("GOLBAT_WEBHOOK_IP", "127.0.0.1")
golbat_webhook_port = get_env_int("GOLBAT_WEBHOOK_PORT", 8080)
golbat_db_host = get_env_var("GOLBAT_DB_HOST", "127.0.0.1")
golbat_db_port = get_env_int("GOLBAT_DB_PORT", 3306)
golbat_db_name = get_env_var("GOLBAT_DB_NAME")
golbat_db_user = get_env_var("GOLBAT_DB_USER")
golbat_db_password = get_env_var("GOLBAT_DB_PASSWORD")

# API
api_host = get_env_var("API_HOST", "127.0.0.1")
api_port = get_env_int("API_PORT", 8090)
