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

# DUCKDB Settings
duckdb_cores = int(config.get('DUCKDB', {}).get("max_cpu_cores", 2))
duckdb_max_ram = int(config.get('DUCKDB', {}).get("max_memory_ram_gb", 2))
duckdb_path = str(config.get('DUCKDB', {}).get("duckdb_path", "/app/data/analytics.duckdb"))
duckdb_poke_clean_days = int(config.get('DUCKDB', {}).get("clean_pokemon_data_older_than_x_days", 30))
duckdb_raid_clean_days = int(config.get('DUCKDB', {}).get("clean_raids_data_older_than_x_days", 30))
duckdb_quest_clean_days = int(config.get('DUCKDB', {}).get("clean_quests_data_older_than_x_days", 30))
duckdb_invasion_clean_days = int(config.get('DUCKDB', {}).get("clean_invasions_data_older_than_x_days", 30))

# Database Settings
db_host = get_env_var('DB_HOST')
db_port = get_env_int('DB_PORT', 3306)
db_name = get_env_var('DB_NAME', "PsyduckV2")
db_user = get_env_var('DB_USER', "root")
# Url encode the password to handle special characters
db_password = get_env_var('DB_PASSWORD', "root_password")
db_retry_connection = 5
db_rest_betwen_connection = 5
db_container_name = get_env_var('DB_CONTAINER_NAME')
db_container_port = get_env_int('DB_CONTAINER_PORT')

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
redis_max_connections = int(config.get("REDIS", {}).get("redis_connections", 30))

# Flusher settings
pokemon_max_threshold = config.get("flusher", {}).get("pokemon_max_threshold", 10000)
shiny_max_threshold = config.get("flusher", {}).get("shiny_max_threshold", 10000)
quest_max_threshold = config.get("flusher", {}).get("quest_max_threshold", 10000)
raid_max_threshold = config.get("flusher", {}).get("raid_max_threshold", 10000)
invasion_max_threshold = config.get("flusher", {}).get("invasion_max_threshold", 10000)
quest_flush_interval = config.get("flusher", {}).get("quest_flush_interval", 60)
raid_flush_interval = config.get("flusher", {}).get("raid_flush_interval", 60)
invasion_flush_interval = config.get("flusher", {}).get("invasion_flush_interval", 60)
shiny_flush_interval = config.get("flusher", {}).get("shiny_flush_interval", 60)
pokemon_flush_interval = config.get("flusher", {}).get("pokemon_flush_interval", 60)


# Redis retention settings
timeseries_pokemon_retention_ms  = retention_ms(config.get("retention_hours", {}).get("timeseries_pokemon", 72))
tth_timeseries_retention_ms      = retention_ms(config.get("retention_hours", {}).get("tth_timeseries_pokemon", 72))
raid_timeseries_retention_ms     = retention_ms(config.get("retention_hours", {}).get("timeseries_raid", 72))
invasion_timeseries_retention_ms = retention_ms(config.get("retention_hours", {}).get("timeseries_invasion", 72))
quests_timeseries_retention_ms   = retention_ms(config.get("retention_hours", {}).get("timeseries_quest", 72))

#Store in Redis
store_pokemon_timeseries = str(config.get('IN-MEMORY', {}).get('store_pokemon_timeseries', True)).upper() == "TRUE"
store_pokemon_tth_timeseries = str(config.get('IN-MEMORY', {}).get('store_pokemon_tth_timeseries', True)).upper() == "TRUE"
store_raids_timeseries = str(config.get('IN-MEMORY', {}).get('store_raids_timeseries', True)).upper() == "TRUE"
store_invasions_timeseries = str(config.get('IN-MEMORY', {}).get('store_invasions_timeseries', True)).upper() == "TRUE"
store_quests_timeseries = str(config.get('IN-MEMORY', {}).get('store_quests_timeseries', True)).upper() == "TRUE"

# Golbat Pokestops
pokestop_cache_expiry_seconds = config.get("golbat_pokestops", {}).get("pokestop_cache_expiry_seconds", 86400)
pokestop_refresh_interval_seconds = config.get("golbat_pokestops", {}).get("pokestop_refresh_interval_seconds", 86300)

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
golbat_webhook_port = get_env_int("GOLBAT_WEBHOOK_PORT", 8080)
golbat_db_host = get_env_var("GOLBAT_DB_HOST", "127.0.0.1")
golbat_db_port = get_env_int("GOLBAT_DB_PORT", 3306)
golbat_db_name = get_env_var("GOLBAT_DB_NAME")
golbat_db_user = get_env_var("GOLBAT_DB_USER")
golbat_db_password = get_env_var("GOLBAT_DB_PASSWORD")

# API
webhook_ip = get_env_var("WEBHOOK_IP", "127.0.0.1")
validated_remote_address = get_env_var("ALLOW_WEBHOOK_HOST")
allowed_ips = get_env_list("ALLOWED_IPS")
api_header_name = get_env_var("API_HEADER_NAME")
api_header_secret = get_env_var("API_HEADER_SECRET")
api_secret_key = get_env_var("API_SECRET_KEY")
