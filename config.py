import os
from typing import List, Optional
import dotenv
import sys
from utils.logger import logger
import urllib.parse
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


# Database Settings
db_host = get_env_var('DB_HOST')
db_port = get_env_int('DB_PORT', 3306)
db_name = get_env_var('DB_NAME', "PsyduckV2")
db_user = get_env_var('DB_USER', "root")
# Url encode the password to handle special characters
db_password = get_env_var('DB_PASSWORD', "root_password")
db_retry_connection = 5
db_rest_betwen_connection = 5

# Redis
redis_password = get_env_var("REDIS_PASSWORD", "myveryredisstrongpassword")
redis_encoded_password = urllib.parse.quote(redis_password)  # Ensures safe encoding
redis_host = get_env_var("REDIS_HOST", "localhost")
redis_server_port = get_env_int("REDIS_PORT", 6379)
redis_gui_port = get_env_int("REDIS_GUI_PORT", 8001)
redis_db = get_env_int("REDIS_DB", 1)
# Build Redis url connection
redis_url = f"redis://:{redis_encoded_password}@{redis_host}:{redis_server_port}/{redis_db}"

# Log Level
log_level = get_env_var("LOG_LEVEL", "INFO").upper()
log_file = get_env_var("LOG_FILE", "FALSE").upper() == "TRUE"

# Koji
koji_bearer_token = get_env_var("KOJI_TOKEN")
koji_ip = get_env_var("KOJI_IP", "127.0.0.1")
koji_port = get_env_int("KOJI_PORT", 8080)
koji_project_name = get_env_var("KOJI_PROJECT_NAME")
koji_geofence_api_url = f"http://{koji_ip}:{koji_port}/api/v1/geofence/feature-collection/{koji_project_name}"

# Golbat
golbat_host = get_env_var("GOLBAT_HOST", "127.0.0.1")
golbat_webhook_port = get_env_int("GOLBAT_WEBHOOK_PORT", 8080)
golbat_db_host = get_env_var("GOLBAT_DB_HOST", "127.0.0.1")
golbat_db_port = get_env_int("GOLBAT_DB_PORT", 3306)
golbat_db_name = get_env_var("GOLBAT_DB_NAME")
golbat_db_user = get_env_var("GOLBAT_DB_USER")
golbat_db_password = get_env_var("GOLBAT_DB_PASSWORD")
