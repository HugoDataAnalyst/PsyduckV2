import os
from typing import List, Optional
import dotenv
import sys
from loguru import logger
# Read environment variables from .env file
env_file = os.path.join(os.getcwd(), ".env")
dotenv.load_dotenv(env_file, override=True)


def get_env_var(name: str, default = None) -> Optional[str]:
    value = os.getenv(name, default)
    if value is None or value == '':
        logger.warning(f"Missing environment variable: {name}. Using default: {default}")
        return default
    return value


def get_env_list(env_var_name: str, default = None) -> List[str]:
    if default is None:
        default = []
    value = os.getenv(env_var_name, '')
    if not value:
        logger.warning(f"Missing environment variable: {env_var_name}. Using default: {default}")
        return default
    return [item.strip() for item in value.split(',') if item.strip()]


def get_env_int(name: str, default = None) -> Optional[int]:
    value = os.getenv(name)
    if value is None:
        logger.warning(f"Missing environment variable: {name}. Using default: {default}")
        return default
    try:
        return int(value)
    except ValueError:
        logger.error(f"Invalid value for environment variable {name}: {value}. Using default: {default}")
        return default


# Database Settings
db_host = get_env_var('DB_HOST')
db_port = get_env_int('DB_PORT', 3306)
db_name = get_env_var('DB_NAME', "chronos")
db_user = get_env_var('DB_USER', "root")
db_password = get_env_var('DB_PASSWORD', "root_password")
db_retry_connection = 5
db_rest_betwen_connection = 5
