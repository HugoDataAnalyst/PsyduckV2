from tortoise import Tortoise
import config as AppConfig
import asyncio
from loguru import logger

db_config = {
    'connections': {
        'default': {
            'engine': 'tortoise.backends.mysql',
            'credentials': {
                'host': AppConfig.db_host,
                'port': AppConfig.db_port,
                'user': AppConfig.db_user,
                'password': AppConfig.db_password,
                'database': AppConfig.db_name,
            }
        }
    },
    'apps': {
        'models': {
            'models': ['sql.models'],
            'default_connection': 'default',
        }
    }
}

async def init_db():
    retries = AppConfig.db_retry_connection
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Initializing database connection (Attempt {attempt}/{retries})...")
            await Tortoise.init(config=db_config)
            logger.info("Attempting to generate schemas.")
            await Tortoise.generate_schemas()
            logger.success("Database connection initialized and schemas generated.")
            break
        except Exception as e:
            logger.error(f"Attempt {attempt} failed with error: {e}")
            if attempt < retries:
                logger.info("Retrying in 5 seconds...")
                await asyncio.sleep(5)
            else:
                logger.error("All attempts to initialize the database have failed.")
                raise

async def close_db():
    logger.info("Closing database connections...")
    await Tortoise.close_connections()
    logger.info("Database connections closed.")

