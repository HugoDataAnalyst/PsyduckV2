from tortoise import Tortoise
import config as AppConfig
from loguru import logger

# Tortoise ORM Database URL
DB_URL = f"mysql://{AppConfig.db_user}:{AppConfig.db_password}@{AppConfig.db_host}:{AppConfig.db_port}/{AppConfig.db_name}"

async def init_db():
    """Initialize database connection."""
    try:
        logger.info("Connecting to the database...")
        await Tortoise.init(
            db_url=DB_URL,
            modules={"models": ["sql.models"]}
        )
        await Tortoise.generate_schemas()  # Ensures tables exist
        logger.success("Database connected successfully!")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

async def close_db():
    """Close database connection."""
    await Tortoise.close_connections()
    logger.info("Database connection closed.")
