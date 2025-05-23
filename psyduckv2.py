import asyncio
import uvicorn
import subprocess
import config as AppConfig
from sql.connect_db import init_db, close_db
from utils.logger import setup_logging, logger
from utils.koji_geofences import KojiGeofences
from my_redis.connect_redis import RedisManager
import warnings
warnings.filterwarnings("ignore", message="Duplicate entry")

# Initialize logging
setup_logging(AppConfig.log_level, {"file": AppConfig.log_file, "function": True})

# Initialize Redis connection
redis_manager = RedisManager()

async def apply_migrations():
    """Apply pending database migrations using Aerich before starting the app."""
    logger.info("🔃 Checking for pending migrations...")
    try:
        result = subprocess.run(
            ["aerich", "upgrade"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.success(f"✅ Migrations applied successfully! Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Migration failed: {e.stderr}")

async def start_servers():
    """
    Start the PsyduckV2 webhook API.
    """
    # Configure the webhook API server
    webhook_api_config = uvicorn.Config(
        "server_fastapi.webhook_app:app",
        host=AppConfig.webhook_ip,
        port=AppConfig.golbat_webhook_port,
        workers=1,
        reload=True
    )
    webhook_api_server = uvicorn.Server(webhook_api_config)
    logger.info("⬆️ Starting PsyduckV2 API server...")
    await webhook_api_server.serve()

async def main():
    await init_db()  # Initialize DB (Automatically creates tables if needed)
    await apply_migrations()  # Apply any new migrations

    logger.info("✅ Psyduck is ready to process data!")

    # Start both API servers concurrently
    await start_servers()

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("🫣 Shutting down...")
    finally:
        await close_db()
        await redis_manager.close_redis()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🫣 Exiting due to keyboard interrupt.")
