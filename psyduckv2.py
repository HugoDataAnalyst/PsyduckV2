import sys
import asyncio
import uvicorn
import subprocess
import config as AppConfig
from pathlib import Path
from sql.connect_db import init_db, close_db
from sql.utils.create_partitions import ensure_iv_partitions
from alembic.config import Config as AlembicConfig
from alembic import command as alembic_command
from utils.logger import setup_logging, logger
from utils.koji_geofences import KojiGeofences
from my_redis.connect_redis import RedisManager
import warnings
warnings.filterwarnings("ignore", message="Duplicate entry")

# Initialize logging
setup_logging(AppConfig.log_level, {"file": AppConfig.log_file, "function": True})

# Initialize Redis connection
redis_manager = RedisManager()

def _project_root() -> Path:
    return Path(__file__).resolve().parent


def apply_migrations() -> None:
    """
    Run 'alembic upgrade head' programmatically.
    Ensures Alembic uses the local alembic.ini and alembic/ directory.
    """
    root = _project_root()
    alembic_ini = root / "alembic.ini"
    alembic_dir = root / "alembic"

    # Make sure Python can import config inside alembic/env.py
    # Config.py expects project root on sys.path
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    logger.info("🔃 Checking and applying Alembic migrations...")
    cfg = AlembicConfig(str(alembic_ini))
    # In case alembic.ini points elsewhere, force the script_location:
    cfg.set_main_option("script_location", str(alembic_dir))


    alembic_command.upgrade(cfg, "head")
    logger.success("✅ Alembic migrations are up to date.")

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
        reload=False
    )
    webhook_api_server = uvicorn.Server(webhook_api_config)
    logger.info("⬆️ Starting PsyduckV2 API server...")
    await webhook_api_server.serve()

async def main():
    apply_migrations()  # Apply any new migrations
    await init_db()  # Initialize DB (Automatically creates tables if needed)
    """
    try:
        back = 24
        forward = 24
        summary = await ensure_iv_partitions(back, forward)
        logger.info(
            "🧾 Partition ensure summary — added=%d, skipped=%d",
            len(summary.get("added", [])), len(summary.get("skipped", []))
        )
    except Exception as e:
        logger.error(f"⚠️ ensure_iv_partitions failed: {e}", exc_info=True)
    """
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
