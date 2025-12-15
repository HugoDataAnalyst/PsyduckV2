import sys
import asyncio
import uvicorn
import subprocess
import config as AppConfig
from pathlib import Path
from sql.connect_db import init_db, close_db
from alembic.config import Config as AlembicConfig #type: ignore
from alembic import command as alembic_command
from utils.logger import setup_logging, logger
from utils.koji_geofences import KojiGeofences
from my_redis.connect_redis import RedisManager
import warnings
warnings.filterwarnings("ignore", message="Duplicate entry")


# Initialize logging
# Enable process identification when using multiple workers
setup_logging(
    AppConfig.log_level,
    {
        "to_file": AppConfig.log_file,
        "file_path": "logs/psyduckv2.log",
        "rotation": "5 MB",
        "keep_total": 5,
        "compression": "gz",
        "show_file": True,
        "show_function": True,
        "show_process": AppConfig.uvicorn_workers > 1,
    },
)
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

def _validate_worker_config():
    """
    Validate that worker configuration is compatible with Redis pool size.
    """
    workers = AppConfig.uvicorn_workers
    # MAX_CONCURRENT_REDIS_OPS is redis_max_connections // 2 (min 5) per worker
    max_per_worker = max(5, AppConfig.redis_max_connections // 2)
    total_possible = workers * max_per_worker

    if total_possible > AppConfig.redis_max_connections:
        logger.warning(
            f"Workers ({workers}) * concurrent Redis ops ({max_per_worker}) = {total_possible} "
            f"may exceed Redis pool ({AppConfig.redis_max_connections}). "
            f"Consider increasing redis_connections in config.json."
        )

    if workers > 1:
        logger.info(f"Multi-worker mode: {workers} workers configured")


def start_servers():
    """
    Start the PsyduckV2 webhook API.

    Uses uvicorn.run() which properly supports the workers parameter.
    Note: uvicorn.run() is blocking - it handles the event loop internally.
    """
    # Validate worker configuration
    _validate_worker_config()

    logger.info(f"⬆️ Starting PsyduckV2 API server with {AppConfig.uvicorn_workers} worker(s)...")

    # uvicorn.run() properly supports workers parameter (unlike Server.serve())
    # It will spawn multiple worker processes when workers > 1
    uvicorn.run(
        "server_fastapi.webhook_app:app",
        host=AppConfig.webhook_ip,
        port=AppConfig.golbat_webhook_port,
        workers=AppConfig.uvicorn_workers,
        reload=False,
        log_level="warning",  # Reduce uvicorn's own logging, we use loguru
    )

def main():
    """
    Main entry point for PsyduckV2.

    1. Apply database migrations (sync, main process only)
    2. Start uvicorn with worker processes
       - Each worker initializes its own DB pool and Redis connection in the lifespan
       - Leader election determines which worker runs background tasks
    """
    # Apply migrations in main process before spawning workers
    apply_migrations()

    # Initialize DB temporarily to verify connection works
    # Workers will create their own pools in their lifespan
    async def verify_db():
        await init_db()
        await close_db()

    asyncio.run(verify_db())

    logger.info("✅ Psyduck is ready to process data!")

    # Start uvicorn - this is blocking and handles worker processes internally
    # Each worker will run the lifespan in webhook_app.py which initializes DB/Redis
    start_servers()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🫣 Exiting due to keyboard interrupt.")
