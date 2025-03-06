import asyncio
import os
import subprocess
from sql.connect_db import init_db, close_db
from loguru import logger

async def apply_migrations():
    """Apply pending database migrations using Aerich before starting the app."""
    logger.info("Checking for pending migrations...")
    try:
        result = subprocess.run(
            ["aerich", "upgrade"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.success(f"Migrations applied successfully! Output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration failed: {e.stderr}")

async def main():
    await init_db()  # Initialize DB (Automatically creates tables if needed)
    await apply_migrations()  # Apply any new migrations
    logger.info("Psyduck is ready to process data!")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(main())
