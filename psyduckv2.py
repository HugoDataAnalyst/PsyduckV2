import asyncio
import os
from sql.connect_db import init_db, close_db
from loguru import logger

async def apply_migrations():
    """Apply any pending database migrations before starting the app."""
    logger.info("Checking for pending migrations...")
    os.system("tortoise-orm upgrade")  # Runs pending migrations
    logger.success("Migrations applied!")

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
