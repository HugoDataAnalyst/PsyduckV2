"""
Emergency Redis backup handler.

Registers sys.excepthook and atexit callbacks so that a final backup
is attempted even when the process exits unexpectedly (unhandled exception,
interpreter shutdown). Called only when REDIS_MYSQL_BACKUPS = true.

SIGKILL / OOM kills cannot be caught — this is accepted.
"""

import asyncio
import atexit
import sys

from utils.logger import logger

_redis_manager = None
_registered    = False
_backup_done   = False   # prevent double-run between excepthook and atexit


def register_emergency_backup(redis_manager) -> None:
    """
    Register sys.excepthook and atexit handlers.
    Safe to call multiple times — only registers once.
    """
    global _redis_manager, _registered
    if _registered:
        return

    _redis_manager = redis_manager
    _original_excepthook = sys.excepthook

    def _excepthook(exc_type, exc_val, exc_tb):
        logger.error(
            "Unhandled exception ({}) — attempting emergency Redis backup before exit",
            exc_type.__name__ if exc_type else "unknown",
        )
        _run_backup()
        _original_excepthook(exc_type, exc_val, exc_tb)

    sys.excepthook = _excepthook
    atexit.register(_run_backup)
    _registered = True
    logger.debug("Emergency Redis backup handler registered")


def _run_backup() -> None:
    """Synchronous entry point — opens a fresh event loop to run the async backup."""
    global _backup_done
    if _backup_done:
        return
    _backup_done = True

    if _redis_manager is None:
        logger.warning("Emergency backup: redis_manager not set — skipping")
        return

    try:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_do_backup())
    except Exception as e:
        logger.error("Emergency backup failed: {}", e)
    finally:
        try:
            loop.close()
        except Exception:
            pass


async def _do_backup() -> None:
    from my_redis.utils.mysql_backup import backup_all
    from sql.connect_db import init_db, close_db

    try:
        # Re-initialise DB pool in the fresh event loop (the original pool is gone)
        await init_db()
        client = await _redis_manager.check_redis_connection()
        if client:
            await backup_all(client)
        else:
            logger.error("Emergency backup: could not obtain Redis connection")
    finally:
        try:
            await close_db()
        except Exception:
            pass
