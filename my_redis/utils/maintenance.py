"""
Serialises the leader's heavy Redis maintenance jobs.

The timeseries cleanup and the Redis -> MySQL backup both walk most of the
keyspace. Run together they compete for Redis's single thread - cleanup went
from ~160s to ~340s when they overlapped in production, and webhook latency
suffered for the whole window. Both run in the leader process, so an asyncio
lock is enough: whichever starts second waits for the first.
"""
import asyncio
import time
from contextlib import asynccontextmanager

from utils.logger import logger

_lock = asyncio.Lock()


@asynccontextmanager
async def exclusive(job: str):
    if _lock.locked():
        logger.info(f"⏸️ {job} waiting for another Redis maintenance job to finish")
    t0 = time.monotonic()
    async with _lock:
        waited = time.monotonic() - t0
        if waited >= 1:
            logger.info(f"▶️ {job} starting after waiting {waited:.0f}s for Redis maintenance")
        yield
