import asyncio
import time
from utils.logger import logger
from my_redis.connect_redis import RedisManager
from my_redis.queries.buffer.geography_bulk_buffer import GeographyRedisBuffer


class GeographyBufferFlusher:
    def __init__(self, flush_interval: int = 300):
        self.flush_interval = flush_interval
        self._running = False
        self._task = None

    async def flush_loop(self):
        self._running = True
        await asyncio.sleep(5)
        logger.info(f"⏳ Starting Geography buffer flusher every {self.flush_interval}s")

        while self._running:
            try:
                redis = await RedisManager().check_redis_connection()
                if not redis:
                    logger.warning("⚠️ Redis not ready. Skipping geography flush cycle.")
                    await asyncio.sleep(self.flush_interval)
                    continue

                start = time.perf_counter()
                added = await GeographyRedisBuffer.flush_if_ready(redis)
                dt = time.perf_counter() - start
                if added:
                    logger.success(f"🗺️ Geography flush: {added} cells in {dt:.2f}s ⏱️")
                else:
                    logger.debug(f"🗺️ No cell geography to flush. Took {dt:.2f}s ⏱️")

            except asyncio.CancelledError:
                logger.info("🛑 Geography buffer flusher loop was cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Exception in geography buffer flusher loop: {e}")
            finally:
                await asyncio.sleep(self.flush_interval)

    async def start(self):
        if self._running:
            logger.warning("⚠️ Geography buffer flusher is already running.")
            return
        self._task = asyncio.create_task(self.flush_loop())
        logger.info("🚀 Started Geography buffer flusher.")

    async def stop(self):
        if not self._running:
            logger.warning("⚠️ Geography flusher already stopped")
            return

        self._running = False

        try:
            redis = await RedisManager().check_redis_connection()
            if redis:
                start = time.perf_counter()
                count = await GeographyRedisBuffer.force_flush(redis)
                logger.success(f"🔚 Final Geography flush ({count} cells in {time.perf_counter()-start:.2f}s)")
        except Exception as e:
            logger.error(f"❌ Final Geography flush failed: {e}")

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("🛑 Geography flusher stopped")
