import asyncio
import time
from utils.logger import logger
from my_redis.connect_redis import RedisManager
from my_redis.queries.buffer.raids_bulk_buffer import RaidsRedisBuffer


class RaidsBufferFlusher:
    def __init__(self, flush_interval: int = 300):
        self.flush_interval = flush_interval
        self._running = False
        self._task = None

    async def flush_loop(self):
        """Periodically flush the raids buffer."""
        self._running = True
        await asyncio.sleep(5)
        logger.info(f"⏳ Starting Raids buffer flusher every {self.flush_interval}s")

        cycle = 0
        while self._running:
            try:
                redis = await RedisManager().check_redis_connection()
                if not redis:
                    logger.warning("⚠️ Redis not ready. Skipping raids flush cycle.")
                    await asyncio.sleep(self.flush_interval)
                    continue

                start = time.perf_counter()

                if cycle % 6 == 0:
                    added = await RaidsRedisBuffer.force_flush(redis)
                    mode = "force"
                else:
                    added = await RaidsRedisBuffer.flush_if_ready(redis)
                    mode = "threshold/ready"

                dt = time.perf_counter() - start
                if added:
                    logger.success(f"🏰 Raids flush ({mode}): +{added} rows in {dt:.2f}s ⏱️")
                else:
                    logger.debug(f"🏰 No new raids rows to flush ({mode}). Took {dt:.2f}s ⏱️")

            except asyncio.CancelledError:
                logger.info("🛑 Raids buffer flusher loop was cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Exception in raids buffer flusher loop: {e}")
            finally:
                cycle += 1
                await asyncio.sleep(self.flush_interval)

    async def start(self):
        if self._running:
            logger.warning("⚠️ Raids buffer flusher is already running.")
            return
        self._task = asyncio.create_task(self.flush_loop())
        logger.info("🚀 Started Raids buffer flusher.")

    async def stop(self):
        if not self._running:
            logger.warning("⚠️ Raids flusher already stopped")
            return

        self._running = False

        try:
            redis = await RedisManager().check_redis_connection()
            if redis:
                start = time.perf_counter()
                count = await RaidsRedisBuffer.force_flush(redis)
                logger.success(f"🔚 Final Raids flush completed (+{count} rows in {time.perf_counter()-start:.2f}s)")
        except Exception as e:
            logger.error(f"❌ Final Raids flush failed: {e}")

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("🛑 Raids flusher stopped")
            except Exception as e:
                logger.error(f"❌ Error stopping Raids flusher: {e}")
