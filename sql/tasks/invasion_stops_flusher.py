import asyncio
import time
from utils.logger import logger
from my_redis.connect_redis import RedisManager
from my_redis.queries.buffer.invasions_bulk_buffer import InvasionsRedisBuffer


class InvasionsBufferFlusher:
    def __init__(self, flush_interval: int = 300):
        self.flush_interval = flush_interval
        self._running = False
        self._task = None

    async def flush_loop(self):
        self._running = True
        await asyncio.sleep(5)
        logger.info(f"⏳ Starting Invasions buffer flusher every {self.flush_interval}s")

        cycle = 0
        while self._running:
            try:
                redis = await RedisManager().check_redis_connection()
                if not redis:
                    logger.warning("⚠️ Redis not ready. Skipping invasions flush cycle.")
                    await asyncio.sleep(self.flush_interval)
                    continue

                start = time.perf_counter()
                if cycle % 6 == 0:
                    added = await InvasionsRedisBuffer.force_flush(redis)
                    mode = "force"
                else:
                    added = await InvasionsRedisBuffer.flush_if_ready(redis)
                    mode = "threshold/ready"

                dt = time.perf_counter() - start
                if added:
                    logger.success(f"🛰️ Invasions flush ({mode}): +{added} rows in {dt:.2f}s ⏱️")
                else:
                    logger.info(f"🛰️ No new invasions rows to flush ({mode}). Took {dt:.2f}s ⏱️")

            except asyncio.CancelledError:
                logger.info("🛑 Invasions buffer flusher loop was cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Exception in invasions buffer flusher loop: {e}")
            finally:
                cycle += 1
                await asyncio.sleep(self.flush_interval)

    async def start(self):
        if self._running:
            logger.warning("⚠️ Invasions buffer flusher is already running.")
            return
        self._task = asyncio.create_task(self.flush_loop())
        logger.info("🚀 Started Invasions buffer flusher.")

    async def stop(self):
        if not self._running:
            logger.warning("⚠️ Invasions flusher already stopped")
            return

        self._running = False

        try:
            redis = await RedisManager().check_redis_connection()
            if redis:
                start = time.perf_counter()
                count = await InvasionsRedisBuffer.force_flush(redis)
                logger.success(f"🔚 Final Invasions flush (+{count} rows in {time.perf_counter()-start:.2f}s)")
        except Exception as e:
            logger.error(f"❌ Final Invasions flush failed: {e}")

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("🛑 Invasions flusher stopped")
            except Exception as e:
                logger.error(f"❌ Error stopping Invasions flusher: {e}")
