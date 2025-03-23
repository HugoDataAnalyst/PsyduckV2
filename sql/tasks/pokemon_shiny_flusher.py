import time
import asyncio
from utils.logger import logger
from my_redis.connect_redis import RedisManager
from my_redis.queries.buffer.pokemon_bulk_buffer import ShinyRateRedisBuffer

class ShinyRateBufferFlusher:
    def __init__(self, flush_interval: int = 60):
        self.flush_interval = flush_interval
        self._running = False
        self._task = None

    async def flush_loop(self):
        """Periodically flush the shiny rate buffer."""
        self._running = True
        redis_manager = RedisManager()
        shiny_buffer = ShinyRateRedisBuffer()

        while self._running:
            try:
                client = await redis_manager.get_client("flush_shiny_pool")
                if not client:
                    logger.error("❌ Redis is not connected. Skipping flush.")
                    await asyncio.sleep(self.flush_interval)
                    continue
                # Measure the time taken for the flush operation
                start_time = time.perf_counter()

                # Flush the buffer
                await shiny_buffer.flush_if_ready(client)

                # Calculate the duration of the flush operation
                duration = time.perf_counter() - start_time
                logger.success(f"✨ Completed shiny rate buffer flush in {duration:.2f}s ⏱️")

            except asyncio.CancelledError:
                logger.info("🛑 Shiny rate buffer flusher loop was cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Exception in aggregated buffer flusher loop: {e}")
            finally:
                await asyncio.sleep(self.flush_interval)

    async def start(self):
        """Start the flush loop."""
        if self._running:
            logger.warning("⚠️ Shiny rate buffer flusher is already running.")
            return

        self._task = asyncio.create_task(self.flush_loop())
        logger.info("🚀 Started shiny rate buffer flusher.")

    async def stop(self):
        """Stop the flush loop."""
        if not self._running:
            logger.warning("⚠️ Shiny rate buffer flusher is not running.")
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("🛑 Shiny rate buffer flusher stopped.")
            except Exception as e:
                logger.error(f"❌ Error stopping shiny rate buffer flusher: {e}")
