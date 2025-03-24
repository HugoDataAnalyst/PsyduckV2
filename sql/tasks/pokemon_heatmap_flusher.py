import asyncio
import time
from utils.logger import logger
from my_redis.connect_redis import RedisManager
from my_redis.queries.buffer.pokemon_bulk_buffer import PokemonIVRedisBuffer

class PokemonIVBufferFlusher:
    def __init__(self, flush_interval: int = 300):
        self.flush_interval = flush_interval
        self._running = False
        self._task = None

    async def flush_loop(self):
        """Periodically flush the Pokémon IV buffer."""
        self._running = True
        await asyncio.sleep(5)  # Initial delay
        logger.info(f"⏳ Starting Pokémon IV aggregated buffer flusher every {self.flush_interval}s")

        while self._running:
            try:
                redis = await RedisManager.check_redis_connection("flush_heatmap_pool")
                if not redis:
                    logger.warning("⚠️ Redis not ready. Skipping flush cycle.")
                    await asyncio.sleep(self.flush_interval)
                    continue

                start = time.perf_counter()
                # Force flush regardless of threshold every 5 minutes
                await PokemonIVRedisBuffer.flush_if_ready(redis)
                duration = time.perf_counter() - start

                logger.success(f"👻 Completed aggregated Pokémon heatmap flush in {duration:.2f}s ⏱️")
            except asyncio.CancelledError:
                logger.info("🛑 Pokémon IV buffer flusher loop was cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Exception in aggregated buffer flusher loop: {e}")
            finally:
                await asyncio.sleep(self.flush_interval)

    async def start(self):
        """Start the flush loop."""
        if self._running:
            logger.warning("⚠️ Pokémon IV buffer flusher is already running.")
            return

        self._task = asyncio.create_task(self.flush_loop())
        logger.info("🚀 Started Pokémon IV buffer flusher.")

    async def stop(self):
        """Stop the flush loop."""
        if not self._running:
            logger.warning("⚠️ Pokémon IV buffer flusher is not running.")
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("🛑 Pokémon IV buffer flusher stopped.")
            except Exception as e:
                logger.error(f"❌ Error stopping Pokémon IV buffer flusher: {e}")
