import asyncio
import time
from utils.logger import logger
from my_redis.connect_redis import RedisManager
from my_redis.queries.buffer.pokemon_bulk_buffer import PokemonIVRedisBuffer
from sql.tasks.pokemon_processor import PokemonSQLProcessor

class PokemonIVBufferFlusher:
    def __init__(self, flush_interval: int = 300):  # 5 minutes
        self.flush_interval = flush_interval

    async def flush_loop(self):
        await asyncio.sleep(5)  # Initial delay
        logger.info(f"⏳ Starting Pokémon IV aggregated buffer flusher every {self.flush_interval}s")
        while True:
            try:
                redis = await RedisManager.check_redis_connection("pokemon_pool")
                if not redis:
                    logger.warning("⚠️ Redis not ready. Skipping flush cycle.")
                    await asyncio.sleep(self.flush_interval)
                    continue

                start = time.perf_counter()
                # Force flush regardless of threshold every 5 minutes
                await PokemonIVRedisBuffer.flush_if_ready(redis)
                duration = time.perf_counter() - start

                logger.success(f"👻 Completed aggregated pokemon heatmap flush in {duration:.2f}s ⏱️")
            except Exception as e:
                logger.error(f"❌ Exception in aggregated buffer flusher loop: {e}")
            finally:
                await asyncio.sleep(self.flush_interval)

