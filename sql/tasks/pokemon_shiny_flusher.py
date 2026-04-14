import time
import asyncio
from utils.logger import logger
from my_redis.queries.buffer.pokemon_bulk_buffer import ShinyRateBuffer
from sql.tasks.pokemon_processor import PokemonSQLProcessor


class ShinyRateBufferFlusher:
    def __init__(self, flush_interval: int = 60):
        self.flush_interval = flush_interval
        self._running = False
        self._task = None

    async def flush_loop(self):
        """Periodically flush the shiny rate buffer."""
        self._running = True

        cycle = 0
        while self._running:
            try:
                start_time = time.perf_counter()
                mode = "force" if cycle % 6 == 0 else "threshold/ready"

                counts = await ShinyRateBuffer.flush()
                if counts:
                    data_batch, malformed = ShinyRateBuffer.build_batch(counts)
                    if malformed:
                        logger.warning(f"⚠️ Shiny buffer: {malformed} malformed record(s) discarded")
                    added = await PokemonSQLProcessor.bulk_upsert_shiny_username_rate_batch(data_batch)
                    duration = time.perf_counter() - start_time
                    logger.success(f"✨ Shiny buffer flush ({mode}): +{added} rows in {duration:.2f}s ⏱️")
                else:
                    duration = time.perf_counter() - start_time
                    logger.debug(f"✨ No shiny rows to flush ({mode}). Took {duration:.2f}s ⏱️")

            except asyncio.CancelledError:
                logger.info("🛑 Shiny rate buffer flusher loop was cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Exception in shiny buffer flusher loop: {e}")
            finally:
                cycle += 1
                await asyncio.sleep(self.flush_interval)

    async def start(self):
        """Start the flush loop."""
        if self._running:
            logger.warning("⚠️ Shiny rate buffer flusher is already running.")
            return
        self._task = asyncio.create_task(self.flush_loop())
        logger.info("🚀 Started shiny rate buffer flusher.")

    async def stop(self):
        """Stop the flusher and perform one final forced flush."""
        if not self._running:
            logger.warning("⚠️ Shiny rate flusher already stopped")
            return

        self._running = False

        # Final flush
        try:
            start = time.perf_counter()
            counts = await ShinyRateBuffer.flush()
            if counts:
                data_batch, _ = ShinyRateBuffer.build_batch(counts)
                count = await PokemonSQLProcessor.bulk_upsert_shiny_username_rate_batch(data_batch)
                logger.success(
                    f"🔚 Final shiny ✨ flush completed (+{count} rows in {time.perf_counter()-start:.2f}s)"
                )
        except Exception as e:
            logger.error(f"❌ Final shiny flush failed: {e}")

        # Cancel task
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("🛑 Shiny rate flusher stopped")
            except Exception as e:
                logger.error(f"❌ Error stopping shiny flusher: {e}")
