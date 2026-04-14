import asyncio
import time
from utils.logger import logger
from my_redis.queries.buffer.pokemon_bulk_buffer import PokemonIVBuffer
from sql.tasks.pokemon_processor import PokemonSQLProcessor


class PokemonIVBufferFlusher:
    def __init__(self, flush_interval: int = 300):
        self.flush_interval = flush_interval
        self._running = False
        self._task = None

    async def flush_loop(self):
        """Periodically flush the Pokémon IV buffer."""
        self._running = True
        await asyncio.sleep(5)  # Initial delay
        logger.info(f"⏳ Starting Pokémon IV events buffer flusher every {self.flush_interval}s")

        cycle = 0
        while self._running:
            try:
                start = time.perf_counter()
                mode = "force" if cycle % 6 == 0 else "threshold/ready"

                data = await PokemonIVBuffer.flush()
                if data:
                    events, coords = data
                    data_batch, used_coords, missing_coords, malformed = PokemonIVBuffer.build_batch(events, coords)
                    if malformed:
                        logger.warning(f"⚠️ Pokémon IV buffer: {malformed} malformed event(s) discarded")
                    added = await PokemonSQLProcessor.bulk_insert_iv_daily_events(data_batch)
                    duration = time.perf_counter() - start
                    logger.success(
                        f"👻 Pokémon IV Events flush ({mode}): +{added} rows in {duration:.2f}s ⏱️ "
                        f"[coords: {used_coords} used, {missing_coords} missing]"
                    )
                else:
                    duration = time.perf_counter() - start
                    logger.debug(f"👻 No new Pokémon IV events rows to flush ({mode}). Took {duration:.2f}s ⏱️")

            except asyncio.CancelledError:
                logger.info("🛑 Pokémon IV buffer flusher loop was cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Exception in aggregated buffer flusher loop: {e}")
            finally:
                cycle += 1
                await asyncio.sleep(self.flush_interval)

    async def start(self):
        """Start the flush loop."""
        if self._running:
            logger.warning("⚠️ Pokémon IV buffer flusher is already running.")
            return
        self._task = asyncio.create_task(self.flush_loop())
        logger.info("🚀 Started Pokémon IV buffer flusher.")

    async def stop(self):
        """Stop the flusher and perform one final forced flush."""
        if not self._running:
            logger.warning("⚠️ Pokémon IV flusher already stopped")
            return

        self._running = False

        # Final flush
        try:
            start = time.perf_counter()
            data = await PokemonIVBuffer.flush()
            if data:
                events, coords = data
                data_batch, _, _, _ = PokemonIVBuffer.build_batch(events, coords)
                count = await PokemonSQLProcessor.bulk_insert_iv_daily_events(data_batch)
                logger.success(
                    f"🔚 Final Pokémon 👻 IV flush completed (+{count} rows in {time.perf_counter()-start:.2f}s)"
                )
        except Exception as e:
            logger.error(f"❌ Final Pokémon IV flush failed: {e}")

        # Cancel task
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                logger.info("🛑 Pokémon IV flusher stopped")
            except Exception as e:
                logger.error(f"❌ Error stopping Pokémon IV flusher: {e}")
