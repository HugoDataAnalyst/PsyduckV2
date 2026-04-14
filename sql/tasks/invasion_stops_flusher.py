import asyncio
import time
from utils.logger import logger
from my_redis.queries.buffer.invasions_bulk_buffer import InvasionsBuffer
from sql.tasks.invasions_processor import InvasionSQLProcessor


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
                start = time.perf_counter()
                mode = "force" if cycle % 6 == 0 else "threshold/ready"

                events = await InvasionsBuffer.flush()
                if events:
                    data_batch, malformed = InvasionsBuffer.build_batch(events)
                    if malformed:
                        logger.warning(f"⚠️ Invasions buffer: {malformed} malformed event(s) discarded")
                    added = await InvasionSQLProcessor.bulk_insert_invasions_daily_events(data_batch)
                    dt = time.perf_counter() - start
                    logger.success(f"🛰️ Invasions flush ({mode}): +{added} rows in {dt:.2f}s ⏱️")
                else:
                    dt = time.perf_counter() - start
                    logger.debug(f"🛰️ No new invasions rows to flush ({mode}). Took {dt:.2f}s ⏱️")

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

        # Final flush
        try:
            start = time.perf_counter()
            events = await InvasionsBuffer.flush()
            if events:
                data_batch, _ = InvasionsBuffer.build_batch(events)
                count = await InvasionSQLProcessor.bulk_insert_invasions_daily_events(data_batch)
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
