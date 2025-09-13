import asyncio, random
from utils.logger import logger
from sql.utils.create_partitions import ensure_daily_partitions

class DailyPartitionEnsurer:
    def __init__(
        self,
        ensure_interval: int = 24 * 60 * 60,  # seconds
        days_back: int = 2,
        days_forward: int = 35,
        table: str = "",
        column: str = "day_date",
        initial_jitter_s: float = 5.0,
    ):
        self.ensure_interval = int(ensure_interval)
        self.days_back = int(days_back)
        self.days_forward = int(days_forward)
        self.table = table
        self.column = column
        self.initial_jitter_s = float(initial_jitter_s)
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def _loop(self):
        # run once immediately
        if self.initial_jitter_s > 0:
            await asyncio.sleep(random.uniform(0, self.initial_jitter_s))
        await self._run_once("startup")
        # then run periodically
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.ensure_interval)
            except asyncio.TimeoutError:
                await self._run_once("interval")

    async def _run_once(self, tag: str):
        try:
            res = await ensure_daily_partitions(
                table=self.table,
                column=self.column,
                days_back=self.days_back,
                days_forward=self.days_forward,
            )
            logger.success(f"🧩 [{tag}] partitions ensured for `{self.table}`: {res}")
        except Exception as e:
            logger.error(f"❌ [{tag}] partition ensure failed for {self.table}: {e}", exc_info=True)

    async def start(self):
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._loop())
        logger.info(f"🚀 Daily partition ensurer started for {self.table}.")

    async def stop(self):
        if not self._task:
            return
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        logger.info(f"🛑 Daily partition ensurer stopped for {self.table}.")
