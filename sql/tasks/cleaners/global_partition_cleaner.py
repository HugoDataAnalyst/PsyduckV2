from __future__ import annotations
import asyncio, random
from typing import Callable, Awaitable, List, Tuple
from utils.logger import logger
import config as AppConfig
from sql.tasks.cleaners.partitions.daily_partition_cleaner import clean_daily_partitions
from sql.tasks.cleaners.partitions.monthly_partition_cleaner import clean_monthly_partitions


class PeriodicCleaner:
    """
    Runs a list of (name, async_callable) cleaners on a schedule.
    """
    def __init__(self, interval_sec: int = 6 * 60 * 60, initial_jitter_s: float = 15.0):
        self.interval = int(interval_sec)
        self.initial_jitter_s = float(initial_jitter_s)
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._jobs: List[Tuple[str, Callable[[], Awaitable[dict]]]] = []

    def add_job(self, name: str, fn: Callable[[], Awaitable[dict]]):
        self._jobs.append((name, fn))

    async def _run_once(self, tag: str):
        for name, fn in self._jobs:
            try:
                res = await fn()
                logger.info(f"🧹 [{tag}] cleanup `{name}` → {res}")
            except Exception as e:
                logger.error(f"❌ cleaner `{name}` failed: {e}", exc_info=True)

    async def _loop(self):
        if self.initial_jitter_s > 0:
            await asyncio.sleep(random.uniform(0, self.initial_jitter_s))
        await self._run_once("startup")
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                await self._run_once("interval")

    async def start(self):
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._loop())
        logger.info("🚀 Partition cleaners started.")

    async def stop(self):
        if not self._task:
            return
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        logger.info("🛑 Partition cleaners stopped.")


def build_default_cleaner() -> PeriodicCleaner:
    """
    Uses AppConfig.* retention knobs:
      - clean_pokemon_older_than_x_days
      - clean_raid_older_than_x_days
      - clean_quest_older_than_x_days
      - clean_invasion_older_than_x_days
      - clean_pokemon_shiny_older_than_x_months  (monthly)
    """
    cleaner = PeriodicCleaner(interval_sec=12 * 60 * 60, initial_jitter_s=10.0)  # run twice a day by default

    # DAILY tables
    def daily_job(table: str, keep_days: int):
        async def _run():
            return await clean_daily_partitions(
                table=table,
                column="day_date",
                keep_days=keep_days,
                dry_run=False,
            )
        return _run

    def _maybe_add(enabled: bool, name: str, factory: Callable[[], Callable[[], Awaitable[dict]]]):
        if not enabled:
            logger.info(f"⏭️ Skipping cleaner `{name}` (disabled by config).")
            return
        cleaner.add_job(name, factory())
        logger.success(f"✅ Registered cleaner `{name}`.")

    def _skip_if_nonpositive(keep_value: int, unit: str, name: str) -> bool:
        try:
            v = int(keep_value)
        except Exception:
            logger.warning(f"⚠️ `{name}` retention value invalid: {keep_value!r}. Will still register.")
            return False
        if v <= 0:
            logger.info(f"⏭️ Skipping cleaner `{name}` (keep_{unit} <= 0).")
            return True
        return False

    # Add tables for cleaning
    # Daily Tables
    if not _skip_if_nonpositive(AppConfig.clean_pokemon_older_than_x_days, "days", "pokemon_iv_daily_events"):
        _maybe_add(
            AppConfig.store_sql_pokemon_aggregation,
            "pokemon_iv_daily_events",
            lambda: daily_job("pokemon_iv_daily_events", AppConfig.clean_pokemon_older_than_x_days),
        )

    if not _skip_if_nonpositive(AppConfig.clean_raid_older_than_x_days, "days", "raids_daily_events"):
        _maybe_add(
            AppConfig.store_sql_raid_aggregation,
            "raids_daily_events",
            lambda: daily_job("raids_daily_events", AppConfig.clean_raid_older_than_x_days),
        )

    if not _skip_if_nonpositive(AppConfig.clean_quest_older_than_x_days, "days", "quests_daily_events"):
        _maybe_add(
            AppConfig.store_sql_quest_aggregation,
            "quests_item_daily_events",
            lambda: daily_job("quests_item_daily_events", AppConfig.clean_quest_older_than_x_days),
        )
        _maybe_add(
            AppConfig.store_sql_quest_aggregation,
            "quests_pokemon_daily_events",
            lambda: daily_job("quests_pokemon_daily_events", AppConfig.clean_quest_older_than_x_days),
        )

    if not _skip_if_nonpositive(AppConfig.clean_invasion_older_than_x_days, "days", "invasions_daily_events"):
        _maybe_add(
            AppConfig.store_sql_invasion_aggregation,
            "invasions_daily_events",
            lambda: daily_job("invasions_daily_events", AppConfig.clean_invasion_older_than_x_days),
        )

    # MONTHLY table
    SHINY_TABLE = "shiny_username_rates"
    def shiny_monthly():
        async def _run():
            return await clean_monthly_partitions(
                table=SHINY_TABLE,
                column="month_year",
                keep_months=AppConfig.clean_pokemon_shiny_older_than_x_months,
                dry_run=False,
            )
        return _run

    if not _skip_if_nonpositive(AppConfig.clean_pokemon_shiny_older_than_x_months, "months", "pokemon_shiny_rates_monthly"):
        _maybe_add(
            AppConfig.store_sql_pokemon_shiny,
            "pokemon_shiny_rates_monthly",
            shiny_monthly,
        )

    return cleaner
