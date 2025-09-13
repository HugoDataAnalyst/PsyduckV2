from __future__ import annotations
import asyncio
from datetime import date
import duckdb
from utils.logger import logger
import config as AppConfig
from myduckdb.connect_duck import (
    connect_duck,
    mysql_query_attached,
    mysql_self_test,
)

DUCK_TABLE  = "shiny_username_rates"
MYSQL_TABLE = "shiny_username_rates"

DEFAULT_MIN_AGE_MONTHS   = 1      # finalize only after at least this many months old
DEFAULT_MIN_STABLE_RUNS  = 2      # require N identical MySQL counts before finalizing

# ---------- date helpers ----------
def _first_of_month(d: date) -> date:
    return d.replace(day=1)

def _add_months(d: date, delta: int) -> date:
    y = d.year
    m = d.month + delta
    y += (m - 1) // 12
    m = ((m - 1) % 12) + 1
    return date(y, m, 1)

def _yymm(d: date) -> int:
    # 2509 for 2025-09
    return int(d.strftime("%y%m"))

def _age_in_months(then: date, now: date) -> int:
    # whole month difference (e.g., 2025-08-01 to 2025-09-01 -> 1)
    return (now.year - then.year) * 12 + (now.month - then.month)

class ShinyRatesDuckIngestor:
    """
    Periodically mirrors MySQL.shiny_username_rates into DuckDB.shiny_username_rates by month (YYMM).
    - Skips the current month by default (to avoid churning during active upserts)
    - Uses a small meta table to track stability and 'finalized' months
    """

    def __init__(
        self,
        interval_sec: int = 3600,
        months_back: int = 2,
        *,
        include_current_month: bool = False,
        min_age_months: int = DEFAULT_MIN_AGE_MONTHS,
        min_stable_runs: int = DEFAULT_MIN_STABLE_RUNS,
        mysql_alias: str = "mys",
    ):
        self.interval = int(interval_sec)
        self.months_back = max(0, int(months_back))
        self.include_current_month = bool(include_current_month)
        self.min_age_months = int(min_age_months)
        self.min_stable_runs = int(min_stable_runs)
        self.mysql_alias = mysql_alias
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    # lifecycle
    async def start(self):
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._loop())
        logger.success("🚀 DuckDB monthly shiny ingestor started.")

    async def stop(self):
        if not self._task:
            return
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        logger.info("🛑 DuckDB monthly shiny ingestor stopped.")

    async def _loop(self):
        await self._run_once("startup")
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                await self._run_once("interval")

    async def _run_once(self, tag: str):
        try:
            await asyncio.to_thread(self._ingest_window, tag)
        except Exception as e:
            logger.error(f"❌ DuckDB (shiny) ingest [{tag}] failed: {e}", exc_info=True)

    # internal sync
    def _ingest_window(self, tag: str):
        con = connect_duck(read_only=False)
        try:
            self._ensure_duck_schema(con)

            ok, msg = mysql_self_test(con, alias=self.mysql_alias, probe_table=MYSQL_TABLE)
            if not ok:
                logger.error(f"🧪 MySQL self-test failed: {msg}")
                return
            logger.info(f"🧪 {msg}")

            today_m1 = _first_of_month(date.today())  # e.g., 2025-09-01
            # Build target first-of-months, newest → oldest (or vice versa; we’ll just iterate)
            targets: list[date] = []
            start_delta = 0 if self.include_current_month else 1  # 0 = include current, 1 = start at prev month
            for d in range(start_delta, self.months_back + 1):
                targets.append(_add_months(today_m1, -d))

            for mfirst in targets:
                synced, mysql_rows, duck_rows = self._sync_one_month(con, mfirst, use_alias=True)
                logger.success(
                    f"🦆 Ingest Shiny {mfirst.strftime('%Y-%m')} "
                    f"| synced={synced} mysql_rows={mysql_rows} duck_rows={duck_rows}"
                )
        finally:
            try:
                con.close()
            except Exception:
                pass

    # core
    def _ensure_duck_schema(self, con: duckdb.DuckDBPyConnection):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {DUCK_TABLE} (
              username    TEXT,
              pokemon_id  SMALLINT,
              form        VARCHAR,
              shiny       TINYINT,
              area_id     SMALLINT,
              month_year  SMALLINT,
              total_count INTEGER
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS _meta_shiny_month_ingest (
              month_yymm    SMALLINT PRIMARY KEY,
              first_of_month DATE,
              last_mysql_rows BIGINT,
              last_duck_rows  BIGINT,
              last_synced_at  TIMESTAMP,
              stable_runs     SMALLINT DEFAULT 0,
              finalized       BOOLEAN  DEFAULT FALSE
            )
        """)

    # counting & loading
    def _mysql_count_for_month(self, con: duckdb.DuckDBPyConnection, mfirst: date, *, use_alias: bool) -> int:
        assert use_alias, "mysql_query requires attached alias (DuckDB 1.3.x)"
        yymm = _yymm(mfirst)
        pname = f"p{mfirst.strftime('%y%m')}"
        # try partition fast-path
        try:
            sql = f"SELECT COUNT(*) AS cnt FROM {MYSQL_TABLE} PARTITION ({pname})"
            return int(mysql_query_attached(con, self.mysql_alias, sql).fetchone()[0])
        except Exception:
            # fallback by predicate on month_year
            sql = f"SELECT COUNT(*) AS cnt FROM {MYSQL_TABLE} WHERE month_year = {yymm}"
            return int(mysql_query_attached(con, self.mysql_alias, sql).fetchone()[0])

    def _duck_count_for_month(self, con: duckdb.DuckDBPyConnection, mfirst: date) -> int:
        return int(con.execute(
            f"SELECT COUNT(*) FROM {DUCK_TABLE} WHERE month_year = ?",
            [_yymm(mfirst)]
        ).fetchone()[0])

    def _load_month_full(self, con: duckdb.DuckDBPyConnection, mfirst: date, *, use_alias: bool) -> int:
        assert use_alias
        yymm = _yymm(mfirst)
        # Replace the month atomically in DuckDB
        con.execute(f"DELETE FROM {DUCK_TABLE} WHERE month_year = ?", [yymm])
        sel = (
            f"SELECT username, pokemon_id, form, shiny, area_id, month_year, total_count "
            f"FROM {self.mysql_alias}.{AppConfig.db_name}.{MYSQL_TABLE} "
            f"WHERE month_year = {yymm}"
        )
        con.execute(f"INSERT INTO {DUCK_TABLE} BY NAME {sel}")
        return self._duck_count_for_month(con, mfirst)

    # meta helpers
    def _read_meta(self, con: duckdb.DuckDBPyConnection, mfirst: date):
        yymm = _yymm(mfirst)
        r = con.execute(
            """
            SELECT month_yymm, first_of_month, last_mysql_rows, last_duck_rows,
                   last_synced_at, stable_runs, finalized
            FROM _meta_shiny_month_ingest
            WHERE month_yymm = ?
            """,
            [yymm],
        ).fetchone()
        if not r:
            return None
        return {
            "month_yymm": r[0],
            "first_of_month": r[1],
            "last_mysql_rows": r[2],
            "last_duck_rows": r[3],
            "last_synced_at": r[4],
            "stable_runs": r[5],
            "finalized": bool(r[6]),
        }

    def _write_meta(self, con, mfirst: date, mysql_rows: int, duck_rows: int, stable_runs: int, finalized: bool):
        yymm = _yymm(mfirst)
        con.execute(
            """
            INSERT INTO _meta_shiny_month_ingest
              (month_yymm, first_of_month, last_mysql_rows, last_duck_rows, last_synced_at, stable_runs, finalized)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            ON CONFLICT (month_yymm) DO UPDATE SET
              first_of_month = EXCLUDED.first_of_month,
              last_mysql_rows = EXCLUDED.last_mysql_rows,
              last_duck_rows  = EXCLUDED.last_duck_rows,
              last_synced_at  = EXCLUDED.last_synced_at,
              stable_runs     = EXCLUDED.stable_runs,
              finalized       = EXCLUDED.finalized
            """,
            [yymm, mfirst, mysql_rows, duck_rows, stable_runs, finalized],
        )

    # sync one month
    def _load_or_skip(self, con, mfirst: date, mysql_rows: int, prev_meta: dict | None, *, use_alias: bool):
        if prev_meta and prev_meta.get("finalized"):
            # Already frozen, don’t touch unless you want to reprocess manually
            return False, self._duck_count_for_month(con, mfirst)

        duck_rows = self._duck_count_for_month(con, mfirst)
        if duck_rows == mysql_rows and mysql_rows > 0:
            return False, duck_rows

        duck_rows_after = self._load_month_full(con, mfirst, use_alias=use_alias)
        return True, duck_rows_after

    def _sync_one_month(self, con, mfirst: date, *, use_alias: bool):
        # Optional policy: don’t process future months (shouldn’t happen)
        if mfirst > _first_of_month(date.today()):
            logger.warning(f"⛔ Skipping future month {mfirst.strftime('%Y-%m')}")
            mysql_rows = 0
            duck_rows = self._duck_count_for_month(con, mfirst)
            return False, mysql_rows, duck_rows

        mysql_rows = self._mysql_count_for_month(con, mfirst, use_alias=use_alias)
        prev = self._read_meta(con, mfirst)

        # if MySQL rowcount unchanged → bump stability
        prev_mysql = (prev or {}).get("last_mysql_rows")
        stable_runs = (int((prev or {}).get("stable_runs") or 0) + 1) if (prev_mysql == mysql_rows) else 0

        synced, duck_rows = self._load_or_skip(con, mfirst, mysql_rows, prev, use_alias=use_alias)

        # finalize policy
        age_months = _age_in_months(mfirst, _first_of_month(date.today()))
        finalized = (age_months >= self.min_age_months and
                     stable_runs >= self.min_stable_runs and
                     mysql_rows == duck_rows)

        self._write_meta(con, mfirst, mysql_rows, duck_rows, stable_runs, finalized)
        return synced, mysql_rows, duck_rows
