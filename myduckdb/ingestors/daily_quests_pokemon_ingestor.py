from __future__ import annotations
import asyncio
from datetime import date, timedelta
import duckdb
from utils.logger import logger
import config as AppConfig
from myduckdb.connect_duck import connect_duck, mysql_query_attached, mysql_self_test

DUCK_TABLE  = "quests_pokemon_daily_events"
MYSQL_TABLE = "quests_pokemon_daily_events"

DEFAULT_MIN_AGE_DAYS    = 2
DEFAULT_MIN_STABLE_RUNS = 2

def _pname(d: date) -> str:
    return "p" + d.strftime("%Y%m%d")

class QuestsPokemonDuckIngestor:
    def __init__(self, interval_sec: int = 3600, days_back: int = 2,
                 min_age_days: int = DEFAULT_MIN_AGE_DAYS,
                 min_stable_runs: int = DEFAULT_MIN_STABLE_RUNS,
                 mysql_alias: str = "mys"):
        self.interval = int(interval_sec)
        self.days_back = int(days_back)
        self.min_age_days = int(min_age_days)
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
        logger.success("🚀 DuckDB quests-pokemon ingestor started.")

    async def stop(self):
        if not self._task:
            return
        self._stop.set()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        logger.info("🛑 DuckDB quests-pokemon ingestor stopped.")

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
            logger.error(f"❌ DuckDB (quests-pokemon) ingest [{tag}] failed: {e}", exc_info=True)

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

            today = date.today()
            targets = [today - timedelta(days=d) for d in range(1, self.days_back + 1)]
            for day in targets:
                synced, mysql_rows, duck_rows = self._sync_one_day(con, day)
                logger.success(f"🦆 Quests-Pokemon ingest {day} | synced={synced} mysql_rows={mysql_rows} duck_rows={duck_rows}")
        finally:
            try:
                con.close()
            except Exception:
                pass

    # core
    def _ensure_duck_schema(self, con: duckdb.DuckDBPyConnection):
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {DUCK_TABLE} (
              pokestop    TEXT,
              area_id     SMALLINT,
              seen_at     TIMESTAMP,
              day_date    DATE,
              mode        TINYINT,
              task_type   SMALLINT,
              poke_id     SMALLINT,
              poke_form   VARCHAR,
              total_count INTEGER
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS _meta_qpokemon_daily_ingest (
              day_date        DATE PRIMARY KEY,
              last_mysql_rows BIGINT,
              last_duck_rows  BIGINT,
              last_synced_at  TIMESTAMP,
              stable_runs     SMALLINT DEFAULT 0,
              finalized       BOOLEAN  DEFAULT FALSE
            )
        """)

    def _mysql_count_for_day(self, con: duckdb.DuckDBPyConnection, day: date) -> int:
        pname = _pname(day)
        try:
            sql = f"SELECT COUNT(*) AS cnt FROM {MYSQL_TABLE} PARTITION ({pname})"
            return int(mysql_query_attached(con, self.mysql_alias, sql).fetchone()[0])
        except Exception:
            sql = f"SELECT COUNT(*) AS cnt FROM {MYSQL_TABLE} WHERE day_date = DATE '{day.isoformat()}'"
            return int(mysql_query_attached(con, self.mysql_alias, sql).fetchone()[0])

    def _duck_count_for_day(self, con: duckdb.DuckDBPyConnection, day: date) -> int:
        return int(con.execute(
            f"SELECT COUNT(*) FROM {DUCK_TABLE} WHERE day_date = ?", [day]
        ).fetchone()[0])

    def _load_day_full(self, con: duckdb.DuckDBPyConnection, day: date) -> int:
        con.execute(f"DELETE FROM {DUCK_TABLE} WHERE day_date = ?", [day])
        sel = (
            f"SELECT pokestop, area_id, CAST(seen_at AS TIMESTAMP) AS seen_at, day_date, "
            f"       mode, task_type, poke_id, poke_form, total_count "
            f"FROM {self.mysql_alias}.{AppConfig.db_name}.{MYSQL_TABLE} "
            f"WHERE day_date = DATE '{day.isoformat()}'"
        )
        con.execute(f"INSERT INTO {DUCK_TABLE} BY NAME {sel}")
        return self._duck_count_for_day(con, day)

    def _read_meta(self, con, day):
        r = con.execute("""
            SELECT day_date, last_mysql_rows, last_duck_rows, last_synced_at, stable_runs, finalized
            FROM _meta_qpokemon_daily_ingest WHERE day_date = ?""", [day]).fetchone()
        if not r:
            return None
        return {
            "day_date": r[0], "last_mysql_rows": r[1], "last_duck_rows": r[2],
            "last_synced_at": r[3], "stable_runs": r[4], "finalized": bool(r[5]),
        }

    def _write_meta(self, con, day, mysql_rows, duck_rows, stable_runs, finalized):
        con.execute("""
            INSERT INTO _meta_qpokemon_daily_ingest
            (day_date, last_mysql_rows, last_duck_rows, last_synced_at, stable_runs, finalized)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            ON CONFLICT (day_date) DO UPDATE SET
              last_mysql_rows = EXCLUDED.last_mysql_rows,
              last_duck_rows  = EXCLUDED.last_duck_rows,
              last_synced_at  = EXCLUDED.last_synced_at,
              stable_runs     = EXCLUDED.stable_runs,
              finalized       = EXCLUDED.finalized
        """, [day, mysql_rows, duck_rows, stable_runs, finalized])

    def _load_or_skip(self, con, day, mysql_rows, prev):
        if prev and prev.get("finalized"):
            return False, self._duck_count_for_day(con, day)
        duck_rows = self._duck_count_for_day(con, day)
        if duck_rows == mysql_rows and mysql_rows > 0:
            return False, duck_rows
        return True, self._load_day_full(con, day)

    def _sync_one_day(self, con, day: date):
        if day >= date.today():
            logger.warning(f"⛔ Skipping live/future day {day}")
            return False, 0, self._duck_count_for_day(con, day)

        mysql_rows = self._mysql_count_for_day(con, day)
        prev = self._read_meta(con, day)
        stable_runs = (int(prev.get("stable_runs") or 0) + 1) if (prev and prev.get("last_mysql_rows") == mysql_rows) else 0

        synced, duck_rows = self._load_or_skip(con, day, mysql_rows, prev or {})
        age_days = (date.today() - day).days
        finalized = (age_days >= self.min_age_days and
                     stable_runs >= self.min_stable_runs and
                     mysql_rows == duck_rows)

        self._write_meta(con, day, mysql_rows, duck_rows, stable_runs, finalized)
        return synced, mysql_rows, duck_rows
