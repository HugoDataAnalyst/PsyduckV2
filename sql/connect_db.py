import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Optional
import aiomysql
import config as AppConfig
from utils.logger import logger

_pool: Optional[aiomysql.Pool] = None


def _cfg(name: str, default: Any) -> Any:
    return getattr(AppConfig, name, default)


async def _test_connection(pool: aiomysql.Pool) -> None:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1")


async def init_db() -> None:
    """Initialize a global aiomysql pool with retries."""
    global _pool
    retries = _cfg("db_retry_connection", 5)
    delay_s = _cfg("db_retry_delay_sec", 5)

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"⬆️ Initializing DB pool (Attempt {attempt}/{retries})...")
            _pool = await aiomysql.create_pool(
                host=_cfg("db_host", "127.0.0.1"),
                port=_cfg("db_port", 3306),
                user=_cfg("db_user", "root"),
                password=_cfg("db_password", ""),
                db=_cfg("db_name", ""),
                minsize=_cfg("db_pool_min", 1),
                maxsize=_cfg("db_pool_max", 10),
                autocommit=True,                  # default runtime mode
                charset="utf8mb4",
                connect_timeout=_cfg("db_connect_timeout", 10),
                pool_recycle=_cfg("db_pool_recycle_sec", 1800),  # prevent stale conns
            )
            await _test_connection(_pool)
            logger.success("✅ Database pool initialized.")
            return
        except Exception as e:
            logger.error(f"❌ DB init failed: {e!r}")
            if attempt < retries:
                logger.info(f"🔃 Retrying in {delay_s}s...")
                await asyncio.sleep(delay_s)
            else:
                logger.error("❌ Exhausted all DB init attempts.")
                raise


async def close_db() -> None:
    """Close the global pool."""
    global _pool
    if _pool is not None:
        logger.info("🫣 Closing database pool...")
        _pool.close()
        await _pool.wait_closed()
        _pool = None
        logger.info("🙌 Database pool closed.")


def _require_pool() -> aiomysql.Pool:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized. Call init_db() first.")
    return _pool


@asynccontextmanager
async def acquire_cursor(dict_cursor: bool = True):
    """
    Autocommit cursor (useful for reads and single statements).
    Usage:
        async with acquire_cursor() as cur:
            await cur.execute("SELECT ...", params)
            rows = await cur.fetchall()
    """
    pool = _require_pool()
    conn = await pool.acquire()
    try:
        cursor_cls = aiomysql.DictCursor if dict_cursor else aiomysql.Cursor
        async with conn.cursor(cursor_cls) as cur:
            # Keepalive: ensure the connection is alive before first use
            try:
                await conn.ping(reconnect=True)
            except Exception:
                # On ping failure, reopen by releasing & reacquiring
                pool.release(conn)
                conn = await pool.acquire()
                async with conn.cursor(cursor_cls) as cur2:
                    yield cur2
                return
            yield cur
    finally:
        pool.release(conn)


@asynccontextmanager
async def transaction(dict_cursor: bool = True, isolation: str | None = None, lock_wait_timeout: int | None = None):
    pool = _require_pool()
    conn = await pool.acquire()
    try:
        await conn.ping(reconnect=True)
        if isolation:
            async with conn.cursor() as c:
                await c.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation}")
        if lock_wait_timeout is not None:
            async with conn.cursor() as c:
                await c.execute("SET SESSION innodb_lock_wait_timeout = %s", (int(lock_wait_timeout),))
        conn.autocommit = False
        cursor_cls = aiomysql.DictCursor if dict_cursor else aiomysql.Cursor
        async with conn.cursor(cursor_cls) as cur:
            try:
                yield cur
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
    finally:
        try:
            conn.autocommit = True
        except Exception:
            pass
        pool.release(conn)


@dataclass
class ExecResult:
    rowcount: int
    lastrowid: Optional[int]


async def fetch_all(sql: str, params: Optional[Iterable[Any]] = None) -> list[dict]:
    async with acquire_cursor() as cur:
        await cur.execute(sql, params or ())
        return list(await cur.fetchall())


async def fetch_one(sql: str, params: Optional[Iterable[Any]] = None) -> Optional[dict]:
    async with acquire_cursor() as cur:
        await cur.execute(sql, params or ())
        return await cur.fetchone()


async def fetch_val(sql: str, params: Optional[Iterable[Any]] = None) -> Any:
    row = await fetch_one(sql, params)
    if not row:
        return None
    # DictCursor: return first column value
    return next(iter(row.values()))


async def execute(sql: str, params: Optional[Iterable[Any]] = None) -> ExecResult:
    async with acquire_cursor(dict_cursor=False) as cur:
        await cur.execute(sql, params or ())
        return ExecResult(rowcount=cur.rowcount, lastrowid=getattr(cur, "lastrowid", None))


async def executemany(sql: str, seq_params: Iterable[Iterable[Any]]) -> ExecResult:
    async with acquire_cursor(dict_cursor=False) as cur:
        await cur.executemany(sql, seq_params)
        return ExecResult(rowcount=cur.rowcount, lastrowid=getattr(cur, "lastrowid", None))
