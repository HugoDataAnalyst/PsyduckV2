from __future__ import annotations
from pathlib import Path
from urllib.parse import quote_plus
import os
import duckdb
import config as AppConfig
from utils.logger import logger

def _project_root() -> Path:
    try:
        return Path(AppConfig.__file__).resolve().parent
    except Exception:
        # Fallback to CWD if something odd happens
        return Path.cwd().resolve()

PROJECT_ROOT = _project_root()

def _resolve_duckdb_path(raw_path: str) -> Path:
    p = Path(raw_path)
    if p.is_absolute():
        return p
    # Make relative paths live under the project root
    return (PROJECT_ROOT / p).resolve()

def _ensure_parent(path: Path) -> None:
    if not path.parent.exists():
        logger.info(f"🗂️ Creating DuckDB parent directory: {path.parent}")
        path.parent.mkdir(parents=True, exist_ok=True)

def connect_duck(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    target = _resolve_duckdb_path(AppConfig.duckdb_path)
    logger.info(f"🦆 DuckDB path: {target} (project_root={PROJECT_ROOT})")
    _ensure_parent(target)
    con = duckdb.connect(database=str(target), read_only=read_only)

    try:
        cores = AppConfig.duckdb_cores
        if cores > 0:
            con.execute(f"PRAGMA threads={cores}")
    except Exception:
        pass
    try:
        max_gb = AppConfig.duckdb_max_ram
        if max_gb > 0:
            con.execute(f"PRAGMA memory_limit='{max_gb}GB'")
    except Exception:
        pass

    try:
        con.execute("SET allow_unsigned_extensions = true;")
    except Exception:
        pass

    return con

# MySQL helpers
def _select_mysql_host_port() -> tuple[str, int]:
    host = AppConfig.db_host
    port = AppConfig.db_port
    c_name = AppConfig.db_container_name
    c_port = AppConfig.db_container_port
    if c_name:
        host, port = c_name, c_port
    return host, port

def mysql_attach_kv() -> str:
    user = AppConfig.db_user
    pw   = AppConfig.db_password
    db   = AppConfig.db_name
    host, port = _select_mysql_host_port()

    def esc(s: str) -> str: return str(s).replace("'", "''")
    return (
        f"host={esc(host)} "
        f"user={esc(user)} "
        f"passwd={esc(pw)} "
        f"port={int(port)} "
        f"db={esc(db)}"
    )

def ensure_mysql_ext(con: duckdb.DuckDBPyConnection) -> bool:
    try:
        con.execute("LOAD mysql;")
        return True
    except duckdb.Error:
        pass
    try:
        con.execute("INSTALL mysql;")
        con.execute("LOAD mysql;")
        return True
    except duckdb.Error as e:
        logger.warning(f"❌ DuckDB MySQL extension unavailable: {e}")
        return False

def _alias_already_attached(con: duckdb.DuckDBPyConnection, alias: str) -> bool:
    try:
        rows = con.execute("SELECT name FROM duckdb_databases()").fetchall()
        return any(r[0] == alias for r in rows)
    except Exception:
        return False

def attach_mysql(con: duckdb.DuckDBPyConnection, alias: str = "mys", read_only: bool = True) -> bool:
    if not ensure_mysql_ext(con):
        return False

    if _alias_already_attached(con, alias):
        return True

    kv = mysql_attach_kv()
    ro = ", READ_ONLY" if read_only else ""
    try:
        con.execute(f"ATTACH '{kv}' AS {alias} (TYPE mysql_scanner{ro});")
        return True
    except duckdb.Error as e:
        logger.warning(f"ATTACH mysql_scanner failed: {e}")
        return False

def mysql_query_attached(
    con: duckdb.DuckDBPyConnection,
    alias: str,
    sql: str,
    *,
    return_df: bool = False,
    return_arrow: bool = False
):
    """
    Runs mysql_query() inside the attached MySQL DB.
    Default: returns a cursor you can .fetchone()/.fetchall() from.
    """
    cur = con.execute("SELECT * FROM mysql_query(?, ?);", [alias, sql])
    if return_df:
        return cur.fetchdf()
    if return_arrow:
        return cur.fetch_arrow_table()
    return cur


def mysql_self_test(
    con: duckdb.DuckDBPyConnection,
    alias: str = "mys",
    probe_table: str | None = None,
) -> tuple[bool, str]:
    """
    1) Ensure extension
    2) ATTACH MySQL (READ_ONLY)
    3) Run `SELECT 1` in MySQL
    4) Optional: COUNT(*) on probe_table

    Returns (ok, message)
    """
    # 1 + 2
    if not attach_mysql(con, alias=alias, read_only=True):
        host, port = _select_mysql_host_port()
        return False, (
            "Could not ATTACH MySQL. "
            f"Check reachability/creds (host={host}, port={port}, db={AppConfig.db_name}, user={AppConfig.db_user})."
        )

    # 3: lightweight probe
    try:
        con.execute("SELECT * FROM mysql_query(?, 'SELECT 1');", [alias]).fetchone()
    except Exception as e:
        return False, f"MySQL probe SELECT 1 failed: {e}"

    # 4: optional table probe
    if probe_table:
        try:
            # Prefer attached-path query (less quoting hassle)
            q = f"SELECT COUNT(*) FROM {alias}.{AppConfig.db_name}.{probe_table}"
            con.execute(q).fetchone()
        except Exception as e:
            return False, f"MySQL table probe failed on {probe_table}: {e}"

    return True, "MySQL attach & probe OK"

def query(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql)
