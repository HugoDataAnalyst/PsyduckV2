"""cell_geography: the game geography per S2 level-16 cell, replacing the per-spawnpoint columns

Golbat's geography is identical for every spawnpoint of an S2 level-16 cell, so the spawnpoints
columns repeated it ~9x. cell_geography holds one row per cell (UTC geo_first_seen / geo_seen_day /
geo_changed_at), cell_geography_changes one row per change (old_* = previous values, new_* = new ones).

The rows already stored are carried over: per cell the values of its most recently seen spawnpoint,
MIN(geo_first_seen), MAX(geo_seen_day), MAX(geo_changed_at); the change log by cell (INSERT IGNORE: one
row per cell and changed_at). Both copies use INSERT IGNORE, so a run stopped at the column drop can
run again. A spawnpoint's cell: (spawnpoint << 20) is its level-20 cell id; the level-16 parent clears
the bits below the level-16 lsb (1 << 28) and sets it.

The geography columns are dropped from spawnpoints with ALGORITHM=INSTANT (metadata only on MariaDB
10.4+); if the server cannot, small tables get a normal ALTER and large ones stop here with
instructions rather than rewriting the table during app startup.

Revision ID: 8d646827baf3
Revises: b8d3f5a2c619
Create Date: 2026-11-20 12:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = '8d646827baf3'
down_revision: Union[str, Sequence[str], None] = 'b8d3f5a2c619'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_GEO = ("landform", "moisture", "landcover", "temperature", "features", "park_status", "settlement")
_SP_COLUMNS = (
    *((c, "TINYINT UNSIGNED NULL DEFAULT NULL") for c in _GEO),
    ("geo_first_seen", "DATETIME NULL DEFAULT NULL"),
    ("geo_seen_day", "DATE NULL DEFAULT NULL"),
    ("geo_changed_at", "DATETIME NULL DEFAULT NULL"),
)
_L16 = "(((spawnpoint << 20) & ~((1 << 28) - 1)) | (1 << 28))"
_SMALL_TABLE_ROWS = 5_000_000
_OLD = ", ".join(f"old_{c}" for c in _GEO)
_NEW = ", ".join(f"new_{c}" for c in _GEO)


def _change_table(name: str, key: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {name} (
          {key} BIGINT UNSIGNED NOT NULL,
          changed_at DATETIME NOT NULL,
          {", ".join(f"old_{c} TINYINT UNSIGNED NULL" for c in _GEO)},
          {", ".join(f"new_{c} TINYINT UNSIGNED NULL" for c in _GEO)},
          PRIMARY KEY ({key}, changed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    """


def _has_table(conn, name: str) -> bool:
    return bool(conn.execute(sa.text(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = :t"
    ), {"t": name}).scalar())


def _has_sp_geography(conn) -> bool:
    return bool(conn.execute(sa.text(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() "
        "AND table_name = 'spawnpoints' AND column_name = 'temperature'"
    )).scalar())


def upgrade() -> None:
    conn = op.get_bind()
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS cell_geography (
          l16 BIGINT UNSIGNED NOT NULL PRIMARY KEY,
          {", ".join(f"{c} TINYINT UNSIGNED NULL" for c in _GEO)},
          geo_first_seen DATETIME NULL,
          geo_seen_day DATE NULL,
          geo_changed_at DATETIME NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    """)
    op.execute(_change_table("cell_geography_changes", "l16"))

    if _has_sp_geography(conn):
        cols = ", ".join(_GEO)
        op.execute(f"""
            INSERT IGNORE INTO cell_geography (l16, {cols}, geo_first_seen, geo_seen_day, geo_changed_at)
            SELECT l16, {cols}, first_seen, seen_day, changed_at
            FROM (
              SELECT {_L16} AS l16, {cols},
                     MIN(geo_first_seen) OVER w AS first_seen,
                     MAX(geo_seen_day) OVER w AS seen_day,
                     MAX(geo_changed_at) OVER w AS changed_at,
                     ROW_NUMBER() OVER (PARTITION BY {_L16}
                                        ORDER BY geo_seen_day DESC,
                                                 COALESCE(geo_changed_at, geo_first_seen) DESC,
                                                 spawnpoint DESC) AS rn
              FROM spawnpoints
              WHERE temperature IS NOT NULL
              WINDOW w AS (PARTITION BY {_L16})
            ) s
            WHERE rn = 1
        """)

    if _has_table(conn, "spawnpoint_geography_changes"):
        op.execute(f"""
            INSERT IGNORE INTO cell_geography_changes (l16, changed_at, {_OLD}, {_NEW})
            SELECT {_L16}, changed_at, {_OLD}, {_NEW}
            FROM spawnpoint_geography_changes
            ORDER BY changed_at, spawnpoint
        """)

    if _has_sp_geography(conn):
        drop = "ALTER TABLE spawnpoints " + ", ".join(f"DROP COLUMN {c}" for c, _ in _SP_COLUMNS)
        try:
            conn.execute(sa.text(drop + ", ALGORITHM=INSTANT"))
        except Exception as instant_error:
            rows = conn.execute(sa.text(
                "SELECT COALESCE(table_rows, 0) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = 'spawnpoints'"
            )).scalar() or 0
            if int(rows) > _SMALL_TABLE_ROWS:
                raise RuntimeError(
                    f"spawnpoints (~{int(rows):,} rows) cannot drop the geography columns instantly on this "
                    f"server ({instant_error}). The geography is already copied to cell_geography. Drop them "
                    f"off-peak by hand, then restart PsyduckV2:\n    {drop};"
                ) from instant_error
            conn.execute(sa.text(drop))

    op.execute("DROP TABLE IF EXISTS spawnpoint_geography_changes")


def downgrade() -> None:
    op.execute("ALTER TABLE spawnpoints " + ", ".join(f"ADD COLUMN {c} {t}" for c, t in _SP_COLUMNS))
    op.execute(_change_table("spawnpoint_geography_changes", "spawnpoint"))
    op.execute("DROP TABLE IF EXISTS cell_geography_changes")
    op.execute("DROP TABLE IF EXISTS cell_geography")
