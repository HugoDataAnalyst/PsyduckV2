"""spawnpoints: game geography, plus spawnpoint_geography_changes

Golbat sends each encountered spawnpoint's game geography (landform, moisture,
landcover, temperature, vista features, park status, settlement). It belongs to
the place, so it lives on the spawnpoints row; features is a bitmask with bit
(1 << v) set for each VistaFeature v. geo_first_seen / geo_changed_at are UTC
datetimes, geo_seen_day the UTC day it was last seen. spawnpoint_geography_changes
gets a row only when a stored geography changes (old_* = previous, new_* = incoming).

Columns are added with ALGORITHM=INSTANT (metadata only on MariaDB 10.3+); if
the server cannot, small tables get a normal ALTER and large ones stop here with
instructions rather than rewriting the table during app startup.

Revision ID: b8d3f5a2c619
Revises: e4c9a1b7d352
Create Date: 2026-11-20 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'b8d3f5a2c619'
down_revision: Union[str, Sequence[str], None] = 'e4c9a1b7d352'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_GEO = ("landform", "moisture", "landcover", "temperature", "features", "park_status", "settlement")
_COLUMNS = (
    *((c, "TINYINT UNSIGNED NULL DEFAULT NULL") for c in _GEO),
    ("geo_first_seen", "DATETIME NULL DEFAULT NULL"),
    ("geo_seen_day", "DATE NULL DEFAULT NULL"),
    ("geo_changed_at", "DATETIME NULL DEFAULT NULL"),
)
_SMALL_TABLE_ROWS = 5_000_000


def _missing_columns(conn) -> list[tuple[str, str]]:
    present = {r[0] for r in conn.execute(sa.text(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() "
        "AND table_name = 'spawnpoints'"
    ))}
    return [(c, t) for c, t in _COLUMNS if c not in present]


def upgrade() -> None:
    conn = op.get_bind()
    missing = _missing_columns(conn)
    if missing:
        add = "ALTER TABLE spawnpoints " + ", ".join(f"ADD COLUMN {c} {t}" for c, t in missing)
        try:
            conn.execute(sa.text(add + ", ALGORITHM=INSTANT"))
        except Exception as instant_error:
            rows = conn.execute(sa.text(
                "SELECT COALESCE(table_rows, 0) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = 'spawnpoints'"
            )).scalar() or 0
            if int(rows) > _SMALL_TABLE_ROWS:
                raise RuntimeError(
                    f"spawnpoints (~{int(rows):,} rows) cannot get the geography columns instantly on this "
                    f"server ({instant_error}). Add them off-peak by hand, then restart PsyduckV2:\n    {add};"
                ) from instant_error
            conn.execute(sa.text(add))

    old = ", ".join(f"old_{c} TINYINT UNSIGNED NULL" for c in _GEO)
    new = ", ".join(f"new_{c} TINYINT UNSIGNED NULL" for c in _GEO)
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS spawnpoint_geography_changes (
          spawnpoint BIGINT UNSIGNED NOT NULL,
          changed_at DATETIME NOT NULL,
          {old},
          {new},
          PRIMARY KEY (spawnpoint, changed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS spawnpoint_geography_changes")
    op.execute("ALTER TABLE spawnpoints " + ", ".join(f"DROP COLUMN {c}" for c, _ in _COLUMNS))
