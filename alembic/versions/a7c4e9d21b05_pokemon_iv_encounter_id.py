"""pokemon_iv_daily_events.encounter_id

Stores Golbat's encounter_id (uint64) with every IV event, for analysis.
No index. Rows written before this migration keep NULL.

Added with ALGORITHM=INSTANT: a metadata-only change on MariaDB 10.3+, even
on the partitioned, ~500M-row production table. If the server cannot do it
instantly, small tables get a normal ALTER and large ones stop here with
instructions rather than rewriting the table during app startup.

Revision ID: a7c4e9d21b05
Revises: f3a91c2d7b40
Create Date: 2026-10-16 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'a7c4e9d21b05'
down_revision: Union[str, Sequence[str], None] = 'f3a91c2d7b40'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_ADD = "ALTER TABLE pokemon_iv_daily_events ADD COLUMN encounter_id BIGINT UNSIGNED NULL DEFAULT NULL"
_SMALL_TABLE_ROWS = 5_000_000


def _has_column(conn) -> bool:
    return bool(conn.execute(sa.text(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() "
        "AND table_name = 'pokemon_iv_daily_events' AND column_name = 'encounter_id'"
    )).scalar())


def upgrade() -> None:
    conn = op.get_bind()
    if _has_column(conn):
        return
    try:
        conn.execute(sa.text(_ADD + ", ALGORITHM=INSTANT"))
        return
    except Exception as instant_error:
        rows = conn.execute(sa.text(
            "SELECT COALESCE(SUM(table_rows), 0) FROM information_schema.partitions "
            "WHERE table_schema = DATABASE() AND table_name = 'pokemon_iv_daily_events'"
        )).scalar() or 0
        if int(rows) <= _SMALL_TABLE_ROWS:
            conn.execute(sa.text(_ADD))
            return
        raise RuntimeError(
            f"pokemon_iv_daily_events (~{int(rows):,} rows) cannot get encounter_id instantly on this "
            f"server ({instant_error}). Add it off-peak by hand, then restart PsyduckV2:\n    {_ADD};"
        ) from instant_error


def downgrade() -> None:
    op.execute("ALTER TABLE pokemon_iv_daily_events DROP COLUMN encounter_id")
