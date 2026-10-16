"""pokemon_iv_daily_events: drop the unused index, add level to the other two

See sql/utils/pokemon_iv_indexes.py for the measurements behind it.

Applied here only when the table is small (fresh or small installs: seconds).
A large table would take a long online index build during app startup, so it
is left as it is - with a warning - for the manual tool, run off-peak:

    python -m sql.utils.pokemon_iv_indexes --apply

Indexes only affect speed, never results, so both layouts work meanwhile.

Revision ID: c5e2b7f84a13
Revises: a7c4e9d21b05
Create Date: 2026-10-16 00:00:01.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

from sql.utils.pokemon_iv_indexes import AUTO_MAX_ROWS, OBSOLETE, TABLE, TARGET, plan

revision: str = 'c5e2b7f84a13'
down_revision: Union[str, Sequence[str], None] = 'a7c4e9d21b05'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _existing(conn) -> set[str]:
    rows = conn.execute(sa.text(
        "SELECT DISTINCT index_name FROM information_schema.statistics "
        "WHERE table_schema = DATABASE() AND table_name = :t AND index_name <> 'PRIMARY'"
    ), {"t": TABLE})
    return {r[0] for r in rows}


def upgrade() -> None:
    conn = op.get_bind()
    steps = plan(_existing(conn))
    if not steps:
        return
    rows = int(conn.execute(sa.text(
        "SELECT COALESCE(SUM(table_rows), 0) FROM information_schema.partitions "
        "WHERE table_schema = DATABASE() AND table_name = :t"
    ), {"t": TABLE}).scalar() or 0)
    if rows > AUTO_MAX_ROWS:
        print(f"⚠️ {TABLE} has ~{rows:,} rows: index change NOT applied at startup. "
              f"Run off-peak: python -m sql.utils.pokemon_iv_indexes --apply")
        return
    for s in steps:
        conn.execute(sa.text(s))


def downgrade() -> None:
    conn = op.get_bind()
    existing = _existing(conn)
    old = {
        "ix_ev_area_day_sp_pkf": "(area_id, day_date, spawnpoint, pokemon_id, form)",
        "ix_ev_area_day_iv_sp_pkf": "(area_id, day_date, iv, spawnpoint, pokemon_id, form)",
        "ix_ev_area_day_pid_iv_sp_f": "(area_id, day_date, pokemon_id, iv, spawnpoint, form)",
    }
    add = [f"ADD INDEX {n} {c}" for n, c in old.items() if n not in existing]
    if add:
        conn.execute(sa.text(f"ALTER TABLE {TABLE} {', '.join(add)}"))
    drop = [f"DROP INDEX {n}" for n in TARGET if n in existing]
    if drop:
        conn.execute(sa.text(f"ALTER TABLE {TABLE} {', '.join(drop)}"))
