"""weather_daily_events: one row per (cell, area)

A weather cell now counts for every area it overlaps, not only the area holding
its centre, so the same change is stored once per overlapping area and area_id
joins the primary key. The table holds a few thousand rows a day, so the
rebuild is quick.

Revision ID: e4c9a1b7d352
Revises: d2b8e6f41c07
Create Date: 2026-10-31 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

revision: str = 'e4c9a1b7d352'
down_revision: Union[str, Sequence[str], None] = 'd2b8e6f41c07'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE weather_daily_events DROP PRIMARY KEY, "
               "ADD PRIMARY KEY (day_date, s2_cell_id, area_id, seen_at)")


def downgrade() -> None:
    # rows of the same cell in several areas collapse onto one key again
    op.execute("ALTER IGNORE TABLE weather_daily_events DROP PRIMARY KEY, "
               "ADD PRIMARY KEY (day_date, s2_cell_id, seen_at)")
