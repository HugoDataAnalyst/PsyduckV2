"""invasions_daily_events partitioned

Revision ID: a677ec29504a
Revises: dcd1a7aa8227
Create Date: 2025-09-10 02:30:32.109789

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision: str = 'a677ec29504a'
down_revision: Union[str, Sequence[str], None] = 'dcd1a7aa8227'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    CREATE TABLE IF NOT EXISTS invasions_daily_events (
        pokestop     VARCHAR(50) NOT NULL,
        display_type SMALLINT UNSIGNED NOT NULL,
        `character`  SMALLINT UNSIGNED NOT NULL,
        grunt        SMALLINT UNSIGNED NOT NULL,
        confirmed    TINYINT  UNSIGNED NOT NULL,
        area_id      SMALLINT UNSIGNED NOT NULL,
        seen_at      DATETIME NOT NULL,
        day_date     DATE NOT NULL,
        PRIMARY KEY (day_date, pokestop, seen_at),
        KEY ix_idv_area_day_pstop_disp_char (area_id, day_date, pokestop, display_type, `character`),
        KEY ix_idv_area_day_char_disp_pstop (area_id, day_date, `character`, display_type, pokestop)
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_ai_ci
    PARTITION BY RANGE COLUMNS (day_date) (
        PARTITION pMAX  VALUES LESS THAN (MAXVALUE)
    );
    """)

def downgrade():
    op.execute("DROP TABLE IF EXISTS invasions_daily_events;")
