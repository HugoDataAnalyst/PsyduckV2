"""raids_daily_events partitioned

Revision ID: b672594104a3
Revises: 1de6f00bdda8
Create Date: 2025-09-10 02:39:23.018522

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b672594104a3'
down_revision: Union[str, Sequence[str], None] = '1de6f00bdda8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    CREATE TABLE IF NOT EXISTS raids_daily_events (
        gym                 VARCHAR(50) NOT NULL,
        raid_pokemon        SMALLINT UNSIGNED NOT NULL,
        raid_level          SMALLINT UNSIGNED NOT NULL,
        raid_form           VARCHAR(15) NOT NULL,
        raid_team           SMALLINT UNSIGNED NOT NULL,
        raid_costume        VARCHAR(15) NOT NULL,
        raid_is_exclusive   TINYINT  UNSIGNED NOT NULL,
        raid_ex_raid_eligible TINYINT UNSIGNED NOT NULL,
        area_id             SMALLINT UNSIGNED NOT NULL,
        seen_at             DATETIME NOT NULL,
        day_date          DATE NOT NULL,

        PRIMARY KEY (day_date, gym, seen_at),
        KEY ix_dv_area_day_gym_rp_rf_rl (area_id, day_date, gym, raid_pokemon, raid_form, raid_level),
        KEY ix_dv_area_day_rp_rf_rl_gym (area_id, day_date, raid_pokemon, raid_form, raid_level, gym)
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_ai_ci
    PARTITION BY RANGE COLUMNS (day_date) (
        PARTITION pMAX  VALUES LESS THAN (MAXVALUE)
    );
    """)

def downgrade():
    op.execute("DROP TABLE IF EXISTS raids_daily_events;")
