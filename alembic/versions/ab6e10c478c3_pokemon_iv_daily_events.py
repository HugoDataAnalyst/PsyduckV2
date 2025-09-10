"""pokemon_iv_daily_events (daily partitions)

Revision ID: ab6e10c478c3
Revises: 9c57f83b970a
Create Date: 2025-09-10 00:04:51.286518

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ab6e10c478c3'
down_revision: Union[str, Sequence[str], None] = '9c57f83b970a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    CREATE TABLE IF NOT EXISTS pokemon_iv_daily_events (
      spawnpoint  BIGINT UNSIGNED NOT NULL,
      pokemon_id  SMALLINT UNSIGNED NOT NULL,
      form        VARCHAR(15) CHARACTER SET ascii NOT NULL,
      iv          SMALLINT UNSIGNED NOT NULL,
      level       TINYINT  UNSIGNED NOT NULL,
      area_id     SMALLINT UNSIGNED NOT NULL,
      seen_at     DATETIME NOT NULL,
      day_date    DATE NOT NULL,
      PRIMARY KEY (day_date, spawnpoint, seen_at),
      KEY ix_ev_area_day         (area_id, day_date),
      KEY ix_ev_species_day      (pokemon_id, form, day_date),
      KEY ix_ev_iv_day           (iv, day_date),
      KEY ix_ev_lvl_day          (level, day_date),
      KEY ix_ev_sp_day           (spawnpoint, day_date)
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_ai_ci
    PARTITION BY RANGE COLUMNS (day_date) (
      PARTITION pMAX        VALUES LESS THAN (MAXVALUE)
    );
    """)

def downgrade():
    op.execute("DROP TABLE IF EXISTS pokemon_iv_daily_events;")
