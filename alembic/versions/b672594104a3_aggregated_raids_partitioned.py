"""aggregated_raids partitioned

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
    CREATE TABLE IF NOT EXISTS aggregated_raids (
        gym                 VARCHAR(50) NOT NULL,
        raid_pokemon        SMALLINT UNSIGNED NOT NULL,
        raid_level          SMALLINT UNSIGNED NOT NULL,
        raid_form           VARCHAR(15) NOT NULL,
        raid_team           SMALLINT UNSIGNED NOT NULL,
        raid_costume        VARCHAR(15) NOT NULL,
        raid_is_exclusive   TINYINT  UNSIGNED NOT NULL,
        raid_ex_raid_eligible TINYINT UNSIGNED NOT NULL,
        area_id             SMALLINT UNSIGNED NOT NULL,
        month_year          SMALLINT UNSIGNED NOT NULL,
        total_count         INT      UNSIGNED NOT NULL DEFAULT 0,

        -- make month_year first for pruning
        PRIMARY KEY (
          month_year, gym,
          raid_pokemon, raid_level, raid_form, raid_team,
          raid_costume, raid_is_exclusive, raid_ex_raid_eligible,
          area_id
        ),

        KEY ix_ar_month_area   (month_year, area_id),
        KEY ix_ar_gym_month    (gym, month_year),
        KEY ix_ar_area_species_month (area_id, raid_pokemon, raid_form, month_year)
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_ai_ci
    PARTITION BY RANGE (month_year) (
        PARTITION p2508 VALUES LESS THAN (2509),
        PARTITION p2509 VALUES LESS THAN (2510),
        PARTITION p2510 VALUES LESS THAN (2511),
        PARTITION pMAX  VALUES LESS THAN (MAXVALUE)
    );
    """)

def downgrade():
    op.execute("DROP TABLE IF EXISTS aggregated_raids;")
