"""aggregated_quests partitioned

Revision ID: 1de6f00bdda8
Revises: a677ec29504a
Create Date: 2025-09-10 02:34:30.951918

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1de6f00bdda8'
down_revision: Union[str, Sequence[str], None] = 'a677ec29504a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    CREATE TABLE IF NOT EXISTS aggregated_quests_item (
        pokestop       VARCHAR(50) NOT NULL,
        area_id        SMALLINT UNSIGNED NOT NULL,
        month_year     SMALLINT UNSIGNED NOT NULL,   -- YYMM
        mode           TINYINT  UNSIGNED NOT NULL,   -- 0=normal, 1=ar
        task_type      SMALLINT UNSIGNED NOT NULL,   -- normal_type or ar_type

        item_id        SMALLINT UNSIGNED NOT NULL,
        item_amount    SMALLINT UNSIGNED NOT NULL,

        total_count    INT      UNSIGNED NOT NULL DEFAULT 0,

        PRIMARY KEY (month_year, pokestop, area_id, mode, task_type, item_id, item_amount),

        KEY ix_aqi_month_area     (month_year, area_id),
        KEY ix_aqi_pokestop_month (pokestop, month_year),
        KEY ix_aqi_task_month     (task_type, month_year)
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

    op.execute("""
    CREATE TABLE IF NOT EXISTS aggregated_quests_pokemon (
        pokestop       VARCHAR(50) NOT NULL,
        area_id        SMALLINT UNSIGNED NOT NULL,
        month_year     SMALLINT UNSIGNED NOT NULL,   -- YYMM
        mode           TINYINT  UNSIGNED NOT NULL,   -- 0=normal, 1=ar
        task_type      SMALLINT UNSIGNED NOT NULL,   -- normal_type or ar_type

        poke_id        SMALLINT UNSIGNED NOT NULL,
        poke_form      VARCHAR(15) NOT NULL,

        total_count    INT      UNSIGNED NOT NULL DEFAULT 0,

        PRIMARY KEY (month_year, pokestop, area_id, mode, task_type, poke_id, poke_form),

        KEY ix_aqp_month_area     (month_year, area_id),
        KEY ix_aqp_pokestop_month (pokestop, month_year),
        KEY ix_aqp_task_month     (task_type, month_year)
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
    op.execute("DROP TABLE IF EXISTS aggregated_quests_item;")
    op.execute("DROP TABLE IF EXISTS aggregated_quests_pokemon;")
