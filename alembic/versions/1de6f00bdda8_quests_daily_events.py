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
    CREATE TABLE IF NOT EXISTS quests_item_daily_events (
        pokestop       VARCHAR(50) NOT NULL,
        area_id        SMALLINT UNSIGNED NOT NULL,
        seen_at        DATETIME NOT NULL,
        day_date       DATE NOT NULL,
        mode           TINYINT  UNSIGNED NOT NULL,   -- 0=normal, 1=ar
        task_type      SMALLINT UNSIGNED NOT NULL,   -- normal_type or ar_type

        item_id        SMALLINT UNSIGNED NOT NULL,
        item_amount    SMALLINT UNSIGNED NOT NULL,

        PRIMARY KEY (day_date, pokestop, seen_at, mode),

        KEY ix_qidv_daily_area     (day_date, seen_at, area_id),
        KEY ix_qidv_pokestop_daily (pokestop, seen_at, day_date),
        KEY ix_qidv_task_daily    (task_type, seen_at day_date),
        KEY ix_qidv_item_daily    (item_id, item_amount, day_date),
        KEY ix_qidv_pokestop_item_daily (pokestop, item_id, item_amount, seen_at, day_date),
        KEY ix_qidv_task_item_daily (task_type, item_id, item_amount, seen_at, day_date),
        KEY ix_qidv_pokestop_task_item_daily (pokestop, task_type, item_id, item_amount, seen_at, day_date),
        KEY ix_qidv_pokestop_task_item_area_daily (area_id, pokestop, task_type, item_id, item_amount, seen_at, day_date)

    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_ai_ci
    PARTITION BY RANGE (day_date) (
      PARTITION pMAX  VALUES LESS THAN (MAXVALUE)
    );
    """)

    op.execute("""
    CREATE TABLE IF NOT EXISTS quests_pokemon_daily_events (
        pokestop       VARCHAR(50) NOT NULL,
        area_id        SMALLINT UNSIGNED NOT NULL,
        seen_at        DATETIME NOT NULL,
        day_date       DATE NOT NULL,
        mode           TINYINT  UNSIGNED NOT NULL,   -- 0=normal, 1=ar
        task_type      SMALLINT UNSIGNED NOT NULL,   -- normal_type or ar_type

        poke_id        SMALLINT UNSIGNED NOT NULL,
        poke_form      VARCHAR(15) NOT NULL,

        total_count    INT      UNSIGNED NOT NULL DEFAULT 0,

        PRIMARY KEY (day_date, pokestop, seen_at, mode),

        KEY ix_qpdv_daily_area     (day_date, seen_at, area_id),
        KEY ix_qpdv_pokestop_daily (pokestop, seen_at, day_date),
        KEY ix_qpdv_task_daily     (task_type, seen_at, day_date),
        KEY ix_qpdv_poke_daily     (poke_id, poke_form, seen_at, day_date),
        KEY ix_qpdv_pokestop_poke_daily (pokestop, poke_id, poke_form, seen_at, day_date),
        KEY ix_qpdv_task_poke_daily (task_type, poke_id, poke_form, seen_at, day_date),
        KEY ix_qpdv_pokestop_task_poke_daily (pokestop, task_type, poke_id, poke_form, seen_at, day_date),
        KEY ix_qpdv_pokestop_task_poke_area_daily (area_id, pokestop, task_type, poke_id, poke_form, seen_at, day_date)
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_ai_ci
    PARTITION BY RANGE (day_date) (
      PARTITION pMAX  VALUES LESS THAN (MAXVALUE)
    );
    """)

def downgrade():
    op.execute("DROP TABLE IF EXISTS quests_item_daily_events;")
    op.execute("DROP TABLE IF EXISTS quests_pokemon_daily_events;")
