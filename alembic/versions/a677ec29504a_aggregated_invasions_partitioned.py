"""aggregated_invasions partitioned

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
    CREATE TABLE IF NOT EXISTS aggregated_invasions (
        pokestop     VARCHAR(50) NOT NULL,
        display_type SMALLINT UNSIGNED NOT NULL,
        `character`  SMALLINT UNSIGNED NOT NULL,
        grunt        SMALLINT UNSIGNED NOT NULL,
        confirmed    TINYINT  UNSIGNED NOT NULL,
        area_id      SMALLINT UNSIGNED NOT NULL,
        month_year   SMALLINT UNSIGNED NOT NULL,
        total_count  INT      UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (month_year, pokestop, display_type, `character`, grunt, confirmed, area_id),
        KEY ix_ai_month_area     (month_year, area_id),
        KEY ix_ai_pokestop_month (pokestop, month_year)
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
    op.execute("DROP TABLE IF EXISTS aggregated_invasions;")
