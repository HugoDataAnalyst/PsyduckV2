"""shiny_username_rates partitioned

Revision ID: dcd1a7aa8227
Revises: ab6e10c478c3
Create Date: 2025-09-10 00:05:39.300283

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dcd1a7aa8227'
down_revision: Union[str, Sequence[str], None] = 'ab6e10c478c3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.execute("""
    CREATE TABLE IF NOT EXISTS shiny_username_rates (
        username    VARCHAR(255) NOT NULL,
        pokemon_id  SMALLINT UNSIGNED NOT NULL,
        form        VARCHAR(15) CHARACTER SET ascii NOT NULL,
        shiny       TINYINT UNSIGNED NOT NULL DEFAULT 0,
        area_id     SMALLINT UNSIGNED NOT NULL,
        month_year  SMALLINT UNSIGNED NOT NULL,
        total_count INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (month_year, username, pokemon_id, form, shiny, area_id),
        KEY ix_sur_my_pid_form_user_shiny_cnt (month_year, pokemon_id, form, username, shiny, total_count),
        KEY ix_sur_my_area_pid_form_user_shiny_cnt (month_year, area_id, pokemon_id, form, username, shiny, total_count)
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_0900_ai_ci
    PARTITION BY RANGE (month_year) (
        PARTITION pMAX  VALUES LESS THAN (MAXVALUE)
    );
    """)

def downgrade():
    op.execute("DROP TABLE IF EXISTS shiny_username_rates;")
