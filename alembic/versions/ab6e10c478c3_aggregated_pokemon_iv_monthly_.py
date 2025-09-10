"""aggregated_pokemon_iv_monthly partitioned

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
    CREATE TABLE IF NOT EXISTS aggregated_pokemon_iv_monthly (
        spawnpoint  BIGINT UNSIGNED NOT NULL,
        pokemon_id  SMALLINT UNSIGNED NOT NULL,
        form        VARCHAR(15) CHARACTER SET ascii NOT NULL,
        iv          SMALLINT UNSIGNED NOT NULL,
        area_id     SMALLINT UNSIGNED NOT NULL,
        month_year  SMALLINT UNSIGNED NOT NULL,
        total_count INT UNSIGNED NOT NULL DEFAULT 0,
        PRIMARY KEY (month_year, spawnpoint, pokemon_id, form, iv, area_id),
        KEY ix_apim_spawnpoint_month (spawnpoint, month_year),
        KEY ix_apim_area_month      (area_id, month_year),
        KEY ix_apim_area_species_month (area_id, pokemon_id, form, month_year)
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
    op.execute("DROP TABLE IF EXISTS aggregated_pokemon_iv_monthly;")
