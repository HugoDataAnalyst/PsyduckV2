"""redis_mysql_backup_tables

Revision ID: e7f2c0a1b3d9
Revises: b672594104a3
Create Date: 2026-04-09 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op

revision: str = 'e7f2c0a1b3d9'
down_revision: Union[str, Sequence[str], None] = 'b672594104a3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS redis_counter_backup (
        redis_key    VARCHAR(768) NOT NULL,
        hash_data    JSON         NOT NULL,
        backed_up_at TIMESTAMP    NOT NULL
                     DEFAULT CURRENT_TIMESTAMP
                     ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (redis_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """)

    op.execute("""
    CREATE TABLE IF NOT EXISTS redis_timeseries_backup (
        redis_key    VARCHAR(768) NOT NULL,
        hash_data    JSON         NOT NULL,
        backed_up_at TIMESTAMP    NOT NULL
                     DEFAULT CURRENT_TIMESTAMP
                     ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (redis_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS redis_timeseries_backup;")
    op.execute("DROP TABLE IF EXISTS redis_counter_backup;")
