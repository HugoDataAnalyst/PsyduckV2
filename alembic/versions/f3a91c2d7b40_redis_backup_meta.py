"""redis_backup_meta

Records which Redis dataset (a marker key stored in Redis) the MySQL backup
belongs to, so the app can tell "Redis still holds that data - leave it alone"
apart from "Redis lost its data - restore it", plus backup bookkeeping.

Revision ID: f3a91c2d7b40
Revises: e7f2c0a1b3d9
Create Date: 2026-10-15 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op

revision: str = 'f3a91c2d7b40'
down_revision: Union[str, Sequence[str], None] = 'e7f2c0a1b3d9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE IF NOT EXISTS redis_backup_meta (
        name       VARCHAR(64)  NOT NULL,
        value      VARCHAR(255) NOT NULL,
        updated_at TIMESTAMP    NOT NULL
                   DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS redis_backup_meta;")
