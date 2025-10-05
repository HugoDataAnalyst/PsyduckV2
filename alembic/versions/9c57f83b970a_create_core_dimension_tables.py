"""create core dimension tables

Revision ID: 9c57f83b970a
Revises:
Create Date: 2025-09-10 00:03:57.378980

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


# revision identifiers, used by Alembic.
revision: str = '9c57f83b970a'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # area_names keeps surrogate id + unique natural name
    op.create_table(
        "area_names",
        sa.Column("id", mysql.SMALLINT(unsigned=True), primary_key=True, autoincrement=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.UniqueConstraint("name", name="uq_area_names_name"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_0900_ai_ci",
    )

    # spawnpoints: natural PK on spawnpoint (BIGINT) - store lat/lon as DOUBLE
    op.create_table(
        "spawnpoints",
        sa.Column("spawnpoint", mysql.BIGINT(unsigned=True), primary_key=True, nullable=False),
        sa.Column("latitude",  mysql.DOUBLE(asdecimal=False), nullable=False),
        sa.Column("longitude", mysql.DOUBLE(asdecimal=False), nullable=False),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_0900_ai_ci",
    )
    op.create_index("idx_spawnpoints_latlon", "spawnpoints", ["latitude", "longitude"])

    # pokestops: natural PK on pokestop (string id)
    op.create_table(
        "pokestops",
        sa.Column("pokestop", sa.String(50), primary_key=True, nullable=False),
        sa.Column("pokestop_name", sa.String(255), nullable=False),
        sa.Column("latitude",  mysql.DOUBLE(asdecimal=False), nullable=False),
        sa.Column("longitude", mysql.DOUBLE(asdecimal=False), nullable=False),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_0900_ai_ci",
    )

    # gyms: natural PK on gym (string id)
    op.create_table(
        "gyms",
        sa.Column("gym", sa.String(50), primary_key=True, nullable=False),
        sa.Column("gym_name", sa.String(255), nullable=False),
        sa.Column("latitude",  mysql.DOUBLE(asdecimal=False), nullable=False),
        sa.Column("longitude", mysql.DOUBLE(asdecimal=False), nullable=False),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_0900_ai_ci",
    )


def downgrade():
    op.drop_table("gyms")
    op.drop_table("pokestops")
    op.drop_index("idx_spawnpoints_latlon", table_name="spawnpoints")
    op.drop_table("spawnpoints")
    op.drop_table("area_names")
