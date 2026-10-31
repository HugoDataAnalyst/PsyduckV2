"""weather: pokemon_iv_daily_events.weather + weather_daily_events

pokemon_iv_daily_events.weather: the condition boosting that Pokémon, as
Golbat sends it per spawn (GameplayWeatherProto.WeatherCondition, 0 = not
boosted). Added like encounter_id: ALGORITHM=INSTANT, old rows keep NULL.

weather_daily_events: one row per weather change of an S2 level-10 cell, from
Golbat's `weather` webhook (sent only when a cell's condition or warning
changes), daily-partitioned like the other event tables.

Revision ID: d2b8e6f41c07
Revises: c5e2b7f84a13
Create Date: 2026-10-31 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'd2b8e6f41c07'
down_revision: Union[str, Sequence[str], None] = 'c5e2b7f84a13'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_ADD = "ALTER TABLE pokemon_iv_daily_events ADD COLUMN weather TINYINT UNSIGNED NULL DEFAULT NULL"
_SMALL_TABLE_ROWS = 5_000_000


def _add_pokemon_weather(conn) -> None:
    exists = conn.execute(sa.text(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() "
        "AND table_name = 'pokemon_iv_daily_events' AND column_name = 'weather'"
    )).scalar()
    if exists:
        return
    try:
        conn.execute(sa.text(_ADD + ", ALGORITHM=INSTANT"))
        return
    except Exception as instant_error:
        rows = conn.execute(sa.text(
            "SELECT COALESCE(SUM(table_rows), 0) FROM information_schema.partitions "
            "WHERE table_schema = DATABASE() AND table_name = 'pokemon_iv_daily_events'"
        )).scalar() or 0
        if int(rows) <= _SMALL_TABLE_ROWS:
            conn.execute(sa.text(_ADD))
            return
        raise RuntimeError(
            f"pokemon_iv_daily_events (~{int(rows):,} rows) cannot get the weather column instantly on this "
            f"server ({instant_error}). Add it off-peak by hand, then restart PsyduckV2:\n    {_ADD};"
        ) from instant_error


def upgrade() -> None:
    _add_pokemon_weather(op.get_bind())
    op.execute("""
    CREATE TABLE IF NOT EXISTS weather_daily_events (
        s2_cell_id           BIGINT NOT NULL,
        area_id              SMALLINT UNSIGNED NOT NULL,
        gameplay_condition   TINYINT UNSIGNED NOT NULL,
        warn_weather         TINYINT UNSIGNED NOT NULL,
        severity             TINYINT UNSIGNED NOT NULL,
        wind_direction       SMALLINT UNSIGNED NOT NULL,
        cloud_level          TINYINT UNSIGNED NOT NULL,
        rain_level           TINYINT UNSIGNED NOT NULL,
        wind_level           TINYINT UNSIGNED NOT NULL,
        snow_level           TINYINT UNSIGNED NOT NULL,
        fog_level            TINYINT UNSIGNED NOT NULL,
        special_effect_level TINYINT UNSIGNED NOT NULL,
        latitude             DOUBLE NOT NULL,
        longitude            DOUBLE NOT NULL,
        seen_at              DATETIME NOT NULL,
        day_date             DATE NOT NULL,
        PRIMARY KEY (day_date, s2_cell_id, seen_at),
        KEY ix_wde_area_day_seen (area_id, day_date, seen_at)
    )
    ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_general_ci
    PARTITION BY RANGE COLUMNS (day_date) (
        PARTITION pMAX VALUES LESS THAN (MAXVALUE)
    );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS weather_daily_events;")
    op.execute("ALTER TABLE pokemon_iv_daily_events DROP COLUMN weather")
