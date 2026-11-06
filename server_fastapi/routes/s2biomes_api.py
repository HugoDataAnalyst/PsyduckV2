"""Endpoints for S2Biomes (biome and nest analysis): raw spawn and weather rows of one day.

Rows stream as gzip-compressed TSV with a header line: a day holds ~20M spawn rows, far too many for JSON,
and TSV needs no extra dependency here while S2Biomes parses it directly. Times are area-local, as stored.
Spawns are paged: when a page returns `limit` rows, ask for the next one with
`after=<spawnpoint>,<seen_at>` taken from its last row.
"""
import asyncio
import zlib
from datetime import date, datetime
from typing import AsyncIterator, Optional

import config as AppConfig
from fastapi import APIRouter, Depends, HTTPException, Query
from starlette.responses import StreamingResponse

from server_fastapi.utils import secure_api
from sql.queries.s2biomes_gets import (
    SPAWN_COLUMNS,
    WEATHER_COLUMNS,
    fetch_days,
    stream_spawns,
    stream_weather,
)
from utils.logger import logger
from utils.timer import time_execution

router = APIRouter()

dependencies_list = [
    Depends(secure_api.validate_path),
    Depends(secure_api.validate_ip),
]
if AppConfig.api_secret_key:
    dependencies_list.append(Depends(secure_api.verify_token))

SPAWN_PAGE_DEFAULT = 500_000
SPAWN_PAGE_MAX = 2_000_000


GZIP_LEVEL = 6


def _encode(gz, batch: list[tuple]) -> bytes:
    """One TSV line per tuple row (NULL -> empty), compressed: runs in a worker thread, off the event loop."""
    lines = "".join("\t".join("" if v is None else str(v) for v in row) + "\n" for row in batch)
    return gz.compress(lines.encode())


async def _gzip_tsv(columns: tuple[str, ...], batches: AsyncIterator[list[tuple]], label: str) -> AsyncIterator[bytes]:
    """Header line + one TSV line per row, gzip-compressed as it streams. Formatting and compression run in a
    worker thread (zlib releases the GIL), so webhook processing on the event loop keeps running during an export;
    the batches are compressed one after another, so the one compressor is never used from two threads at once."""
    gz = zlib.compressobj(GZIP_LEVEL, zlib.DEFLATED, 31)
    yield gz.compress(("\t".join(columns) + "\n").encode())
    rows = 0
    async for batch in batches:
        rows += len(batch)
        chunk = await asyncio.to_thread(_encode, gz, batch)
        if chunk:
            yield chunk
    yield gz.flush()
    logger.info(f"🧬 S2Biomes {label}: streamed {rows:,} rows")


def _tsv_response(columns: tuple[str, ...], batches: AsyncIterator[list[dict]], label: str) -> StreamingResponse:
    return StreamingResponse(
        _gzip_tsv(columns, batches, label),
        media_type="text/tab-separated-values; charset=utf-8",
        headers={"Content-Encoding": "gzip", "Cache-Control": "no-store"},
    )


def _parse_after(after: Optional[str]) -> Optional[tuple[int, datetime]]:
    if not after:
        return None
    try:
        spawnpoint, seen_at = after.split(",", 1)
        return int(spawnpoint), datetime.fromisoformat(seen_at)
    except ValueError:
        raise HTTPException(status_code=400, detail="❌ after must be <spawnpoint>,<YYYY-MM-DD HH:MM:SS>")


@router.get("/api/s2biomes/days", tags=["S2Biomes"], dependencies=dependencies_list)
@time_execution(label="S2BIOMES_DAYS")
async def get_s2biomes_days(api_secret_header: Optional[str] = secure_api.get_secret_header_param()):
    """Days stored for spawns and weather, with approximate row counts from partition metadata."""
    await secure_api.check_secret_header_value(api_secret_header)
    return {
        "spawns": await fetch_days("pokemon_iv_daily_events"),
        "weather": await fetch_days("weather_daily_events"),
    }


@router.get("/api/s2biomes/spawns", tags=["S2Biomes"], dependencies=dependencies_list)
async def get_s2biomes_spawns(
    day: date = Query(..., description="day_date of the partition (area-local)"),
    since: Optional[datetime] = Query(None, description="area-local seen_at, inclusive (YYYY-MM-DD HH:MM:SS)"),
    until: Optional[datetime] = Query(None, description="area-local seen_at, exclusive"),
    after: Optional[str] = Query(None, description="<spawnpoint>,<seen_at> of the last row of the previous page"),
    limit: int = Query(SPAWN_PAGE_DEFAULT, ge=1, le=SPAWN_PAGE_MAX),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
):
    """Spawn rows (spawnpoint, pokemon_id, form, seen_at, encounter_id, weather) of one day, in key order."""
    await secure_api.check_secret_header_value(api_secret_header)
    cursor = _parse_after(after)
    logger.info(f"🧬 S2Biomes spawns day={day} since={since} until={until} after={after} limit={limit}")
    return _tsv_response(SPAWN_COLUMNS, stream_spawns(day, since, until, cursor, limit), f"spawns {day}")


@router.get("/api/s2biomes/weather", tags=["S2Biomes"], dependencies=dependencies_list)
async def get_s2biomes_weather(
    day: date = Query(..., description="day_date of the partition (area-local)"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
):
    """Weather condition changes (s2_cell_id, area_id, gameplay_condition, seen_at) of one day."""
    await secure_api.check_secret_header_value(api_secret_header)
    return _tsv_response(WEATHER_COLUMNS, stream_weather(day), f"weather {day}")
