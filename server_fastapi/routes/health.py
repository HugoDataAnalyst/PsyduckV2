"""
GET /health: MySQL, Redis/Dragonfly, last webhook, leader and buffer flushes.

  ok        everything fine                                       HTTP 200
  degraded  stale webhooks, no leader or a buffer not flushing    HTTP 200 (503 with ?strict=true)
  down      MySQL or Redis unreachable                            HTTP 503

Auth works like the data API (ALLOWED_IPS, secret header, bearer token), except
that loopback always passes the IP check so a Docker healthcheck inside the
container works. With no auth configured at all only {"status": ...} is
returned, so an open instance does not expose its internals.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials

import config as AppConfig
from my_redis.connect_redis import RedisManager
from my_redis.queries.buffer.flush_guard import FLUSH_OK_KEY
from server_fastapi.utils import secure_api
from sql.connect_db import fetch_val
from utils.leader_election import LeaderElection
from utils.runtime_health import LAST_WEBHOOK_KEY, STARTED_AT, ingest, rss_mb

router = APIRouter()

# (name, live buffer key, enabled, flush interval seconds)
_BUFFERS = (
    ("pokemon_iv", "buffer:pokemon_iv_events", AppConfig.store_sql_pokemon_aggregation, AppConfig.pokemon_flush_interval),
    ("shiny_rates", "buffer:agg_shiny_rates_hash", AppConfig.store_sql_pokemon_shiny, AppConfig.shiny_flush_interval),
    ("quests", "buffer:quest_events", AppConfig.store_sql_quest_aggregation, AppConfig.quest_flush_interval),
    ("raids", "buffer:raid_events", AppConfig.store_sql_raid_aggregation, AppConfig.raid_flush_interval),
    ("invasions", "buffer:invasion_events", AppConfig.store_sql_invasion_aggregation, AppConfig.invasion_flush_interval),
    ("weather", "buffer:weather_events", AppConfig.store_sql_weather, AppConfig.weather_flush_interval),
    ("cell_geography", "buffer:cell_geography", AppConfig.store_sql_geography, AppConfig.geography_flush_interval),
)
_LOOPBACK = {"127.0.0.1", "::1"}
_server_info: str | None = None


def _auth_configured() -> bool:
    return bool(AppConfig.allowed_ips or (AppConfig.api_header_name and AppConfig.api_header_secret)
                or AppConfig.api_secret_key)


async def _timed(coro) -> dict:
    t0 = time.perf_counter()
    try:
        await asyncio.wait_for(coro, AppConfig.health_check_timeout_seconds)
    except asyncio.TimeoutError:
        return {"ok": False, "error": f"timeout after {AppConfig.health_check_timeout_seconds:g}s"}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}
    return {"ok": True, "latency_ms": round((time.perf_counter() - t0) * 1000, 1)}


async def _ping_mysql():
    await fetch_val("SELECT 1")


async def _ping_redis(client):
    global _server_info
    if client is None:
        raise RuntimeError("no Redis connection")
    await client.ping()
    if _server_info is None:
        info = await client.info("server")
        _server_info = (f"dragonfly {info['dragonfly_version']}" if "dragonfly_version" in info
                        else f"redis {info.get('redis_version', '?')}")


def _ago(epoch, now: float) -> int | None:
    return None if epoch is None else max(0, int(now - float(epoch)))


def _webhook_check(marks: dict, source: str, now: float, uptime: float) -> dict:
    stale = AppConfig.health_webhook_stale_seconds
    received, accepted = _ago(marks.get("received"), now), _ago(marks.get("accepted"), now)
    check = {"last_received_s_ago": received, "last_accepted_s_ago": accepted,
             "stale_after_s": stale, "source": source}
    if stale <= 0:
        check["ok"] = True
    elif received is None:
        check["ok"] = uptime < stale   # just started: give Golbat time to send
    else:
        # received but nothing accepted = every batch is being shed
        check["ok"] = received <= stale and accepted is not None and accepted <= stale
    return check


def _flush_check(flushed: dict, now: float, uptime: float) -> dict:
    buffers, ok = {}, True
    for name, key, enabled, interval in _BUFFERS:
        if not enabled:
            continue
        stale = max(300, int(AppConfig.health_flush_stale_multiplier * interval))
        ago = _ago(flushed.get(key), now)
        good = uptime < stale if ago is None else ago <= stale
        buffers[name] = {"ok": good, "last_flush_s_ago": ago, "stale_after_s": stale}
        ok = ok and good
    return {"ok": ok, "buffers": buffers}


@router.get("/health", include_in_schema=False)
async def health(
    request: Request,
    strict: bool = Query(False, description="Answer 503 when degraded, not only when down"),
    api_secret_header: Optional[str] = secure_api.get_secret_header_param(),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(secure_api.security),
):
    host = request.client.host if request.client else ""
    if AppConfig.allowed_ips and host not in AppConfig.allowed_ips and host not in _LOOPBACK:
        raise HTTPException(status_code=403, detail="❌ IP not allowed")
    # only when configured: their "skipping" info lines would log on every healthcheck
    if AppConfig.api_header_name and AppConfig.api_header_secret:
        await secure_api.check_secret_header_value(api_secret_header)
    if AppConfig.api_secret_key:
        await secure_api.verify_token(credentials)

    client = RedisManager().redis_client
    mysql, redis = await asyncio.gather(_timed(_ping_mysql()), _timed(_ping_redis(client)))
    if redis["ok"]:
        redis["server"] = _server_info

    now = time.time()
    uptime = now - STARTED_AT
    marks, flushed, leader_id = None, None, None
    if redis["ok"]:
        async def _read():
            nonlocal marks, flushed, leader_id
            async with client.pipeline(transaction=False) as pipe:
                pipe.hgetall(LAST_WEBHOOK_KEY)
                pipe.hgetall(FLUSH_OK_KEY)
                pipe.get(LeaderElection.LOCK_KEY)
                marks, flushed, leader_id = await pipe.execute()
        read = await _timed(_read())
        if not read["ok"]:
            redis = read

    if marks is not None:
        webhook = _webhook_check(marks, "redis", now, uptime)
    else:
        webhook = _webhook_check({"received": ingest.last_received, "accepted": ingest.last_accepted},
                                 "this worker", now, uptime)

    checks = {"mysql": mysql, "redis": redis, "webhook": webhook}
    if leader_id is not None or marks is not None:
        # worker id is hostname:pid:startup_ms
        checks["leader"] = {"ok": bool(leader_id),
                            "held_by": ":".join(leader_id.split(":")[:2]) if leader_id else None}
        checks["flushers"] = _flush_check(flushed or {}, now, uptime)

    if not (mysql["ok"] and redis["ok"]):
        status = "down"
    elif all(c["ok"] for c in checks.values()):
        status = "ok"
    else:
        status = "degraded"
    code = 503 if status == "down" or (strict and status == "degraded") else 200

    if not _auth_configured():
        return JSONResponse({"status": status}, status_code=code)
    rss = rss_mb()
    return JSONResponse({
        "status": status,
        "checked_at": int(now),
        "checks": checks,
        "worker": {"pid": os.getpid(), "uptime_s": int(uptime), "inflight_events": ingest.inflight_events,
                   "rss_mb": round(rss) if rss is not None else None},
    }, status_code=code)
