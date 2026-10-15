"""
Re-encode existing counter hashes after raising Redis' hash-max-listpack-entries.

Redis picks a hash's encoding when it grows and never converts a hashtable
back to the compact listpack, so raising the limit only helps keys created
afterwards. This rewrites the existing ones: DUMP + RESTORE REPLACE makes Redis
load each hash again under the current limit.

Only keys whose time bucket is closed (a past hour/day/week/month, which
nothing writes to any more) are touched, so no increment can land between the
DUMP and the RESTORE. Keys still being written are left alone; they are
replaced by new, already-compact keys as their bucket rolls over.

    # 1. raise the limit (and persist it in redis.conf)
    redis-cli CONFIG SET hash-max-listpack-entries 4096
    # 2. see what would change, then do it
    python -m my_redis.utils.reencode_hashes
    python -m my_redis.utils.reencode_hashes --apply
"""
from __future__ import annotations

import argparse
import asyncio
import time

from my_redis.utils.mysql_backup import BACKUP_GRACE_S, counter_last_write_utc

SCAN_COUNT = 2000
CHUNK = 200
CHUNK_SLEEP_S = 0.05


async def reencode(client, pattern: str = "counter:*", apply: bool = False, log=print) -> dict:
    limit = int((await client.config_get("hash-max-listpack-entries"))["hash-max-listpack-entries"])
    now = time.time()
    used_before = int((await client.info("memory"))["used_memory"])
    stats = {"scanned": 0, "hashtable": 0, "too_big": 0, "still_written": 0, "converted": 0}

    batch: list[str] = []

    async def flush(keys: list[str]):
        async with client.pipeline(transaction=False) as pipe:
            for k in keys:
                pipe.object("encoding", k)
                pipe.hlen(k)
            res = await pipe.execute()
        todo = []
        for i, k in enumerate(keys):
            enc, n = res[2 * i], res[2 * i + 1]
            if enc not in ("hashtable", b"hashtable"):
                continue
            stats["hashtable"] += 1
            if n > limit:
                stats["too_big"] += 1
                continue
            last = counter_last_write_utc(k, {})          # {}: unknown offsets -> most cautious
            if last is None or last + BACKUP_GRACE_S >= now:
                stats["still_written"] += 1
                continue
            todo.append(k)
        if not todo or not apply:
            stats["converted"] += len(todo) if not apply else 0
            return
        async with client.pipeline(transaction=False) as pipe:
            for k in todo:
                pipe.dump(k)
            payloads = await pipe.execute()
        async with client.pipeline(transaction=False) as pipe:
            for k, payload in zip(todo, payloads):
                if payload is not None:
                    pipe.restore(k, 0, payload, replace=True)
            await pipe.execute()
        stats["converted"] += len(todo)
        await asyncio.sleep(CHUNK_SLEEP_S)

    async for key in client.scan_iter(match=pattern, count=SCAN_COUNT, _type="hash"):
        stats["scanned"] += 1
        batch.append(key if isinstance(key, str) else key.decode())
        if len(batch) >= CHUNK:
            await flush(batch)
            batch = []
    if batch:
        await flush(batch)

    used_after = int((await client.info("memory"))["used_memory"])
    stats["limit"] = limit
    stats["used_memory_mb_before"] = round(used_before / 1048576)
    stats["used_memory_mb_after"] = round(used_after / 1048576)
    verb = "converted" if apply else "would convert"
    log(f"hash-max-listpack-entries={limit}: scanned {stats['scanned']} hashes, {stats['hashtable']} hashtable-encoded; "
        f"{verb} {stats['converted']}, skipped {stats['still_written']} still being written and "
        f"{stats['too_big']} over the limit; used_memory {stats['used_memory_mb_before']} MB -> "
        f"{stats['used_memory_mb_after']} MB")
    return stats


async def _main(apply: bool, pattern: str) -> None:
    from my_redis.connect_redis import RedisManager
    manager = RedisManager()
    await manager.init_redis()
    client = await manager.check_redis_connection()
    if not client:
        raise SystemExit("Redis not reachable")
    await reencode(client, pattern, apply)
    await manager.close_redis()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="rewrite the keys (default: dry run)")
    ap.add_argument("--pattern", default="counter:*")
    a = ap.parse_args()
    asyncio.run(_main(a.apply, a.pattern))
