"""
Scenario: the startup MySQL -> Redis restore must only ever fill an EMPTY Redis.

Part A - resurrection on a graceful restart
  Cleanup deletes expired keys from Redis, but their rows linger in the
  backup table for hours. Delete some timeseries keys after a backup, restart
  the app gracefully, and check they stay deleted.

Part B - rollback after a crash
  Feed, wait for a backup, feed more (counters move past the snapshot), then
  SIGKILL the app (no final backup - same as an OOM kill) and start it again.
  Live counters must not be rewound to the snapshot.

Part C - Redis really lost its data
  Stop the app, restart Redis (persistence off: empty), start the
  app: the restore MUST still run and bring the data back.

Part D - Redis restarts underneath a running app (the Oct 15 prod incident:
  needrestart restarted redis-server after a systemd upgrade)
  The leader must notice (its lock vanished with Redis), re-take the lock and
  merge the backup back in right away; the next backup cycle must neither merge
  again nor overwrite the backup with a near-empty Redis.

Part E - first deploy of this version onto production's state after Oct 15
  No dataset id recorded, Redis partly refilled, those keys' backup rows already
  overwritten: restore every missing key, leave the refilled ones alone.

Part F - Redis restarts but reloads its data (RDB persistence, or a dump.rdb
  restored by hand): new process, same data. Nothing may be merged.

  python test/scenario_restore_guard.py
"""
import asyncio
import json
import subprocess
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
import harness as h  # noqa: E402
from feeder import Feeder  # noqa: E402


def counter_totals() -> dict:
    """Sum of every '<pid>:<form>:total' field across daily and hourly pokemon counters."""
    r = h.rds()
    out = {}
    for fam in ("counter:pokemon_daily", "counter:pokemon_hourly"):
        total = 0
        for k in r.scan_iter(match=f"{fam}:*", count=1000):
            total += sum(int(v) for f, v in r.hgetall(k).items() if f.endswith(":total"))
        out[fam] = total
    return out


def ts_keys() -> list:
    return sorted(h.rds().scan_iter(match="ts:pokemon:total:*", count=1000))


async def feed(seconds: int, seed: int):
    # 20 species, one form: events keep incrementing the same counter fields,
    # like production's concentrated traffic (needed for a rollback to show)
    f = Feeder(h.WEBHOOK_URL, rate=100, mode="realistic", seed=seed,
               pokemon_ids=(1, 20), forms=[0])
    await f.run(seconds)
    await f.drain(60)
    await f.close()
    return f.summary()["events_accepted"]


def wait_for_backup(since: str, timeout: int = 180) -> float:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if "Backup cycle complete" in h.app_logs(since):
            return time.time() - t0
        time.sleep(3)
    raise TimeoutError("no backup cycle completed")


def restore_ran(since: str) -> bool:
    logs = h.app_logs(since)
    return "counter keys restored from MySQL" in logs


async def part_a() -> dict:
    h.banner("Part A - deleted keys must not be resurrected by a graceful restart")
    since = h._now_iso()
    await feed(20, seed=11)
    waited = wait_for_backup(since)
    print(f"backup cycle completed ({waited:.0f}s)")

    keys = ts_keys()
    victims = keys[: max(1, len(keys) // 4)]
    h.rds().delete(*victims)
    print(f"deleted {len(victims)} of {len(keys)} ts:pokemon:total keys (as cleanup would)")

    since = h._now_iso()
    h.restart_app()
    back = sum(1 for k in victims if h.rds().exists(k))
    result = {"deleted": len(victims), "resurrected": back, "restore_ran": restore_ran(since),
              "pass": back == 0}
    print(f"resurrected after restart: {back}/{len(victims)}   restore ran: {result['restore_ran']}")
    print("PART A:", "PASS" if result["pass"] else "FAIL")
    return result


async def part_b() -> dict:
    h.banner("Part B - crash (SIGKILL) must not rewind live counters to the snapshot")
    since = h._now_iso()
    await feed(15, seed=21)
    waited = wait_for_backup(since)
    print(f"backup cycle completed ({waited:.0f}s); feeding past the snapshot ...")
    await feed(20, seed=22)
    time.sleep(3)
    before = counter_totals()
    print("live counters before crash:", before)

    subprocess.run(["docker", "kill", h.APP_CONTAINER], check=True, capture_output=True)
    # Let the dead process's leader lock expire, so the new instance is leader
    # at startup and runs its restore (see scenario_leader_failover for the
    # immediate-restart case).
    r = h.rds()
    for _ in range(40):
        if not r.exists("psyduckv2:leader:main"):
            break
        time.sleep(1)
    since = h._now_iso()
    h.start("app")
    h.wait_app_ready(since)
    after = counter_totals()
    print("live counters after restart:", after)
    lost = {k: before[k] - after[k] for k in before}
    result = {"before": before, "after": after, "rolled_back_by": lost,
              "restore_ran": restore_ran(since),
              "pass": all(after[k] >= before[k] for k in before)}
    print("rolled back by:", lost, "  restore ran:", result["restore_ran"])
    print("PART B:", "PASS" if result["pass"] else "FAIL")
    return result


def restart_redis():
    """A real Redis restart: every key gone (persistence is off, no dump.rdb)."""
    h.compose("restart", "redis")
    r = h.rds()
    for _ in range(30):
        try:
            if r.ping():
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError("redis did not come back")


def backup_totals() -> dict:
    """Same sums as counter_totals(), read from the MySQL backup rows."""
    out = {}
    for fam in ("counter:pokemon_daily", "counter:pokemon_hourly"):
        total = 0
        for (data,) in h.sql("SELECT hash_data FROM redis_counter_backup WHERE redis_key LIKE %s", (fam + ":%",)):
            total += sum(int(v) for f, v in json.loads(data).items() if f.endswith(":total"))
        out[fam] = total
    return out


async def part_c() -> dict:
    h.banner("Part C - Redis restarted while the app was down: the backup must be restored")
    since = h._now_iso()
    await feed(10, seed=31)
    wait_for_backup(since)
    h.stop("app")   # graceful: its final backup runs
    expected_keys = int(h.sql_val("SELECT COUNT(*) FROM redis_counter_backup"))
    restart_redis()
    print(f"Redis restarted (empty); backup table holds {expected_keys} counter rows")
    since = h._now_iso()
    h.start("app")
    h.wait_app_ready(since)
    r = h.rds()
    restored = sum(1 for _ in r.scan_iter(match="counter:*", count=1000))
    result = {"backup_rows": expected_keys, "counter_keys_after_restart": restored,
              "restore_ran": restore_ran(since),
              "pass": restore_ran(since) and restored >= expected_keys}
    print(f"counter keys after restart: {restored} (backup rows {expected_keys}); restore ran: {result['restore_ran']}")
    print("PART C:", "PASS" if result["pass"] else "FAIL")
    return result


def wait_for_log(text: str, since: str, timeout: int = 60) -> float | None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if text in h.app_logs(since):
            return time.time() - t0
        time.sleep(1)
    return None


async def part_d() -> dict:
    h.banner("Part D - Redis restarted under a running app (Oct 15 prod incident)")
    since = h._now_iso()
    await feed(15, seed=41)
    wait_for_backup(since)
    snap = backup_totals()
    print("backed-up counters:", snap)

    since = h._now_iso()
    restart_redis()
    print("Redis restarted underneath the running app; waiting for the leader to notice ...")
    merged_after = wait_for_log("counter keys restored from MySQL", since, timeout=45)
    time.sleep(2)
    right_after = counter_totals()
    print(f"merge finished {merged_after}s after the restart; live counters: {right_after}")

    await feed(15, seed=42)
    time.sleep(3)
    before_cycle = counter_totals()
    cycle_since = h._now_iso()
    wait_for_backup(cycle_since)
    time.sleep(2)
    after_cycle = counter_totals()
    in_backup = backup_totals()
    logs = h.app_logs(since)
    result = {"backup_before": snap, "live_after_merge": right_after,
              "merge_seconds_after_restart": merged_after,
              "live_before_next_cycle": before_cycle, "live_after_next_cycle": after_cycle,
              "backup_after_next_cycle": in_backup,
              "lock_reacquired": "re-acquired" in logs,
              "merges": logs.count("counter keys restored from MySQL")}
    result["pass"] = (merged_after is not None and right_after == snap
                      and result["lock_reacquired"] and result["merges"] == 1
                      and after_cycle == before_cycle and in_backup == after_cycle)
    print("live right after the merge (must equal the backup):", right_after)
    print("live before / after the next backup cycle (must not change):", before_cycle, after_cycle)
    print("backup after that cycle (must equal live):", in_backup)
    print(f"leader lock re-taken: {result['lock_reacquired']}   merges: {result['merges']} (want 1)")
    print("PART D:", "PASS" if result["pass"] else "FAIL")
    return result


async def part_e() -> dict:
    h.banner("Part E - first deploy onto production's state today")
    # Production after Oct 15: Redis restarted and refilled for hours while the
    # old code kept backing it up - rows of keys written since the restart now
    # hold partial values, rows of older keys still hold the full history, and
    # no dataset id has been recorded yet. Deploying this version must restore the
    # missing history without double counting the keys Redis already has.
    since = h._now_iso()
    await feed(15, seed=51)
    wait_for_backup(since)
    h.stop("app")
    rows = {k: json.loads(v) for k, v in h.sql(
        "SELECT redis_key, hash_data FROM redis_counter_backup WHERE redis_key LIKE 'counter:pokemon_daily:%'")}
    h.sql("DELETE FROM redis_backup_meta")
    restart_redis()
    time.sleep(2)

    # Old code after the restart: some keys refilled with partial counts, and
    # its backup cycles overwrote those keys' rows with the partial values.
    r = h.rds()
    refilled = sorted(rows)[: max(1, len(rows) // 3)]
    partial = {}
    for k in refilled:
        partial[k] = {f: 1 for f in list(rows[k])[:5]}
        r.hset(k, mapping=partial[k])
        h.sql("UPDATE redis_counter_backup SET hash_data = %s, backed_up_at = NOW() WHERE redis_key = %s",
              (json.dumps(partial[k]), k))
    print(f"{len(rows)} pokemon_daily rows backed up; {len(refilled)} keys refilled in the new Redis "
          f"and their rows overwritten with partial values; provenance cleared")

    since = h._now_iso()
    h.start("app")
    h.wait_app_ready(since)
    wrong_refilled = [k for k in refilled if {f: int(v) for f, v in r.hgetall(k).items()} != partial[k]]
    others = [k for k in rows if k not in partial]
    wrong_restored = [k for k in others
                      if {f: int(v) for f, v in r.hgetall(k).items()} != {f: int(v) for f, v in rows[k].items()}]
    logs = h.app_logs(since)
    result = {"rows": len(rows), "refilled": len(refilled),
              "refilled_changed": len(wrong_refilled), "history_wrong_or_missing": len(wrong_restored),
              "path": "absent-only" if "restoring only keys backed up before Redis started" in logs else "other"}
    result["pass"] = not wrong_refilled and not wrong_restored and result["path"] == "absent-only"
    print(f"history keys restored correctly: {len(others) - len(wrong_restored)}/{len(others)}; "
          f"refilled keys left untouched: {len(refilled) - len(wrong_refilled)}/{len(refilled)}; path: {result['path']}")
    print("PART E:", "PASS" if result["pass"] else "FAIL")
    return result


async def part_f() -> dict:
    h.banner("Part F - Redis restarts and reloads its data (persistence / RDB restore)")
    # The case a run_id check gets wrong: the process (and run_id) is new but
    # the data is all there. Merging the backup on top would count it twice.
    since = h._now_iso()
    await feed(15, seed=61)
    wait_for_backup(since)
    time.sleep(2)
    before = counter_totals()
    r = h.rds()
    r.save()                                   # what RDB persistence / a restored dump.rdb provides
    since = h._now_iso()
    restart_redis()
    reloaded = h.rds().dbsize() > 0
    time.sleep(25)                             # heartbeat notices the restart, reconcile runs
    wait_for_backup(since)
    time.sleep(2)
    after = counter_totals()
    logs = h.app_logs(since)
    subprocess.run(["docker", "exec", "psyduck-test-redis-1", "rm", "-f", "/data/dump.rdb"],
                   check=False, capture_output=True)
    result = {"before": before, "after": after, "data_reloaded": reloaded,
              "merged": "merging the MySQL backup into Redis" in logs or "restored from MySQL" in logs,
              "skipped": "still holds the data the backup came from" in logs}
    result["pass"] = reloaded and after == before and not result["merged"] and result["skipped"]
    print(f"data reloaded by Redis: {reloaded}; counters before {before} after {after}")
    print(f"backup merged again: {result['merged']} (want False); recognised as same data: {result['skipped']}")
    print("PART F:", "PASS" if result["pass"] else "FAIL")
    return result


async def main():
    results = {"part_a_resurrection": await part_a(),
               "part_b_crash_rollback": await part_b(),
               "part_c_empty_redis_restored": await part_c(),
               "part_d_redis_restart_under_app": await part_d(),
               "part_e_first_deploy_after_incident": await part_e(),
               "part_f_redis_reloaded_its_data": await part_f()}
    path = h.write_result("restore_guard", results)
    print(f"\nresults -> {path}")
    ok = all(v["pass"] for v in results.values())
    print("SCENARIO:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
