"""
Count an event once, even when its source sends it again.

Golbat re-sends a max battle whenever its station's battle list changes and a
tappable whenever its reward changes, so plain HINCRBYs would count those
twice. stage() queues one Lua call on the caller's pipeline: it sets a marker
key (SET NX with a TTL) and only when the marker is new applies the
increments - atomically, and without an extra round trip.

EVAL, not EVALSHA: the script cache is lost on a Redis restart.
"""
from __future__ import annotations

_SCRIPT = """
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
  for i = 2, #KEYS do
    redis.call('HINCRBY', KEYS[i], ARGV[i * 2 - 2], ARGV[i * 2 - 1])
  end
  return 1
end
return 0
"""

MIN_TTL_S = 60


def stage(pipe, marker: str, ttl_s: int, increments: list[tuple[str, str, int]]) -> None:
    """Queue: if `marker` is new (kept ttl_s seconds), HINCRBY every (key, field, amount)."""
    keys = [marker] + [key for key, _, _ in increments]
    args = [max(MIN_TTL_S, int(ttl_s))]
    for _, field, amount in increments:
        args += [field, int(amount)]
    pipe.eval(_SCRIPT, len(keys), *keys, *args)
