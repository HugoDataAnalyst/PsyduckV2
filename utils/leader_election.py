"""
Redis-based Leader Election for Multi-Worker Uvicorn Support.

Only the leader worker runs background tasks (flushers, partition ensurers, cleanup).
Other workers handle webhooks and API requests but defer background work to the leader.
"""
import asyncio
import os
import socket
import time
from typing import TYPE_CHECKING

from utils.logger import logger

if TYPE_CHECKING:
    from my_redis.connect_redis import RedisManager


class LeaderElection:
    """
    Redis-based leader election using SETNX with TTL.

    - Lock key: psyduckv2:leader:main
    - Worker ID: {hostname}:{pid}:{startup_timestamp}
    - TTL: 30 seconds, heartbeat every 10 seconds
    - Atomic release using Lua script
    """

    LOCK_KEY = "psyduckv2:leader:main"
    LOCK_TTL = 30  # seconds
    HEARTBEAT_INTERVAL = 10  # seconds

    # Lua script for atomic check-and-delete (only delete if we own the lock)
    RELEASE_SCRIPT = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("DEL", KEYS[1])
    else
        return 0
    end
    """

    # Lua script for atomic check-and-extend.
    #   1 = we own the lock, TTL extended
    #   2 = the lock had vanished (e.g. Redis restarted) and we re-took it
    #   0 = another worker holds it
    EXTEND_SCRIPT = """
    local current = redis.call("GET", KEYS[1])
    if current == ARGV[1] then
        redis.call("EXPIRE", KEYS[1], ARGV[2])
        return 1
    elseif not current then
        redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
        return 2
    else
        return 0
    end
    """

    def __init__(self, redis_manager: "RedisManager"):
        self.redis_manager = redis_manager
        self.worker_id = self._generate_worker_id()
        self._is_leader = False
        self._heartbeat_task: asyncio.Task | None = None
        # Called (as a task) when the heartbeat finds the lock gone and re-takes
        # it - in practice a Redis restart, so the app can react right away.
        self.on_reacquired = None

    @staticmethod
    def _generate_worker_id() -> str:
        """Generate a unique worker ID: hostname:pid:timestamp"""
        hostname = socket.gethostname()
        pid = os.getpid()
        timestamp = int(time.time() * 1000)
        return f"{hostname}:{pid}:{timestamp}"

    @property
    def is_leader(self) -> bool:
        """Check if this worker is currently the leader."""
        return self._is_leader

    async def try_acquire(self, quiet: bool = False) -> bool:
        """
        Attempt to acquire leadership.

        Returns True if this worker became the leader, False otherwise.
        quiet logs a failed attempt at DEBUG (used by the takeover watcher).
        """
        try:
            client = await self.redis_manager.check_redis_connection()
            if not client:
                logger.error("Cannot acquire leadership: Redis connection unavailable")
                return False

            # Try to acquire the lock with SETNX + EX
            result = await client.set(
                self.LOCK_KEY,
                self.worker_id,
                nx=True,  # Only set if not exists
                ex=self.LOCK_TTL
            )

            if result:
                self._is_leader = True
                # Load Lua scripts for atomic operations
                # Start heartbeat to maintain leadership
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                logger.info(f"[LEADER] Worker {self.worker_id} acquired leadership")
                return True
            else:
                # Someone else is leader
                current_leader = await client.get(self.LOCK_KEY)
                (logger.debug if quiet else logger.info)(
                    f"[FOLLOWER] Worker {self.worker_id} - current leader: {current_leader}")
                return False

        except Exception as e:
            logger.error(f"Error acquiring leadership: {e}")
            return False

    async def _heartbeat_loop(self):
        """Background task to maintain leadership by extending TTL."""
        while self._is_leader:
            try:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                if not self._is_leader:
                    break

                result = await self._extend_lock()
                if result is None:
                    # Redis unreachable: we are still running the leader's
                    # services, so keep trying rather than give the role up.
                    continue
                if result == 2:
                    logger.warning(f"[LEADER] Lock had vanished (Redis restart?) - re-acquired by {self.worker_id}")
                    if self.on_reacquired:
                        asyncio.create_task(self.on_reacquired())
                elif result == 0:
                    logger.error(f"[LEADER] Lost leadership - lock is held by another worker")
                    self._is_leader = False
                    break

            except asyncio.CancelledError:
                logger.debug("[LEADER] Heartbeat task cancelled")
                break
            except Exception as e:
                logger.error(f"[LEADER] Heartbeat error: {e}")
                # Don't break - try again on next iteration

    async def _extend_lock(self) -> int | None:
        """
        Extend (or re-take a vanished) lock. Returns 1 extended, 2 re-acquired,
        0 held by another worker, None when Redis could not be reached.
        """
        try:
            client = await self.redis_manager.check_redis_connection()
            if not client:
                return None
            # EVAL (not EVALSHA): the script cache is empty after a Redis restart
            result = await client.eval(self.EXTEND_SCRIPT, 1, self.LOCK_KEY, self.worker_id, str(self.LOCK_TTL))
            return int(result)
        except Exception as e:
            logger.error(f"Error extending lock: {e}")
            return None

    async def watch_for_takeover(self, on_acquired):
        """
        Followers: keep trying to take leadership and run on_acquired() once it
        is ours. Covers a leader that died without releasing its lock - e.g. a
        crash or OOM kill followed by a restart within the lock's 30s TTL, which
        otherwise left the only worker as a follower with no background services.
        """
        while not self._is_leader:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            if await self.try_acquire(quiet=True):
                logger.warning(f"[LEADER] Worker {self.worker_id} took over leadership")
                await on_acquired()
                return

    async def release(self):
        """
        Release leadership gracefully.

        Uses Lua script to atomically check ownership before deleting.
        """
        if not self._is_leader:
            return

        # Stop heartbeat first
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        try:
            client = await self.redis_manager.check_redis_connection()
            if not client:
                logger.warning("Cannot release leadership: Redis connection unavailable")
                self._is_leader = False
                return

            # Atomic check-and-delete. EVAL (not EVALSHA): the script cache is
            # empty after a Redis restart.
            result = await client.eval(self.RELEASE_SCRIPT, 1, self.LOCK_KEY, self.worker_id)
            if result == 1:
                logger.info(f"[LEADER] Worker {self.worker_id} released leadership")
            else:
                logger.warning(f"[LEADER] Lock was already taken by another worker")

        except Exception as e:
            logger.error(f"Error releasing leadership: {e}")
        finally:
            self._is_leader = False

    async def wait_for_leader(self, timeout: float = 30.0) -> bool:
        """
        Wait for a leader to be elected (useful for followers).

        Returns True if a leader exists, False if timeout reached.
        """
        start = time.monotonic()
        while (time.monotonic() - start) < timeout:
            try:
                client = await self.redis_manager.check_redis_connection()
                if client:
                    leader = await client.get(self.LOCK_KEY)
                    if leader:
                        return True
            except Exception as e:
                logger.debug(f"Waiting for leader: {e}")

            await asyncio.sleep(0.5)

        logger.warning(f"Timeout waiting for leader after {timeout}s")
        return False
