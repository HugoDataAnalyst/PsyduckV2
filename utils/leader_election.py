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

    # Lua script for atomic check-and-extend (only extend if we own the lock)
    EXTEND_SCRIPT = """
    if redis.call("GET", KEYS[1]) == ARGV[1] then
        return redis.call("EXPIRE", KEYS[1], ARGV[2])
    else
        return 0
    end
    """

    def __init__(self, redis_manager: "RedisManager"):
        self.redis_manager = redis_manager
        self.worker_id = self._generate_worker_id()
        self._is_leader = False
        self._heartbeat_task: asyncio.Task | None = None
        self._release_script_sha: str | None = None
        self._extend_script_sha: str | None = None

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

    async def try_acquire(self) -> bool:
        """
        Attempt to acquire leadership.

        Returns True if this worker became the leader, False otherwise.
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
                self._release_script_sha = await client.script_load(self.RELEASE_SCRIPT)
                self._extend_script_sha = await client.script_load(self.EXTEND_SCRIPT)
                # Start heartbeat to maintain leadership
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                logger.info(f"[LEADER] Worker {self.worker_id} acquired leadership")
                return True
            else:
                # Someone else is leader
                current_leader = await client.get(self.LOCK_KEY)
                logger.info(f"[FOLLOWER] Worker {self.worker_id} - current leader: {current_leader}")
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

                success = await self._extend_lock()
                if not success:
                    logger.warning(f"[LEADER] Lost leadership - lock was taken by another worker")
                    self._is_leader = False
                    break

            except asyncio.CancelledError:
                logger.debug("[LEADER] Heartbeat task cancelled")
                break
            except Exception as e:
                logger.error(f"[LEADER] Heartbeat error: {e}")
                # Don't break - try again on next iteration

    async def _extend_lock(self) -> bool:
        """Extend the lock TTL if we still own it."""
        try:
            client = await self.redis_manager.check_redis_connection()
            if not client:
                return False

            # Use Lua script for atomic check-and-extend
            if self._extend_script_sha:
                result = await client.evalsha(
                    self._extend_script_sha,
                    1,  # Number of keys
                    self.LOCK_KEY,
                    self.worker_id,
                    str(self.LOCK_TTL)
                )
                return result == 1
            else:
                # Fallback: non-atomic version (less safe but works)
                current = await client.get(self.LOCK_KEY)
                if current == self.worker_id:
                    await client.expire(self.LOCK_KEY, self.LOCK_TTL)
                    return True
                return False

        except Exception as e:
            logger.error(f"Error extending lock: {e}")
            return False

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

            # Use Lua script for atomic check-and-delete
            if self._release_script_sha:
                result = await client.evalsha(
                    self._release_script_sha,
                    1,  # Number of keys
                    self.LOCK_KEY,
                    self.worker_id
                )
                if result == 1:
                    logger.info(f"[LEADER] Worker {self.worker_id} released leadership")
                else:
                    logger.warning(f"[LEADER] Lock was already taken by another worker")
            else:
                # Fallback: non-atomic version
                current = await client.get(self.LOCK_KEY)
                if current == self.worker_id:
                    await client.delete(self.LOCK_KEY)
                    logger.info(f"[LEADER] Worker {self.worker_id} released leadership")

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
