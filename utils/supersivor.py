from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Iterable, Optional
from utils.logger import logger

StartFn = Callable[[], Awaitable[None]]
StopFn  = Callable[[], Awaitable[None]]

@dataclass
class Service:
    name: str
    enabled: bool
    start: StartFn
    stop: Optional[StopFn] = None

async def start_services(services: Iterable[Service]) -> None:
    for s in services:
        if not s.enabled:
            logger.info(f"⏭️  Skipping {s.name} (disabled)")
            continue
        try:
            await s.start()
            logger.success(f"✅ Started {s.name}")
        except Exception as e:
            logger.error(f"❌ Failed to start {s.name}: {e}", exc_info=True)

async def stop_services(services: Iterable[Service]) -> None:
    # Stop in reverse order
    for s in list(services)[::-1]:
        if not s.enabled or not s.stop:
            continue
        try:
            await s.stop()
            logger.info(f"🛑 Stopped {s.name}")
        except Exception as e:
            logger.error(f"❌ Failed to stop {s.name}: {e}", exc_info=True)
