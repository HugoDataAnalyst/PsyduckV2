"""
Drop expired partitions a few at a time.

One `ALTER TABLE ... DROP PARTITION a, b, c, ...` holds the table's metadata
lock for the whole drop, and InnoDB sweeps the buffer pool for every dropped
partition's pages while it does - with 30 partitions (a retention cut from 60
to 30 days) that stalled every insert into the table for the duration. Small
batches with a pause between them keep each lock short and let the flushers
write in between.
"""
from __future__ import annotations

import asyncio
import time
from typing import List

import config as AppConfig
from sql.connect_db import transaction
from utils.logger import logger


async def drop_partitions(table: str, partitions: List[str], label: str, dropped: List[str]) -> None:
    """Drop `partitions` in batches of partition_drop_batch, appending each batch to `dropped` as it goes."""
    batch = max(1, AppConfig.partition_drop_batch)
    pause = max(0.0, AppConfig.partition_drop_pause_seconds)
    done = 0
    for i in range(0, len(partitions), batch):
        chunk = partitions[i:i + batch]
        sql = f"ALTER TABLE `{table}` DROP PARTITION " + ", ".join(f"`{p}`" for p in chunk)
        t0 = time.monotonic()
        async with transaction(dict_cursor=False, isolation="READ COMMITTED", lock_wait_timeout=10) as cur:
            await cur.execute(sql)
        dropped.extend(chunk)
        done += len(chunk)
        logger.warning(f"🧹 {label} `{table}` dropped {chunk} in {time.monotonic() - t0:.1f}s "
                       f"({done}/{len(partitions)})")
        if i + batch < len(partitions):
            await asyncio.sleep(pause)
