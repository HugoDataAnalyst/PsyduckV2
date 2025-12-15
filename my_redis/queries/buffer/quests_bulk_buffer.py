from __future__ import annotations
from redis.asyncio.client import Redis
from utils.logger import logger
from utils.safe_values import _safe_int, _norm_str, _norm_name, _valid_coords, _to_float
from sql.tasks.quests_processor import QuestSQLProcessor
import config as AppConfig
import asyncio


async def _get_client_with_retry(redis_client: Redis | None, max_attempts: int = 3, delay: float = 0.3) -> Redis | None:
    """
    Try to get a working Redis client with retry logic.
    First tries the provided client, then falls back to getting a fresh connection.
    """
    from my_redis.connect_redis import RedisManager
    redis_manager = RedisManager()

    for attempt in range(1, max_attempts + 1):
        # Try provided client first
        if redis_client:
            try:
                if await redis_client.ping():
                    return redis_client
            except Exception:
                pass

        # Fall back to getting a fresh connection
        try:
            client = await redis_manager.get_connection_with_retry(max_attempts=2, delay=0.2)
            if client:
                return client
        except Exception:
            pass

        if attempt < max_attempts:
            await asyncio.sleep(delay)
            logger.debug(f"⏳ Quests buffer retry attempt {attempt}/{max_attempts}")

    return None


class QuestsRedisBuffer:
    """
    Buffers quest events as pipe-delimited lines in a Redis LIST, then flushes in batches.

    Composite key (pipe-delimited):
      pokestop|mode|task_type|area_id|first_seen|kind|item_id|item_amount|poke_id|poke_form

    Where:
      - mode: 0=normal, 1=ar
      - kind: 0=item, 1=pokemon
      - item_id/item_amount used when kind=0; poke_id/poke_form when kind=1 (others set to 0/"")
    """
    redis_key = "buffer:quest_events"
    aggregation_threshold = int(AppConfig.quest_max_threshold)

    @classmethod
    async def increment_event(cls, redis_client: Redis, event_data: dict):
        """
        Accepts the normalized quest_data.
        Required keys:
          pokestop_id, pokestop_name, latitude, longitude, area_id, first_seen,
          ar_type/normal_type (one of them), reward_* (first reward only)
        """
        try:
            pokestop     = _norm_str(event_data.get("pokestop_id"))
            pokestop_name= _norm_name(event_data.get("pokestop_name"))
            lat          = _to_float(event_data.get("latitude"))
            lon          = _to_float(event_data.get("longitude"))

            area_id      = _safe_int(event_data.get("area_id"))
            first_seen   = _safe_int(event_data.get("first_seen"))
            mode         = 1 if _safe_int(event_data.get("ar_type"), 0) > 0 else 0
            task_type    = _safe_int(event_data.get("ar_type") if mode else event_data.get("normal_type"), 0)

            if not pokestop or area_id is None or first_seen is None or task_type == 0:
                logger.debug("❌ quests: missing pokestop/area_id/first_seen/task_type")
                return

            # Skip if coords are missing/zero/invalid
            if not _valid_coords(lat, lon):
                logger.debug(f"↩️  Quests: dropped event for `{pokestop}` due to invalid coords lat={lat} lon={lon}")
                return

            # Reward resolution
            poke_id   = _safe_int(event_data.get("reward_ar_poke_id" if mode else "reward_normal_poke_id"), 0)
            poke_form = _norm_str(event_data.get("reward_ar_poke_form" if mode else "reward_normal_poke_form"), "")
            item_id   = _safe_int(event_data.get("reward_ar_item_id" if mode else "reward_normal_item_id"), 0)
            item_amt  = _safe_int(event_data.get("reward_ar_item_amount" if mode else "reward_normal_item_amount"), 0)

            if poke_id and poke_id > 0:
                kind = 1
                if not poke_form:
                    poke_form = "0"
                item_id = 0
                item_amt = 0
            elif item_id and item_id > 0:
                kind = 0
                if item_amt is None or item_amt <= 0:
                    item_amt = 1
                poke_id = 0
                poke_form = ""
            else:
                logger.debug("❌ quests: no usable reward (item or pokemon)")
                return

            # pokestop|name|lat|lon|mode|task_type|area_id|first_seen|kind|item_id|item_amount|poke_id|poke_form
            line = (
                f"{pokestop}|{pokestop_name}|{lat}|{lon}|{mode}|{task_type}|{area_id}|{first_seen}|{kind}|"
                f"{item_id}|{item_amt}|{poke_id}|{poke_form}"
            )

            # Get a working client with retry
            client = await _get_client_with_retry(redis_client)
            if not client:
                logger.error("❌ Quests buffer: No Redis connection available after retries. Event lost.")
                return

            # Append with retry
            new_len = None
            for attempt in range(3):
                try:
                    new_len = await client.rpush(cls.redis_key, line)
                    break
                except Exception as e:
                    if attempt < 2:
                        client = await _get_client_with_retry(None)
                        if not client:
                            logger.error(f"❌ Quests buffer: rpush failed, no client available: {e}")
                            return
                    else:
                        logger.error(f"❌ Quests buffer: rpush failed after retries: {e}")
                        return

            logger.debug(f"➕ Appended quest event. len={new_len}")

            queued = await client.llen(cls.redis_key)
            if queued >= cls.aggregation_threshold:
                logger.warning(f"📊 📜 Quest buffer threshold {queued} reached. Flushing…")
                await cls.flush_if_ready(client)

        except Exception as e:
            logger.error(f"❌ quests increment_event error: {e}", exc_info=True)

    @classmethod
    async def _consume_list_to_batch(cls, redis_client: Redis, temp_key: str) -> list[dict]:
        rows = await redis_client.lrange(temp_key, 0, -1)
        if not rows:
            return []

        batch: list[dict] = []
        malformed = 0
        for b in rows:
            try:
                line = b.decode() if isinstance(b, (bytes, bytearray)) else b
                # pokestop|name|lat|lon|mode|task_type|area_id|first_seen|kind|item_id|item_amount|poke_id|poke_form
                (
                    pokestop, name, lat_s, lon_s, mode_s, task_type_s, area_id_s, first_seen_s, kind_s,
                    item_id_s, item_amount_s, poke_id_s, poke_form
                ) = line.split("|", 12)

                batch.append({
                    "pokestop": pokestop,
                    "pokestop_name": name,
                    "latitude": float(lat_s),
                    "longitude": float(lon_s),
                    "mode": int(mode_s),
                    "task_type": int(task_type_s),
                    "area_id": int(area_id_s),
                    "first_seen": int(first_seen_s),
                    "kind": int(kind_s),
                    "item_id": int(item_id_s),
                    "item_amount": int(item_amount_s),
                    "poke_id": int(poke_id_s),
                    "poke_form": poke_form,
                })
            except Exception:
                malformed += 1

        if malformed:
            logger.warning(f"⚠️ quests buffer: skipped {malformed} malformed line(s)")
        return batch

    @classmethod
    async def flush_if_ready(cls, redis_client: Redis) -> int:
        # Get a working client with retry
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error("❌ Quests flush: No Redis connection available after retries.")
            return 0

        temp_key = None
        try:
            if not await client.exists(cls.redis_key):
                return 0

            temp_key = cls.redis_key + ":flushing"
            try:
                await client.rename(cls.redis_key, temp_key)
            except Exception as e:
                if "no such key" in str(e).lower():
                    return 0
                raise

            data_batch = await cls._consume_list_to_batch(client, temp_key)
            if not data_batch:
                return 0

            inserted = await QuestSQLProcessor.bulk_insert_quests_daily_events(data_batch)
            logger.success(f"📬 Quests daily-events inserted +{inserted} rows.")
            return inserted

        except Exception as e:
            logger.error(f"❌ quests flush error: {e}", exc_info=True)
            return 0
        finally:
            try:
                if temp_key and client:
                    await client.delete(temp_key)
            except Exception:
                pass

    @classmethod
    async def force_flush(cls, redis_client: Redis) -> int:
        # Get a working client with retry
        client = await _get_client_with_retry(redis_client)
        if not client:
            logger.error("❌ Quests force-flush: No Redis connection available after retries.")
            return 0

        temp_key = None
        try:
            if not await client.exists(cls.redis_key):
                return 0

            temp_key = cls.redis_key + ":force_flushing"
            try:
                await client.rename(cls.redis_key, temp_key)
            except Exception as e:
                if "no such key" in str(e).lower():
                    return 0
                raise

            data_batch = await cls._consume_list_to_batch(client, temp_key)
            if not data_batch:
                return 0

            inserted = await QuestSQLProcessor.bulk_insert_quests_daily_events(data_batch)
            logger.success(f"📬 FORCE quests daily-events inserted +{inserted} rows.")
            return inserted

        except Exception as e:
            logger.error(f"❌ quests force-flush error: {e}")
            return 0
        finally:
            try:
                if temp_key and client:
                    await client.delete(temp_key)
            except Exception:
                pass
