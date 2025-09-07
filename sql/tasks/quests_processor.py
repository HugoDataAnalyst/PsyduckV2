import asyncio
import random
import aiomysql
from datetime import datetime
import time
from utils.logger import logger
import config as AppConfig

async def get_mysql_pool():
    pool = await aiomysql.create_pool(
        host=AppConfig.db_host,
        port=AppConfig.db_port,
        user=AppConfig.db_user,
        password=AppConfig.db_password,
        db=AppConfig.db_name,
        autocommit=True,
        loop=asyncio.get_running_loop()
    )
    return pool

class QuestSQLProcessor:
    @classmethod
    async def upsert_aggregated_quest_from_filtered(cls, filtered_data, increment: int = 1, pool=None):
        """
        Upsert a single quest with robust session settings and retries.
        Keeps pokestops fresh and bumps aggregated_quests.total_count.
        """
        created_pool = False
        try:
            if pool is None:
                pool = await get_mysql_pool()
                created_pool = True

            # --- Validate required fields ---
            pokestop_id = filtered_data.get('pokestop_id')
            if not pokestop_id:
                logger.warning("⚠️ Missing pokestop_id in quest data")
                return 0

            # Coordinates (required for pokestops, but don't crash on weird values)
            try:
                latitude = float(filtered_data.get('latitude'))
                longitude = float(filtered_data.get('longitude'))
            except (TypeError, ValueError):
                logger.warning(f"⚠️ Invalid coordinates for pokestop {pokestop_id}")
                return 0

            area_id = filtered_data.get('area_id')
            if area_id is None:
                logger.warning(f"⚠️ Missing area_id for pokestop {pokestop_id}")
                return 0

            # month_year from first_seen
            try:
                first_seen = int(filtered_data.get('first_seen'))
                dt = datetime.fromtimestamp(first_seen)
                month_year = int(dt.strftime("%y%m"))
            except Exception:
                logger.warning(f"⚠️ Invalid first_seen timestamp for pokestop {pokestop_id}")
                return 0

            inc = int(increment)

            # --- Execute with retry ---
            start_time = time.perf_counter()
            success = await cls._upsert_quest_with_retry(
                pool=pool,
                pokestop_id=str(pokestop_id),
                pokestop_name=str(filtered_data.get('pokestop_name') or ""),
                latitude=latitude,
                longitude=longitude,
                ar_type=filtered_data.get('ar_type', 0),
                normal_type=filtered_data.get('normal_type', 0),
                reward_ar_type=filtered_data.get('reward_ar_type', 0),
                reward_normal_type=filtered_data.get('reward_normal_type', 0),
                reward_ar_item_id=filtered_data.get('reward_ar_item_id', 0),
                reward_ar_item_amount=filtered_data.get('reward_ar_item_amount', 0),
                reward_normal_item_id=filtered_data.get('reward_normal_item_id', 0),
                reward_normal_item_amount=filtered_data.get('reward_normal_item_amount', 0),
                reward_ar_poke_id=filtered_data.get('reward_ar_poke_id', 0),
                reward_ar_poke_form=str(filtered_data.get('reward_ar_poke_form') or ""),
                reward_normal_poke_id=filtered_data.get('reward_normal_poke_id', 0),
                reward_normal_poke_form=str(filtered_data.get('reward_normal_poke_form') or ""),
                area_id=int(area_id),
                month_year=month_year,
                increment=inc,
                max_retries=10
            )
            elapsed = time.perf_counter() - start_time

            if success:
                logger.debug(f"✅ Processed quest for pokestop {pokestop_id} in area {area_id} in {elapsed:.4f}s")
                return 1
            else:
                logger.warning(f"⚠️ Failed to process quest for pokestop {pokestop_id} after {elapsed:.4f}s")
                return 0

        except Exception as e:
            logger.error(f"❌ Unexpected error processing quest for pokestop {filtered_data.get('pokestop_id')}: {e}", exc_info=True)
            return 0
        finally:
            if created_pool and pool is not None:
                pool.close()
                await pool.wait_closed()

    @classmethod
    async def _upsert_quest_with_retry(cls, pool, **q):
        """
        Do the two-step upsert inside a short, atomic transaction with good session knobs.
        Retries on 1213 (deadlock) and 1205 (lock wait timeout).
        """
        max_retries = int(q.get('max_retries', 8))
        inc = int(q.get('increment', 1))

        for attempt in range(max_retries):
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        # Session tuning: keep waits short and reduce gap locks
                        await cur.execute("SET SESSION innodb_lock_wait_timeout = 10")
                        await cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        await cur.execute("SET autocommit = 0")

                        # ---- Step 1: upsert pokestop (id/name/coords) ----
                        pokestop_sql = """
                            INSERT INTO pokestops (pokestop, pokestop_name, latitude, longitude)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                pokestop_name = VALUES(pokestop_name),
                                latitude     = VALUES(latitude),
                                longitude    = VALUES(longitude)
                        """
                        await cur.execute(
                            pokestop_sql,
                            (q['pokestop_id'], q['pokestop_name'], q['latitude'], q['longitude'])
                        )

                        # ---- Step 2: bump aggregated_quests via SELECT p.id ----
                        quest_sql = """
                            INSERT INTO aggregated_quests (
                                pokestop_id,
                                ar_type, normal_type,
                                reward_ar_type,   reward_normal_type,
                                reward_ar_item_id,    reward_ar_item_amount,
                                reward_normal_item_id, reward_normal_item_amount,
                                reward_ar_poke_id,     reward_ar_poke_form,
                                reward_normal_poke_id, reward_normal_poke_form,
                                area_id, month_year, total_count
                            )
                            SELECT
                                p.id,
                                COALESCE(%s, 0),
                                COALESCE(%s, 0),
                                COALESCE(%s, 0),
                                COALESCE(%s, 0),
                                COALESCE(%s, 0),
                                COALESCE(%s, 0),
                                COALESCE(%s, 0),
                                COALESCE(%s, 0),
                                COALESCE(%s, 0),
                                COALESCE(%s, ''),
                                COALESCE(%s, 0),
                                COALESCE(%s, ''),
                                %s,
                                %s,
                                %s
                            FROM pokestops p
                            WHERE p.pokestop = %s
                            ON DUPLICATE KEY UPDATE
                                total_count = total_count + VALUES(total_count)
                        """
                        await cur.execute(
                            quest_sql,
                            (
                                q['ar_type'],
                                q['normal_type'],
                                q['reward_ar_type'],
                                q['reward_normal_type'],
                                q['reward_ar_item_id'],
                                q['reward_ar_item_amount'],
                                q['reward_normal_item_id'],
                                q['reward_normal_item_amount'],
                                q['reward_ar_poke_id'],
                                q['reward_ar_poke_form'],
                                q['reward_normal_poke_id'],
                                q['reward_normal_poke_form'],
                                q['area_id'],
                                q['month_year'],
                                inc,
                                q['pokestop_id']
                            )
                        )

                        # One atomic commit for both steps
                        await conn.commit()
                        # restore autocommit for this connection
                        await cur.execute("SET autocommit = 1")

                        # timings (optional but helpful)
                        logger.debug("⏱️ DB ops done for quest.")
                        return True

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):  # deadlock or lock wait
                    # small jittered backoff; grows slightly each attempt
                    backoff = min(2.0, 0.25 * (attempt + 1)) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ Quest upsert {('deadlock' if code==1213 else 'lock timeout')}."
                        f" Retrying {attempt+1}/{max_retries} in {backoff:.2f}s…"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ Database error processing quest: {e}", exc_info=True)
                return False
            except Exception as e:
                logger.error(f"❌ Unexpected error processing quest: {e}", exc_info=True)
                return False

        logger.error("❌ Max retries reached for quest upsert")
        return False
