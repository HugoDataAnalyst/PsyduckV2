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
        autocommit=True,  # we'll toggle during critical section
        loop=asyncio.get_running_loop()
    )
    return pool

class RaidSQLProcessor:
    @classmethod
    async def upsert_aggregated_raid_from_filtered(cls, filtered_data, increment: int = 1, pool=None):
        """
        Upsert a single raid with robust session settings and retries.
        Keeps gyms fresh and bumps aggregated_raids.total_count.
        """
        created_pool = False
        try:
            if pool is None:
                pool = await get_mysql_pool()
                created_pool = True

            gym_id = filtered_data.get('raid_gym_id')
            if not gym_id:
                logger.warning("⚠️ Missing gym_id in raid data")
                return 0

            # Coords (required for gyms)
            try:
                latitude = float(filtered_data.get('raid_latitude'))
                longitude = float(filtered_data.get('raid_longitude'))
            except (TypeError, ValueError):
                logger.warning(f"⚠️ Invalid coordinates for gym {gym_id}")
                return 0

            area_id = filtered_data.get('area_id')
            if area_id is None:
                logger.warning(f"⚠️ Missing area_id for gym {gym_id}")
                return 0

            # Raid fields
            raid_pokemon       = int(filtered_data.get('raid_pokemon', 0))
            raid_level         = int(filtered_data.get('raid_level', 0))
            raid_form          = str(filtered_data.get('raid_form', '0'))
            raid_team          = int(filtered_data.get('raid_team_id', 0))
            raid_costume       = str(filtered_data.get('raid_costume', '0'))
            raid_is_exclusive  = int(filtered_data.get('raid_is_exclusive', 0))
            raid_ex_eligible   = int(filtered_data.get('raid_ex_raid_eligible', 0))

            # Month/year
            try:
                first_seen = int(filtered_data.get('raid_first_seen'))
                dt = datetime.fromtimestamp(first_seen)
                month_year = int(dt.strftime("%y%m"))
            except Exception:
                logger.warning(f"⚠️ Invalid first_seen timestamp for gym {gym_id}")
                return 0

            inc = int(increment)

            start_time = time.perf_counter()
            ok = await cls._upsert_raid_with_retry(
                pool=pool,
                gym_id=str(gym_id),
                gym_name=str(filtered_data.get('raid_gym_name') or ""),
                latitude=latitude,
                longitude=longitude,
                raid_pokemon=raid_pokemon,
                raid_level=raid_level,
                raid_form=raid_form,
                raid_team=raid_team,
                raid_costume=raid_costume,
                raid_is_exclusive=raid_is_exclusive,
                raid_ex_raid_eligible=raid_ex_eligible,
                area_id=int(area_id),
                month_year=month_year,
                increment=inc,
                max_retries=10
            )
            elapsed = time.perf_counter() - start_time

            if ok:
                logger.debug(f"✅ Processed raid for gym {gym_id} in area {area_id} in {elapsed:.4f}s")
                return 1
            logger.warning(f"⚠️ Failed to process raid for gym {gym_id} after {elapsed:.4f}s")
            return 0

        except Exception as e:
            logger.error(f"❌ Unexpected error processing raid for gym {filtered_data.get('raid_gym_id')}: {e}", exc_info=True)
            return 0
        finally:
            if created_pool and pool is not None:
                pool.close()
                await pool.wait_closed()

    @classmethod
    async def _upsert_raid_with_retry(cls, pool, **r):
        """
        Two-step upsert (gyms then aggregated_raids) in one short transaction.
        Retries on 1213/1205 with small jittered backoff.
        """
        max_retries = int(r.get('max_retries', 8))
        inc = int(r.get('increment', 1))

        for attempt in range(max_retries):
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SET SESSION innodb_lock_wait_timeout = 10")
                        await cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        await cur.execute("SET autocommit = 0")

                        # 1) Upsert gym
                        await cur.execute(
                            """
                            INSERT INTO gyms (gym, gym_name, latitude, longitude)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                gym_name = VALUES(gym_name),
                                latitude = VALUES(latitude),
                                longitude = VALUES(longitude)
                            """,
                            (r['gym_id'], r['gym_name'], r['latitude'], r['longitude'])
                        )

                        # 2) Bump aggregated_raids via gyms.id
                        await cur.execute(
                            """
                            INSERT INTO aggregated_raids (
                                gym_id, raid_pokemon, raid_level, raid_form, raid_team,
                                raid_costume, raid_is_exclusive, raid_ex_raid_eligible,
                                area_id, month_year, total_count
                            )
                            SELECT
                                g.id, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            FROM gyms g
                            WHERE g.gym = %s
                            ON DUPLICATE KEY UPDATE
                                total_count = total_count + VALUES(total_count)
                            """,
                            (
                                r['raid_pokemon'],
                                r['raid_level'],
                                r['raid_form'],
                                r['raid_team'],
                                r['raid_costume'],
                                r['raid_is_exclusive'],
                                r['raid_ex_raid_eligible'],
                                r['area_id'],
                                r['month_year'],
                                inc,
                                r['gym_id']
                            )
                        )

                        await conn.commit()
                        await cur.execute("SET autocommit = 1")
                        return True

            except aiomysql.Error as e:
                code = e.args[0] if e.args else None
                if code in (1213, 1205):
                    backoff = min(2.0, 0.25 * (attempt + 1)) + random.uniform(0, 0.1)
                    logger.warning(
                        f"⚠️ Raid upsert {('deadlock' if code==1213 else 'lock timeout')}."
                        f" Retrying {attempt+1}/{max_retries} in {backoff:.2f}s…"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ Database error processing raid: {e}", exc_info=True)
                return False
            except Exception as e:
                logger.error(f"❌ Unexpected error processing raid: {e}", exc_info=True)
                return False

        logger.error("❌ Max retries reached for raid upsert")
        return False
