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

class InvasionSQLProcessor:
    @classmethod
    async def upsert_aggregated_invasion_from_filtered(cls, filtered_data, increment: int = 1, pool=None):
        """
        Upsert a single invasion with robust session settings and retries.
        Keeps pokestops fresh and bumps aggregated_invasions.total_count.
        """
        created_pool = False
        try:
            if pool is None:
                pool = await get_mysql_pool()
                created_pool = True

            pokestop_id = filtered_data.get('invasion_pokestop_id')
            if not pokestop_id:
                logger.warning("⚠️ Missing pokestop_id in invasion data")
                return 0

            # Coords (required for pokestops)
            try:
                latitude = float(filtered_data.get('invasion_latitude'))
                longitude = float(filtered_data.get('invasion_longitude'))
            except (TypeError, ValueError):
                logger.warning(f"⚠️ Invalid coordinates for pokestop {pokestop_id}")
                return 0

            area_id = filtered_data.get('area_id')
            if area_id is None:
                logger.warning(f"⚠️ Missing area_id for pokestop {pokestop_id}")
                return 0

            # Invasion fields
            display_type = int(filtered_data.get('invasion_type', 0))
            character    = int(filtered_data.get('invasion_character', 0))
            grunt        = int(filtered_data.get('invasion_grunt_type', 0))
            confirmed    = int(filtered_data.get('invasion_confirmed', 0))

            # Month/year
            try:
                first_seen = int(filtered_data.get('invasion_first_seen'))
                dt = datetime.fromtimestamp(first_seen)
                month_year = int(dt.strftime("%y%m"))
            except Exception:
                logger.warning(f"⚠️ Invalid first_seen timestamp for pokestop {pokestop_id}")
                return 0

            inc = int(increment)

            start_time = time.perf_counter()
            ok = await cls._upsert_invasion_with_retry(
                pool=pool,
                pokestop_id=str(pokestop_id),
                pokestop_name=str(filtered_data.get('invasion_pokestop_name') or ""),
                latitude=latitude,
                longitude=longitude,
                display_type=display_type,
                character=character,
                grunt=grunt,
                confirmed=confirmed,
                area_id=int(area_id),
                month_year=month_year,
                increment=inc,
                max_retries=10
            )
            elapsed = time.perf_counter() - start_time

            if ok:
                logger.debug(f"✅ Processed invasion for pokestop {pokestop_id} in area {area_id} in {elapsed:.4f}s")
                return 1
            logger.warning(f"⚠️ Failed to process invasion for pokestop {pokestop_id} after {elapsed:.4f}s")
            return 0

        except Exception as e:
            logger.error(f"❌ Unexpected error processing invasion for pokestop {filtered_data.get('invasion_pokestop_id')}: {e}", exc_info=True)
            return 0
        finally:
            if created_pool and pool is not None:
                pool.close()
                await pool.wait_closed()

    @classmethod
    async def _upsert_invasion_with_retry(cls, pool, **inv):
        """
        Two-step upsert (pokestops then aggregated_invasions) in one short transaction.
        Retries on 1213/1205 with small jittered backoff.
        """
        max_retries = int(inv.get('max_retries', 8))
        inc = int(inv.get('increment', 1))

        for attempt in range(max_retries):
            try:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SET SESSION innodb_lock_wait_timeout = 10")
                        await cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                        await cur.execute("SET autocommit = 0")

                        # 1) Upsert pokestop
                        await cur.execute(
                            """
                            INSERT INTO pokestops (pokestop, pokestop_name, latitude, longitude)
                            VALUES (%s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                pokestop_name = VALUES(pokestop_name),
                                latitude     = VALUES(latitude),
                                longitude    = VALUES(longitude)
                            """,
                            (inv['pokestop_id'], inv['pokestop_name'], inv['latitude'], inv['longitude'])
                        )

                        # 2) Bump aggregated_invasions via pokestops.id
                        await cur.execute(
                            """
                            INSERT INTO aggregated_invasions (
                                pokestop_id, display_type, `character`, grunt, confirmed,
                                area_id, month_year, total_count
                            )
                            SELECT
                                p.id, %s, %s, %s, %s, %s, %s, %s
                            FROM pokestops p
                            WHERE p.pokestop = %s
                            ON DUPLICATE KEY UPDATE
                                total_count = total_count + VALUES(total_count)
                            """,
                            (
                                inv['display_type'],
                                inv['character'],
                                inv['grunt'],
                                inv['confirmed'],
                                inv['area_id'],
                                inv['month_year'],
                                inc,
                                inv['pokestop_id']
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
                        f"⚠️ Invasion upsert {('deadlock' if code==1213 else 'lock timeout')}."
                        f" Retrying {attempt+1}/{max_retries} in {backoff:.2f}s…"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"❌ Database error processing invasion: {e}", exc_info=True)
                return False
            except Exception as e:
                logger.error(f"❌ Unexpected error processing invasion: {e}", exc_info=True)
                return False

        logger.error("❌ Max retries reached for invasion upsert")
        return False
