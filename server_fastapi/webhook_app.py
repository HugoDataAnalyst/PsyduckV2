import asyncio
import config as AppConfig
from fastapi import FastAPI
from contextlib import asynccontextmanager
from utils.logger import logger
from utils.koji_geofences import KojiGeofences
from server_fastapi.routes import data_api, webhook_router
from server_fastapi import global_state
from my_redis.connect_redis import RedisManager
from server_fastapi.utils import (
    details,
    secure_api,
)

redis_manager = RedisManager()

async def retry_call(coro_func, *args, max_attempts=5, initial_delay=2, delay_increment=2, **kwargs):
    """
    Retry an async function call up to `max_attempts` times with an increasing delay.

    :param coro_func: The asynchronous function to call.
    :param args: Positional arguments for the function.
    :param kwargs: Keyword arguments for the function.
    :param max_attempts: Maximum number of attempts.
    :param initial_delay: Delay before the first retry (in seconds).
    :param delay_increment: How much to increase the delay after each failed attempt.
    :return: The result of the function if successful.
    :raises Exception: If all attempts fail.
    """
    attempt = 0
    delay = initial_delay
    while attempt < max_attempts:
        try:
            result = await coro_func(*args, **kwargs)
            if result:
                return result
            else:
                raise Exception("❌ No result returned from function.")
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                logger.error(f"❌ Maximum attempts reached for {coro_func.__name__}.")
                raise e
            logger.warning(f"⚠️ Attempt {attempt} for {coro_func.__name__} failed: {e}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay += delay_increment

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Koji Instance
    koji_instance = KojiGeofences(AppConfig.geofence_refresh_cache_seconds)
    global_state.geofences = await retry_call(koji_instance.get_cached_geofences)
    if not global_state.geofences:
        logger.error("⚠️ No geofences available at startup. Exiting application.")
        raise Exception("❌ No geofences available at startup, stopping application.")

    """Initialize all Redis pools on startup."""
    pool_names = ["pokemon_pool", "quest_pool", "raid_pool", "invasion_pool", "retrieval_pool"]

    for pool_name in pool_names:
        max_conn = RedisManager.get_max_connections_for_pool(pool_name)
        await RedisManager.init_pool(pool_name, max_connections=max_conn)

    # Wrap the refresh task in a safe retry wrapper
    async def safe_refresh():
        # This will retry refresh_geofences if it raises an exception.
        await retry_call(koji_instance.refresh_geofences)
    # Start the background refresh task
    asyncio.create_task(safe_refresh())

    yield
    logger.info("👋 Shutting down Webhook Receiver application.")

# Customise FastAPI instance.
app = FastAPI(
    title=details.TITLE,
    description=details.DESCRIPTION,
    version=details.VERSION,
    openapi_tags=details.TAGS_METADATA,
    docs_url="/docs",  # Swagger UI available at /docs
    redoc_url=None,    # Disable ReDoc UI
    lifespan=lifespan,
)

app.add_middleware(secure_api.AllowedPathsMiddleware)

# Include the webhook router
app.include_router(webhook_router.router)
# Include the Data retriving router
app.include_router(data_api.router)
