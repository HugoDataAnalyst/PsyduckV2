import asyncio
import config as AppConfig
from fastapi import FastAPI
from contextlib import asynccontextmanager
from utils.logger import logger
from utils.koji_geofences import KojiGeofences
from server_fastapi.routes import webhook_router
from server_fastapi import global_state


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
            return result
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

    # Wrap the refresh task in a safe retry wrapper
    async def safe_refresh():
        # This will retry refresh_geofences if it raises an exception.
        await retry_call(koji_instance.refresh_geofences)
    # Start the background refresh task
    asyncio.create_task(safe_refresh())

    yield
    logger.info("👋 Shutting down Webhook Receiver application.")

app = FastAPI(lifespan=lifespan)

# Include the webhook router
app.include_router(webhook_router.router)
