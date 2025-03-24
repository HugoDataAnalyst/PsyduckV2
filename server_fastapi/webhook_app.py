import asyncio
import config as AppConfig
from fastapi import FastAPI, Response, openapi
from fastapi.staticfiles import StaticFiles
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
from fastapi.openapi.docs import get_swagger_ui_html
from sql.tasks.pokemon_heatmap_flusher import PokemonIVBufferFlusher
from sql.tasks.pokemon_shiny_flusher import ShinyRateBufferFlusher

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
    pool_names = ["pokemon_pool", "quest_pool", "raid_pool",
                  "invasion_pool", "retrieval_pool", "koji_geofence_pool",
                  "flush_heatmap_pool", "flush_shiny_pool", "sql_pokemon_pool",
                  "redis_cleanup_pool"
                ]

    for pool_name in pool_names:
        max_conn = RedisManager.get_max_connections_for_pool(pool_name)
        await RedisManager.init_pool(pool_name, max_connections=max_conn)

    redis_client = await redis_manager.get_client("redis_cleanup_pool")

    cleanup_task = asyncio.create_task(
        redis_manager.idle_client_cleanup(redis_client)
    )

    # Wrap the refresh task in a safe retry wrapper
    async def safe_refresh():
        # This will retry refresh_geofences if it raises an exception.
        await retry_call(koji_instance.refresh_geofences)
    # Start the background refresh task
    refresh_task = asyncio.create_task(safe_refresh())

    # Initialize and start buffer flushers
    pokemon_buffer_flusher = PokemonIVBufferFlusher(flush_interval=60)  # 1 minute
    shiny_rate_buffer_flusher = ShinyRateBufferFlusher(flush_interval=60)  # 1 minute

    # Start the flusher tasks
    await pokemon_buffer_flusher.start()
    await shiny_rate_buffer_flusher.start()

    # Yield control back to FastAPI
    yield

    # Shutdown logic
    logger.info("👋 Shutting down Webhook Receiver application.")

    # Cancel and await the cleanup task
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        logger.info("🛑 Idle client cleanup task cancelled.")

    # Cancel and await the refresh task
    refresh_task.cancel()
    try:
        await refresh_task
    except asyncio.CancelledError:
        logger.info("🛑 Geofence refresh task cancelled.")

    # Stop the flusher tasks
    await pokemon_buffer_flusher.stop()
    await shiny_rate_buffer_flusher.stop()

    # Close Redis pools
    await RedisManager.close_all_pools()

# Custom Swagger UI HTML template
def custom_swagger_ui_html(*args, **kwargs) -> Response:
    # Get the default HTMLResponse from FastAPI
    default_response = get_swagger_ui_html(*args, **kwargs)
    # Convert the response body (bytes) to a string.
    html_str = default_response.body.decode("utf-8")
    # Add your custom CSS and custom button
    custom_css = (
        '<style>'
        '.my-custom-button { '
        '   color: #fff; '
        '   background-color: #007BFF; '
        '   padding: 10px 20px; '
        '   border-radius: 5px; '
        '   text-decoration: none; '
        '}'
        '</style>'
    )
    #custom_button = '<a href="https://example.com/docs" target="_blank" class="my-custom-button">Custom Docs</a>'
    # Insert the custom CSS and button before the closing </head>
    favicon_link = '<link rel="icon" href="/static/psyduck.ico" type="image/x-icon">'
    html_str = html_str.replace('</head>', f'{favicon_link}{custom_css}</head>')
    #html_str = html_str.replace('</head>', f'{favicon_link}{custom_css}{custom_button}</head>')
    # Return a new Response with the modified HTML
    return Response(content=html_str, media_type="text/html")

# Customise FastAPI instance.
app = FastAPI(
    title=details.TITLE,
    description=details.DESCRIPTION,
    version=details.VERSION,
    openapi_tags=details.TAGS_METADATA,
    docs_url=None,   # Swagger UI available at /docs
    redoc_url=None,    # Disable ReDoc UI
    lifespan=lifespan,
)

app.add_middleware(secure_api.AllowedPathsMiddleware)
@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui():
    return custom_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=f"{app.title} - UI"
    )
# Mount static folder to serve favicon and other assets
app.mount("/static", StaticFiles(directory="server_fastapi/static"), name="static")
# Include the webhook router
app.include_router(webhook_router.router)
# Include the Data retriving router
app.include_router(data_api.router)
