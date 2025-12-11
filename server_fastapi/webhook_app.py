import asyncio
import os
import config as AppConfig
from fastapi import FastAPI, Response, openapi
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from server_fastapi.routes import data_api, webhook_router
from server_fastapi.routes.webhook_router import cleanup_semaphore
from server_fastapi import global_state
from my_redis.connect_redis import RedisManager
from server_fastapi.utils import details, secure_api
from fastapi.openapi.docs import get_swagger_ui_html
from utils.logger import setup_logging, logger
from utils.koji_geofences import KojiGeofences
from utils.leader_election import LeaderElection
from utils.global_state_manager import GlobalStateManager
import sql.connect_db as ConnectDB
from sql.utils.create_partitions import ensure_daily_partitions, ensure_monthly_partitions
from sql.tasks.partition_ensurer import DailyPartitionEnsurer, MonthlyPartitionEnsurer
from sql.tasks.golbat_pokestops import GolbatSQLPokestops
from sql.tasks.cleaners.global_partition_cleaner import build_default_cleaner
from sql.tasks.pokemon_heatmap_flusher import PokemonIVBufferFlusher
from sql.tasks.pokemon_shiny_flusher import ShinyRateBufferFlusher
from sql.tasks.invasion_stops_flusher import InvasionsBufferFlusher
from sql.tasks.quest_stops_flusher import QuestsBufferFlusher
from sql.tasks.raid_gyms_flusher import RaidsBufferFlusher
from my_redis.utils.expire_timeseries import periodic_cleanup
from tzlocal import get_localzone
from datetime import datetime, timedelta
from utils.supersivor import Service, start_services, stop_services

# Initialize logging for THIS worker process
# Each uvicorn worker is a separate process that imports this module,
# so each worker will configure logging with its own PID
setup_logging(
    AppConfig.log_level,
    {
        "to_file": AppConfig.log_file,
        "file_path": "logs/psyduckv2.log",
        "rotation": "5 MB",
        "keep_total": 5,
        "compression": "gz",
        "show_file": True,
        "show_function": True,
        "show_process": AppConfig.uvicorn_workers > 1,
    },
)

# Log that this worker process has started (module import = worker started)
logger.info(f"[W-{os.getpid()}] Worker process started (module imported)")

# Multi-worker support:
# - Leader election ensures only one worker runs background tasks (flushers, partition ensurers, cleanup)
# - Global state is shared via Redis (geofences, timezone, pokestops)
# - All workers handle webhooks and API requests

redis_manager = RedisManager()

def detect_and_store_local_timezone():
    """Detects and loads the local machine's timezone (e.g., 'Europe/Lisbon')."""
    local_tz = get_localzone()
    global_state.user_timezone = local_tz
    logger.success(f"✅ Local timezone detected and stored: {local_tz}")


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
    """
    FastAPI lifespan context manager with multi-worker support.

    - All workers: Initialize Redis, sync global state from Redis
    - Leader only: Run background tasks (flushers, partition ensurers, cleanup, refreshers)
    """
    worker_id = f"W-{os.getpid()}"
    logger.info(f"[{worker_id}] Lifespan startup beginning...")


    # Common initialization for ALL workers


    # Detect and store the local timezone
    detect_and_store_local_timezone()

    # Initialize database pool for this worker process
    # Each worker needs its own pool since forked processes don't share the main process's pool
    logger.info(f"[{worker_id}] Initializing database connection pool...")
    await ConnectDB.init_db()

    # Initialize Redis connection
    logger.info(f"[{worker_id}] Initializing Redis connection...")
    redis_client = await redis_manager.init_redis()
    if not redis_client:
        logger.error(f"[{worker_id}] ❌ Failed to initialize Redis connection. Exiting application.")
        raise Exception("❌ Failed to initialize Redis connection, stopping application.")
    logger.info(f"[{worker_id}] Redis connection established")

    # Set up GlobalStateManager with Redis
    GlobalStateManager.set_redis_manager(redis_manager)


    # Leader election


    leader = LeaderElection(redis_manager)
    is_leader = await leader.try_acquire()

    # Track background tasks and services only populated for leader
    background_tasks: list[tuple[asyncio.Task, str]] = []
    services: list[Service] = []

    if is_leader:
        # LEADER: Initialize state and start background tasks

        logger.info(f"[{worker_id}] [LEADER] This worker is the leader - starting background services")

        # Initialize Koji geofences and cache in Redis
        koji_instance = KojiGeofences(AppConfig.geofence_refresh_cache_seconds)
        geofences = await retry_call(koji_instance.get_cached_geofences)
        if not geofences:
            logger.error("⚠️ No geofences available at startup. Exiting application.")
            raise Exception("❌ No geofences available at startup, stopping application.")

        # Store geofences in Redis for other workers
        await GlobalStateManager.set_geofences(geofences, expiry=AppConfig.geofence_expire_cache_seconds)

        # Store timezone in Redis for other workers
        await GlobalStateManager.set_timezone(global_state.user_timezone)

        # Sync to legacy global_state for backward compatibility
        global_state.geofences = geofences

        # Start background refresh tasks leader only

        async def safe_refresh():
            await retry_call(koji_instance.refresh_geofences)

        async def safe_refresh_pokestops():
            await retry_call(GolbatSQLPokestops.run_refresh_loop, AppConfig.pokestop_refresh_interval_seconds)

        refresh_task = asyncio.create_task(safe_refresh())
        pokestop_refresh_task = asyncio.create_task(safe_refresh_pokestops())
        cleanup_timeseries_task = asyncio.create_task(periodic_cleanup())

        background_tasks = [
            (refresh_task, "geofence refresh"),
            (pokestop_refresh_task, "pokestop refresh"),
            (cleanup_timeseries_task, "periodic cleanup"),
        ]

        # Initialize buffer flushers leader only

        pokemon_buffer_flusher = PokemonIVBufferFlusher(flush_interval=AppConfig.pokemon_flush_interval)
        shiny_rate_buffer_flusher = ShinyRateBufferFlusher(flush_interval=AppConfig.shiny_flush_interval)
        quests_buffer_flusher = QuestsBufferFlusher(flush_interval=AppConfig.quest_flush_interval)
        raids_buffer_flusher = RaidsBufferFlusher(flush_interval=AppConfig.raid_flush_interval)
        invasions_buffer_flusher = InvasionsBufferFlusher(flush_interval=AppConfig.invasion_flush_interval)

        # Initialize partition ensurers leader only

        partition_pokemon_ensurer = DailyPartitionEnsurer(
            ensure_interval=86400, days_back=2, days_forward=30,
            table="pokemon_iv_daily_events", column="day_date",
        )
        partition_quests_items_ensurer = DailyPartitionEnsurer(
            ensure_interval=86400, days_back=2, days_forward=30,
            table="quests_item_daily_events", column="day_date",
        )
        partition_quests_pokemon_ensurer = DailyPartitionEnsurer(
            ensure_interval=86400, days_back=2, days_forward=30,
            table="quests_pokemon_daily_events", column="day_date",
        )
        partition_raids_ensurer = DailyPartitionEnsurer(
            ensure_interval=86400, days_back=2, days_forward=30,
            table="raids_daily_events", column="day_date",
        )
        partition_invasions_ensurer = DailyPartitionEnsurer(
            ensure_interval=86400, days_back=2, days_forward=30,
            table="invasions_daily_events", column="day_date",
        )
        partition_shiny_rates_ensurer = MonthlyPartitionEnsurer(
            ensure_interval=86400, months_back=2, months_forward=12,
            table="shiny_username_rates", column="month_year",
        )
        partition_cleaner = build_default_cleaner()

        # Ensure partitions exist on first run leader only

        for tbl in (
            "pokemon_iv_daily_events",
            "quests_item_daily_events",
            "quests_pokemon_daily_events",
            "raids_daily_events",
            "invasions_daily_events",
        ):
            await ensure_daily_partitions(tbl, "day_date", days_back=2, days_forward=30)

        await ensure_monthly_partitions(
            table="shiny_username_rates", column="month_year",
            months_back=2, months_forward=12,
        )

        # Register all services leader only

        services = [
            # Pokemon IV daily
            Service("partitions:pokemon_iv_daily", AppConfig.store_sql_pokemon_aggregation,
                    partition_pokemon_ensurer.start, partition_pokemon_ensurer.stop),
            Service("flusher:pokemon_iv_daily", AppConfig.store_sql_pokemon_aggregation,
                    pokemon_buffer_flusher.start, pokemon_buffer_flusher.stop),
            # Shiny
            Service("partitions:shiny_rates_month", AppConfig.store_sql_pokemon_shiny,
                    partition_shiny_rates_ensurer.start, partition_shiny_rates_ensurer.stop),
            Service("flusher:shiny_rates", AppConfig.store_sql_pokemon_shiny,
                    shiny_rate_buffer_flusher.start, shiny_rate_buffer_flusher.stop),
            # Quests
            Service("partitions:quests_item_daily", AppConfig.store_sql_quest_aggregation,
                    partition_quests_items_ensurer.start, partition_quests_items_ensurer.stop),
            Service("partitions:quests_poke_daily", AppConfig.store_sql_quest_aggregation,
                    partition_quests_pokemon_ensurer.start, partition_quests_pokemon_ensurer.stop),
            Service("flusher:quests_daily", AppConfig.store_sql_quest_aggregation,
                    quests_buffer_flusher.start, quests_buffer_flusher.stop),
            # Raids
            Service("partitions:raids_daily", AppConfig.store_sql_raid_aggregation,
                    partition_raids_ensurer.start, partition_raids_ensurer.stop),
            Service("flusher:raids_daily", AppConfig.store_sql_raid_aggregation,
                    raids_buffer_flusher.start, raids_buffer_flusher.stop),
            # Invasions
            Service("partitions:invasions_daily", AppConfig.store_sql_invasion_aggregation,
                    partition_invasions_ensurer.start, partition_invasions_ensurer.stop),
            Service("flusher:invasions_daily", AppConfig.store_sql_invasion_aggregation,
                    invasions_buffer_flusher.start, invasions_buffer_flusher.stop),
            # Partition Cleaner
            Service("partitions:cleaner", True, partition_cleaner.start, partition_cleaner.stop),
        ]

        await start_services(services)

    else:

        # FOLLOWER Wait for leader to populate state, then sync

        logger.info(f"[{worker_id}] [FOLLOWER] This worker is a follower - waiting for leader state")

        # Wait for leader to populate global state in Redis
        state_available = await GlobalStateManager.wait_for_state(timeout=30.0)
        if not state_available:
            logger.error("❌ Timeout waiting for leader to populate state. Exiting.")
            raise Exception("❌ Timeout waiting for leader state, stopping application.")


    # Sync global state from Redis to local all workers


    await GlobalStateManager.sync_to_legacy_global_state()
    logger.info(f"[{worker_id}] Global state synced from Redis")

    # YIELD. Application runs

    logger.success(f"[{worker_id}] ✅ Worker ready and accepting requests")
    yield
    logger.info(f"[{worker_id}] Received shutdown signal...")

    # Shutdown

    logger.info(f"[{worker_id}] 👋 Shutting down Webhook Receiver application.")

    if is_leader:
        # Stop services leader only
        await stop_services(services)

        # Cancel background tasks leader only
        for task, name in background_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.info(f"[{worker_id}] 🛑 {name} task cancelled.")

        # Release leadership
        await leader.release()

    # Clean up semaphore to prevent "leaked semaphore" warnings
    cleanup_semaphore()
    # Close Redis pools all workers
    await redis_manager.close_redis()
    # Close DB connection all workers
    await ConnectDB.close_db()

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
