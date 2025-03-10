import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from utils.logger import logger
from utils.koji_geofences import KojiGeofences
from server_fastapi.routes import webhook_router
from server_fastapi import global_state



@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Koji Instance
    koji_instance = KojiGeofences(3500)
    global_state.geofences = await koji_instance.get_cached_geofences()
    if not global_state.geofences:
        logger.error("⚠️ No geofences available at startup. Exiting application.")
        raise Exception("❌ No geofences available at startup, stopping application.")

    # Start periodic geofence refresh in the background
    asyncio.create_task(koji_instance.refresh_geofences())

    yield
    logger.info("Shutting down FastAPI application.")

app = FastAPI(lifespan=lifespan)

# Include the webhook router
app.include_router(webhook_router.router)
