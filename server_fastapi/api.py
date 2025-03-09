from fastapi import FastAPI
from contextlib import asynccontextmanager
from utils.logger import logger
from utils.koji_geofences import KojiGeofences
from server_fastapi.routes import webhook_router

geofences = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global geofences
    geofences = await KojiGeofences(3600).get_cached_geofences()
    if not geofences:
        logger.warning("⚠️ No geofences available at startup.")
    yield
    logger.info("Shutting down FastAPI application.")

app = FastAPI(lifespan=lifespan)

# Include the webhook router
app.include_router(webhook_router)
