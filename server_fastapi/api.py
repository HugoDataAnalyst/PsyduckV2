from fastapi import FastAPI, Request
import asyncio
from utils.logger import logger
from utils.koji_geofences import KojiGeofences
from webhook.filter_data import WebhookFilter

app = FastAPI()

# ✅ Initialize Geofence Data on Startup
geofences = None

@app.on_event("startup")
async def startup_event():
    """Fetch and cache geofences when FastAPI starts."""
    global geofences
    geofences = await KojiGeofences(3600).get_cached_geofences()
    if not geofences:
        logger.warning("⚠️ No geofences available at startup.")

# ✅ Webhook Endpoint
@app.post("/webhook")
async def receive_webhook(request: Request):
    """Receives and processes incoming webhooks."""
    try:
        data = await request.json()  # ✅ Parse incoming webhook JSON
        logger.debug(f"📥 Received Webhook: {data}")

        data_type = data.get("type")
        if not data_type:
            logger.warning("❌ Invalid webhook format: Missing 'type'.")
            return {"status": "error", "message": "Invalid webhook format"}

        # ✅ Initialize WebhookFilter per-type
        webhook_filter = WebhookFilter(allowed_types={data_type}, geofences=geofences)

        # ✅ Process Webhook Data for Specific Type
        filtered_data = await webhook_filter.filter_webhook_data(data)
        if filtered_data:
            logger.success(f"✅ Webhook processed successfully: {filtered_data}")
            return {"status": "success", "processed_data": filtered_data}
        else:
            logger.warning("⚠️ Webhook ignored (filtered out).")
            return {"status": "ignored"}

    except Exception as e:
        logger.error(f"❌ Error processing webhook: {e}")
        return {"status": "error", "message": str(e)}
