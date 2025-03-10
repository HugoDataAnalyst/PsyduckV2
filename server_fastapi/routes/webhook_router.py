# webhook_routes.py
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from utils.logger import logger
from utils.koji_geofences import KojiGeofences
from webhook.filter_data import WebhookFilter
from webhook.parser_data import process_pokemon_data
from server_fastapi import global_state

router = APIRouter()

async def process_single_event(event: dict):
    """Processes a single webhook event."""
    data_type = event.get("type")
    if not data_type:
        logger.warning("❌ Invalid webhook format: Missing 'type'.")
        return {"status": "error", "message": "Invalid webhook format"}

    # Initialize WebhookFilter with the global geofences
    webhook_filter = WebhookFilter(allowed_types={data_type}, geofences=global_state.geofences)
    filtered_data = await webhook_filter.filter_webhook_data(event)

    if not filtered_data:
        logger.debug("⚠️ Webhook ignored (filtered out).")
        return {"status": "ignored"}

    if data_type == "pokemon":
        logger.info("✅ Processing Pokémon data.")
        result = await process_pokemon_data(filtered_data)
        if result:
            logger.success(f"✅ Webhook processed successfully: {result}")
            return {"status": "success", "processed_data": result}
    else:
        logger.warning(f"⚠️ Webhook type '{data_type}' not handled by parser yet.")
        return {"status": "ignored", "message": f"Webhook type '{data_type}' not processed."}


@router.post("/webhook")
async def receive_webhook(request: Request):
    """Receives and processes incoming webhooks."""
    #try:
    data = await request.json()  # Parse incoming webhook JSON
    logger.debug(f"📥 Received Webhook: {data}")

    # If the received data is a list, process each event individually.
    if isinstance(data, list):
        results = []
        for event in data:
            result = await process_single_event(event)
            results.append(result)
        return {"status": "success", "processed_data": results}
    else:
        return await process_single_event(data)
    #except Exception as e:
    #    logger.error(f"❌ Error processing webhook: {e}")
    #    return {"status": "error", "message": str(e)}
