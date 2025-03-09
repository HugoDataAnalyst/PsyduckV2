# webhook_routes.py
from fastapi import APIRouter, Request
from utils.logger import logger
from utils.koji_geofences import KojiGeofences
from webhook.filter_data import WebhookFilter
from webhook.parser_data import process_pokemon_data
from server_fastapi.api import geofences

router = APIRouter()

@router.post("/webhook")
async def receive_webhook(request: Request):
    """Receives and processes incoming webhooks."""
    try:
        data = await request.json()  # Parse incoming webhook JSON
        logger.debug(f"📥 Received Webhook: {data}")

        data_type = data.get("type")
        if not data_type:
            logger.warning("❌ Invalid webhook format: Missing 'type'.")
            return {"status": "error", "message": "Invalid webhook format"}

        # Initialize WebhookFilter per type;
        # Use the imported global geofences in the filter
        webhook_filter = WebhookFilter(allowed_types={data_type}, geofences=geofences)

        # Process webhook data based on type
        filtered_data = await webhook_filter.filter_webhook_data(data)
        if filtered_data:
            if data_type == "pokemon":
                logger.info("✅ Processing Pokémon data.")
                result = await process_pokemon_data(filtered_data)
                if result:
                    logger.success(f"✅ Webhook processed successfully: {result}")
                    return {"status": "success", "processed_data": result}
            else:
                logger.warning(f"⚠️ Webhook type '{data_type}' not handled by parser yet.")
                return {"status": "ignored", "message": f"Webhook type '{data_type}' not processed."}
        else:
            logger.warning("⚠️ Webhook ignored (filtered out).")
            return {"status": "ignored"}

    except Exception as e:
        logger.error(f"❌ Error processing webhook: {e}")
        return {"status": "error", "message": str(e)}
