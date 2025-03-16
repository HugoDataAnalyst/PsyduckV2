# webhook_routes.py
import asyncio
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from utils.logger import logger
from utils.koji_geofences import KojiGeofences
from webhook.filter_data import WebhookFilter
from webhook.parser_data import (
    process_pokemon_data,
    process_raid_data,
    process_quest_data,
    process_invasion_data
)
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
        logger.info("✅ Processing 👻 Pokémon data.")
        result = await process_pokemon_data(filtered_data)
        if result:
            logger.success(f"✅ 👻 Pokemon Webhook processed successfully:\n{result}")
            return {"status": "success", "processed_data": result}
    elif data_type == "raid":
        logger.info("✅ Processing 👹 Raid data.")
        result = await process_raid_data(filtered_data)
        if result:
            logger.success(f"✅ 👹 Raid Webhook processed successfully:\n{result}")
            return {"status": "success", "processed_data": result}
    elif data_type == "quest":
        logger.info("✅ Processing 🔎 Quest data.")
        result = await process_quest_data(filtered_data)
        if result:
            logger.success(f"✅ 🔎 Quest Webhook processed successfully:\n{result}")
            return {"status": "success", "processed_data": result}
    elif data_type == "invasion":
        logger.info("✅ Processing 🕴️ Invasion data.")
        result = await process_invasion_data(filtered_data)
        if result:
            logger.success(f"✅ 🕴️ Invasion Webhook processed successfully:\n{result}")
            return {"status": "success", "processed_data": result}
    else:
        logger.debug(f"⚠️ Webhook type '{data_type}' not handled by parser yet.")
        return {"status": "ignored", "message": f"Webhook type '{data_type}' not processed."}


@router.post("/webhook")
async def receive_webhook(request: Request):
    """Receives and processes incoming webhooks."""
    #try:
    data = await request.json()  # Parse incoming webhook JSON
    logger.debug(f"📥 Received Webhook: {data}")

    if not isinstance(data, list):
        return await process_single_event(data)  # Handle single webhook

    # Group events by type
    grouped_events = {}
    for event in data:
        event_type = event.get("type")
        if event_type:
            grouped_events.setdefault(event_type, []).append(event)

    results = {}

    # Process each event type **concurrently**, but handle each type sequentially
    async def process_event_group(event_type, events):
        logger.info(f"🔄 Processing {len(events)} {event_type} events...")
        results[event_type] = []
        for event in events:  # Sequential processing per event type
            result = await process_single_event(event)
            results[event_type].append(result)

    # Run different event types **concurrently**
    await asyncio.gather(*[process_event_group(event_type, events) for event_type, events in grouped_events.items()])

    return {"status": "success", "processed_data": results}

    #except Exception as e:
    #    logger.error(f"❌ Error processing webhook: {e}")
    #    return {"status": "error", "message": str(e)}
