# webhook_routes.py
import asyncio
import time
from fastapi import APIRouter, Request, Depends
from fastapi.responses import RedirectResponse
from utils.logger import logger
from utils.koji_geofences import KojiGeofences
from utils.global_state_manager import GlobalStateManager
from webhook.filter_data import WebhookFilter
from webhook.parser_data import (
    process_pokemon_data,
    process_raid_data,
    process_quest_data,
    process_invasion_data
)
from server_fastapi import global_state
from server_fastapi.utils import secure_api
import config as AppConfig

router = APIRouter()

# Semaphore to limit concurrent Redis connections
# Use half of max connections to leave room for API queries
MAX_CONCURRENT_REDIS_OPS = max(5, AppConfig.redis_max_connections // 2)
redis_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REDIS_OPS)
logger.info(f"🔧 Webhook concurrency limit: {MAX_CONCURRENT_REDIS_OPS} (Redis max: {AppConfig.redis_max_connections})")

async def process_single_event(event: dict):
    """Processes a single webhook event."""
    data_type = event.get("type")
    if not data_type:
        logger.warning("❌ Invalid webhook format: Missing 'type'.")
        return {"status": "error", "message": "Invalid webhook format"}

    # Get geofences from GlobalStateManager
    # This ensures all workers have up-to-date geofences even after leader refreshes
    geofences = await GlobalStateManager.get_geofences()
    if geofences is None:
        # Fallback to legacy global_state if GlobalStateManager not initialized
        geofences = global_state.geofences

    webhook_filter = WebhookFilter(allowed_types={data_type}, geofences=geofences)
    filtered_data = await webhook_filter.filter_webhook_data(event)

    if not filtered_data:
        logger.debug("⚠️ Webhook ignored (filtered out).")
        return {"status": "ignored"}

    # Use semaphore to limit concurrent Redis operations
    async with redis_semaphore:
        if data_type == "pokemon":
            logger.debug("✅ Processing 👻 Pokémon data.")
            result = await process_pokemon_data(filtered_data)
            if result:
                logger.debug(f"✅ 👻 Pokemon Webhook processed successfully:\n{result}")
                return {"status": "success", "processed_data": result}
        elif data_type == "raid":
            logger.debug("✅ Processing 👹 Raid data.")
            result = await process_raid_data(filtered_data)
            if result:
                logger.debug(f"✅ 👹 Raid Webhook processed successfully:\n{result}")
                return {"status": "success", "processed_data": result}
        elif data_type == "quest":
            logger.debug("✅ Processing 🔎 Quest data.")
            result = await process_quest_data(filtered_data)
            if result:
                logger.debug(f"✅ 🔎 Quest Webhook processed successfully:\n{result}")
                return {"status": "success", "processed_data": result}
        elif data_type == "invasion":
            logger.debug("✅ Processing 🕴️ Invasion data.")
            result = await process_invasion_data(filtered_data)
            if result:
                logger.debug(f"✅ 🕴️ Invasion Webhook processed successfully:\n{result}")
                return {"status": "success", "processed_data": result}
        else:
            logger.debug(f"⚠️ Webhook type '{data_type}' not handled by parser yet.")
            return {"status": "ignored", "message": f"Webhook type '{data_type}' not processed."}


@router.post("/webhook", dependencies=[Depends(secure_api.validate_remote_addr)], include_in_schema=False)
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

    # Process each event type **concurrently**, and events within each type also concurrently
    async def process_event_group(event_type, events):
        logger.info(f"🔄 Processing {len(events)} {event_type} events...")
        results[event_type] = []

        start_time = time.perf_counter()

        # Process events concurrently in batches
        # Batch size is 2x semaphore to allow for queuing, but capped at 50
        # This ensures we fully utilize Redis connections without creating too many waiting tasks
        batch_size = min(MAX_CONCURRENT_REDIS_OPS * 2, 50)
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            # Process batch concurrently
            batch_results = await asyncio.gather(
                *[process_single_event(event) for event in batch],
                return_exceptions=True
            )

            # Collect results
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"❌ Error processing event: {result}")
                    results[event_type].append({"status": "error", "message": str(result)})
                elif result:
                    results[event_type].append(result)

        valid_count = sum(1 for r in results[event_type] if r.get("status") not in ["ignored", "error"])
        elapsed = time.perf_counter() - start_time  # End stopwatch
        if valid_count:
            logger.success(f"⏱️ Done processing {len(events)} {event_type} events in {elapsed:.2f} seconds.")
        else:
            logger.debug(f"⏱️ No valid {event_type} events processed in {elapsed:.2f} seconds.")

    # Run different event types **concurrently**
    await asyncio.gather(*[process_event_group(event_type, events) for event_type, events in grouped_events.items()])

    return {"status": "success", "processed_data": results}

    #except Exception as e:
    #    logger.error(f"❌ Error processing webhook: {e}")
    #    return {"status": "error", "message": str(e)}
