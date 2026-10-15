# webhook_routes.py
import asyncio
import time
from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from utils.logger import logger
from utils.runtime_health import ingest, overload_reason, RateLimitedWarning
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

_shed_warning = RateLimitedWarning(every=30)

# Do NOT instantiate the semaphore globally. Set it to None -> Otherwise we have a global state outside of each worker..
_redis_semaphore: asyncio.Semaphore | None = None
_max_concurrent_ops: int | None = None

def _calculate_max_concurrent_ops() -> int:
    """
    Calculate max concurrent Redis operations per worker.
    Uses half of the per-worker Redis pool to leave room for API queries.
    """
    total_max = AppConfig.redis_max_connections
    workers = max(1, AppConfig.uvicorn_workers)
    per_worker_pool = max(10, total_max // workers)
    # Use half of per-worker pool for webhooks, minimum 5
    return max(5, per_worker_pool // 2)


def get_redis_semaphore() -> asyncio.Semaphore:
    """
    Lazy loader ensures the Semaphore is created inside the CURRENT worker's event loop.
    """
    global _redis_semaphore, _max_concurrent_ops
    if _redis_semaphore is None:
        _max_concurrent_ops = _calculate_max_concurrent_ops()
        _redis_semaphore = asyncio.Semaphore(_max_concurrent_ops)
        logger.info(f"🔧 Webhook concurrency limit initialized: {_max_concurrent_ops} for this worker.")
    return _redis_semaphore


def cleanup_semaphore():
    """
    Clean up the semaphore during worker shutdown.
    This helps prevent 'leaked semaphore' warnings from the resource_tracker.
    """
    global _redis_semaphore
    if _redis_semaphore is not None:
        logger.debug("🧹 Cleaning up webhook semaphore")
        _redis_semaphore = None

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

    semaphore = get_redis_semaphore()

    # Use semaphore to limit concurrent Redis operations
    async with semaphore:
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
    """
    Receives and processes incoming webhooks.

    Load shedding: while this worker is overloaded (too many events in flight,
    or over its memory cap) a new batch is refused with 503 instead of being
    queued. Golbat never times out a webhook POST, so queueing grows memory
    without bound - this is what took the host down on Oct 14. Golbat ignores
    the status code, so a shed batch is dropped, never retried.
    """
    reason = overload_reason()
    if reason:
        body = await request.body()
        shed = max(1, body.count(b'"message"'))
        ingest.shed(shed)
        _shed_warning(f"🚦 Webhook overloaded ({reason}) - shedding batches (latest: {shed} events)")
        return JSONResponse({"status": "overloaded", "reason": reason}, status_code=503,
                            headers={"Retry-After": "1"})

    data = await request.json()  # Parse incoming webhook JSON
    # Lazy: formatting the whole batch costs real CPU on every POST, so only do it at DEBUG
    logger.opt(lazy=True).debug("📥 Received Webhook: {}", lambda: data)

    if not isinstance(data, list):
        ingest.begin(1)
        try:
            return await process_single_event(data)  # Handle single webhook
        finally:
            ingest.end(1)

    total_events = len(data)
    ingest.begin(total_events)
    try:
        return await _process_batch(request, data)
    finally:
        ingest.end(total_events)


async def _process_batch(request: Request, data: list):
    # Group events by type
    grouped_events = {}
    for event in data:
        event_type = event.get("type")
        if event_type:
            grouped_events.setdefault(event_type, []).append(event)
    del data

    # Only per-type tallies are kept: holding every event's result until the
    # whole batch finished multiplied memory per in-flight event, and nobody
    # reads the body (Golbat ignores the response).
    summary = {}
    disconnected = False

    # Process each event type **concurrently**, and events within each type also concurrently
    async def process_event_group(event_type, events):
        nonlocal disconnected
        logger.info(f"🔄 Processing {len(events)} {event_type} events...")
        counts = summary[event_type] = {"processed": 0, "ignored": 0, "error": 0}

        start_time = time.perf_counter()

        # Process events concurrently in batches
        # Batch size is 2x semaphore to allow for queuing, but capped at 50
        # This ensures we fully utilize Redis connections without creating too many waiting tasks
        max_ops = _max_concurrent_ops or _calculate_max_concurrent_ops()
        batch_size = min(max_ops * 2, 50)
        for i in range(0, len(events), batch_size):
            # A sender that gave up on this request will not see the result;
            # stop spending Redis capacity on it.
            if AppConfig.webhook_abort_on_disconnect and (disconnected or await request.is_disconnected()):
                disconnected = True
                left = len(events) - i
                ingest.aborted(left)
                counts["aborted"] = left
                break
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
                    counts["error"] += 1
                elif not result or result.get("status") == "ignored":
                    counts["ignored"] += 1
                elif result.get("status") == "error":
                    counts["error"] += 1
                else:
                    counts["processed"] += 1

        elapsed = time.perf_counter() - start_time  # End stopwatch
        if counts["processed"]:
            logger.success(f"⏱️ Done processing {len(events)} {event_type} events in {elapsed:.2f} seconds.")
        else:
            logger.debug(f"⏱️ No valid {event_type} events processed in {elapsed:.2f} seconds.")

    # Run different event types **concurrently**
    await asyncio.gather(*[process_event_group(event_type, events) for event_type, events in grouped_events.items()])

    if disconnected:
        logger.warning(f"✂️ Webhook sender disconnected - aborted the rest of the batch: {summary}")
    return {"status": "success", "processed": summary}
