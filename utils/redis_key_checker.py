from utils.logger import logger

async def ensure_timeseries_key(client, key, metric, area, identifier, form, retention_ms, pipe=None):
    """
    Ensure that the time series for the given key exists.
    If not, create it with TS.CREATE including retention, duplicate policy, and labels.
    Supports Redis pipelines for batch processing.
    """
    try:
        key_exists = await client.exists(key)

        if not key_exists:
            logger.debug(f"🔑 Time series key {key} does not exist. Creating it with TS.CREATE.")

            if metric == "tth":
                # For TTH, 'identifier' is actually the bucket string.
                command = [
                    "TS.CREATE",
                    key,
                    "RETENTION", retention_ms,
                    "DUPLICATE_POLICY", "SUM",
                    "LABELS",
                    "metric", metric,
                    "area", str(area),
                    "bucket", str(identifier)
                ]
                # Optionally, if you want to add form only when non-empty:
                if form:
                    command.extend(["form", str(form)])
            elif metric.startswith("raid"):
                # For raid timeseries, 'identifier' is the combined raid attributes string,
                # e.g., "10:5:0:0:1:1" representing raid_pokemon, raid_level, raid_form, raid_costume,
                # raid_is_exclusive, and raid_ex_raid_eligible.
                command = [
                    "TS.CREATE",
                    key,
                    "RETENTION", retention_ms,
                    "DUPLICATE_POLICY", "SUM",
                    "LABELS",
                    "metric", metric,
                    "area", str(area),
                    "raid", str(identifier),
                    "form", str(form)
                ]
            elif metric.startswith("quest"):
                # For quest timeseries, use 'identifier' to store combined quest reward details.
                command = [
                    "TS.CREATE",
                    key,
                    "RETENTION", retention_ms,
                    "DUPLICATE_POLICY", "SUM",
                    "LABELS",
                    "metric", metric,
                    "area", str(area),
                    "quest", str(identifier),
                ]
                if form not in (None, ""):
                    command.extend(["mode", str(form)])
            elif metric.startswith("invasion"):
                # For invasion timeseries, 'identifier' can hold combined invasion attributes.
                command = [
                    "TS.CREATE",
                    key,
                    "RETENTION", retention_ms,
                    "DUPLICATE_POLICY", "SUM",
                    "LABELS",
                    "metric", metric,
                    "area", str(area),
                    "invasion", str(identifier),
                ]
                if form not in (None, ""):
                    command.extend(["grunt", str(form)])
            else:
                command = [
                    "TS.CREATE",
                    key,
                    "RETENTION", retention_ms,
                    "DUPLICATE_POLICY", "SUM",
                    "LABELS",
                    "metric", metric,
                    "area", str(area),
                    "pokemon_id", str(identifier),
                    "form", str(form)
                ]

            if pipe:
                pipe.execute_command(*command)  # Add to Redis pipeline
            else:
                await client.execute_command(*command)  # Execute immediately if no pipeline

            logger.debug(f"✅ Time series key {key} created.")
        else:
            logger.debug(f"🚨 Time series key {key} already exists.")

    except Exception as e:
        logger.error(f"❌ Error ensuring timeseries key {key}: {e}")
