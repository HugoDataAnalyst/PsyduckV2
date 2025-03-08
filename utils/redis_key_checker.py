from utils.logger import logger

async def ensure_timeseries_key(client, key, metric, area, pokemon_id, form, retention_ms):
    """
    Ensure that the time series for the given key exists.
    If not, create it with TS.CREATE including retention, duplicate policy, and labels.
    """
    exists = await client.exists(key)
    if not exists:
        logger.debug(f"🔑 Time series key {key} does not exist. Creating it with TS.CREATE.")
        # TS.CREATE <key> RETENTION <retention_ms> DUPLICATE_POLICY SUM LABELS metric <metric> area <area> pokemon_id <pokemon_id> form <form>
        result = await client.execute_command(
            "TS.CREATE",
            key,
            "RETENTION", retention_ms,
            "DUPLICATE_POLICY", "SUM",
            "LABELS",
            "metric", metric,
            "area", str(area),
            "pokemon_id", int(pokemon_id),
            "form", str(form)
        )
        if result:
            logger.debug(f"✅ Time series key {key} created.")
    else:
        logger.debug(f"❌ Time series key {key} already exists.")
