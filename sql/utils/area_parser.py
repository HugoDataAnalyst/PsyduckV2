from sql.connect_db import fetch_val
from utils.logger import logger

async def resolve_area_id_by_name(area_name: str) -> int:
    """
    Look up area_id by its human-readable name (case-insensitive, unique).
    Raises ValueError if not found.
    """
    name = (area_name or "").strip()
    if not name or name.lower() in {"all", "global"} or "," in name:
        raise ValueError("Provide exactly one area name (no lists, no 'all/global').")
    area_id = await fetch_val("SELECT id FROM area_names WHERE name = %s", (name,))
    if area_id is None:
        raise ValueError(f"Unknown area name: {name}")
    return int(area_id)
