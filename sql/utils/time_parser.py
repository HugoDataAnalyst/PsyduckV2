from utils.logger import logger

def parse_time_input(value: str) -> int:
    """
    Accepts '2502', '202502', or '2025-02' and converts to 2502
    """
    value = value.replace("-", "").strip()
    if len(value) == 6:
        # E.g. 202502 → 2502
        return int(value[2:])
    elif len(value) == 4:
        return int(value)
    logger.error(f"❌ Invalid time format: {value}")
    raise ValueError("Invalid time format. Must be YYMM or YYYYMM.")
