from typing import Optional
from utils.logger import logger

def get_iv_bucket(iv: int) -> Optional[int]:
    """
    Convert a raw IV (0-100) into a bucket.

    Buckets:
    - Exactly 0:         0
    - >0 and <=25:      25
    - >25 and <=50:     50
    - >50 and <=75:     75
    - >75 and <=90:     90
    - >90 and <100:     95
    - Exactly 100:      100

    If the IV is out of the 0–100 range, returns None.
    """
    if iv < 0 or iv > 100:
        logger.warning(f"IV value {iv} out of range (0-100); returning None.")
        return None

    bucket = None
    if iv == 0:
        bucket = 0
    elif iv == 100:
        bucket = 100
    elif iv <= 25:
        bucket = 25
    elif iv <= 50:
        bucket = 50
    elif iv <= 75:
        bucket = 75
    elif iv <= 90:
        bucket = 90
    elif iv < 100:
        bucket = 95

    logger.debug(f"Raw IV {iv} mapped to bucket {bucket}.")
    return bucket
