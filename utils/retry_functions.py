import asyncio
from utils.logger import logger

async def retry(coro_func, *args, max_attempts=5, delay=0.1, exponential_backoff=True, **kwargs):
    """
    Retry an asynchronous function up to `max_attempts` times with exponential backoff.

    :param coro_func: The coroutine function to execute.
    :param args: Positional arguments for the coroutine function.
    :param max_attempts: Maximum number of retry attempts.
    :param delay: Initial delay in seconds between attempts (default 0.1s).
    :param exponential_backoff: If True, use exponential backoff (delay doubles each attempt).
    :param kwargs: Keyword arguments for the coroutine function.
    :return: The result of the coroutine if successful.
    :raises Exception: The exception from the last attempt if all retries fail.
    """
    attempt = 1
    current_delay = delay

    while attempt <= max_attempts:
        try:
            result = await coro_func(*args, **kwargs)
            if attempt > 1:
                logger.debug(f"✅ Operation '{coro_func.__name__}' succeeded on attempt {attempt}")
            return result
        except Exception as e:
            if attempt == max_attempts:
                logger.error(
                    f"⚠️ Operation {coro_func.__name__} ❌ failed after {max_attempts} attempts: {e}",
                    exc_info=True,
                )
                raise
            else:
                # Check if it's a BUSY error from Redis - these benefit from shorter delays
                is_busy_error = "BUSY" in str(e) or "busy" in str(e).lower()
                retry_delay = current_delay if not is_busy_error else min(current_delay, 0.01)

                logger.warning(
                    f"⚠️ Operation {coro_func.__name__} ❌ failed on attempt {attempt}/{max_attempts} with error: {e}. Retrying in {retry_delay:.3f} seconds."
                )
                await asyncio.sleep(retry_delay)

                # Exponential backoff: double the delay for next attempt (max 5 seconds)
                if exponential_backoff:
                    current_delay = min(current_delay * 2, 5.0)

                attempt += 1
