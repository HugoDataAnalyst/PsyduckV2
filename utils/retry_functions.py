import asyncio
from utils.logger import logger

async def retry(coro_func, *args, max_attempts=5, delay=2, **kwargs):
    """
    Retry an asynchronous function up to `max_attempts` times with a fixed delay between attempts.

    :param coro_func: The coroutine function to execute.
    :param args: Positional arguments for the coroutine function.
    :param max_attempts: Maximum number of retry attempts.
    :param delay: Delay in seconds between attempts.
    :param kwargs: Keyword arguments for the coroutine function.
    :return: The result of the coroutine if successful.
    :raises Exception: The exception from the last attempt if all retries fail.
    """
    attempt = 1
    while attempt <= max_attempts:
        try:
            result = await coro_func(*args, **kwargs)
            return result
        except Exception as e:
            if attempt == max_attempts:
                logger.error(
                    f"⚠️ Operation {coro_func.__name__} ❌ failed after {max_attempts} attempts: {e}",
                    exc_info=True,
                )
                raise
            else:
                logger.warning(
                    f"⚠️ Operation {coro_func.__name__} ❌ failed on attempt {attempt}/{max_attempts} with error: {e}. Retrying in {delay} seconds."
                )
                await asyncio.sleep(delay)
                attempt += 1
