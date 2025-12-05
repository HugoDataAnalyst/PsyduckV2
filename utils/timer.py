import time
import functools
import inspect
from contextlib import ContextDecorator
from utils.logger import logger

class time_execution(ContextDecorator):
    """
    Universal timer. Works as:
    1. Decorator: @time_execution(label="My Endpoint")
    2. Context Manager: with time_execution(label="Specific Block"):

    Automatically detects and handles async/await functions.
    """
    def __init__(self, label=None, log_level="info"):
        self.label = label
        self.log_level = log_level.lower()
        self.start_time = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.perf_counter() - self.start_time
        self._log(elapsed)
        return False  # Propagate exceptions

    def _log(self, elapsed):
        lbl = self.label or "Task"
        msg = f"⏱️  [{lbl}] executed in {elapsed:.4f}s"

        if self.log_level == "debug":
            logger.debug(msg)
        elif self.log_level == "warning":
            logger.warning(msg)
        elif self.log_level == "success":
            logger.success(msg)
        else:
            logger.info(msg)

    def __call__(self, func):
        """Custom decorator logic to handle async functions correctly."""
        # Fallback label if none provided
        if not self.label:
            self.label = func.__name__

        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                with self:
                    return await func(*args, **kwargs)
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                with self:
                    return func(*args, **kwargs)
            return sync_wrapper
