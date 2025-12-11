# utils/logger.py
from __future__ import annotations
import sys, os, logging, asyncio
from pathlib import Path
from typing import TypedDict, Optional
from loguru import logger


def get_worker_id() -> str:
    """
    Get a unique worker identifier based on PID.
    Useful for multi-worker uvicorn deployments.
    """
    return f"W-{os.getpid()}"


class LoggingOptions(TypedDict, total=False):
    # formatting toggles
    show_file: bool
    show_function: bool
    show_process: bool
    show_thread: bool
    # file sink options
    to_file: bool
    file_path: str
    rotation: str
    keep_total: int
    compression: str

class WaitressQueueFilter(logging.Filter):
    """
    Filters out the spammy 'Task queue depth' warning from Waitress.
    """
    def filter(self, record: logging.LogRecord) -> bool:
        return "Task queue depth" not in record.getMessage()


def setup_logging(log_lvl: str = "DEBUG", options: Optional[LoggingOptions] = None) -> None:
    """
    - Console sink with colors.
    - Optional file sink with rotation, retention (count-based) and compression.
    - Bridges stdlib logging (uvicorn/sqlalchemy/waitress/etc.) to Loguru.
    - Captures unhandled exceptions (sync + asyncio).
    """
    if options is None:
        options = {}

    show_file     = bool(options.get("show_file", False))
    show_function = bool(options.get("show_function", False))
    show_process  = bool(options.get("show_process", False))
    show_thread   = bool(options.get("show_thread", False))
    to_file       = bool(options.get("to_file", False))
    file_path     = options.get("file_path", "logs/psyduckv2.log")
    rotation      = options.get("rotation", "5 MB")
    keep_total    = int(options.get("keep_total", 5))
    compression   = options.get("compression", "gz")

    # FIX: Use the patcher to inject the worker ID dynamically.
    # We now correctly use the get_worker_id() function we defined above.
    def worker_id_patcher(record):
        record["extra"]["worker_id"] = get_worker_id()

    # Configure the core logger with the patcher
    logger.configure(patcher=worker_id_patcher)

    log_fmt = (
        "<n><d><level>{time:YYYY-MM-DD HH:mm:ss.SSS} | "
        f"{'{file:>15.15}:' if show_file else ''}"
        f"{'{function:>15.15}' if show_function else ''}"
        f"{':{line:<4} | ' if (show_file or show_function) else ''}"
        f"{'{extra[worker_id]:>8.8} | ' if show_process else ''}"
        f"{'{thread.name:<11.11} | ' if show_thread else ''}"
        "{level:1.1} | </level></d></n><level>{message}</level>"
    )

    # Remove defaults to avoid duplicates if setup called twice
    logger.remove()

    # Console
    logger.add(
        sys.stdout,
        level=log_lvl,
        format=log_fmt,
        colorize=True,
        backtrace=True,
        diagnose=True,
        enqueue=True,
    )

    # File - optional
    if to_file:
        # Ensure directory exists
        Path(os.path.dirname(file_path) or ".").mkdir(parents=True, exist_ok=True)

        # We want N total files: 1 current + (N-1) rotated
        rotated_keep = max(0, keep_total - 1)

        # Loguru handles:
        # - rotation: by size/time
        # - retention: count-based (applies to rotated set)
        # - compression: rotated files compressed
        #
        # Rotated file naming will be:
        #   psyduckv2.log.<YYYY-MM-DD_HH-MM-SS>_<pid>.gz
        # (current file stays psyduckv2.log)
        logger.add(
            file_path,
            level=log_lvl,
            format=log_fmt,
            colorize=False,
            backtrace=True,
            diagnose=True,
            rotation=rotation,
            retention=rotated_keep,
            compression=compression,
            enqueue=True,
        )

    # bridge stdlib logging to Loguru
    class InterceptHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            msg = record.getMessage()
            if "Task queue depth" in msg:
                return
            try:
                level = logger.level(record.levelname).name
            except Exception:
                level = record.levelno
            logger.opt(depth=6, exception=record.exc_info).log(level, record.getMessage())

    logging.root.handlers = [InterceptHandler()]
    logging.root.setLevel(logging.getLevelName(log_lvl))

    for name in (
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "fastapi",
        "sqlalchemy",
        "alembic",
        "aiomysql",
        "asyncio",
        "waitress",
    ):
        lg = logging.getLogger(name)
        lg.handlers = [InterceptHandler()]
        lg.propagate = False
        lg.setLevel(logging.getLevelName(log_lvl))

    logging.getLogger("waitress").addFilter(WaitressQueueFilter())

    # Quiet extremely chatty engine SQL logs:
    logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)

    # capture unhandled exceptions
    def _excepthook(exc_type, exc, tb):
        if issubclass(exc_type, KeyboardInterrupt):
            return
        logger.opt(exception=(exc_type, exc, tb)).error("Unhandled exception")

    sys.excepthook = _excepthook

    # asyncio task exceptions
    try:
        loop = asyncio.get_event_loop()
        def _asyncio_excepthook(loop, context):
            err = context.get("exception")
            msg = context.get("message", "")
            if err:
                logger.opt(exception=err).error(f"Unhandled asyncio exception: {msg}")
            else:
                logger.error(f"Unhandled asyncio error: {msg or context}")
        loop.set_exception_handler(_asyncio_excepthook)
    except RuntimeError:
        pass

    logger.debug(
        "Loguru configured (lvl=%s, to_file=%s, file=%s, rotation=%s, keep_total=%d, compression=%s)",
        log_lvl, to_file, file_path, rotation, keep_total, compression
    )


__all__ = ["logger", "setup_logging", "get_worker_id"]

