from loguru import logger
from datetime import datetime
from typing import TypedDict, Optional

class LoggingOptions(TypedDict, total=False):
    file: bool
    function: bool
    process: bool
    thread: bool

def setup_logging(log_lvl: str = "DEBUG", options: Optional[LoggingOptions] = None) -> None:
    if options is None:
        options = {}

    file: bool = options.get("file", False)
    function: bool = options.get("function", False)
    process: bool = options.get("process", False)
    thread: bool = options.get("thread", False)

    log_fmt = (u"<n><d><level>{time:HH:mm:ss.SSS} | " +
               f"{'{file:>15.15}:' if file else ''}" +
               f"{'{function:>15.15}' if function else ''}" +
               f"{':{line:<4} | ' if file or function else ''}" +
               f"{'{process.name:>12.12} | ' if process else ''}" +
               f"{'{thread.name:<11.11} | ' if thread else ''}" +
               u"{level:1.1} | </level></d></n><level>{message}</level>")

    logger.configure(
        handlers=[{
            "sink": lambda x: print(x, end=""),
            "level": log_lvl,
            "format": log_fmt,
            "colorize": True,
            "backtrace": True,
            "diagnose": True
        }],
        levels=[
            {"name": "TRACE", "color": "<white><dim>"},
            {"name": "DEBUG", "color": "<cyan><dim>"},
            {"name": "INFO", "color": "<white>"}
        ]
    )

    # ✅ File Logging (If Enabled)
    if file:
        log_filename = f"logs/psyduck_{datetime.now().strftime('%Y-%m-%d')}.log"
        logger.add(
            log_filename,
            rotation="10MB",  # Create a new log file every 10MB
            retention=5,  # ✅ Keep only the last 5 log files
            level=log_lvl,
            format=log_fmt
        )
# ✅ Make logger globally available
__all__ = ["logger", "setup_logging"]
