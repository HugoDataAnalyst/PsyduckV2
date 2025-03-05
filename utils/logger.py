from loguru import logger
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

