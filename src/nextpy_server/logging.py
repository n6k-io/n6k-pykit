import logging
import os

LOG_FORMAT = "%(levelname)s:     [%(name)s] %(message)s"


def setup_logging(log_level: str = "info") -> None:
    level = getattr(logging, log_level.upper(), logging.INFO)
    if os.environ.get("N6K_DEBUG") == "1":
        level = logging.DEBUG
    formatter = logging.Formatter(LOG_FORMAT)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    for name in ("nextpy_server", "n6k_app", "n6k", "n6k_server"):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if not logger.handlers:
            logger.addHandler(handler)
