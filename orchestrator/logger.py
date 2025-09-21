import os
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional

LOG_FILE = os.getenv("LOG_FILE", "app.log")

def configure_logging(name: str = "microservice", level: Optional[str] = None):
    """
    Configure root logging with a console handler and rotating file handler.
    Simple and reproducible for both local dev and containers.
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO").upper()
    level_val = getattr(logging, level, logging.INFO)

    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(base, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, LOG_FILE)

    root = logging.getLogger()
    root.setLevel(level_val)
    if root.handlers:
        root.handlers.clear()

    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    formatter = logging.Formatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(level_val)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    fh = RotatingFileHandler(log_path, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    root.addHandler(fh)

def get_logger(name: str):
    return logging.getLogger(name)
