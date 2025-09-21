import os
from typing import Any, Callable, Optional

"""
Centralized configuration for the Dats-challenge.
Reads env vars (Docker Compose) or uses sensible defaults.
"""

def _getenv(key: str, default: Any = None, cast: Optional[Callable[[str], Any]] = None) -> Any:
    """
    Read an environment variable with an optional default and caster.

    - If the env var is not set or is an empty string, returns `default`.
    - If `cast` is provided (e.g. int, float), tries to cast and returns the cast value.
      On cast error it returns `default` (keeps behaviour simple for demo/PoC).
    """
    v = os.getenv(key, None)

    # Treat empty string as not set (common when .env has placeholders)
    if v == "":
        v = None

    if v is None:
        return default

    if cast:
        try:
            return cast(v)
        except Exception:
            return default

    return v

# Kafka
KAFKA_BOOTSTRAP = _getenv("KAFKA_BOOTSTRAP", "kafka:9092")

# Redis
REDIS_HOST = _getenv("REDIS_HOST", "redis")
REDIS_PORT = _getenv("REDIS_PORT", 6379, cast=int)
RETRY_TTL_SECONDS = _getenv("RETRY_TTL_SECONDS", 3600, cast=int)

# Topics
TOPIC_READY = _getenv("TOPIC_READY", "event.load.ready")
TOPIC_LOADED = _getenv("TOPIC_LOADED", "event.load.loaded")
TOPIC_FAILED = _getenv("TOPIC_FAILED", "event.load.failed")
TOPIC_DLQ = _getenv("TOPIC_DLQ", "event.load.dlq")

# Orchestrator
#OBJECTS_PATH = _getenv("OBJECTS_PATH", "/app/tools/objects.json")
OBJECTS_PATH = _getenv("OBJECTS_PATH", "/app/tools/objects_complete.json")
REDIS_PREFIX = _getenv("REDIS_PREFIX", "orch")

# Runtime tuning
MAX_WORKERS = _getenv("MAX_WORKERS", 4, cast=int)
MAX_RETRIES = _getenv("MAX_RETRIES", 3, cast=int)
LOG_FILE = _getenv("LOG_FILE", "app.log")

# Worker-specific defaults: use *zone* keys so scheduler and workers speak the same language
WORKER_PROCESS_SECONDS = {
    "1-Raw": _getenv("WORKER_1_PROC_SEC", 2, cast=int),
    "2-Standardized": _getenv("WORKER_2_PROC_SEC", 5, cast=int),
    "3-Application": _getenv("WORKER_3_PROC_SEC", 7, cast=int),
}

# mapping worker type -> zone (kept for convenience if needed elsewhere)
WORKER_ZONE = {
    "ingestion": _getenv("WORKER_INGESTION_ZONE", "1-Raw"),
    "standardization": _getenv("WORKER_STANDARDIZATION_ZONE", "2-Standardized"),
    "application": _getenv("WORKER_APPLICATION_ZONE", "3-Application"),
}

# mapping zone -> suffix list (strings)
WORKER_PRECHECK_SUFFIXES = {
    "1-Raw": ['9'],
    "3-Application": ["5"],        # les objets finissant par 5 échouent (exemple)
    "2-Standardized": ["7"],      # les objets finissant par 7 échouent (exemple)
}
