import json
import time
import threading
from typing import Optional, Dict, Any, Tuple, Union, Iterable
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, RetryError
from orchestrator.config import KAFKA_BOOTSTRAP, MAX_RETRIES
from orchestrator.logger import get_logger

logger = get_logger("kafka_client")

_CREATE_ATTEMPTS = max(3, int(MAX_RETRIES))
_BACKOFF = 1

def wait_for_kafka(bootstrap_servers: Optional[str] = None, retries: int = 10, delay: int = 5):
    """
    Wait for Kafka to accept connections by creating a temporary producer.

    Args:
        bootstrap_servers: Optional bootstrap address (defaults to KAFKA_BOOTSTRAP).
        retries: Number of attempts.
        delay: Delay (seconds) between attempts.

    Returns:
        True when Kafka is reachable.

    Raises:
        RuntimeError if Kafka is not reachable after the configured number of retries.
    """
    bootstrap = bootstrap_servers or KAFKA_BOOTSTRAP
    attempt = 0
    while attempt < retries:
        try:
            p = KafkaProducer(bootstrap_servers=[bootstrap])
            p.close()
            logger.info("Kafka ready after %d attempt(s)", attempt + 1)
            return True
        except NoBrokersAvailable:
            attempt += 1
            logger.warning("Kafka not ready, retry %d/%d in %ds...", attempt, retries, delay)
            time.sleep(delay)
    raise RuntimeError("Kafka not available after multiple retries")

def _encode_headers(h: Optional[Dict[str, Any]]):
    """Encode header dict to kafka-python header format (list of tuples)."""
    if not h:
        return None
    return [(k, str(v).encode("utf-8")) for k, v in h.items()]

def _kkey(k):
    """Serialize key to bytes for Kafka."""
    return str(k).encode("utf-8") if k is not None else None

def _v(v):
    """Serialize value to JSON bytes for Kafka."""
    return json.dumps(v).encode("utf-8")

@retry(stop=stop_after_attempt(_CREATE_ATTEMPTS),
       wait=wait_exponential(multiplier=_BACKOFF, max=30),
       retry=retry_if_exception_type(NoBrokersAvailable))
def _create_producer(bootstrap):
    """Create and return a KafkaProducer with basic settings and retries via tenacity."""
    logger.info("Trying to connect KafkaProducer -> %s", bootstrap)
    p = KafkaProducer(
        bootstrap_servers=[bootstrap],
        key_serializer=lambda k: _kkey(k),
        value_serializer=lambda v: _v(v),
        acks="all",
        retries=3,
        linger_ms=10,
    )
    time.sleep(0.1)
    logger.info("KafkaProducer ready")
    return p

@retry(stop=stop_after_attempt(_CREATE_ATTEMPTS),
       wait=wait_exponential(multiplier=_BACKOFF, max=30),
       retry=retry_if_exception_type(NoBrokersAvailable))
def _create_consumer(bootstrap, topics: Union[str, Iterable[str]], group_id, auto_offset_reset="earliest"):
    """Create and return a KafkaConsumer subscribed to the given topics.

    The consumer is configured to deserialize JSON payloads.
    """
    logger.info("Trying to connect KafkaConsumer -> %s (topic=%s group=%s)", bootstrap, topics, group_id)
    c = KafkaConsumer(
        *([t for t in topics] if isinstance(topics, (list, tuple, set)) else [topics]),
        bootstrap_servers=[bootstrap],
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v is not None else None,
        key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
        consumer_timeout_ms=1000,
        enable_auto_commit=True,
    )
    logger.info("KafkaConsumer connected for topic=%s", topics)
    return c

_singleton = None
_singleton_lock = threading.Lock()

class KafkaClient:
    """Singleton wrapper around kafka-python producer & consumers.

    Provides convenience send/consumer methods with lazy creation and retry
    behavior. The send() method supports both sync and async modes.
    """
    def __init__(self, bootstrap: Optional[str] = None):
        self.bootstrap = bootstrap or KAFKA_BOOTSTRAP
        self._producer = None
        self._consumers: Dict[Tuple[Tuple[str, ...], str], KafkaConsumer] = {}

    def _ensure_producer(self, raise_on_fail: bool = False):
        """Ensure the internal producer is created.

        Args:
            raise_on_fail: If True, raise the underlying exception when creation fails.
        Returns:
            KafkaProducer instance or None if creation failed and raise_on_fail is False.
        """
        if self._producer is not None:
            return self._producer
        try:
            self._producer = _create_producer(self.bootstrap)
            return self._producer
        except RetryError as re:
            logger.warning("Kafka producer not available yet: %s", re)
            if raise_on_fail:
                raise
            return None
        except Exception as e:
            logger.exception("Unexpected error creating producer: %s", e)
            if raise_on_fail:
                raise
            return None

    def send(self, topic: str, value: Dict[str, Any], key: Optional[Any] = None,
             headers: Optional[Dict[str, Any]] = None, sync: bool = False, timeout: int = 10):
        """Send a message to Kafka.

        Args:
            topic: Kafka topic name.
            value: JSON-serializable payload (dict).
            key: Optional message key (will be stringified).
            headers: Optional headers dict.
            sync: If True, block until the send completes (or raises).
            timeout: Timeout (seconds) for sync waiting.
        Returns:
            None for async sends, or the send result for sync sends.
        """
        prod = self._ensure_producer(raise_on_fail=False)
        if prod is None:
            logger.warning("Producer not ready, cannot send topic=%s key=%s (will return without blocking)", topic, key)
            if sync:
                prod = self._ensure_producer(raise_on_fail=True)
            else:
                return None
        hdrs = _encode_headers(headers)
        try:
            fut = prod.send(topic, key=key, value=value, headers=hdrs)
        except Exception as e:
            logger.exception("Immediate send failed topic=%s key=%s: %s", topic, key, e)
            if sync:
                raise
            return None

        if sync:
            try:
                res = fut.get(timeout=timeout)
                return res
            except Exception as e:
                logger.exception("Sync send failed topic=%s key=%s: %s", topic, key, e)
                raise
        else:
            try:
                fut.add_errback(lambda exc: logger.exception("Async send error for topic=%s key=%s: %s", topic, key, exc))
            except Exception:
                pass
            return None

    def consumer(self, topics: Union[str, Iterable[str]], group_id: str, auto_offset_reset: str = "earliest"):
        """Return a KafkaConsumer for the specified topics and group_id.

        Consumers are cached per (topics, group_id) tuple to avoid recreating multiple times.
        """
        if isinstance(topics, (list, tuple, set)):
            tkey = tuple(str(t) for t in topics)
        else:
            tkey = (str(topics),)
        key = (tkey, group_id)
        if key in self._consumers:
            return self._consumers[key]
        try:
            c = _create_consumer(self.bootstrap, list(tkey), group_id, auto_offset_reset)
            self._consumers[key] = c
            return c
        except RetryError as re:
            logger.warning("Kafka consumer not ready for %s:%s - %s", topics, group_id, re)
            raise
        except Exception as e:
            logger.exception("Consumer creation unexpected error: %s", e)
            raise

    def close(self, timeout=5):
        """Flush and close producer and close cached consumers.

        Args:
            timeout: seconds to wait when flushing/closing the producer.
        """
        logger.info("Closing Kafka client (flush & close)")
        try:
            if self._producer:
                try:
                    self._producer.flush(timeout=timeout)
                except Exception:
                    logger.exception("Producer flush failed")
                try:
                    self._producer.close(timeout=timeout)
                except Exception:
                    logger.exception("Producer close failed")
                self._producer = None
        finally:
            for k, c in list(self._consumers.items()):
                try:
                    c.close()
                except Exception:
                    logger.exception("Consumer close failed for %s", k)
                self._consumers.pop(k, None)
            logger.info("Kafka client closed")

def get_kafka_client():
    """Return a process-wide KafkaClient singleton."""
    global _singleton
    with _singleton_lock:
        if _singleton is None:
            _singleton = KafkaClient()
        return _singleton
