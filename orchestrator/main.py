import threading
import time
import signal
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

from prometheus_client import generate_latest

from orchestrator.logger import configure_logging, get_logger
from orchestrator.scheduler import Orchestrator
from orchestrator.kafka_client import get_kafka_client
import redis
from orchestrator.config import REDIS_HOST, REDIS_PORT

configure_logging("orchestrator")
log = get_logger("main")

# Global process control
orch: Optional[Orchestrator] = None
httpd: Optional[HTTPServer] = None
stop_event = threading.Event()
PORT = 8000


class HealthHandler(BaseHTTPRequestHandler):
    """
    Health and metrics HTTP handler.

    Exposes:
      - /health/liveness : lightweight liveness probe (process alive)
      - /health/readiness : readiness probe that checks Kafka connection and Redis ping quickly
      - /metrics : Prometheus metrics (uses prometheus_client.generate_latest())

    Implementation notes:
      - Readiness checks must be fast and non-blocking; timeouts are short.
      - This handler intentionally keeps logic minimal to remain responsive for probes.
    """

    def _respond(self, code: int = 200, body: bytes = b'{"status":"ok"}', content_type: str = "application/json"):
        """
        Send a simple HTTP response and close the connection.

        This helper avoids leaving connections open (explicit Connection: close).
        """
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            # Best-effort: ignore write failures for health endpoints
            pass

    def do_GET(self):
        """
        Handle GET requests for liveness, readiness and metrics.

        Liveness must be trivial. Readiness does quick checks:
         - Kafka: check presence of a warm producer (non-blocking)
         - Redis: ping with short socket timeout

        Metrics uses generate_latest() from prometheus_client.
        """
        path = self.path.split("?", 1)[0]

        if path == "/health/liveness":
            # Liveness: process-level health only; must not check external deps
            self._respond(200, b'{"status":"ok"}')
            return

        if path == "/health/readiness":
            ready = True

            # Kafka readiness: check whether producer exists (set by kafka client)
            try:
                client = get_kafka_client()
                kafka_ready = getattr(client, "_producer", None) is not None
                if not kafka_ready:
                    ready = False
            except Exception:
                ready = False

            # Redis readiness: quick ping with short socket timeouts
            try:
                r = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    socket_connect_timeout=0.5,
                    socket_timeout=0.5,
                    decode_responses=True,
                )
                r.ping()
            except Exception:
                ready = False

            if ready:
                self._respond(200, b'{"status":"ready"}')
            else:
                self._respond(503, b'{"status":"not ready"}')
            return

        if path == "/metrics":
            try:
                data = generate_latest()
                self._respond(200, data, content_type="text/plain; version=0.0.4")
            except Exception as e:
                log.exception("Failed to generate metrics: %s", e)
                self._respond(500, b'{"status":"error"}')
            return

        # Unknown path
        self._respond(404, b'{"status":"not found"}')


def _start_http_server(port: int):
    """
    Start the HTTP server (blocking call) that serves HealthHandler.

    This function is intended to be launched in a daemon thread.
    """
    global httpd
    httpd = HTTPServer(("0.0.0.0", port), HealthHandler)
    log.info("HTTP server listening on :%d", port)
    try:
        httpd.serve_forever()
    except Exception as e:
        log.warning("HTTP server stopped: %s", e)
    finally:
        try:
            httpd.server_close()
        except Exception:
            pass
        log.info("HTTP server closed")


def _start_orchestrator():
    """
    Create and run the Orchestrator instance.

    This function is launched in a non-daemon thread so that the orchestrator
    lifecycle is controlled by the main thread signals and stop_event.
    """
    global orch
    try:
        orch = Orchestrator()
        orch.run()
    except Exception:
        log.exception("Orchestrator stopped with error")


def _shutdown(signum, frame):
    """
    Signal handler to request graceful shutdown.

    Steps (best-effort):
      - signal orchestrator to stop
      - close Kafka client
      - shutdown HTTP server
      - set the global stop_event so main loop can exit
    """
    log.info("Signal %s received — initiating shutdown", signum)
    stop_event.set()
    try:
        if orch:
            orch.stop()
    except Exception:
        log.exception("Error while stopping orchestrator")
    try:
        get_kafka_client().close()
    except Exception:
        log.exception("Error while closing Kafka client")
    try:
        if httpd:
            httpd.shutdown()
    except Exception:
        log.exception("Error while shutting down HTTP server")


def main():
    """
    Entrypoint for the orchestrator process.

    Behavior:
      - Install signal handlers (must be in main thread)
      - Start HTTP server thread for health & metrics
      - Start orchestrator thread
      - Wait for stop_event (set by signals) and join threads on exit
    """
    # Install signal handlers in main thread
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Start HTTP server in a daemon thread so it does not block process shutdown
    http_thread = threading.Thread(target=_start_http_server, args=(PORT,), daemon=True)
    http_thread.start()

    # Start orchestrator in a non-daemon thread so shutdown can join it
    orch_thread = threading.Thread(target=_start_orchestrator)
    orch_thread.start()

    log.info("Orchestrator main running. Ctrl+C to stop.")
    try:
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received in main loop")
        stop_event.set()
    finally:
        # Wait a short while for components to stop gracefully
        orch_thread.join(timeout=10)
        # http thread is daemon; give it a moment to close cleanly
        http_thread.join(timeout=2)
        log.info("Shutdown complete")


if __name__ == "__main__":
    main()
