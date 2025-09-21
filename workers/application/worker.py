from orchestrator.logger import configure_logging, get_logger
configure_logging("application")
log = get_logger("application")
from orchestrator.config import WORKER_ZONE, WORKER_PROCESS_SECONDS
ZONE = WORKER_ZONE.get("application")
PROC = WORKER_PROCESS_SECONDS.get(ZONE, 5)
from workers.worker_base import start_worker_loop

def process_item(obj_id):
    """
    Simulate application processing.
    """
    import time
    time.sleep(PROC)
    if obj_id.endswith("5"):
        return False, "simulated application error"
    return True, f"applied {obj_id}"

if __name__ == "__main__":
    log.info("Starting application worker zone=%s", ZONE)
    start_worker_loop(ZONE, process_item)
