from orchestrator.logger import configure_logging, get_logger
configure_logging("ingestion")
log = get_logger("ingestion")
from orchestrator.config import WORKER_ZONE, WORKER_PROCESS_SECONDS
ZONE = WORKER_ZONE.get("ingestion")
PROC = WORKER_PROCESS_SECONDS.get(ZONE, 20)
from workers.worker_base import start_worker_loop

def process_item(obj_id):
    """
    Simulate ingestion processing.
    """
    import time
    time.sleep(PROC)
    if obj_id.endswith("9"):
        return False, "simulated ingestion error"
    return True, f"ingested {obj_id}"

if __name__ == "__main__":
    log.info("Starting ingestion worker zone=%s", ZONE)
    start_worker_loop(ZONE, process_item)
