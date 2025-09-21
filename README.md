# Orchestrator Microservice (Kafka + Redis)

This microservice orchestrates the ingestion, standardization, and application of business objects (Data Objects) according to their hierarchical dependencies.

- Objects are classified into three layers: initial (1000), intermediate (2000), and final (3000).
- The orchestrator traverses dependencies between objects (`IdObjet` ↔ `IdObjet_Parent`) to determine the optimal processing order using topological sorting.
- Kafka events are generated for each stage: *Object Ready*, *Object Loaded*, *Object Failed*, ensuring asynchronous processing and parallel execution across worker zones.
- Deduplication ensures shared dependencies are processed only once.

Redis is used to track retries for failed objects, and Prometheus metrics provide monitoring for orchestration health.
It includes unit and integration tests, Docker setup, and a simple Prometheus metrics interface.

---

## Project Structure

- **orchestrator/**  
  Core orchestrator logic:
  - `main.py` — entrypoint with HTTP health & metrics server  
  - `scheduler.py` — schedules tasks & manages orchestration loops  
  - `kafka_client.py` — Kafka producer/consumer helper  
  - `logger.py` — centralized logging configuration  
  - `config.py` — configuration variables & worker tuning  
  - `graph_utils.py` — utilities for graph and dependency handling  
  - `metrics.py` — Prometheus metrics definitions 

- **workers/**  
  Worker zones:
  - `ingestion/worker.py`  
  - `standardization/worker.py`  
  - `application/worker.py`  
  Each uses `worker_base.py` for the main processing loop.

- **tools/**  
  Sample input objects JSON files.

- **tests/**  
  Unit & integration tests:
  - `test_graph_utils.py` — utility functions  
  - `test_scheduler.py` — scheduler logic  
  - `test_worker_base.py` — core worker loop  
  - `test_kafka_client.py` — Kafka client helpers  
  - `test_integration_worker.py` — full worker integration (with mocks)

- **Dockerfile.orchestrator** — Docker image for orchestrator  
- **Dockerfile.worker** — Docker image for worker  
- **docker-compose.yml** — full stack (Kafka, Zookeeper, Redis, orchestrator, workers)  
- **requirements.txt** — Python dependencies  

---

## Prerequisites

- Docker & Docker Compose  
- Python 3.11+ (for local development)  

---

## Quick Start with Docker

1. **Clone the repo**  
```bash
git clone https://github.com/pydevmarwa/Orchestrator-Microservice.git
cd Orchestrator-Microservice
```

2. **Build and start the stack**  
```bash
docker-compose up --build
```

- Kafka UI: http://localhost:8080  
- RedisInsight: http://localhost:5540  
- Orchestrator HTTP endpoints:  
  - Liveness: http://localhost:8000/health/liveness  
  - Readiness: http://localhost:8000/health/readiness  
  - Metrics: http://localhost:8000/metrics

3. **Check logs**  
```bash
docker-compose logs -f orchestrator
```

## Running Locally

1. **Install dependencies**  
```bash
pip install -r requirements.txt
```

2. **Start orchestrator**  
```bash
python orchestrator/main.py
```

3. **Start a worker (example: ingestion)**  
```bash
WORKER_TYPE=ingestion python workers/ingestion/worker.py
```

## Testing

- **Run all tests**  
```bash
pytest tests
```

- Tests include:  
  - Unit tests for graph utils, scheduler, worker base, Kafka client  
  - Integration tests for full worker lifecycle (with mocks for Kafka and Redis)

## How It Works

- Orchestrator schedules object processing across worker zones.  
- Workers consume messages from Kafka, simulate processing, and emit ready, loaded, or failed events.  
- Redis tracks retries for failed messages.  
- Orchestrator metrics are exposed via Prometheus `/metrics`.  
- Health endpoints: `/health/liveness` and `/health/readiness`.

## Scalability & Production Readiness

- Worker concurrency is tunable per zone via environment variables.  
- Retries are handled automatically using Redis.  
- Metrics & observability via Prometheus for monitoring orchestration health.  
- Dockerized stack allows easy scaling and deployment.

## Author

**Marwa Amri**  
Email: amri.marwa9999@gmail.com
