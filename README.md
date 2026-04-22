markdown# Data Engineering Challenge

## Project Structure
data-pipeline-project/
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── etl/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── pipeline.py
├── docker-compose.yml
├── .env
└── README.md

## Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [Git](https://git-scm.com/) installed

## How to Run

### 1. Clone the Repository
```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
cd YOUR_REPO_NAME
```

### 2. Create the Environment File
Create a `.env` file in the root folder with these values:
```env
POSTGRES_DB=marketdb
POSTGRES_USER=etluser
POSTGRES_PASSWORD=supersecretpassword
POSTGRES_HOST=db
POSTGRES_PORT=5432
API_URL=http://api:8000/v1/market-data
POLL_INTERVAL_SECONDS=10
```

### 3. Start the Entire Stack (One Command)
```bash
docker-compose up --build
```
This will spin up all three services:
- **API** on http://localhost:8000
- **PostgreSQL** on port 5432
- **ETL pipeline** starts automatically after API and DB are healthy

### 4. Verify Everything is Running
Open a new terminal and run:
```bash
docker ps
```
You should see three containers running:
- `data-pipeline-project-api-1`
- `data-pipeline-project-db-1`
- `data-pipeline-project-etl-1`

## Testing & Inspection

### Check the API
Open in your browser:
http://localhost:8000/v1/market-data   → live market data feed
http://localhost:8000/health           → health check
http://localhost:8000/docs             → Swagger UI (interactive API explorer)

### Watch ETL Logs
```bash
docker-compose logs -f etl
```
Expected output every 10 seconds:
{"level":"INFO","msg":"Batch done | processed=7 dropped=0 inserted=7 duration=0.123s"}

### Inspect Data in PostgreSQL
```bash
# Step 1 - connect to the database
docker exec -it data-pipeline-project-db-1 psql -U etluser -d marketdb

# Step 2 - run queries inside psql
SELECT instrument_id, price, vwap, is_outlier, timestamp 
FROM market_data 
LIMIT 20;

# Count total records ingested
SELECT COUNT(*) FROM market_data;

# See outliers only
SELECT instrument_id, price, vwap, timestamp 
FROM market_data 
WHERE is_outlier = TRUE;

# Latest price per instrument
SELECT DISTINCT ON (instrument_id) 
  instrument_id, price, vwap, timestamp
FROM market_data 
ORDER BY instrument_id, timestamp DESC;

# Exit psql
\q
```

### Stop the Stack
```bash
docker-compose down
```

### Stop and Delete All Data (Fresh Start)
```bash
docker-compose down -v
```

---

## System Design Answers

### 1. Scaling to 1 Billion Events/Day
- **Ingest**: Replace the polling ETL with an **Apache Kafka** producer (API → Kafka topic).
  Multiple partitions allow parallel consumption.
- **Process**: Use **Apache Spark Structured Streaming** or **Flink** for stateful VWAP
  calculations across time windows.
- **Store**: Swap PostgreSQL for **Apache Iceberg** on S3 (cold) + **ClickHouse** (hot
  analytical queries). Use **Redshift / BigQuery** for ad-hoc SQL.
- **Orchestrate**: **Apache Airflow** or **Prefect** for DAG scheduling and retry logic.

### 2. Production Health Monitoring
- **Pipeline metrics**: Emit `records_processed`, `records_dropped`, `lag_seconds` to
  **Prometheus** via a `/metrics` endpoint, visualise in **Grafana**.
- **Alerting**: PagerDuty alert if `lag_seconds > 60` or `error_rate > 5%`.
- **DB health**: Track row counts per minute; alert on flatline (pipeline stalled).
- **Container health**: Docker / Kubernetes liveness + readiness probes (already wired
  in `docker-compose.yml`).
- **Dead-letter queue**: Failed records go to a DLQ topic/table for manual review.

### 3. Idempotency & Recovery
- **Checkpointing**: Record the last successfully processed `timestamp` in a `pipeline_state`
  table. On restart, resume from that checkpoint.
- **Upsert, never blind insert**: The `ON CONFLICT (instrument_id, timestamp) DO NOTHING`
  clause guarantees no duplicates even if the batch is replayed.
- **Atomic transactions**: Wrap each batch in a single DB transaction — either all records
  commit or none do, preventing partial writes.
- **Kafka offsets** (at scale): Commit consumer offsets only after a successful DB commit,
  giving at-least-once delivery with idempotent upserts achieving exactly-once semantics.