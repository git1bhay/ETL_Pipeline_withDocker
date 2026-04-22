import logging
import os
import statistics
import time
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError, field_validator

load_dotenv()

# ── Structured logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
log = logging.getLogger(__name__)

API_URL  = os.getenv("API_URL", "http://api:8000/v1/market-data")
DB_DSN   = (
    f"host={os.getenv('POSTGRES_HOST','db')} "
    f"port={os.getenv('POSTGRES_PORT','5432')} "
    f"dbname={os.getenv('POSTGRES_DB')} "
    f"user={os.getenv('POSTGRES_USER')} "
    f"password={os.getenv('POSTGRES_PASSWORD')}"
)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))

# ── Pydantic schema ──────────────────────────────────────────────────────────
class MarketRecord(BaseModel):
    instrument_id: str
    price: float
    volume: float
    timestamp: str

    @field_validator("price", "volume", mode="before")
    @classmethod
    def must_be_numeric(cls, v):
        try:
            return float(v)
        except (TypeError, ValueError):
            raise ValueError(f"Expected numeric, got: {v!r}")

# ── DB helpers ───────────────────────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(DB_DSN)

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_data (
                id            SERIAL PRIMARY KEY,
                instrument_id VARCHAR(20)    NOT NULL,
                price         NUMERIC(18,6)  NOT NULL,
                volume        NUMERIC(18,6)  NOT NULL,
                timestamp     TIMESTAMPTZ    NOT NULL,
                vwap          NUMERIC(18,6),
                is_outlier    BOOLEAN        DEFAULT FALSE,
                ingested_at   TIMESTAMPTZ    DEFAULT NOW(),
                UNIQUE (instrument_id, timestamp)
            )
        """)
    conn.commit()

# ── ETL logic ────────────────────────────────────────────────────────────────
def fetch_records() -> list[dict]:
    resp = requests.get(API_URL, timeout=5)
    resp.raise_for_status()
    return resp.json()

def validate_records(raw: list[dict]) -> tuple[list[MarketRecord], int]:
    valid, dropped = [], 0
    for item in raw:
        try:
            valid.append(MarketRecord(**item))
        except (ValidationError, TypeError):
            dropped += 1
            log.warning(f"Dropped invalid record: {item}")
    return valid, dropped

def compute_vwap(records: list[MarketRecord]) -> dict[str, float]:
    """Volume Weighted Average Price per instrument."""
    buckets: dict[str, list] = {}
    for r in records:
        buckets.setdefault(r.instrument_id, []).append(r)

    vwap = {}
    for inst, recs in buckets.items():
        total_vol   = sum(r.volume for r in recs)
        if total_vol > 0:
            vwap[inst] = sum(r.price * r.volume for r in recs) / total_vol
        else:
            vwap[inst] = 0.0
    return vwap

def flag_outliers(records: list[MarketRecord]) -> dict[str, bool]:
    """Flag records where price deviates > 15% from the batch average."""
    buckets: dict[str, list[float]] = {}
    for r in records:
        buckets.setdefault(r.instrument_id, []).append(r.price)

    outlier_keys: set[tuple] = set()
    for inst, prices in buckets.items():
        avg = statistics.mean(prices)
        for r in records:
            if r.instrument_id == inst and abs(r.price - avg) / avg > 0.15:
                outlier_keys.add((r.instrument_id, r.timestamp))

    return {f"{r.instrument_id}|{r.timestamp}": (r.instrument_id, r.timestamp) in outlier_keys
            for r in records}

def write_records(conn, records: list[MarketRecord], vwap: dict, outliers: dict):
    inserted = 0
    with conn.cursor() as cur:
        for r in records:
            key = f"{r.instrument_id}|{r.timestamp}"
            try:
                cur.execute("""
                    INSERT INTO market_data
                        (instrument_id, price, volume, timestamp, vwap, is_outlier)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (instrument_id, timestamp) DO NOTHING
                """, (
                    r.instrument_id,
                    r.price,
                    r.volume,
                    r.timestamp,
                    vwap.get(r.instrument_id),
                    outliers.get(key, False),
                ))
                if cur.rowcount:
                    inserted += 1
            except Exception as e:
                log.error(f"DB insert error for {r.instrument_id}: {e}")
    conn.commit()
    return inserted

# ── Main loop ────────────────────────────────────────────────────────────────
def run():
    conn = get_connection()
    ensure_table(conn)
    log.info("ETL pipeline started")

    while True:
        start = time.time()
        processed = dropped = inserted = 0
        try:
            raw      = fetch_records()
            valid, dropped = validate_records(raw)
            processed      = len(raw)

            vwap     = compute_vwap(valid)
            outliers = flag_outliers(valid)
            inserted = write_records(conn, valid, vwap, outliers)

        except requests.exceptions.RequestException as e:
            log.error(f"API fetch failed: {e}")
        except Exception as e:
            log.error(f"Unexpected error: {e}")
            conn = get_connection()   # reconnect on DB errors

        elapsed = round(time.time() - start, 3)
        log.info(
            f"Batch done | processed={processed} dropped={dropped} "
            f"inserted={inserted} duration={elapsed}s"
        )
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    run()