import random
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Response

app = FastAPI(title="Mock Market Data API")

INSTRUMENTS = ["AAPL", "BTC-USD", "ETH-USD", "GOOGL", "TSLA", "MSFT", "AMZN"]

def generate_record(instrument_id: str) -> dict:
    return {
        "instrument_id": instrument_id,
        "price": round(random.uniform(50, 5000), 4),
        "volume": round(random.uniform(100, 100000), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

@app.get("/v1/market-data")
def get_market_data(response: Response):
    # 5% chance of fault injection
    fault_roll = random.random()

    if fault_roll < 0.025:
        # Return 500 error
        response.status_code = 500
        return {"error": "Internal Server Error (injected fault)"}

    if fault_roll < 0.05:
        # Return malformed data (string in numeric field)
        records = [generate_record(i) for i in INSTRUMENTS]
        bad_idx = random.randint(0, len(records) - 1)
        records[bad_idx]["price"] = "NOT_A_NUMBER"   # intentional bad data
        return records

    # Happy path
    return [generate_record(i) for i in INSTRUMENTS]

@app.get("/health")
def health():
    return {"status": "ok"}