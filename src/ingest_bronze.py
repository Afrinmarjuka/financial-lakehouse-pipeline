"""
Bronze Layer Ingestion:
Generates raw, untransformed transactional data mimicking an external financial stream.
Includes realistic data engineering challenges: null values, duplicates, and unformatted dates.
"""

import json
import os
import random
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")


def generate_mock_transactions(num_records: int = 100) -> list[dict]:
    tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA"]
    transaction_types = ["BUY", "SELL"]
    records = []

    base_time = datetime.now()

    for i in range(1, num_records + 1):
        # Introduce a few duplicate transaction IDs deliberately for Silver deduplication testing
        tx_id = f"TXN_{1000 + (i if i <= 90 else random.randint(1, 10))}"

        ticker = random.choice(tickers)
        tx_type = random.choice(transaction_types)
        price = round(random.uniform(50.0, 450.0), 2)
        shares = random.randint(1, 500)
        total_amount = round(price * shares, 2)

        # Introduce edge cases: random nulls and negative amounts to test schema validation & quality rules
        if i == 5:
            ticker = None  # Missing ticker
        elif i == 12:
            total_amount = -150.00  # Invalid negative transaction
        elif i == 20:
            shares = 0

        timestamp = (base_time - timedelta(minutes=random.randint(0, 1440))).strftime("%Y-%m-%d %H:%M:%S")

        record = {
            "transaction_id": tx_id,
            "ticker": ticker,
            "transaction_type": tx_type,
            "price": price,
            "shares": shares,
            "total_amount": total_amount,
            "raw_timestamp": timestamp,
            "source_feed": "FIN_MARKET_STREAM_v1"
        }
        records.append(record)

    return records


def ingest_to_bronze():
    os.makedirs(DATA_DIR, exist_ok=True)
    today_str = datetime.now().strftime("%Y_%m_%d")
    output_file = os.path.join(DATA_DIR, f"transactions_{today_str}.json")

    records = generate_mock_transactions(num_records=120)

    with open(output_file, "w") as f:
        json.dump(records, f, indent=2)

    print(f"Bronze ingestion complete: {len(records)} raw records saved to {output_file}")


if __name__ == "__main__":
    ingest_to_bronze()