"""
Silver Layer Processing:
Reads raw Bronze JSON, performs schema validation, filters invalid rows,
deduplicates transaction records via SQL Window functions, and writes partitioned Parquet.
"""

from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "transactions"


def process_bronze_to_silver():
    raw_files = list(BRONZE_DIR.glob("*.json"))
    if not raw_files:
        raise FileNotFoundError(f"No JSON files found in {BRONZE_DIR}")

    print("Reading Bronze raw data...")
    con = duckdb.connect()

    # Core transformation query without trailing semicolon so it nests cleanly
    query = f"""
    WITH raw_feed AS (
        SELECT 
            transaction_id,
            ticker,
            transaction_type,
            CAST(price AS DOUBLE) AS price,
            CAST(shares AS INTEGER) AS shares,
            CAST(total_amount AS DOUBLE) AS total_amount,
            TRY_CAST(raw_timestamp AS TIMESTAMP) AS trade_timestamp,
            source_feed
        FROM read_json_auto('{BRONZE_DIR.as_posix()}/*.json')
    ),
    validated AS (
        SELECT 
            *,
            CAST(trade_timestamp AS DATE) AS trade_date,
            CURRENT_TIMESTAMP AS ingested_at
        FROM raw_feed
        WHERE transaction_id IS NOT NULL
          AND ticker IS NOT NULL
          AND trade_timestamp IS NOT NULL
          AND shares > 0
          AND total_amount > 0.0
    ),
    deduped AS (
        SELECT 
            transaction_id,
            ticker,
            transaction_type,
            price,
            shares,
            total_amount,
            trade_timestamp,
            source_feed,
            trade_date,
            ingested_at,
            ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY trade_timestamp DESC) AS rn
        FROM validated
    )
    SELECT 
        transaction_id,
        ticker,
        transaction_type,
        price,
        shares,
        total_amount,
        trade_timestamp,
        source_feed,
        trade_date,
        ingested_at
    FROM deduped
    WHERE rn = 1
    """

    total_raw = con.execute(f"SELECT count(*) FROM read_json_auto('{BRONZE_DIR.as_posix()}/*.json')").fetchone()[0]
    print(f"Total raw records read from Bronze: {total_raw}")

    result_df = con.execute(query).df()
    silver_count = len(result_df)
    dropped_count = total_raw - silver_count
    print(f"Records successfully cleaned and deduplicated: {silver_count} (Dropped {dropped_count} invalid/duplicate records)")

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    silver_output_path = SILVER_DIR.as_posix()
    print(f"Writing Silver Parquet to: {silver_output_path}")

    # Write partitioned Parquet files safely
    copy_sql = f"""
    COPY (
        {query}
    ) TO '{silver_output_path}' 
    (FORMAT PARQUET, PARTITION_BY (trade_date), OVERWRITE_OR_IGNORE 1)
    """
    con.execute(copy_sql)

    print("Silver layer processing completed successfully.")


if __name__ == "__main__":
    process_bronze_to_silver()