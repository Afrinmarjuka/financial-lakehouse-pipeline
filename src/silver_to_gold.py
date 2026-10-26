"""
Gold Layer Processing:
Reads clean Silver Parquet partitions, computes daily aggregate trading KPIs,
calculates cumulative trading volumes via SQL window functions,
and writes business-level Parquet datasets for analytics and Athena querying.
"""

from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "transactions"
GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "daily_ticker_metrics"


def process_silver_to_gold():
    parquet_files = list(SILVER_DIR.glob("**/*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Silver Parquet files found in {SILVER_DIR}")

    print("Reading Silver layer Parquet partitions...")
    con = duckdb.connect()

    # Core Gold Aggregation Query:
    # Aggregates daily trading volumes, value traded, and running cumulative volume per ticker
    query = f"""
    WITH daily_aggregates AS (
        SELECT 
            trade_date,
            ticker,
            COUNT(transaction_id) AS total_trades,
            SUM(shares) AS total_shares_traded,
            ROUND(SUM(total_amount), 2) AS total_value_traded,
            ROUND(AVG(price), 2) AS avg_trade_price,
            ROUND(MIN(price), 2) AS min_price,
            ROUND(MAX(price), 2) AS max_price
        FROM read_parquet('{SILVER_DIR.as_posix()}/*/*.parquet')
        GROUP BY trade_date, ticker
    ),
    with_window_metrics AS (
        SELECT 
            trade_date,
            ticker,
            total_trades,
            total_shares_traded,
            total_value_traded,
            avg_trade_price,
            min_price,
            max_price,
            -- Rolling cumulative volume per ticker ordered chronologically
            SUM(total_value_traded) OVER (
                PARTITION BY ticker 
                ORDER BY trade_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_value_traded,
            CURRENT_TIMESTAMP AS calculated_at
        FROM daily_aggregates
    )
    SELECT *
    FROM with_window_metrics
    ORDER BY trade_date DESC, total_value_traded DESC
    """

    df = con.execute(query).df()
    print("Gold Layer Business Aggregations preview:")
    print(df.to_string(index=False))

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    gold_output_path = GOLD_DIR.as_posix()
    print(f"\nWriting Gold aggregated Parquet to: {gold_output_path}")

    # Write out Gold dataset partitioned by trade_date
    copy_sql = f"""
    COPY (
        {query}
    ) TO '{gold_output_path}' 
    (FORMAT PARQUET, PARTITION_BY (trade_date), OVERWRITE_OR_IGNORE 1)
    """
    con.execute(copy_sql)

    print("Gold layer processing completed successfully.")


if __name__ == "__main__":
    process_silver_to_gold()