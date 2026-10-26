"""
Data Quality Validation Suite (pytest):
Runs automated assertions on Silver and Gold layers.
Verifies uniqueness, non-null constraints, and valid business logic ranges.
"""

from pathlib import Path
import duckdb
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "transactions"
GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "daily_ticker_metrics"


@pytest.fixture(scope="module")
def db_conn():
    conn = duckdb.connect()
    yield conn
    conn.close()


def test_silver_no_null_primary_keys(db_conn):
    """Ensure every record in Silver has a valid, non-null transaction_id."""
    query = f"""
        SELECT COUNT(*) 
        FROM read_parquet('{SILVER_DIR.as_posix()}/*/*.parquet')
        WHERE transaction_id IS NULL OR TRIM(transaction_id) = ''
    """
    null_count = db_conn.execute(query).fetchone()[0]
    assert null_count == 0, f"Found {null_count} records with null transaction_ids in Silver"


def test_silver_uniqueness(db_conn):
    """Ensure deduplication worked: transaction_id must be unique across all partitions."""
    query = f"""
        SELECT transaction_id, COUNT(*) as cnt
        FROM read_parquet('{SILVER_DIR.as_posix()}/*/*.parquet')
        GROUP BY transaction_id
        HAVING COUNT(*) > 1
    """
    duplicates = db_conn.execute(query).fetchall()
    assert len(duplicates) == 0, f"Found duplicate transaction_ids: {duplicates}"


def test_silver_positive_numerical_values(db_conn):
    """Ensure no corrupt negative prices, shares, or amounts exist."""
    query = f"""
        SELECT COUNT(*)
        FROM read_parquet('{SILVER_DIR.as_posix()}/*/*.parquet')
        WHERE price <= 0 OR shares <= 0 OR total_amount <= 0
    """
    invalid_rows = db_conn.execute(query).fetchone()[0]
    assert invalid_rows == 0, f"Found {invalid_rows} rows with negative or zero financial values"


def test_gold_cumulative_values_non_negative(db_conn):
    """Ensure all aggregated values in Gold are valid positive metrics."""
    query = f"""
        SELECT COUNT(*)
        FROM read_parquet('{GOLD_DIR.as_posix()}/*/*.parquet')
        WHERE total_shares_traded <= 0 
           OR total_value_traded <= 0 
           OR cumulative_value_traded <= 0
    """
    invalid_rows = db_conn.execute(query).fetchone()[0]
    assert invalid_rows == 0, f"Found invalid metrics in Gold layer: {invalid_rows}"