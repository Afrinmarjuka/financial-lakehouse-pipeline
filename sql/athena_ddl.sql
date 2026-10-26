-- ============================================================================
-- AWS Athena / Glue Data Catalog DDL
-- External tables pointing to Silver & Gold Parquet partitions in Amazon S3
-- ============================================================================

-- 1. Create Financial Database
CREATE DATABASE IF NOT EXISTS financial_lakehouse_db;

-- 2. Silver Layer Table: Clean Transactions
CREATE EXTERNAL TABLE IF NOT EXISTS financial_lakehouse_db.silver_transactions (
    transaction_id STRING,
    ticker STRING,
    transaction_type STRING,
    price DOUBLE,
    shares INT,
    total_amount DOUBLE,
    trade_timestamp TIMESTAMP,
    source_feed STRING,
    ingested_at TIMESTAMP
)
PARTITIONED BY (
    trade_date DATE
)
STORED AS PARQUET
LOCATION 's3://your-financial-lakehouse-bucket/data/silver/transactions/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Repair partitions to load new dates dynamically
MSCK REPAIR TABLE financial_lakehouse_db.silver_transactions;


-- 3. Gold Layer Table: Daily Aggregated Metrics
CREATE EXTERNAL TABLE IF NOT EXISTS financial_lakehouse_db.gold_daily_ticker_metrics (
    ticker STRING,
    total_trades BIGINT,
    total_shares_traded BIGINT,
    total_value_traded DOUBLE,
    avg_trade_price DOUBLE,
    min_price DOUBLE,
    max_price DOUBLE,
    cumulative_value_traded DOUBLE,
    calculated_at TIMESTAMP
)
PARTITIONED BY (
    trade_date DATE
)
STORED AS PARQUET
LOCATION 's3://your-financial-lakehouse-bucket/data/gold/daily_ticker_metrics/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

MSCK REPAIR TABLE financial_lakehouse_db.gold_daily_ticker_metrics;