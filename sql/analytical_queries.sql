-- ============================================================================
-- Financial Analytics & Performance SQL
-- Demonstrating CTEs, Window Functions (DENSE_RANK, LAG, Moving Averages)
-- ============================================================================

-- Query 1: Top 3 Tickers by Daily Traded Volume per Date (Dense Ranking)
WITH ranked_daily_volume AS (
    SELECT 
        trade_date,
        ticker,
        total_value_traded,
        total_shares_traded,
        DENSE_RANK() OVER (
            PARTITION BY trade_date 
            ORDER BY total_value_traded DESC
        ) AS rank_by_value
    FROM financial_lakehouse_db.gold_daily_ticker_metrics
)
SELECT 
    trade_date,
    rank_by_value,
    ticker,
    total_value_traded,
    total_shares_traded
FROM ranked_daily_volume
WHERE rank_by_value <= 3
ORDER BY trade_date DESC, rank_by_value ASC;


-- Query 2: Day-over-Day Traded Value Growth Rate using LAG()
WITH dod_comparison AS (
    SELECT 
        ticker,
        trade_date,
        total_value_traded,
        LAG(total_value_traded, 1) OVER (
            PARTITION BY ticker 
            ORDER BY trade_date
        ) AS prev_day_value
    FROM financial_lakehouse_db.gold_daily_ticker_metrics
)
SELECT 
    ticker,
    trade_date,
    total_value_traded,
    prev_day_value,
    ROUND(
        ((total_value_traded - prev_day_value) / NULLIF(prev_day_value, 0)) * 100.0, 
        2
    ) AS dod_growth_pct
FROM dod_comparison
ORDER BY ticker, trade_date DESC;


-- Query 3: Buy vs Sell Order Distribution and Volume Ratio
SELECT 
    trade_date,
    ticker,
    COUNT(CASE WHEN transaction_type = 'BUY' THEN 1 END) AS buy_orders_count,
    COUNT(CASE WHEN transaction_type = 'SELL' THEN 1 END) AS sell_orders_count,
    ROUND(
        SUM(CASE WHEN transaction_type = 'BUY' THEN total_amount ELSE 0 END), 2
    ) AS buy_volume_usd,
    ROUND(
        SUM(CASE WHEN transaction_type = 'SELL' THEN total_amount ELSE 0 END), 2
    ) AS sell_volume_usd,
    ROUND(
        SUM(CASE WHEN transaction_type = 'BUY' THEN total_amount ELSE 0 END) / 
        NULLIF(SUM(CASE WHEN transaction_type = 'SELL' THEN total_amount ELSE 0 END), 0),
        2
    ) AS buy_to_sell_ratio
FROM financial_lakehouse_db.silver_transactions
GROUP BY trade_date, ticker
ORDER BY trade_date DESC, buy_volume_usd DESC;