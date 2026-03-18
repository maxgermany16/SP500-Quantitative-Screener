WITH RevenueGrowth AS (
    SELECT 
        ticker,
        report_date,
        pe_ratio,
        (revenue - LAG(revenue, 1) OVER (PARTITION BY ticker ORDER BY report_date)) / 
        NULLIF(LAG(revenue, 1) OVER (PARTITION BY ticker ORDER BY report_date), 0) AS qoq_growth,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY report_date DESC) as report_rank
    FROM quarterly_fundamentals
),
LatestFundamentals AS (
    SELECT * FROM RevenueGrowth WHERE report_rank = 1
),
LatestMetrics AS (
    SELECT 
        f.ticker,
        c.sector,
        f.pe_ratio,
        f.qoq_growth,
        NTILE(2) OVER (PARTITION BY c.sector ORDER BY f.pe_ratio ASC) AS pe_half
    FROM LatestFundamentals f
    JOIN companies c ON f.ticker = c.ticker
    WHERE f.pe_ratio > 0 
),
LatestPrices AS (
    SELECT 
        ticker, 
        close_price,
        wma_200,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as price_rank
    FROM daily_prices
)
SELECT 
    m.ticker,
    m.sector,
    ROUND(CAST(m.pe_ratio AS NUMERIC), 2) AS pe_ratio,
    ROUND(CAST(m.qoq_growth * 100 AS NUMERIC), 2) AS rev_growth_pct,
    ROUND(CAST(p.close_price AS NUMERIC), 2) AS close_price,
    ROUND(CAST(p.wma_200 AS NUMERIC), 2) AS wma_200
FROM LatestMetrics m
JOIN LatestPrices p ON m.ticker = p.ticker AND p.price_rank = 1
WHERE m.pe_half = 1
  AND m.qoq_growth > 0.1 
  AND p.close_price < p.wma_200
ORDER BY m.sector ASC, m.pe_ratio ASC;