# Quantitative S&P 500 Equity Screener

## Executive Summary
Engineered a localized PostgreSQL database and automated Python ETL pipeline to screen the S&P 500 for relative value, fundamental growth, and technical momentum. 

## Technology Stack
* **Language:** Python 3.x
* **Database:** PostgreSQL
* **Libraries:** `pandas`, `yfinance`, `sqlalchemy`
* **Techniques:** API Rate-Limit Handling, Window Functions, Common Table Expressions (CTEs)

## Pipeline Architecture
1. **Extraction:** Scrapes S&P 500 constituents and utilizes `yfinance` to pull 5 years of daily technical data and 6 quarters of fundamental accounting data. Includes automated error handling for API connection timeouts.
2. **Storage:** Normalizes and loads time-series and categorical data into a relational PostgreSQL database.
3. **Analytics:** Executes a multi-factor SQL screener utilizing `NTILE()` and `LAG()` functions to isolate equities meeting strict quantitative parameters:
   * Bottom 50% P/E Ratio relative to specific GICS Sector
   * Positive Quarter-over-Quarter (QoQ) Revenue Growth (>10%)
   * Current Price trading below the 200-Week Moving Average

## Output
The SQL engine successfully processed 250,000+ rows of historical data to isolate 14 actionable equities meeting the strict multi-factor criteria.