import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
import getpass
import time
import ssl

# --- MAC SSL BYPASS ---
ssl._create_default_https_context = ssl._create_unverified_context
# ----------------------

# 1. Database Connection
username = getpass.getuser()
engine = create_engine(f'postgresql://{username}@localhost:5432/postgres')

def get_sp500_universe():
    print("Scraping S&P 500 constituents from Wikipedia...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    
    # Bypass Wikipedia 403 Forbidden Error
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
    table = pd.read_html(url, storage_options=headers)[0]
    
    # Clean tickers for Yahoo Finance compatibility
    table['Symbol'] = table['Symbol'].str.replace('.', '-')
    
    universe = pd.DataFrame({
        'ticker': table['Symbol'],
        'sector': table['GICS Sector'],
        'industry': table['GICS Sub-Industry']
    })
    return universe

def sync_master_table(universe_df):
    print("Syncing master 'companies' table...")
    with engine.begin() as conn:
        for _, row in universe_df.iterrows():
            query = text("""
                INSERT INTO companies (ticker, sector, industry) 
                VALUES (:ticker, :sector, :industry)
                ON CONFLICT (ticker) DO NOTHING;
            """)
            conn.execute(query, {
                "ticker": row['ticker'], 
                "sector": row['sector'], 
                "industry": row['industry']
            })

def run_extraction(tickers):
    total = len(tickers)
    
    for i, ticker in enumerate(tickers, 1):
        try:
            stock = yf.Ticker(ticker)
            
            # --- PROCESS PRICES (5-Year Window for 200-Week MA) ---
            hist = stock.history(period="5y")
            if not hist.empty:
                # 1000 trading days = ~200 weeks
                hist['wma_200'] = hist['Close'].rolling(window=1000, min_periods=1).mean()
                df_p = hist.reset_index()[['Date', 'Close', 'wma_200']]
                df_p['ticker'] = ticker
                df_p = df_p.rename(columns={'Date': 'date', 'Close': 'close_price'})
                df_p.to_sql('daily_prices', engine, if_exists='append', index=False)

            # --- PROCESS FUNDAMENTALS ---
            inc = stock.quarterly_financials.T
            bal = stock.quarterly_balance_sheet.T
            info = stock.info
            
            if not inc.empty and not bal.empty:
                df_f = pd.DataFrame(index=inc.index)
                df_f['ticker'] = ticker
                df_f['report_date'] = inc.index
                df_f['revenue'] = inc.get('Total Revenue', 0)
                df_f['ebit'] = inc.get('EBIT', 0)
                
                assets = bal.get('Total Assets', pd.Series(0, index=bal.index))
                liabs = bal.get('Total Liabilities Net Minority Interest', pd.Series(0, index=bal.index))
                
                df_f['working_capital'] = assets - liabs
                df_f['retained_earnings'] = bal.get('Retained Earnings', pd.Series(0, index=bal.index))
                df_f['total_assets'] = assets
                df_f['total_liabilities'] = liabs
                df_f['market_cap'] = info.get('marketCap', 0)
                df_f['pe_ratio'] = info.get('trailingPE', 0)
                
                df_f.to_sql('quarterly_fundamentals', engine, if_exists='append', index=False)
            
            print(f"[{i}/{total}] Success: {ticker}")
            time.sleep(1.5) 
            
        except Exception as e:
            print(f"[{i}/{total}] FAILED for {ticker}: {e}")

if __name__ == "__main__":
    universe = get_sp500_universe()
    sync_master_table(universe)
    
    print("Starting main extraction pipeline...")
    run_extraction(universe['ticker'].tolist())
    
    print("Patching initial connection timeouts for AAPL and MSFT...")
    run_extraction(['AAPL', 'MSFT'])
    
    print("S&P 500 Data Pipeline Complete!")