import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# Stocks we're tracking — 10 major financial/market tickers
TICKERS = [
    "AAPL",  # Apple
    "MSFT",  # Microsoft
    "GOOGL", # Google
    "AMZN",  # Amazon
    "JPM",   # JPMorgan Chase (bank)
    "GS",    # Goldman Sachs (bank)
    "SPY",   # S&P 500 ETF (market index)
    "BTC-USD", # Bitcoin
    "GC=F",  # Gold futures
    "CL=F"   # Crude Oil futures
]

def fetch_historical(period="2y"):
    """
    Fetch 2 years of daily OHLCV data for all tickers.
    Returns a single DataFrame with all tickers combined.
    """
    all_data = []

    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        try:
            df = yf.download(ticker, period=period, interval="1d", progress=False)
            
            if df.empty:
                print(f"  No data for {ticker}, skipping.")
                continue

            # Flatten multi-level columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()
            df["ticker"] = ticker
            df = df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            })
            df = df[["date", "ticker", "open", "high", "low", "close", "volume"]]
            df = df.dropna()
            all_data.append(df)
            print(f"  {len(df)} rows fetched.")

        except Exception as e:
            print(f"  Error fetching {ticker}: {e}")

    if not all_data:
        print("No data fetched.")
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal rows fetched: {len(combined)}")
    return combined


def fetch_latest():
    """
    Fetch only the last 5 days of data.
    Used for daily pipeline updates.
    """
    return fetch_historical(period="5d")


if __name__ == "__main__":
    df = fetch_historical(period="2y")
    print(df.head())
    print(f"\nTickers: {df['ticker'].unique()}")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")