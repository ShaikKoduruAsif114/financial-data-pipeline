import sqlite3
import pandas as pd
import os

DB_PATH = "data/financial.db"

def init_db():
    """Create the database and table if they don't exist."""
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            ticker TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            UNIQUE(date, ticker)
        )
    """)
    conn.commit()
    conn.close()
    print("Database initialized.")

def save_to_db(df):
    """Save dataframe to SQLite, skip duplicates."""
    
    # Fix multi-level columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    # Reset index if date is in index
    if "date" not in df.columns:
        df = df.reset_index()
    
    conn = sqlite3.connect(DB_PATH)
    df["date"] = df["date"].astype(str)
    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT INTO stock_prices (date, ticker, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (row["date"], row["ticker"], row["open"],
                  row["high"], row["low"], row["close"], row["volume"]))
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1

    conn.commit()
    conn.close()
    print(f"Inserted: {inserted} rows | Skipped (duplicates): {skipped} rows")

def load_from_db(ticker=None):
    """Load data from SQLite. Optionally filter by ticker."""
    conn = sqlite3.connect(DB_PATH)
    if ticker:
        df = pd.read_sql(f"SELECT * FROM stock_prices WHERE ticker='{ticker}' ORDER BY date", conn)
    else:
        df = pd.read_sql("SELECT * FROM stock_prices ORDER BY date", conn)
    conn.close()
    return df

def get_stats():
    """Get basic stats about what's in the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stock_prices")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM stock_prices")
    tickers = cursor.fetchone()[0]
    cursor.execute("SELECT MIN(date), MAX(date) FROM stock_prices")
    date_range = cursor.fetchone()
    conn.close()
    print(f"Total records: {total}")
    print(f"Tickers: {tickers}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")

if __name__ == "__main__":
    from fetch import fetch_historical
    init_db()
    df = fetch_historical(period="2y")
    save_to_db(df)
    print("\nDatabase stats:")
    get_stats()