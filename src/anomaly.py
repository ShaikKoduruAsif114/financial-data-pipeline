import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from store import load_from_db, DB_PATH
import sqlite3

def add_features(df):
    """Add technical features for anomaly detection."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["ticker", "date"])

    # Daily return
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()

    # Price range (high - low) / close
    df["price_range"] = (df["high"] - df["low"]) / df["close"]

    # Volume change
    df["volume_change"] = df.groupby("ticker")["volume"].pct_change()

    # 7-day rolling average return
    df["rolling_return_7d"] = df.groupby("ticker")["daily_return"].transform(
        lambda x: x.rolling(7).mean()
    )

    # Drop rows with NaN (from pct_change and rolling)
    df = df.dropna()
    return df


def detect_anomalies(contamination=0.03):
    """
    Run Isolation Forest on all tickers.
    contamination=0.03 means we expect ~3% of days to be anomalous.
    """
    df = load_from_db()

    if df.empty:
        print("No data in database.")
        return

    df = add_features(df)

    features = ["daily_return", "price_range", "volume_change", "rolling_return_7d"]
    results = []

    for ticker in df["ticker"].unique():
        ticker_df = df[df["ticker"] == ticker].copy()

        if len(ticker_df) < 30:
            print(f"Not enough data for {ticker}, skipping.")
            continue

        X = ticker_df[features].values

        model = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100
        )
        ticker_df["anomaly"] = model.fit_predict(X)
        ticker_df["anomaly_score"] = model.decision_function(X)

        # -1 = anomaly, 1 = normal → convert to 0/1
        ticker_df["is_anomaly"] = (ticker_df["anomaly"] == -1).astype(int)

        anomaly_count = ticker_df["is_anomaly"].sum()
        total = len(ticker_df)
        print(f"{ticker:10s} → {anomaly_count} anomalies out of {total} days "
              f"({anomaly_count/total*100:.1f}%)")

        results.append(ticker_df)

    if not results:
        print("No results.")
        return

    final = pd.concat(results, ignore_index=True)

    # Save anomaly results back to database
    save_anomalies(final)
    return final


def save_anomalies(df):
    """Save anomaly flags to a separate table."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DROP TABLE IF EXISTS anomalies")
    conn.execute("""
        CREATE TABLE anomalies (
            date TEXT,
            ticker TEXT,
            close REAL,
            daily_return REAL,
            price_range REAL,
            volume_change REAL,
            is_anomaly INTEGER,
            anomaly_score REAL
        )
    """)

    df_save = df[["date", "ticker", "close", "daily_return",
                  "price_range", "volume_change", "is_anomaly", "anomaly_score"]].copy()
    df_save["date"] = df_save["date"].astype(str)
    df_save.to_sql("anomalies", conn, if_exists="replace", index=False)
    conn.close()

    total_anomalies = df["is_anomaly"].sum()
    print(f"\nTotal anomalies detected: {total_anomalies}")
    print(f"Anomaly rate: {total_anomalies/len(df)*100:.2f}%")
    print("Saved to database.")


if __name__ == "__main__":
    detect_anomalies(contamination=0.03)