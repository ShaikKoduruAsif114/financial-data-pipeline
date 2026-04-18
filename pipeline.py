import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from fetch import fetch_latest
from store import init_db, save_to_db, get_stats
from anomaly import detect_anomalies
from datetime import datetime

def run_pipeline():
    print("=" * 50)
    print(f"Pipeline started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # Step 1 — Initialize DB
    print("\n[1/3] Initializing database...")
    init_db()

    # Step 2 — Fetch latest data
    print("\n[2/3] Fetching latest market data...")
    df = fetch_latest()

    if df.empty:
        print("No data fetched. Exiting.")
        return

    save_to_db(df)

    # Step 3 — Run anomaly detection
    print("\n[3/3] Running anomaly detection...")
    detect_anomalies(contamination=0.03)

    # Stats
    print("\n" + "=" * 50)
    print("Pipeline complete.")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    get_stats()

if __name__ == "__main__":
    run_pipeline()