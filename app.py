import os
import sys
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

DB_PATH = "data/financial.db"

# ─── SETUP DATABASE SILENTLY ───────────────────────────────
def setup_database():
    from store import init_db, save_to_db
    from fetch import fetch_historical
    from anomaly import detect_anomalies

    os.makedirs("data", exist_ok=True)
    init_db()

    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM stock_prices").fetchone()[0]
    anomaly_table = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='anomalies'"
    ).fetchone()
    conn.close()

    if count == 0:
        df = fetch_historical(period="3mo")
        save_to_db(df)

    if not anomaly_table:
        detect_anomalies(contamination=0.03)

setup_database()

# ─── DATA LOADERS ──────────────────────────────────────────
def load_prices(ticker):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(
            f"SELECT * FROM stock_prices WHERE ticker='{ticker}' ORDER BY date",
            conn
        )
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","volume"])

def load_anomalies(ticker):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql(
            f"SELECT * FROM anomalies WHERE ticker='{ticker}' ORDER BY date",
            conn
        )
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame(columns=["date","ticker","close","daily_return",
                                     "price_range","volume_change",
                                     "is_anomaly","anomaly_score"])

def load_all_anomalies():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT * FROM anomalies ORDER BY date", conn)
        conn.close()
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame(columns=["date","ticker","close","daily_return",
                                     "price_range","volume_change",
                                     "is_anomaly","anomaly_score"])

# ─── PAGE CONFIG ───────────────────────────────────────────
import streamlit as st

st.set_page_config(
    page_title="Financial Market Dashboard",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Financial Market Intelligence Dashboard")
st.caption("Automated pipeline with anomaly detection across 10 market tickers")

# ─── SIDEBAR ───────────────────────────────────────────────
TICKERS = [
    "AAPL"
]

st.sidebar.header("Controls")
selected_ticker = st.sidebar.selectbox("Select Ticker", TICKERS)
show_anomalies = st.sidebar.checkbox("Show Anomalies", value=True)

# ─── LOAD DATA ─────────────────────────────────────────────
prices = load_prices(selected_ticker)
anomalies = load_anomalies(selected_ticker)
all_anomalies = load_all_anomalies()

# ─── SHOW LOADING MESSAGE IF NO DATA ───────────────────────
if prices.empty:
    st.warning("Data is still loading. Please wait 2-3 minutes and refresh.")
    st.stop()

# ─── TOP METRICS ───────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Records", f"{len(prices):,}")
with col2:
    if len(prices) >= 2:
        latest_close = prices["close"].iloc[-1]
        prev_close = prices["close"].iloc[-2]
        change = ((latest_close - prev_close) / prev_close) * 100
        st.metric("Latest Close", f"${latest_close:.2f}", f"{change:.2f}%")
    else:
        st.metric("Latest Close", "N/A")
with col3:
    anomaly_count = int(anomalies["is_anomaly"].sum()) if not anomalies.empty else 0
    st.metric("Anomalies Detected", anomaly_count)
with col4:
    if not anomalies.empty and len(anomalies) > 0:
        anomaly_rate = anomaly_count / len(anomalies) * 100
        st.metric("Anomaly Rate", f"{anomaly_rate:.1f}%")
    else:
        st.metric("Anomaly Rate", "N/A")

st.divider()

# ─── PRICE CHART ───────────────────────────────────────────
st.subheader(f"{selected_ticker} — Price History")

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=prices["date"],
    y=prices["close"],
    name="Close Price",
    line=dict(color="#00b4d8", width=1.5)
))

if show_anomalies and not anomalies.empty:
    anomaly_days = anomalies[anomalies["is_anomaly"] == 1]
    anomaly_prices = prices[prices["date"].isin(anomaly_days["date"])]
    fig.add_trace(go.Scatter(
        x=anomaly_prices["date"],
        y=anomaly_prices["close"],
        mode="markers",
        name="Anomaly",
        marker=dict(color="red", size=8, symbol="x")
    ))

fig.update_layout(
    height=400,
    template="plotly_dark",
    xaxis_title="Date",
    yaxis_title="Price (USD)",
    legend=dict(orientation="h")
)
st.plotly_chart(fig, use_container_width=True)

# ─── VOLUME CHART ──────────────────────────────────────────
st.subheader(f"{selected_ticker} — Volume")
fig2 = px.bar(prices, x="date", y="volume",
              color_discrete_sequence=["#48cae4"])
fig2.update_layout(height=250, template="plotly_dark")
st.plotly_chart(fig2, use_container_width=True)

# ─── ANOMALY TABLE ─────────────────────────────────────────
st.divider()
st.subheader("🚨 Anomaly Summary — All Tickers")

if not all_anomalies.empty:
    summary = all_anomalies.groupby("ticker").agg(
        total_days=("is_anomaly", "count"),
        anomalies=("is_anomaly", "sum")
    ).reset_index()
    summary["anomaly_rate"] = (summary["anomalies"] / summary["total_days"] * 100).round(2)
    summary = summary.sort_values("anomalies", ascending=False)
    st.dataframe(summary, use_container_width=True)
else:
    st.info("Anomaly data loading...")

# ─── RECENT ANOMALIES ──────────────────────────────────────
st.subheader(f"📋 Recent Anomalies — {selected_ticker}")

if not anomalies.empty:
    recent = anomalies[anomalies["is_anomaly"] == 1].sort_values(
        "date", ascending=False).head(10)
    if not recent.empty:
        recent = recent[["date","close","daily_return","price_range","anomaly_score"]]
        recent["daily_return"] = (recent["daily_return"] * 100).round(2)
        recent["price_range"] = (recent["price_range"] * 100).round(2)
        recent["anomaly_score"] = recent["anomaly_score"].round(4)
        recent.columns = ["Date","Close","Daily Return %","Price Range %","Anomaly Score"]
        st.dataframe(recent, use_container_width=True)
    else:
        st.info("No anomalies found for this ticker.")
else:
    st.info("Anomaly data loading...")