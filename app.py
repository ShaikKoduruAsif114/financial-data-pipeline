import sys
import os
import numpy as np
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import yfinance as yf
from sklearn.ensemble import IsolationForest

# ─── PAGE CONFIG ───────────────────────────────────────────
st.set_page_config(
    page_title="Financial Market Dashboard",
    page_icon="📈",
    layout="wide"
)

TICKERS = ["AAPL", "MSFT", "GOOGL"]

# ─── FETCH + CACHE DATA ────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    all_data = []

    for ticker in TICKERS:
        try:
            df = yf.download(ticker, period="3mo", interval="1d", progress=False)
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df.reset_index()
                df.columns = [c.lower() for c in df.columns]
                df["ticker"] = ticker
                df = df[["date", "ticker", "open", "high", "low", "close", "volume"]]
                df = df.dropna()
                all_data.append(df)
        except Exception:
            pass

    # If no data fetched generate sample data
# If no data fetched generate sample data
    if not all_data:
        n = 60
        dates = [pd.Timestamp.today() - pd.Timedelta(days=x) for x in range(n-1, -1, -1)]
        for i, ticker in enumerate(TICKERS):
            np.random.seed(i)
            close = list(150 + np.cumsum(np.random.randn(n)))
            sample = pd.DataFrame({
                "date": dates,
                "ticker": [ticker] * n,
                "open": [c * 0.99 for c in close],
                "high": [c * 1.01 for c in close],
                "low": [c * 0.98 for c in close],
                "close": close,
                "volume": list(map(float, np.random.randint(1000000, 5000000, n)))
            })
            all_data.append(sample)

    return pd.concat(all_data, ignore_index=True)

@st.cache_data(ttl=3600)
def run_anomaly_detection(df):
    results = []
    for ticker in df["ticker"].unique():
        tdf = df[df["ticker"] == ticker].copy().sort_values("date")
        tdf["daily_return"] = tdf["close"].pct_change()
        tdf["price_range"] = (tdf["high"] - tdf["low"]) / tdf["close"]
        tdf["volume_change"] = tdf["volume"].pct_change()
        tdf = tdf.dropna()
        if len(tdf) < 10:
            continue
        X = tdf[["daily_return", "price_range", "volume_change"]].values
        model = IsolationForest(contamination=0.05, random_state=42)
        tdf["is_anomaly"] = (model.fit_predict(X) == -1).astype(int)
        tdf["anomaly_score"] = model.decision_function(X)
        results.append(tdf)
    return pd.concat(results, ignore_index=True)

# ─── LOAD DATA ─────────────────────────────────────────────
with st.spinner("Loading market data..."):
    df = load_data()
    anomaly_df = run_anomaly_detection(df)

# ─── TITLE ─────────────────────────────────────────────────
st.title("📈 Financial Market Intelligence Dashboard")
st.caption("Live data with anomaly detection across market tickers")

# ─── SIDEBAR ───────────────────────────────────────────────
st.sidebar.header("Controls")
selected_ticker = st.sidebar.selectbox("Select Ticker", TICKERS)
show_anomalies = st.sidebar.checkbox("Show Anomalies", value=True)

# ─── FILTER DATA ───────────────────────────────────────────
prices = df[df["ticker"] == selected_ticker].copy()
anomalies = anomaly_df[anomaly_df["ticker"] == selected_ticker].copy()
total_anomalies = int(anomalies["is_anomaly"].sum())

# ─── TOP METRICS ───────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Records", f"{len(df):,}")
with col2:
    if len(prices) >= 2:
        latest = float(prices["close"].iloc[-1])
        prev = float(prices["close"].iloc[-2])
        change = ((latest - prev) / prev) * 100
        st.metric("Latest Close", f"${latest:.2f}", f"{change:.2f}%")
    else:
        st.metric("Latest Close", "N/A")
with col3:
    st.metric("Anomalies Detected", total_anomalies)
with col4:
    rate = total_anomalies / len(anomalies) * 100 if len(anomalies) > 0 else 0
    st.metric("Anomaly Rate", f"{rate:.1f}%")

st.divider()

# ─── PRICE CHART ───────────────────────────────────────────
st.subheader(f"{selected_ticker} — Price History")
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=prices["date"], y=prices["close"],
    name="Close Price", line=dict(color="#00b4d8", width=1.5)
))

if show_anomalies:
    anom = anomalies[anomalies["is_anomaly"] == 1]
    anom_prices = prices[prices["date"].isin(anom["date"])]
    fig.add_trace(go.Scatter(
        x=anom_prices["date"], y=anom_prices["close"],
        mode="markers", name="Anomaly",
        marker=dict(color="red", size=8, symbol="x")
    ))

fig.update_layout(
    height=400, template="plotly_dark",
    xaxis_title="Date", yaxis_title="Price (USD)",
    legend=dict(orientation="h")
)
st.plotly_chart(fig, use_container_width=True)

# ─── VOLUME CHART ──────────────────────────────────────────
st.subheader(f"{selected_ticker} — Volume")
fig2 = px.bar(prices, x="date", y="volume",
              color_discrete_sequence=["#48cae4"])
fig2.update_layout(height=250, template="plotly_dark")
st.plotly_chart(fig2, use_container_width=True)

# ─── ANOMALY SUMMARY ───────────────────────────────────────
st.divider()
st.subheader("🚨 Anomaly Summary — All Tickers")
summary = anomaly_df.groupby("ticker").agg(
    total_days=("is_anomaly", "count"),
    anomalies=("is_anomaly", "sum")
).reset_index()
summary["anomaly_rate_%"] = (
    summary["anomalies"] / summary["total_days"] * 100
).round(2)
st.dataframe(summary, use_container_width=True)

# ─── RECENT ANOMALIES ──────────────────────────────────────
st.subheader(f"📋 Recent Anomalies — {selected_ticker}")
recent = anomalies[anomalies["is_anomaly"] == 1].sort_values(
    "date", ascending=False).head(10)
if not recent.empty:
    recent = recent[["date", "close", "daily_return", "anomaly_score"]].copy()
    recent["daily_return"] = (recent["daily_return"] * 100).round(2)
    recent["anomaly_score"] = recent["anomaly_score"].round(4)
    recent.columns = ["Date", "Close", "Daily Return %", "Anomaly Score"]
    st.dataframe(recent, use_container_width=True)
else:
    st.info("No anomalies found for this ticker.")