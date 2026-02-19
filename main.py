"""
Arbitrage Monitor — FastAPI Backend
Runs the market data poller in a background thread and exposes REST endpoints
for the dashboard to consume.

Endpoints:
  GET /prices       — latest snapshot of all prices
  GET /alerts       — current active threshold breaches
  GET /history      — last N snapshots
  GET /health       — simple health check
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import threading
import time
from datetime import datetime
from collections import deque

# Import our market data logic
from market_data import fetch_all_prices, THRESHOLDS

# ── Config ─────────────────────────────────────────────────────────────────────

POLL_INTERVAL = 5       # seconds between fetches
MAX_HISTORY = 100       # how many snapshots to keep in memory

# ── Shared state (thread-safe via lock) ────────────────────────────────────────

lock = threading.Lock()
latest_data: dict = {}
active_alerts: list = []
history: deque = deque(maxlen=MAX_HISTORY)

# ── Background poller ──────────────────────────────────────────────────────────

def compute_alerts(data: dict) -> list:
    """Return a list of current threshold breaches."""
    alerts = []

    for name, d in data.get("forex", {}).items():
        if d.get("price") and name in THRESHOLDS:
            t = THRESHOLDS[name]
            price = d["price"]
            if price < t["min"] or price > t["max"]:
                alerts.append({
                    "market": name,
                    "type": "FOREX",
                    "value": price,
                    "threshold": t["min"] if price < t["min"] else t["max"],
                    "direction": "below_min" if price < t["min"] else "above_max",
                    "timestamp": data["timestamp"],
                })

    wti = data.get("commodities", {}).get("WTI Crude Oil", {})
    brent = data.get("commodities", {}).get("Brent Crude Oil", {})
    if wti.get("price") and brent.get("price"):
        spread = round(brent["price"] - wti["price"], 3)
        t = THRESHOLDS.get("Brent-WTI Spread", {})
        if t and (spread < t["min"] or spread > t["max"]):
            alerts.append({
                "market": "Brent-WTI Spread",
                "type": "OIL_SPREAD",
                "value": spread,
                "threshold": t["min"] if spread < t["min"] else t["max"],
                "direction": "below_min" if spread < t["min"] else "above_max",
                "timestamp": data["timestamp"],
            })

    return alerts


def poller():
    """Background thread: fetch prices, compute alerts, update shared state."""
    global latest_data, active_alerts
    while True:
        try:
            data = fetch_all_prices()
            alerts = compute_alerts(data)
            with lock:
                latest_data = data
                active_alerts = alerts
                history.append({**data, "alerts": alerts})
        except Exception as e:
            print(f"[Poller error] {e}")
        time.sleep(POLL_INTERVAL)


# ── App startup/shutdown ───────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=poller, daemon=True)
    thread.start()
    print("Market poller started.")
    yield
    print("Shutting down.")

app = FastAPI(title="Arbitrage Monitor", lifespan=lifespan)

# Allow requests from the dashboard (running on localhost)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/prices")
def prices():
    with lock:
        if not latest_data:
            return {"status": "loading", "data": None}
        return {"status": "ok", "data": latest_data}


@app.get("/alerts")
def alerts():
    with lock:
        return {
            "status": "ok",
            "count": len(active_alerts),
            "alerts": active_alerts,
        }


@app.get("/history")
def get_history(limit: int = 20):
    with lock:
        recent = list(history)[-limit:]
    return {"status": "ok", "count": len(recent), "snapshots": recent}