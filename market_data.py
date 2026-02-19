"""
Market Data Fetcher
Pulls FOREX and Oil (WTI Crude) prices using yfinance.
Polls every N seconds and flags when spreads cross configurable thresholds.
Note: Data is delayed ~15 minutes. For live arbitrage, swap in a real-time API later.
"""

import yfinance as yf
import time
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────────────────────

FOREX_PAIRS = {
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "USDJPY=X",
    "USD/CHF": "USDCHF=X",
}

COMMODITIES = {
    "WTI Crude Oil": "CL=F",
    "Brent Crude Oil": "BZ=F",  # useful for oil arbitrage (WTI vs Brent spread)
}

# ── Thresholds (edit these to tune your alerts) ────────────────────────────────

THRESHOLDS = {
    "Brent-WTI Spread":   {"min": -1.0, "max": 5.0},   # flag if spread < -$1 or > $5
    "EUR/USD":            {"min": 1.05, "max": 1.15},   # flag if outside this range
    "GBP/USD":            {"min": 1.20, "max": 1.35},
}

POLL_INTERVAL = 5  # seconds between each fetch

# ── Alert state (tracks which alerts are "new" vs already fired) ───────────────
active_alerts: set = set()

# ── Fetch current price for a single ticker ────────────────────────────────────

def get_price(ticker_symbol: str) -> dict:
    """Fetch the latest price data for a given ticker."""
    ticker = yf.Ticker(ticker_symbol)
    info = ticker.fast_info  # faster than .info for price lookups

    try:
        price = info.last_price
        previous_close = info.previous_close
        change = price - previous_close
        change_pct = (change / previous_close) * 100

        return {
            "price": round(price, 5),
            "previous_close": round(previous_close, 5),
            "change": round(change, 5),
            "change_pct": round(change_pct, 3),
            "error": None,
        }
    except Exception as e:
        return {"price": None, "error": str(e)}


# ── Fetch all configured markets ───────────────────────────────────────────────

def fetch_all_prices() -> dict:
    """Fetch prices for all configured FOREX pairs and commodities."""
    results = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "forex": {}, "commodities": {}}

    for name, symbol in FOREX_PAIRS.items():
        results["forex"][name] = {"symbol": symbol, **get_price(symbol)}

    for name, symbol in COMMODITIES.items():
        results["commodities"][name] = {"symbol": symbol, **get_price(symbol)}

    return results


# ── Display ────────────────────────────────────────────────────────────────────

def print_prices(data: dict):
    """Pretty-print the fetched price data."""
    print(f"\n{'='*55}")
    print(f"  Market Snapshot  —  {data['timestamp']}  (15min delay)")
    print(f"{'='*55}")

    print("\n📈  FOREX\n" + "-"*40)
    for name, d in data["forex"].items():
        if d["error"]:
            print(f"  {name:<14}  ERROR: {d['error']}")
        else:
            arrow = "▲" if d["change"] >= 0 else "▼"
            print(f"  {name:<14}  {d['price']:<10}  {arrow} {d['change_pct']:+.3f}%")

    print("\n🛢️   Oil Futures\n" + "-"*40)
    for name, d in data["commodities"].items():
        if d["error"]:
            print(f"  {name:<20}  ERROR: {d['error']}")
        else:
            arrow = "▲" if d["change"] >= 0 else "▼"
            print(f"  {name:<20}  ${d['price']:<10}  {arrow} {d['change_pct']:+.3f}%")

    # Simple WTI vs Brent spread (classic oil arbitrage signal)
    wti = data["commodities"].get("WTI Crude Oil", {})
    brent = data["commodities"].get("Brent Crude Oil", {})
    if wti.get("price") and brent.get("price"):
        spread = round(brent["price"] - wti["price"], 3)
        print(f"\n  Brent–WTI Spread: ${spread}  {'(Brent premium)' if spread > 0 else '(WTI premium)'}")

    print(f"\n{'='*55}\n")


# ── Threshold checker ──────────────────────────────────────────────────────────

def check_thresholds(data: dict):
    """Compare current values against thresholds and print alerts for new breaches."""
    global active_alerts
    current_alerts = set()
    alert_lines = []

    # Check FOREX pairs
    for name, d in data["forex"].items():
        if d.get("price") and name in THRESHOLDS:
            t = THRESHOLDS[name]
            price = d["price"]
            if price < t["min"] or price > t["max"]:
                direction = "BELOW min" if price < t["min"] else "ABOVE max"
                limit = t["min"] if price < t["min"] else t["max"]
                key = f"{name}:{direction}"
                current_alerts.add(key)
                if key not in active_alerts:
                    alert_lines.append(f"  🚨  {name} {direction} threshold  |  price={price}  threshold={limit}")

    # Check Brent-WTI spread
    wti = data["commodities"].get("WTI Crude Oil", {})
    brent = data["commodities"].get("Brent Crude Oil", {})
    if wti.get("price") and brent.get("price"):
        spread = round(brent["price"] - wti["price"], 3)
        t = THRESHOLDS.get("Brent-WTI Spread", {})
        if t and (spread < t["min"] or spread > t["max"]):
            direction = "BELOW min" if spread < t["min"] else "ABOVE max"
            limit = t["min"] if spread < t["min"] else t["max"]
            key = f"Brent-WTI:{direction}"
            current_alerts.add(key)
            if key not in active_alerts:
                alert_lines.append(f"  🚨  Brent-WTI spread {direction} threshold  |  spread=${spread}  threshold=${limit}")

    # Print new alerts
    if alert_lines:
        print(f"\n{'!'*55}")
        print("  ⚠️   ARBITRAGE ALERT")
        print(f"{'!'*55}")
        for line in alert_lines:
            print(line)
        print(f"{'!'*55}\n")

    # Print resolved alerts
    resolved = active_alerts - current_alerts
    for key in resolved:
        print(f"  ✅  Alert resolved: {key}")

    active_alerts = current_alerts


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting market monitor — polling every 5 seconds. Press Ctrl+C to stop.\n")
    try:
        while True:
            data = fetch_all_prices()
            print_prices(data)
            check_thresholds(data)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")