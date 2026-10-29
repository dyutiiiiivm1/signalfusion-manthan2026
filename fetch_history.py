# =====================================================================
#  SignalFusion - DOWNLOAD PAST PRICES (for testing the strategy on many years)
#
#  Run:   python fetch_history.py
#  It asks the server for up to 10 years of past daily prices for every stock
#  and saves them in  history_all.csv . Send that file to Claude.
#  It places NO orders and uses only ~10-50 requests (well under the limit).
# =====================================================================

import csv
import time

import requests

import config

BASE_URL = f"http://{config.SERVER_IP}:3002/api/v1"
HEADERS = {"Authorization": f"Bearer {config.ACCESS_TOKEN}"}
TRY_DAYS = [3650, 2500, 1825, 1095, 730, 365]   # 10 years first; shorter if the stock is younger


def get(path):
    try:
        reply = requests.get(BASE_URL + path, headers=HEADERS, timeout=15)
        return reply.status_code, reply.json()
    except Exception as error:
        return None, {"error": str(error)}


status, body = get("/stocks")
tickers = [s.get("ticker") or s.get("symbol") for s in (body or {}).get("stocks", [])] if isinstance(body, dict) else []
if not tickers:
    print(f"Could not load the stock list (HTTP {status}): {body}")
    raise SystemExit(1)
print(f"Stocks: {', '.join(tickers)}")

rows = 0
with open("history_all.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["ticker", "date", "open", "high", "low", "close", "volume"])
    for ticker in tickers:
        prices = None
        for days in TRY_DAYS:
            status, body = get(f"/stocks/{ticker}/history?days={days}")
            if isinstance(body, dict) and body.get("prices"):
                prices = body["prices"]
                break
            time.sleep(0.6)
        if not prices:
            print(f"{ticker}: no history ({body})")
            continue
        for bar in prices:
            writer.writerow([ticker, bar.get("date"), bar.get("open"), bar.get("high"), bar.get("low"),
                             bar.get("close"), bar.get("volume")])
        rows += len(prices)
        print(f"{ticker}: {len(prices)} days, {prices[0].get('date')} to {prices[-1].get('date')}")
        time.sleep(0.6)

print(f"\nDONE. Saved {rows} rows to history_all.csv - send that file to Claude.")
