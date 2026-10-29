# =====================================================================
#  SignalFusion - CONNECTION CHECK
#  Run this FIRST. It tests every connection and saves what the server
#  replies into "check_output.txt". Send that file to Claude.
#  It does NOT place any orders.
# =====================================================================

import json
import time

import requests
from kafka import KafkaConsumer

import config

BASE_URL = f"http://{config.SERVER_IP}:3002/api/v1"
KAFKA_BROKER = f"{config.SERVER_IP}:9094"
HEADERS = {"Authorization": f"Bearer {config.ACCESS_TOKEN}"}

output = open("check_output.txt", "w", encoding="utf-8")


def show(title, text):
    """Print a section on screen and save it to check_output.txt."""
    block = f"\n===== {title} =====\n{text}\n"
    print(block)
    output.write(block)
    output.flush()


def pretty(text, limit=2500):
    """Make JSON easy to read and cut it if it is very long."""
    try:
        text = json.dumps(json.loads(text), indent=2)
    except ValueError:
        pass
    return text if len(text) <= limit else text[:limit] + "\n... (cut, too long)"


def try_get(path, use_token=True):
    """Ask the server for something and show the reply."""
    try:
        reply = requests.get(BASE_URL + path, headers=HEADERS if use_token else {}, timeout=5)
        show(f"GET {path}  ->  HTTP {reply.status_code}", pretty(reply.text))
        try:
            return reply.json()
        except ValueError:
            return None
    except requests.RequestException as error:
        show(f"GET {path}  ->  FAILED", f"{error}\n(Are you on the venue wifi? Is SERVER_IP correct?)")
        return None


# Step 1: Check the settings were filled in
if "PUT_" in config.SERVER_IP or "PUT_" in config.ACCESS_TOKEN or "PUT_" in config.CONSUMER_GROUP:
    print("STOP: open config.py and fill in SERVER_IP, ACCESS_TOKEN and CONSUMER_GROUP first.")
    raise SystemExit

show("SETTINGS", f"Server: {config.SERVER_IP}\nConsumer group: {config.CONSUMER_GROUP}\nPractice mode: {config.DRY_RUN}")

# Step 2: Test the REST API (the "order desk")
try_get("/stage", use_token=False)
try_get("/simulation/clock", use_token=False)
stocks_reply = try_get("/stocks")
try_get("/portfolio")
try_get("/portfolio/cash")
try_get("/orders")

# Step 3: Test past prices for the first company
first_ticker = None
if isinstance(stocks_reply, dict) and stocks_reply.get("stocks"):
    first = stocks_reply["stocks"][0]
    first_ticker = first.get("ticker") or first.get("symbol")
if first_ticker:
    try_get(f"/stocks/{first_ticker}/history?days=10")
else:
    show("HISTORY", "Skipped - could not read the stock list.")

# Step 4: Test the live feed (Kafka) for 20 seconds
show("LIVE FEED", f"Connecting to {KAFKA_BROKER} ... listening for 20 seconds")
try:
    consumer = KafkaConsumer(
        "market.ticks",
        "market.news",
        bootstrap_servers=[KAFKA_BROKER],
        group_id=config.CONSUMER_GROUP.strip().lower(),
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    counts = {}
    samples = {}
    end_time = time.time() + 20
    while time.time() < end_time:
        batch = consumer.poll(timeout_ms=500)
        for _, messages in batch.items():
            for message in messages:
                try:
                    event = json.loads(message.value.decode("utf-8"))
                    kind = event.get("event_type") or event.get("event") or "unknown"
                except ValueError:
                    event, kind = message.value[:300], "not-json"
                counts[kind] = counts.get(kind, 0) + 1
                samples.setdefault(kind, [])
                if len(samples[kind]) < 2:
                    samples[kind].append(event)
    consumer.close()
    show("LIVE FEED - messages received", json.dumps(counts, indent=2) if counts else "NONE received in 20 seconds")
    for kind, examples in samples.items():
        show(f"LIVE FEED - example '{kind}'", json.dumps(examples, indent=2, default=str)[:2500])
except Exception as error:
    show("LIVE FEED  ->  FAILED", f"{error}\n(Check the Kafka address, the wifi, and that kafka-python is installed.)")

output.close()
print("\nDONE. Send the file 'check_output.txt' to Claude (it does not contain your token).")
