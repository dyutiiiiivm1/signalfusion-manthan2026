# SignalFusion — autonomous momentum trading bot

**Team Byte Force (MN2616) · MANTHAN 2026 · Real-Time Predictive Analytics**

SignalFusion trades a live stock market by itself. It reads prices and news from Kafka, always owns the
2 strongest stocks by their 6-month price change, and saves a plain-English reason for every order.

## Strategy (v4.1, momentum rotation)

- Every 3 game days, rank all stocks by their price change over the last 120 trading days.
- Own the top 2, about 49% of our money each (98% invested, 2% cash for fees).
- Keep a stock while it stays in the top 4, so we don't swap back and forth.
- Never own a stock whose 6-month change is negative.
- Sell any stock that falls 15% below our buy price.
- News is only a safety brake: serious bad news that names a company blocks buying it for 2 days.

Tested with the exact bot code on 46 separate 500-day periods of the organizers' price history
(2018–2026): beat buy-and-hold in 43 of 46, typical result +64% (buy-and-hold +40%), worst period -5%.

## Reliability

- Kafka consumer (`market.ticks`, `market.news`), live messages only, auto-reconnect
- Answers every pre-flight heartbeat; start-up loading runs in the background
- Own request limit of 100/min (server limit 120)
- Ignores stale or repeated data; no trades on backed-up days after a wifi blip
- `run_bot.py` restarts the bot within 3 seconds after a crash; it re-reads cash and holdings from the server
- Re-reads cash after a rejected order (for example after the year-end tax deduction)

## Files

| File | What it is |
|---|---|
| `bot_v4.py` | The bot (v4.1) |
| `run_bot.py` | Launcher with automatic restart |
| `dashboard.html` | Live dashboard, served at http://localhost:8000 while the bot runs |
| `check_connection.py` | Tests the API and Kafka connection |
| `fetch_history.py` | Downloads past prices for testing |
| `bot_v3.py` | Previous version (200-day trend), kept for reference |
| `requirements.txt` | Python packages |

`config.py` (server address, access token, consumer group) is private and is not in this repository.

## How to run

```
pip install -r requirements.txt
python run_bot.py
```

Create `config.py` next to the bot first:

```
SERVER_IP = "<server ip>"
ACCESS_TOKEN = "<your token>"
CONSUMER_GROUP = "<your consumer group, lowercase>"
DRY_RUN = False
```

While running, the bot writes `decisions.csv` (every order with its reason), `bot_log.txt`,
`market_data.csv`, `news_log.csv` and `status.json` (for the dashboard).
