# =====================================================================
#  SignalFusion - VERSION 4
#  MANTHAN 2026 - Autonomous Trading Systems Challenge - Team Byte Force
#
#  New in Version 4 - MOMENTUM ROTATION (tested on 8.4 years of real data):
#    Every 3 game days, rank all stocks by how much they rose in the last 120 trading
#    days (about 6 months). Own the TOP 2, about 49% of our money each (98% invested).
#    Keep a stock while it is still in the top 4 (so we don't swap back and forth and
#    waste fees and tax). Never own a stock whose 6-month change is negative.
#    15% safety stop on every stock. Empty slots are refilled every day.
#    Tested with this exact code on 46 separate 500-day periods (2019-2026): beat
#    buy-and-hold in 43 of 46, typical result +64% (buy-and-hold +40%, v3 +19%),
#    worst period -5%. 900-day periods: beat buy-and-hold in 17 of 17, worst +40%.
#    On the test-run dates (Oct 2024 - Oct 2026): +106% before tax (v3: about +15%).
#
#  Kept from Version 3: crash recovery (python run_bot.py), tax tracking per Indian
#  financial year, double-buy guard, Reset Cash detection, wifi catch-up guard.
#
#  Everything from Version 1 (connect, heartbeat, stage watcher, safe orders,
#  stop-loss, trend + RSI strategy, speed limiter, auto-reconnect, decision log)
#  PLUS, new in Version 2:
#    1. Signal board: every stock gets votes (Trend, RSI, News) and a verdict
#    2. News safety brake: serious bad news that NAMES the company
#       (fraud, probe, lawsuit, recall...) blocks buying it for 2 days
#    3. Market mood: Calm / Normal / Wild (shown on the dashboard)
#    4. Alerts, wealth history, speed stats, fee totals, server scorecard
#    5. Built-in dashboard: open  http://localhost:8000  while the bot runs
#
#  How to run:   python run_bot.py      (auto-restart)   or   python bot_v4.py
#  Stop it with Ctrl + C
# =====================================================================

import csv
import json
import os
import re
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import date as Date, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests
from kafka import KafkaConsumer

import config

BOT_VERSION = "v4.1"   # shown in the first line when the bot starts, so you can check which file is running

# ---------------------------------------------------------------------
# Step 1: Addresses (taken from config.py)
# ---------------------------------------------------------------------
BASE_URL = f"http://{config.SERVER_IP}:3002/api/v1"
KAFKA_BROKER = f"{config.SERVER_IP}:9094"
CONSUMER_GROUP = config.CONSUMER_GROUP.strip().lower()  # organizers require all lowercase
HEADERS = {"Authorization": f"Bearer {config.ACCESS_TOKEN}", "Content-Type": "application/json"}
DRY_RUN = config.DRY_RUN

# ---------------------------------------------------------------------
# Step 2: Strategy and safety settings (we will tune these by testing)
# ---------------------------------------------------------------------
TRADE_STAGES = ("simulation", "development")  # stages where trading is allowed
COLLECT_WINDOW_SEC = 0.4       # wait this long after a new day starts, so news arrives (news comes at +0.15 s)
DECISION_DEADLINE_SEC = 2.5    # never send orders later than this into a day (live days are 3-4 s)
LONG_MA_DAYS = 200             # the long-term average: a stock is "in an uptrend" while its price is above this
TREND_BUFFER = 0.02            # [v3 only, not used by v4] need to be 2% above (to buy) or 2% below (to sell) the average - avoids flip-flopping
STOP_LOSS_PCT = 0.15           # safety stop: sell if a stock falls 15% below what we paid (big crashes only)
CASH_RESERVE_PCT = 0.10        # [v3 only, not used by v4] always keep 10% of our wealth as spare cash
MAX_PER_STOCK_PCT = 0.15       # [v3 only, not used by v4] never put more than 15% of wealth in one company (tested best: 23/23 periods profitable)
MAX_HOLDINGS = 12              # [v3 only, not used by v4] own at most 12 companies at once
MAX_BUYS_PER_DAY = 8           # [v3 only, not used by v4] buy at most 8 companies in one game day (keeps us far below the request limit)
MIN_TRADE_VALUE = 10000.0      # skip trades smaller than Rs 10,000 (fees would eat the profit)
RSI_DAYS = 14                  # RSI looks at the last 14 days
RSI_MAX_TO_BUY = 70            # [v3 only, not used by v4] don't buy a stock that is "overheated" (RSI above 70) - tested: big improvement
END_GAME_DAYS = 3              # stop new buys in the last 3 days of the live match
PRACTICE_LOOP_EXIT_DAYS = 2    # practice market only: sell everything 2 days before it loops back to day 1
GAP_RELOAD_DAYS = 10           # if the market jumped ahead more than 10 days (bot was off), reload past prices
CATCH_UP_GAP_SEC = 1.0         # if a new game day arrives less than 1 s after the last one, it's old backed-up data:
                               # we update prices but do NOT trade on it (real days come every 3-10 s)
TOP_K = 2                      # v4.1: own the 2 strongest stocks
KEEP_RANK = 4                  # v4.1: keep a stock while it is still in the top 4 (fewer swaps = less fees and tax)
MOMENTUM_DAYS = 120            # v4: strength = price change over the last 120 trading days (~6 months)
REBALANCE_EVERY = 3            # v4.1: re-rank and swap every 3 game days (empty slots are filled every day)
INVEST_PCT = 0.98              # v4.1: keep 98% of wealth invested, 2% spare cash for fees
COOLDOWN_DAYS = 5              # after selling a stock, don't buy it back for 5 game days
BROKERAGE = 20.0               # fee on every order
DP_CHARGE = 15.34              # extra fee when selling shares we own
MAX_REQUESTS_PER_MIN = 100     # our own limit (the server's limit is 120)
HEARTBEAT_RESERVE = 10         # keep 10 requests per minute free for heartbeats
SYNC_EVERY_SEC = 30            # double-check our money with the server every 30 s
SYNC_QUIET_SEC = 15            # ...but never in the 15 s right after we sent an order (the server needs a moment
                               # to record it - checking too early made us think we didn't own a stock we just bought)
REBUY_GUARD_DAYS = 3           # never buy the same stock again within 3 game days of buying it
HISTORY_DAYS = 420             # past calendar days to load at start (~290 trading days, enough for the 200-day average)
STARTING_CASH = 1000000.0      # Rs 10,00,000
NEWS_VETO_DAYS = 2             # serious bad news blocks buying that company for 2 game days
MOOD_DAYS = 10                 # market mood looks at the last 10 days of price moves
MOOD_CALM_BELOW = 1.35         # average daily move below 1.35% = Calm   (measured from practice data)
MOOD_WILD_ABOVE = 2.0          # average daily move above 2.0%  = Wild   (top 10% of practice days)
WILD_MAX_BUYS = 8              # [v3 only, not used by v4] Market mood is shown on the dashboard. With the long-term strategy, cutting buys in a
WILD_SIZE_FACTOR = 1.0         # [v3 only, not used by v4] "Wild" market tested no better, so it no longer changes trade size (the 200-day rule
                               # plus the emergency drawdown brake already handle rough markets)
DASHBOARD_PORT = 8000          # dashboard address: http://localhost:8000
DRAWDOWN_LOOKBACK_DAYS = 60    # "recent peak" = highest wealth in the last 60 game days
DRAWDOWN_SLOW_PCT = 0.10       # [v3 only, not used by v4] emergency brake: 10% below the recent peak -> buy half size, at most 1 stock a day
DRAWDOWN_DEEP_PCT = 0.20       # [v3 only, not used by v4] 20% below the recent peak -> quarter size (never fully stops, so it can recover)
                               # Tested: a 6% brake HURT (it bought too little on rebounds). 10% never triggers in a
                               # normal year, so it costs nothing - it only protects us in a real crash.
TAX_HARVEST_FROM_DAY = 15      # [v3 only, not used by v4] tax-loss harvesting runs from 15 March to 31 March (end of the Indian financial year)
TAX_HARVEST_MIN_LOSS = 0.02    # [v3 only, not used by v4] only harvest stocks that are at least 2% below what we paid
LTCG_DAYS = 365                # held MORE than 365 days = long-term (10% tax instead of 15%)
LTCG_WAIT_FROM_DAYS = 335      # [v3 only, not used by v4] a winner held 335-365 days waits for long-term status instead of selling on a trend dip
STCG_RATE = 0.15
LTCG_RATE = 0.10
LTCG_EXEMPT = 100000.0         # first Rs 1,00,000 of long-term gains each year is tax-free (Section 112A)

# Files the bot writes
LOG_FILE = "bot_log.txt"
DECISIONS_FILE = "decisions.csv"
MARKET_FILE = "market_data.csv"
NEWS_FILE = "news_log.csv"
STATUS_FILE = "status.json"
PORTFOLIO_RAW_FILE = "portfolio_raw.json"


# ---------------------------------------------------------------------
# Step 3: Small helpers (printing, saving, reading numbers)
# ---------------------------------------------------------------------
log_lock = threading.Lock()


def log(message):
    """Print a message with the time, and also save it to bot_log.txt."""
    line = f"{datetime.now().strftime('%H:%M:%S')}  {message}"
    print(line, flush=True)
    with log_lock:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def append_csv(path, header, row):
    """Add one row to a CSV (spreadsheet) file. Writes the header the first time."""
    is_new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(header)
        writer.writerow(row)


def to_float(value, default=None):
    """Turn something into a number safely. Returns default if it isn't a number."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def find_value(data, keys, depth=0):
    """Search a server reply for the first matching field name (e.g. 'cash').
    We do this because we don't know the exact reply format yet."""
    if depth > 4:
        return None
    if isinstance(data, dict):
        for key in keys:
            if key in data and data[key] is not None and not isinstance(data[key], (dict, list)):
                return data[key]
        for value in data.values():
            if isinstance(value, (dict, list)):
                found = find_value(value, keys, depth + 1)
                if found is not None:
                    return found
    elif isinstance(data, list):
        for value in data:
            found = find_value(value, keys, depth + 1)
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------------
# Step 4: Speed limiter - never send too many requests
#   It never makes the bot wait. If we are near the limit, it simply
#   skips the request (heartbeats always get priority).
# ---------------------------------------------------------------------
request_times = deque()
penalty_until = 0.0
limiter_lock = threading.Lock()


def request_allowed(priority=False):
    now = time.time()
    if now < penalty_until:
        return False
    with limiter_lock:
        while request_times and now - request_times[0] > 60:
            request_times.popleft()
        limit = MAX_REQUESTS_PER_MIN if priority else MAX_REQUESTS_PER_MIN - HEARTBEAT_RESERVE
        if len(request_times) >= limit:
            return False
        request_times.append(now)
        return True


_local = threading.local()


def http_session():
    """Re-use one open connection per thread (much faster than reconnecting for every request)."""
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


def api_call(method, path, payload=None, priority=False):
    """Talk to the order desk (REST API).
    Returns (http_status, reply). http_status is None if nothing was sent."""
    global penalty_until
    if not request_allowed(priority):
        return None, None
    try:
        reply = http_session().request(method, BASE_URL + path, headers=HEADERS, json=payload, timeout=2.5)
    except requests.RequestException as error:
        log(f"Network problem on {path}: {error.__class__.__name__}")
        return None, None
    if reply.status_code == 429:
        penalty_until = time.time() + 31
        log("RATE LIMITED by the server - pausing all requests for 31 seconds")
    try:
        body = reply.json()
    except ValueError:
        body = None
    return reply.status_code, body


# ---------------------------------------------------------------------
# Step 5: Game stage watcher (runs in the background every 2.5 s)
# ---------------------------------------------------------------------
stage_info = {"stage": None, "status": None, "days_completed": 0, "total_days": 0}
trading_paused_until = 0.0


def watch_stage():
    last_seen = None
    while True:
        try:
            reply = requests.get(BASE_URL + "/stage", timeout=2)
            if reply.status_code == 200:
                data = reply.json()
                if "stage" not in data and isinstance(data.get("data"), dict):
                    data = data["data"]
                stage_info.update(data)
                now_seen = (data.get("stage"), data.get("status"))
                if now_seen != last_seen:
                    log(f"GAME STAGE: {now_seen[0]} / {now_seen[1]} "
                        f"(day {data.get('days_completed')} of {data.get('total_days')})")
                    last_seen = now_seen
        except Exception:
            pass  # if the check fails once, we simply try again
        time.sleep(2.5)


def trading_allowed():
    # The server writes "running" in small letters, so we compare without caring about capitals
    stage = str(stage_info.get("stage") or "").lower()
    status = str(stage_info.get("status") or "").upper()
    return stage in TRADE_STAGES and status == "RUNNING" and time.time() >= trading_paused_until


def days_left():
    total = stage_info.get("total_days") or 0
    done = stage_info.get("days_completed") or 0
    return total - done if total else None


# ---------------------------------------------------------------------
# Step 6: Company list and past prices
# ---------------------------------------------------------------------
day_number = 0                                     # counts game days
stocks = {}                                        # ticker -> name and description
closes = defaultdict(lambda: deque(maxlen=400))    # ticker -> recent closing prices
last_price = {}                                    # ticker -> latest closing price


def load_stocks():
    status, body = api_call("GET", "/stocks")
    items = []
    if isinstance(body, dict):
        items = body.get("stocks") or body.get("data") or []
    elif isinstance(body, list):
        items = body
    for item in items:
        if isinstance(item, dict):
            ticker = item.get("ticker") or item.get("symbol")
            if ticker:
                stocks[ticker] = {"name": item.get("name", ""), "description": item.get("description", "")}
    if stocks:
        log(f"Found {len(stocks)} companies: {', '.join(stocks)}")
    else:
        log(f"Could not load the company list yet (HTTP {status}). Will learn companies from the live feed.")
    return bool(stocks)


def warm_start(ticker):
    """Load past prices for one company so our averages work from day 1."""
    status, body = api_call("GET", f"/stocks/{ticker}/history?days={HISTORY_DAYS}")
    bars = body.get("prices") if isinstance(body, dict) else None
    if not bars:  # the portal also lists this second address format - try it
        status, body = api_call("GET", f"/stocks/history?ticker={ticker}&days={HISTORY_DAYS}")
        bars = body.get("prices") if isinstance(body, dict) else None
    if not bars:
        log(f"No past prices for {ticker} (HTTP {status})")
        return
    bars = sorted((b for b in bars if isinstance(b, dict)), key=lambda b: str(b.get("date", "")))
    fresh = deque(maxlen=400)
    for bar in bars:
        price = to_float(bar.get("close"))
        if price and price > 0:
            fresh.append(price)
    if not fresh:
        return
    closes[ticker] = fresh   # swap in the whole new list at once (safe while the bot is running)
    if closes[ticker]:
        last_price[ticker] = closes[ticker][-1]
    log(f"Loaded {len(closes[ticker])} past prices for {ticker}")


def warm_start_all():
    for ticker in list(stocks):
        warm_start(ticker)


# ---------------------------------------------------------------------
# Step 7: Money tracker - our own record of cash and shares,
#         double-checked with the server every 30 seconds
# ---------------------------------------------------------------------
cash = STARTING_CASH
holdings = {}     # ticker -> {"qty": shares, "avg": price we paid, "peak": best price since buying}
saved_raw_portfolio = False
synced_once = False   # v4.1: no trading until we have read our real holdings from the server once


def orders_settling():
    """True for a few seconds after we send orders, while the server is still recording them."""
    return time.time() - last_order_time < SYNC_QUIET_SEC


def sync_cash(force=False):
    """force=True reads the server's cash even right after an order (used when an order was
    rejected for lack of money - e.g. after the server deducted tax at the end of a financial year)."""
    global cash
    if DRY_RUN or (orders_settling() and not force):
        return
    status, body = api_call("GET", "/portfolio/cash")
    value = to_float(find_value(body, ["available_cash", "cash", "capital_remaining", "cash_balance", "balance"]))
    if value is not None:
        cash = value


def extract_positions(body):
    """Find the list of shares we own inside the server's reply."""
    containers = [body]
    if isinstance(body, dict):
        for key in ("data", "portfolio"):
            if isinstance(body.get(key), dict):
                containers.append(body[key])
    for container in containers:
        if not isinstance(container, dict):
            continue
        for key in ("holdings", "positions", "long_positions"):
            value = container.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                return [dict(info, ticker=info.get("ticker", t)) for t, info in value.items() if isinstance(info, dict)]
    return None


def sync_holdings():
    global saved_raw_portfolio
    if DRY_RUN or orders_settling():
        return
    status, body = api_call("GET", "/portfolio")
    if body is None:
        return
    if not saved_raw_portfolio:
        with open(PORTFOLIO_RAW_FILE, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=2)
        saved_raw_portfolio = True
    if isinstance(body, dict):
        for key in ("total_wealth", "cash_utilization_pct", "avg_cash_utilization_pct", "error_count",
                    "rejected_orders_count", "total_orders", "total_charges", "estimated_taxes", "is_blocked",
                    "realized_pnl", "unrealized_pnl", "total_pnl", "total_pnl_pct", "total_taxes", "margin_locked"):
            if key in body:
                server_stats[key] = body[key]
    global synced_once
    items = extract_positions(body)
    if items is None:
        log("Could not read the holdings format - keeping our own record. (Send portfolio_raw.json to Claude.)")
        return
    server_wealth = to_float(body.get("total_wealth")) if isinstance(body, dict) else None
    has_positions = any(int(to_float(i.get("quantity", i.get("qty")), 0) or 0) != 0 for i in items if isinstance(i, dict))
    if (server_wealth is not None and abs(server_wealth - STARTING_CASH) < 1.0 and not has_positions
            and (holdings or (wealth_history and abs(wealth_history[-1]["wealth"] - STARTING_CASH) > 1000))):
        reset_local_records()
    fresh = {}
    for item in items:
        ticker = item.get("ticker") or item.get("symbol")
        qty = int(to_float(item.get("quantity", item.get("qty")), 0) or 0)
        side = str(item.get("side", item.get("type", item.get("position_type", "LONG")))).upper()
        if not ticker or qty <= 0 or "SHORT" in side or item.get("is_short"):
            continue
        avg = to_float(item.get("avg_buy_price", item.get("avg_price", item.get("average_price",
                       item.get("avg_cost", item.get("buy_price"))))))
        old = holdings.get(ticker, {})
        avg = avg or old.get("avg") or last_price.get(ticker)
        if not avg:
            continue
        fresh[ticker] = {"qty": qty, "avg": avg, "peak": max(old.get("peak", avg), avg),
                         "bought": old.get("bought") or current_day}
    # never forget a stock we bought in the last 60 s, even if the server hasn't shown it yet
    for ticker, h in holdings.items():
        if ticker not in fresh and time.time() - last_bought_time.get(ticker, 0) < 60:
            fresh[ticker] = h
    holdings.clear()
    holdings.update(fresh)
    if not synced_once:
        synced_once = True
        log(f"Synced with the server: {len(holdings)} stocks held, cash Rs {cash:,.0f}")


def reset_local_records():
    """The portal's Reset Cash button was pressed: start our own records fresh so old numbers
    (like a big drop) don't trigger the emergency brake or confuse the dashboard."""
    global cash, fees_paid
    holdings.clear()
    cash = STARTING_CASH
    wealth_history.clear()
    tax_book.clear()
    harvest_log.clear()
    last_sold_day.clear()
    last_bought_day.clear()
    last_bought_time.clear()
    trade_marks.clear()
    fees_paid = 0.0
    for key in trade_stats:
        trade_stats[key] = 0 if key in ("closed", "wins", "losses") else (0.0 if key == "realized" else None)
    for key in order_counts:
        order_counts[key] = 0
    log("Reset Cash detected on the portal - starting our records fresh at Rs 10,00,000")
    add_alert("info", "Reset Cash detected - records restarted at Rs 10,00,000")


def total_wealth():
    return cash + sum(h["qty"] * last_price.get(t, h["avg"]) for t, h in holdings.items())


# ---------------------------------------------------------------------
# Step 7b: News reader (safety brake) and market mood
#   We TESTED news on practice data: headlines did not predict price moves
#   (most are general news). So news never triggers a buy. It only blocks
#   buying a company when serious bad news NAMES that company.
# ---------------------------------------------------------------------
SERIOUS_BAD = re.compile(r"\b(fraud|probe|probes|investigation|investigated|lawsuit|lawsuits|sued|sues|recall|recalls|"
                         r"bankrupt|bankruptcy|default|downgrade|downgraded|subpoena|indicted|indictment|scandal|"
                         r"breach|hacked|antitrust|resigns|resignation|ousted|delist|delisted|plunge|plunges|"
                         r"crash|crashes|layoffs|fined|penalty|warning)\b")
GOOD_WORDS = re.compile(r"\b(surge|surges|record|beat|beats|profit|profits|growth|dividend|upgrade|upgraded|"
                        r"partnership|deal|wins|launch|launches|soar|soars|rally|rallies|buyback|expansion|jump|jumps)\b")
NOT_WORDS = re.compile(r"\b(not|no|denies|denied|dismissed|cleared|ends|ended|settles|settled|settlement)\b")

news_feed = deque(maxlen=40)       # latest headlines with their scores (for the dashboard)
news_today = defaultdict(int)      # ticker -> today's news score
veto_until = {}                    # ticker -> last game day the news brake is active
company_keys = {}                  # ticker -> the word that identifies the company in headlines


def company_key(ticker):
    """The company's own name word (e.g. 'havenstay'), used to check a headline is really about it."""
    if ticker in company_keys:
        return company_keys[ticker]
    name = (stocks.get(ticker, {}).get("name") or "").lower()
    words = re.findall(r"[a-z0-9&']+", name)
    key = None
    if words:
        key = words[0] if len(words[0]) >= 6 or len(words) == 1 else " ".join(words[:2])
    company_keys[ticker] = key
    return key


def score_news(ticker, headline, description):
    """Returns (score, veto). Score is for display; veto = serious bad news that names the company."""
    text = f"{headline} {description}".lower()
    good = len(GOOD_WORDS.findall(text))
    bad = len(SERIOUS_BAD.findall(text))
    key = company_key(ticker)
    about_company = bool(key) and re.search(r"\b" + re.escape(key) + r"\b", text) is not None
    negated = NOT_WORDS.search(text) is not None
    score = good - bad
    veto = about_company and bad > 0 and not negated
    if veto:
        score = min(score, -3)
    return score, veto


def news_brake_active(ticker):
    return veto_until.get(ticker, -1) >= day_number


market_mood = "Normal"
mood_value = 0.0


def update_mood():
    """Average size of daily price moves over the last 10 days, across all stocks."""
    global market_mood, mood_value
    moves = []
    for ticker in list(stocks):
        prices = list(closes[ticker])[-(MOOD_DAYS + 1):]
        if len(prices) >= 3:
            moves.append(sum(abs(prices[k] / prices[k - 1] - 1) for k in range(1, len(prices))) / (len(prices) - 1) * 100)
    if not moves:
        return
    mood_value = sum(moves) / len(moves)
    new_mood = "Calm" if mood_value < MOOD_CALM_BELOW else ("Wild" if mood_value > MOOD_WILD_ABOVE else "Normal")
    if new_mood != market_mood:
        add_alert("warn" if new_mood == "Wild" else "info",
                  f"Market mood changed to {new_mood} (avg daily move {mood_value:.2f}%)")
    market_mood = new_mood


# Things the dashboard shows
alerts = deque(maxlen=25)
wealth_history = deque(maxlen=600)
signals = {}
day_times = deque(maxlen=200)
fees_paid = 0.0
order_counts = {"total": 0, "filled": 0, "rejected": 0, "practice": 0}
trade_stats = {"closed": 0, "wins": 0, "losses": 0, "realized": 0.0, "best": None, "best_ticker": None,
               "worst": None, "worst_ticker": None}
trade_marks = deque(maxlen=400)    # buy/sell points for the dashboard chart
server_stats = {}


def add_alert(level, text):
    alerts.appendleft({"time": datetime.now().strftime("%H:%M:%S"), "level": level, "text": text})


# ---------------------------------------------------------------------
# Step 7c: Tax book (Indian financial year = 1 April to 31 March) and drawdown brake
# ---------------------------------------------------------------------
tax_book = defaultdict(lambda: {"st": 0.0, "lt": 0.0})   # "FY2025-26" -> short-term and long-term net gains
harvest_log = []                                           # tax-loss harvests we made (for the dashboard)


def fy_of(sim_date):
    """Indian financial year label for a 'YYYY-MM-DD' date, e.g. 2026-02-10 -> 'FY2025-26'."""
    try:
        d = Date.fromisoformat(str(sim_date)[:10])
    except ValueError:
        return "FY?"
    start = d.year if d.month >= 4 else d.year - 1
    return f"FY{start}-{str(start + 1)[-2:]}"


def estimated_tax(fy):
    """Tax the platform would charge for one financial year, after loss offsets (Sections 70/71/112A)."""
    st, lt = tax_book[fy]["st"], tax_book[fy]["lt"]
    if st < 0:            # short-term losses can also cancel long-term gains
        lt += st
        st = 0.0
    return STCG_RATE * max(0.0, st) + LTCG_RATE * max(0.0, lt - LTCG_EXEMPT)


def drawdown_now():
    """How far (0.0 - 1.0) our wealth is below its highest point in the recent past."""
    recent = list(wealth_history)[-DRAWDOWN_LOOKBACK_DAYS:]
    if not recent:
        return 0.0
    peak = max(p["wealth"] for p in recent)
    return max(0.0, (peak - recent[-1]["wealth"]) / peak) if peak > 0 else 0.0


# ---------------------------------------------------------------------
# Step 8: Placing orders safely + recording every decision
# ---------------------------------------------------------------------
recent_decisions = deque(maxlen=30)
last_sold_day = {}      # ticker -> game day we last sold it
DECISION_HEADER = ["time", "sim_date", "ticker", "action", "quantity", "price", "reason", "result", "decision_ms"]


def record_decision(sim_date, ticker, action, qty, price, reason, result, decision_ms):
    row = [datetime.now().strftime("%H:%M:%S"), sim_date, ticker, action, qty, round(price, 2), reason, result, decision_ms]
    append_csv(DECISIONS_FILE, DECISION_HEADER, row)
    recent_decisions.appendleft(dict(zip(DECISION_HEADER, row)))


def handle_rejection(error_text):
    """When the server says no, react so we don't repeat the mistake."""
    global trading_paused_until
    error = error_text.upper()
    add_alert("danger", f"Order rejected: {error_text}")
    if "SIMULATION_NOT_RUNNING" in error:
        trading_paused_until = time.time() + 10
    elif "NOT_VERIFIED" in error:
        log("!!! TEAM NOT VERIFIED - go to the registration desk !!!")
        trading_paused_until = time.time() + 60
    elif "INSUFFICIENT" in error or "BLOCKED" in error:
        sync_cash(force=True)
        sync_holdings()
    elif "POSITION" in error or "QUANTITY" in error:
        sync_holdings()


def send_to_server(order, sim_date):
    """Send one order and read the reply. Safe to run for several orders at the same time."""
    if DRY_RUN:
        return True, "PRACTICE (not sent)", None
    status, body = api_call("POST", "/orders", {"ticker": order["ticker"], "action": order["action"],
                                                "quantity": order["qty"], "date": sim_date})
    if status is None:
        return False, "NOT SENT (limit or network)", None
    failed = status >= 400
    if isinstance(body, dict):
        failed = failed or str(body.get("status", "")).lower() in ("error", "rejected", "failed") \
            or body.get("success") is False
    error = (find_value(body, ["error_code", "code", "error", "reason", "message"]) or f"HTTP {status}") if failed else None
    return error is None, ("FILLED" if error is None else f"REJECTED: {error}"), error


def apply_result(order, ok, result, error, sim_date, day_start):
    """Update our own money record and the decision log for one order (runs one at a time)."""
    global cash, fees_paid
    ticker, action, qty, price = order["ticker"], order["action"], order["qty"], order["price"]
    decision_ms = int((time.time() - day_start) * 1000)
    order_counts["total"] += 1
    if DRY_RUN:
        order_counts["practice"] += 1
    elif ok:
        order_counts["filled"] += 1
    elif result.startswith("REJECTED"):
        order_counts["rejected"] += 1
    if error:
        handle_rejection(str(error))
        last_sold_day[ticker] = day_number - COOLDOWN_DAYS + 2  # pause this stock for 2 days so we don't repeat a rejected order
    if ok:
        fees_paid += BROKERAGE + (DP_CHARGE if action == "SELL" else 0.0)
        if action == "BUY":
            last_bought_day[ticker] = day_number
            last_bought_time[ticker] = time.time()
            cash -= qty * price + BROKERAGE
            if ticker in holdings:
                h = holdings[ticker]
                h["avg"] = (h["avg"] * h["qty"] + price * qty) / (h["qty"] + qty)
                h["qty"] += qty
            else:
                holdings[ticker] = {"qty": qty, "avg": price, "peak": price, "bought": sim_date}
        elif action == "SELL":
            last_sold_day[ticker] = day_number
            gain = (price - holdings[ticker]["avg"]) * qty
            held_days = days_between(holdings[ticker].get("bought") or sim_date, sim_date)
            tax_book[fy_of(sim_date)]["lt" if held_days > LTCG_DAYS else "st"] += gain
            profit = (price - holdings[ticker]["avg"]) * qty - BROKERAGE - DP_CHARGE
            trade_stats["closed"] += 1
            trade_stats["wins" if profit > 0 else "losses"] += 1
            trade_stats["realized"] += profit
            if trade_stats["best"] is None or profit > trade_stats["best"]:
                trade_stats["best"], trade_stats["best_ticker"] = round(profit, 2), ticker
            if trade_stats["worst"] is None or profit < trade_stats["worst"]:
                trade_stats["worst"], trade_stats["worst_ticker"] = round(profit, 2), ticker
            cash += qty * price - BROKERAGE - DP_CHARGE
            holdings[ticker]["qty"] -= qty
            if holdings[ticker]["qty"] <= 0:
                del holdings[ticker]
        if order.get("alert"):
            add_alert(order["alert"][0], order["alert"][1])
        trade_marks.append({"sim_date": sim_date, "action": action, "ticker": ticker})
    record_decision(sim_date, ticker, action, qty, price, order["reason"], result, decision_ms)
    log(f"{action} {qty} {ticker} @ {price:.2f} -> {result} | {order['reason']} | {decision_ms} ms")


order_pool = ThreadPoolExecutor(max_workers=6)
last_order_time = 0.0
last_bought_day = {}     # ticker -> game day we last bought it (double-buy guard)
last_bought_time = {}    # ticker -> clock time we last bought it


def send_orders(orders, sim_date, day_start):
    """Check every order, send them all to the server AT THE SAME TIME, then record the results.
    Returns the orders that were filled."""
    valid = []
    for order in orders:
        order["qty"] = int(order["qty"])
        if order["qty"] < 1 or order["qty"] > 1000000:
            continue
        if order["action"] == "SELL" and holdings.get(order["ticker"], {}).get("qty", 0) < order["qty"]:
            continue  # never sell shares we don't have
        valid.append(order)
    if not valid:
        return []
    global last_order_time
    last_order_time = time.time()
    results = list(order_pool.map(lambda o: send_to_server(o, sim_date), valid))
    last_order_time = time.time()
    filled = []
    for order, (ok, result, error) in zip(valid, results):
        apply_result(order, ok, result, error, sim_date, day_start)
        if ok:
            filled.append(order)
    return filled


def place_order(ticker, action, qty, price, reason, sim_date, day_start):
    """Send a single order (kept for simple use)."""
    return bool(send_orders([{"ticker": ticker, "action": action, "qty": qty, "price": price, "reason": reason}],
                            sim_date, day_start))


# ---------------------------------------------------------------------
# Step 9: The strategy (Version 3.1 = long-term trend-hold on the 200-day average)
# ---------------------------------------------------------------------
def average(values, n):
    recent = list(values)[-n:]
    return sum(recent) / len(recent)


def trend_gap(ticker):
    """How far today's price is above (+) or below (-) its 200-day average. None if not enough history yet."""
    prices = closes[ticker]
    if len(prices) < LONG_MA_DAYS:
        return None
    long_avg = average(prices, LONG_MA_DAYS)
    return (prices[-1] - long_avg) / long_avg


def rsi(ticker):
    """RSI: 0-100 score of how fast the price rose recently. Above 70 = overheated, likely to cool down."""
    prices = list(closes[ticker])
    if len(prices) < RSI_DAYS + 1:
        return 50.0
    gains = [max(0.0, prices[k] - prices[k - 1]) for k in range(-RSI_DAYS, 0)]
    losses = [max(0.0, prices[k - 1] - prices[k]) for k in range(-RSI_DAYS, 0)]
    avg_gain, avg_loss = sum(gains) / RSI_DAYS, sum(losses) / RSI_DAYS
    if avg_loss == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def momentum(ticker):
    """v4: how much the price changed over the last 120 trading days. None if not enough history."""
    prices = closes[ticker]
    if len(prices) <= MOMENTUM_DAYS:
        return None
    old = prices[-MOMENTUM_DAYS - 1]
    return (prices[-1] - old) / old if old > 0 else None


def momentum_ranking(day_prices):
    """v4: stocks with a positive 6-month change, strongest first."""
    scored = [(m, t) for t in day_prices if (m := momentum(t)) is not None and m > 0]
    scored.sort(reverse=True)
    return [t for _, t in scored]


history_retry_at = 0.0


def reload_history_if_missing(day_prices):
    """v4: if the bot started on bad wifi it has no past prices - fetch them again in the background."""
    global history_retry_at
    missing = [t for t in day_prices if len(closes[t]) <= MOMENTUM_DAYS]
    if missing and time.time() - history_retry_at > 30:
        history_retry_at = time.time()
        log(f"Not enough past prices for {', '.join(missing)} - reloading history in the background")
        threading.Thread(target=lambda: [warm_start(t) for t in missing], daemon=True).start()


def build_signals(day_prices):
    """Every stock's votes and verdict for the dashboard (matches what the bot actually does)."""
    ranking = momentum_ranking({t: 1 for t in stocks})
    for ticker in list(stocks):
        gap = trend_gap(ticker)
        r = rsi(ticker)
        trend_vote = 0 if gap is None else (1 if gap > TREND_BUFFER else (-1 if gap < -TREND_BUFFER else 0))
        rsi_vote = -1 if r >= RSI_MAX_TO_BUY else (1 if r <= 30 else 0)
        news_score = news_today.get(ticker, 0)
        brake = news_brake_active(ticker)
        news_vote = -1 if brake else (1 if news_score > 0 else 0)
        held = ticker in holdings
        rank = ranking.index(ticker) + 1 if ticker in ranking else None
        mom = momentum(ticker)
        if held:
            h = holdings[ticker]
            change = (last_price.get(ticker, h["avg"]) - h["avg"]) / h["avg"]
            verdict = "SELL" if (change <= -STOP_LOSS_PCT or rank is None or rank > KEEP_RANK) else "HOLD"
        else:
            verdict = "BUY" if (rank is not None and rank <= TOP_K and not brake) else "WAIT"
        signals[ticker] = {"trend": trend_vote, "trend_gap_pct": None if gap is None else round(gap * 100, 2),
                           "rsi": round(r), "rsi_vote": rsi_vote, "news": news_vote, "news_score": news_score,
                           "news_veto": brake, "total": trend_vote + rsi_vote + news_vote, "verdict": verdict,
                           "held": held, "rank": rank,
                           "momentum_pct": None if mom is None else round(mom * 100, 2)}


def write_status(sim_date):
    """Save a live summary for the dashboard."""
    status = {
        "updated": datetime.now().strftime("%H:%M:%S"),
        "sim_date": sim_date,
        "stage": stage_info.get("stage"),
        "stage_status": stage_info.get("status"),
        "days_completed": stage_info.get("days_completed"),
        "total_days": stage_info.get("total_days"),
        "practice_mode": DRY_RUN,
        "cash": round(cash, 2),
        "wealth": round(total_wealth(), 2),
        "holdings": [
            {"ticker": t, "qty": h["qty"], "avg": round(h["avg"], 2),
             "price": round(last_price.get(t, h["avg"]), 2),
             "pnl_pct": round((last_price.get(t, h["avg"]) - h["avg"]) / h["avg"] * 100, 2)}
            for t, h in holdings.items()
        ],
        "prices": {t: round(p, 2) for t, p in last_price.items()},
        "trend": {t: round(g * 100, 2) for t in list(stocks) if (g := trend_gap(t)) is not None},
        "recent_decisions": list(recent_decisions),
        "starting_cash": STARTING_CASH,
        "companies": {t: info.get("name", "") for t, info in stocks.items()},
        "price_history": {t: [round(p, 2) for p in list(closes[t])[-30:]] for t in list(stocks)},
        "wealth_history": list(wealth_history),
        "signals": signals,
        "news": list(news_feed),
        "alerts": list(alerts),
        "market_mood": market_mood,
        "mood_value": round(mood_value, 2),
        "latency": {"last_ms": day_times[-1] if day_times else None,
                    "avg_ms": round(sum(day_times) / len(day_times)) if day_times else None,
                    "max_ms": max(day_times) if day_times else None},
        "fees_paid": round(fees_paid, 2),
        "orders": order_counts,
        "server": server_stats,
        "trade_stats": {**trade_stats, "realized": round(trade_stats["realized"], 2)},
        "trade_marks": list(trade_marks),
        "drawdown_pct": round(drawdown_now() * 100, 2),
        "rules": {"stop": round(STOP_LOSS_PCT * 100), "maxHold": TOP_K, "cap": round(INVEST_PCT / TOP_K * 100)},
        "strategy": f"Momentum rotation: top {TOP_K} by {MOMENTUM_DAYS}-day change, re-ranked every {REBALANCE_EVERY} days",
        "tax": {"fy": fy_of(current_day) if current_day else None,
                "short_term_gains": round(tax_book[fy_of(current_day)]["st"], 2) if current_day else 0,
                "long_term_gains": round(tax_book[fy_of(current_day)]["lt"], 2) if current_day else 0,
                "estimated": round(estimated_tax(fy_of(current_day)), 2) if current_day else 0,
                "harvests": harvest_log[-10:]},
    }
    temp = STATUS_FILE + ".tmp"
    try:
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(status, f)
        os.replace(temp, STATUS_FILE)
    except OSError:
        pass  # the dashboard was reading the file at that moment - we simply write it again next day


def process_day(sim_date, day_start, day_prices):
    """Called once per game day, 0.4 s after the day's prices arrive."""
    global day_number
    day_number += 1
    # A) Remember today's prices
    for ticker, price in day_prices.items():
        closes[ticker].append(price)
        last_price[ticker] = price
        if ticker in holdings:
            holdings[ticker]["peak"] = max(holdings[ticker]["peak"], price)

    update_mood()
    wealth_history.append({"sim_date": sim_date, "wealth": round(total_wealth(), 2)})

    if not trading_allowed() or catching_up or (not DRY_RUN and not synced_once):
        build_signals(day_prices)
        write_status(sim_date)
        return

    def too_late():
        return time.time() - day_start > DECISION_DEADLINE_SEC

    stage = str(stage_info.get("stage") or "").lower()
    remaining = days_left()

    # B) v4 MOMENTUM ROTATION
    global trade_days, last_rebalance
    trade_days += 1
    reload_history_if_missing(day_prices)
    ranking = momentum_ranking(day_prices)
    rebalance_today = last_rebalance is None or trade_days - last_rebalance >= REBALANCE_EVERY
    global loop_hold
    loop_exit = stage == "development" and remaining is not None and remaining <= PRACTICE_LOOP_EXIT_DAYS
    if loop_exit:
        loop_hold = True   # practice only: stay out until we actually see the market restart from day 1
    if stage != "development":
        loop_hold = False

    def rank_text(t):
        m = momentum(t)
        return "no 6-month data" if m is None else f"6-month change {m * 100:+.1f}%"

    exits = []
    for ticker in list(holdings):
        if ticker not in day_prices:
            continue
        h = holdings[ticker]
        price = day_prices[ticker]
        change = (price - h["avg"]) / h["avg"]
        rank = ranking.index(ticker) + 1 if ticker in ranking else None
        if change <= -STOP_LOSS_PCT:
            exits.append({"ticker": ticker, "action": "SELL", "qty": h["qty"], "price": price,
                          "reason": f"Safety stop: price fell {abs(change) * 100:.1f}% below our buy price",
                          "alert": ("danger", f"Safety stop: sold {ticker}, down {abs(change) * 100:.1f}%")})
        elif loop_exit:
            exits.append({"ticker": ticker, "action": "SELL", "qty": h["qty"], "price": price,
                          "reason": "Practice market restarts in 2 days: closing so the price reset can't hit us",
                          "alert": ("info", f"Closed {ticker} before the practice market restarts")})
        elif rebalance_today and (rank is None or rank > KEEP_RANK) and len(closes[ticker]) > MOMENTUM_DAYS:
            why = "its 6-month change turned negative" if rank is None else f"it dropped to #{rank} of {len(ranking)}"
            exits.append({"ticker": ticker, "action": "SELL", "qty": h["qty"], "price": price,
                          "reason": f"Rotation: {why} ({rank_text(ticker)}), so we swap it for a stronger stock"})
    if exits and too_late():
        log("Out of time for today - selling tomorrow instead")
    else:
        if exits:
            send_orders(exits, sim_date, day_start)
        if rebalance_today:
            last_rebalance = trade_days

    # C) No new buys in the last few days of the live match, or just before the practice market restarts
    if (stage == "simulation" and remaining is not None and remaining <= END_GAME_DAYS) or loop_exit or loop_hold:
        build_signals(day_prices)
        day_times.append(int((time.time() - day_start) * 1000))
        write_status(sim_date)
        return

    # D) Fill empty slots with the strongest stocks we don't own yet (top 3 by 6-month change)
    slots = TOP_K - len(holdings)
    wealth = total_wealth()
    target = INVEST_PCT * wealth / TOP_K
    planned_cash = cash
    buys = []
    for position, ticker in enumerate(ranking[:TOP_K], start=1):   # only ever buy stocks in the top 3 right now
        if len(buys) >= slots:
            break
        if ticker in holdings or ticker not in day_prices:
            continue
        if ticker in last_sold_day and day_number - last_sold_day[ticker] < COOLDOWN_DAYS:
            continue  # cooling off: we sold this recently
        if news_brake_active(ticker):
            continue  # news safety brake: serious bad news about this company
        if ticker in last_bought_day and day_number - last_bought_day[ticker] < REBUY_GUARD_DAYS:
            continue  # double-buy guard: we just bought this, the server may not show it yet
        price = day_prices[ticker]
        spare_cash = planned_cash - (1 - INVEST_PCT) * wealth
        budget = min(target, spare_cash) - BROKERAGE
        if budget < MIN_TRADE_VALUE:
            break
        qty = int(budget // price)
        if qty * price < MIN_TRADE_VALUE:
            continue
        planned_cash -= qty * price + BROKERAGE
        buys.append({"ticker": ticker, "action": "BUY", "qty": qty, "price": price,
                     "reason": f"Momentum: #{position} strongest of {len(day_prices)} stocks "
                               f"({rank_text(ticker)}), so it gets {target / wealth * 100:.0f}% of our money"})
    if buys:
        if too_late():
            log("Out of time for today - buying tomorrow instead")
        else:
            send_orders(buys, sim_date, day_start)

    build_signals(day_prices)
    day_times.append(int((time.time() - day_start) * 1000))
    write_status(sim_date)


# ---------------------------------------------------------------------
# Step 10: Reading the live feed
# ---------------------------------------------------------------------
current_day = None
trade_days = 0           # v4: game days on which we were allowed to trade
loop_hold = False        # v4.1: practice market is about to restart - no new buys until it does
last_restart_reload = 0.0
last_stale_log = 0.0
last_rebalance = None    # v4: trade_days value when we last re-ranked
day_start = 0.0
day_prices = {}
day_done = True
last_heartbeat = 0.0
catching_up = False      # True while old backed-up days are arriving (after a wifi blip)
catch_up_days = 0


def days_between(a, b):
    """Calendar days between two 'YYYY-MM-DD' dates (0 if they can't be read)."""
    try:
        return (Date.fromisoformat(str(b)[:10]) - Date.fromisoformat(str(a)[:10])).days
    except ValueError:
        return 0


def run_day_safely(sim_date, started, prices):
    try:
        process_day(sim_date, started, prices)
    except Exception as error:
        log(f"Problem while deciding for {sim_date}: {error!r} (bot keeps running)")


def handle_event(topic, event):
    global current_day, day_start, day_prices, day_done, last_heartbeat, catching_up, catch_up_days, loop_hold
    global last_restart_reload
    if not isinstance(event, dict):
        return
    kind = event.get("event_type") or event.get("event")
    data = event.get("data") if isinstance(event.get("data"), dict) else {}

    # Pre-flight check: reply "I'm here" with the sequence number
    if kind == "preflight_tick":
        seq = event.get("seq", data.get("seq"))
        if seq is not None and time.time() - last_heartbeat >= 1.0:
            status, _ = api_call("POST", "/bot/heartbeat", {"seq": seq}, priority=True)
            last_heartbeat = time.time()
            if status is not None and status < 300:
                if seq % 10 == 0:
                    log(f"Heartbeat OK (seq {seq})")
            else:
                log(f"Heartbeat FAILED (seq {seq}, HTTP {status})")
        return

    # News: save it (Version 2 will score it)
    if kind == "news_flash" or topic == "market.news":
        ticker = data.get("ticker")
        headline = data.get("headline", "")
        description = data.get("description", "")
        news_date = event.get("simulated_date") or data.get("date")
        append_csv(NEWS_FILE, ["sim_date", "ticker", "headline", "description"],
                   [news_date, ticker, headline, description])
        if ticker:
            score, veto = score_news(ticker, headline, description)
            if news_date != current_day:
                news_today.clear()
            news_today[ticker] += score
            if veto:
                veto_until[ticker] = day_number + NEWS_VETO_DAYS  # covers today and tomorrow
                add_alert("warn", f"News brake on {ticker}: {headline[:90]}")
                log(f"NEWS BRAKE {ticker}: {headline}")
            news_feed.appendleft({"sim_date": news_date, "ticker": ticker, "headline": headline[:160],
                                  "score": score, "veto": veto})
        return

    # Price: collect today's closing prices
    if kind == "price_tick":
        ticker = data.get("ticker")
        close = to_float(data.get("close"))
        date = event.get("simulated_date") or data.get("date")
        if not ticker or not close or close <= 0 or not date:
            return
        if ticker not in stocks:
            stocks[ticker] = {"name": "", "description": ""}

        # v4.1: an OLDER date than the day we are on is either a real practice-market restart
        # (jumps back many months) or stale / repeated data from the server - ignore the latter,
        # so it can't mess up our price history or make us reload everything again and again
        if current_day is not None and date < current_day:
            stage_now = str(stage_info.get("stage") or "").lower()
            real_restart = stage_now == "development" and days_between(date, current_day) > 30
            if not real_restart or time.time() - last_restart_reload < 20:
                global last_stale_log
                if time.time() - last_stale_log > 10:
                    last_stale_log = time.time()
                    log(f"Ignored old price data for {date} (we are already on {current_day})")
                return

        if date != current_day:
            if current_day is not None and not day_done:
                day_done = True
                run_day_safely(current_day, day_start, day_prices)
            if current_day is not None and date < current_day:
                log("Practice market restarted from day 1 - reloading past prices")
                loop_hold = False
                last_restart_reload = time.time()
                add_alert("info", "Practice market restarted from day 1 - past prices reloaded")
                warm_start_all()
            elif current_day is not None and days_between(current_day, date) > GAP_RELOAD_DAYS:
                log(f"Market jumped ahead {days_between(current_day, date)} days (bot was offline?) - reloading past prices")
                add_alert("warn", f"Market jumped {days_between(current_day, date)} days ahead - past prices reloaded")
                warm_start_all()
            now = time.time()
            was_catching_up = catching_up
            catching_up = current_day is not None and (now - day_start) < CATCH_UP_GAP_SEC
            if catching_up:
                catch_up_days += 1
            elif was_catching_up:
                log(f"Caught up after {catch_up_days} backed-up days (no trades were made on old data)")
                add_alert("warn", f"Connection blip: skipped trading on {catch_up_days} old days, now live again")
                catch_up_days = 0
            current_day, day_start, day_prices, day_done = date, now, {}, False
            news_today.clear()

        if day_done:
            if ticker not in day_prices:  # a late price after we already decided
                day_prices[ticker] = close
                closes[ticker].append(close)
                last_price[ticker] = close
        else:
            day_prices[ticker] = close

        append_csv(MARKET_FILE, ["sim_date", "ticker", "open", "high", "low", "close", "volume"],
                   [date, ticker, data.get("open"), data.get("high"), data.get("low"), close, data.get("volume")])


class DashboardHandler(BaseHTTPRequestHandler):
    """Serves ONLY the dashboard page and status.json (never config.py or other files)."""
    FILES = {"/": ("dashboard.html", "text/html; charset=utf-8"),
             "/dashboard.html": ("dashboard.html", "text/html; charset=utf-8"),
             "/status.json": (STATUS_FILE, "application/json")}

    def do_GET(self):
        entry = self.FILES.get(self.path.split("?")[0])
        if not entry or not os.path.exists(entry[0]):
            self.send_response(404)
            self.end_headers()
            return
        try:
            with open(entry[0], "rb") as f:
                content = f.read()
        except OSError:
            self.send_response(503)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", entry[1])
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, *args):
        pass  # keep the terminal clean


def start_dashboard():
    try:
        server = ThreadingHTTPServer(("127.0.0.1", DASHBOARD_PORT), DashboardHandler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        log(f"DASHBOARD ready: open http://localhost:{DASHBOARD_PORT} in Chrome")
    except OSError as error:
        log(f"Dashboard could not start on port {DASHBOARD_PORT} ({error}). The bot still runs normally.")


def read_json(raw):
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


# ---------------------------------------------------------------------
# Step 11: Start everything
# ---------------------------------------------------------------------
def startup_loader():
    for _ in range(40):          # keep trying for ~2-4 minutes (bad wifi at the venue)
        if load_stocks():
            break
        time.sleep(3)
    sync_cash()
    sync_holdings()              # read our real money first ...
    warm_start_all()             # ... then past prices (missing ones are retried every 30 s while running)
    update_mood()
    build_signals({})
    write_status(None)
    log("Startup finished - ready to trade when the simulation runs")


def main():
    global day_done
    if "PUT_" in config.SERVER_IP or "PUT_" in config.ACCESS_TOKEN or "PUT_" in config.CONSUMER_GROUP:
        print("STOP: open config.py and fill in SERVER_IP, ACCESS_TOKEN and CONSUMER_GROUP first.")
        return

    log("=" * 60)
    log(f"SignalFusion {BOT_VERSION} starting | group {CONSUMER_GROUP} | "
        f"{'PRACTICE MODE (no real orders)' if DRY_RUN else 'LIVE MODE (real orders)'}")

    threading.Thread(target=watch_stage, daemon=True).start()
    start_dashboard()

    # v4.1: load companies, money and past prices in the BACKGROUND, so the live feed (and the
    # pre-flight heartbeats) connect straight away even if the venue wifi is slow
    threading.Thread(target=startup_loader, daemon=True).start()
    last_sync = time.time()
    last_forced_cash = time.time()

    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                "market.ticks",
                "market.news",
                bootstrap_servers=[KAFKA_BROKER],
                group_id=CONSUMER_GROUP,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                value_deserializer=read_json,
            )
            log(f"Connected to the live feed at {KAFKA_BROKER}")
            while True:
                batch = consumer.poll(timeout_ms=100)
                for _, messages in batch.items():
                    for message in messages:
                        handle_event(message.topic, message.value)

                if not day_done and time.time() - day_start >= COLLECT_WINDOW_SEC:
                    day_done = True
                    run_day_safely(current_day, day_start, day_prices)

                if time.time() - last_sync >= SYNC_EVERY_SEC:
                    sync_cash(force=time.time() - last_forced_cash >= 90)   # always re-read cash at least every 90 s
                    sync_holdings()
                    last_sync = time.time()
                    last_forced_cash = time.time() if time.time() - last_forced_cash >= 90 else last_forced_cash
        except KeyboardInterrupt:
            log("Bot stopped by you.")
            raise SystemExit(0)
        except Exception as error:
            log(f"Live feed problem: {error!r} - reconnecting in 3 seconds...")
            time.sleep(3)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Bot stopped by you.")
        raise SystemExit(0)
