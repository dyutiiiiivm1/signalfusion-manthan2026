# =====================================================================
#  SignalFusion - AUTO-RESTART LAUNCHER (crash recovery)
#
#  Run THIS instead of the bot:      python run_bot.py
#
#  It starts bot_v4.py and watches it. If the bot ever crashes, it waits
#  3 seconds and starts it again automatically. On restart the bot reloads
#  our cash and holdings from the server, so nothing is lost.
#  Press Ctrl + C to stop everything.
# =====================================================================

import subprocess
import sys
import time
from datetime import datetime

BOT_FILE = "bot_v4.py"
MAX_QUICK_CRASHES = 5        # if it crashes 5 times within a minute, something is badly wrong: stop and show the error


def say(message):
    line = f"{datetime.now().strftime('%H:%M:%S')}  [launcher] {message}"
    print(line, flush=True)
    with open("bot_log.txt", "a", encoding="utf-8") as f:
        f.write(line + "\n")


crash_times = []
say(f"Starting {BOT_FILE} with automatic restart. Press Ctrl + C to stop.")
while True:
    bot = subprocess.Popen([sys.executable, BOT_FILE])
    try:
        code = bot.wait()
    except KeyboardInterrupt:
        try:
            bot.wait(timeout=5)
        except Exception:
            bot.terminate()
        say("Stopped by you.")
        break

    if code == 0:
        say("Bot stopped normally.")
        break

    now = time.time()
    crash_times = [t for t in crash_times if now - t < 60] + [now]
    if len(crash_times) >= MAX_QUICK_CRASHES:
        say(f"Bot crashed {len(crash_times)} times in one minute. Not restarting - copy the red error above and send it to Claude.")
        break
    say(f"Bot stopped unexpectedly (exit code {code}). Restarting in 3 seconds...")
    try:
        time.sleep(3)
    except KeyboardInterrupt:
        say("Stopped by you.")
        break
