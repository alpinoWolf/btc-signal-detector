import requests
import time
import functools
from datetime import datetime, timezone, timedelta
import pytz

print = functools.partial(print, flush=True)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────

PYTH_URL = "https://benchmarks.pyth.network/v1/shims/tradingview/history"
SYMBOL   = "Crypto.BTC/USD"
ET_TZ    = pytz.timezone("America/New_York")

# Body size thresholds (%)
TINY_MAX  = 0.05
SMALL_MAX = 0.10

# ─────────────────────────────────────────
# PROBABILITY TABLES
# GG: contrarian = RED probability
# RR: contrarian = GREEN probability
# Key: (C1_size, C2_size)
# ─────────────────────────────────────────

GG_CONTRARIAN = {
    ("tiny",  "tiny"):  {"prob": 22.68, "color": "🟡"},
    ("tiny",  "small"): {"prob": 12.42, "color": "🟠"},
    ("small", "tiny"):  {"prob": 13.21, "color": "🟠"},
}

RR_CONTRARIAN = {
    ("tiny",  "tiny"):  {"prob": 22.42, "color": "🟡"},
    ("tiny",  "small"): {"prob": 13.08, "color": "🟠"},
    ("small", "tiny"):  {"prob": 13.30, "color": "🟠"},
}

# ─────────────────────────────────────────
# CANDLE SIZE CLASSIFIER
# ─────────────────────────────────────────

def classify_size(body_pct):
    if body_pct <= TINY_MAX:
        return "tiny"
    elif body_pct <= SMALL_MAX:
        return "small"
    elif body_pct <= 0.20:
        return "medium"
    elif body_pct <= 0.50:
        return "large"
    else:
        return "very_large"

# ─────────────────────────────────────────
# TIME HELPERS
# ─────────────────────────────────────────

def now_et():
    return datetime.now(ET_TZ)

def next_c2_close():
    now = now_et()
    minute = now.minute
    block_start = (minute // 15) * 15
    c2_close_minute = block_start + 10
    c2_close = now.replace(minute=c2_close_minute, second=2, microsecond=0)
    if c2_close <= now:
        c2_close += timedelta(minutes=15)
    return c2_close

def get_block_start(c2_close_et):
    block_start_et = c2_close_et - timedelta(minutes=10)
    return int(block_start_et.astimezone(timezone.utc).timestamp())

# ─────────────────────────────────────────
# PYTH DATA FETCHER
# ─────────────────────────────────────────

def fetch_candles(from_ts, to_ts):
    params = {
        "symbol":     SYMBOL,
        "resolution": "5",
        "from":       from_ts,
        "to":         to_ts,
    }
    try:
        r = requests.get(PYTH_URL, params=params, timeout=10)
        data = r.json()
        if data.get("s") != "ok":
            print(f"  Pyth returned status: {data.get('s')}")
            return None
        return data
    except Exception as e:
        print(f"  Fetch error: {e}")
        return None

def parse_candles(data):
    times  = data["t"]
    opens  = data["o"]
    closes = data["c"]
    candles = []
    for i in range(len(times)):
        o = float(opens[i])
        c = float(closes[i])
        body_pct = abs(c - o) / o * 100
        direction = "green" if c >= o else "red"
        candles.append({
            "time":      times[i],
            "open":      o,
            "close":     c,
            "body_pct":  round(body_pct, 4),
            "direction": direction,
        })
    candles.sort(key=lambda x: x["time"])
    return candles

# ─────────────────────────────────────────
# SIGNAL EVALUATOR
# ─────────────────────────────────────────

def evaluate(c1, c2):
    d1 = c1["direction"]
    d2 = c2["direction"]
    s1 = classify_size(c1["body_pct"])
    s2 = classify_size(c2["body_pct"])

    print(f"  C1 -> {d1.upper()} | body={c1['body_pct']}% | size={s1.upper()}")
    print(f"  C2 -> {d2.upper()} | body={c2['body_pct']}% | size={s2.upper()}")

    # Must be GG or RR
    if d1 == "green" and d2 == "green":
        pattern = "GG"
        table   = GG_CONTRARIAN
    elif d1 == "red" and d2 == "red":
        pattern = "RR"
        table   = RR_CONTRARIAN
    else:
        print(f"  Pattern: {d1[0].upper()}{d2[0].upper()} -> SKIP (not GG or RR)")
        return

    # Must be tiny or small only
    if s1 not in ("tiny", "small") or s2 not in ("tiny", "small"):
        print(f"  Pattern: {pattern} | C1={s1} C2={s2} -> SKIP (size too large)")
        return

    key = (s1, s2)
    result = table.get(key)
    if not result:
        print(f"  Pattern: {pattern} | C1={s1} C2={s2} -> SKIP (small+small not traded)")
        return

    prob  = result["prob"]
    color = result["color"]

    print(f"")
    print(f"  ┌─────────────────────────────────────┐")
    print(f"  │  SIGNAL DETECTED                    │")
    print(f"  │  Asset      : BTC                   │")
    print(f"  │  Pattern    : {pattern}                    │")
    print(f"  │  C1         : {s1.upper():<8}               │")
    print(f"  │  C2         : {s2.upper():<8}               │")
    print(f"  │  Contrarian : {color} {prob}%             │")
    print(f"  └─────────────────────────────────────┘")
    print(f"")

# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────

def main():
    print("=" * 45)
    print("  BTC Signal Detector started")
    print(f"  Time now (ET): {now_et().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 45)

    while True:
        try:
            target = next_c2_close()
            wait_sec = (target - now_et()).total_seconds()

            print(f"\nNext C2 close: {target.strftime('%H:%M:%S ET')} "
                  f"(waiting {int(wait_sec)}s)")

            time.sleep(max(wait_sec, 0))

            block_start_ts = get_block_start(target)
            to_ts = int(target.astimezone(timezone.utc).timestamp()) + 30

            print(f"[{now_et().strftime('%H:%M:%S ET')}] Fetching Pyth candles...")
            data = fetch_candles(block_start_ts, to_ts)

            if not data:
                print("  No data returned. Skipping.")
                time.sleep(30)
                continue

            candles = parse_candles(data)

            if len(candles) < 2:
                print(f"  Only {len(candles)} candle(s) returned. Skipping.")
                time.sleep(30)
                continue

            c1 = candles[0]
            c2 = candles[1]

            evaluate(c1, c2)

            time.sleep(30)

        except Exception as e:
            print(f"  Unexpected error: {e}")
            time.sleep(30)
            continue

if __name__ == "__main__":
    main()
