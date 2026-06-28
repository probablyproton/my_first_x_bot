"""
Ticker Twitter Bot — dual storyline + event-driven edition
EU story:  09:00–17:00 local time  [15 slots]
US story:  15:30–22:00 local time  [15 slots]
Weekend:   08:00–21:30             [30 slots]

Local run:  python bot.py          (browser window visible)
GitHub:     triggered every 15 min by Actions cron (headless, TZ=Europe/Amsterdam)
"""

import os
import ssl
import json
import time
import random
import logging
import datetime
import requests
import yfinance as yf
import urllib.request
import urllib.error
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

ssl._create_default_https_context = ssl._create_unverified_context
os.environ["PYTHONHTTPSVERIFY"] = "0"

load_dotenv("my.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

GEMINI_API_KEY               = os.environ["GEMINI_API_KEY"]

SLOT_JITTER_SECONDS          = int(os.getenv("SLOT_JITTER_SECONDS", "90"))
DRY_RUN                      = os.getenv("DRY_RUN", "false").lower() == "true"
EU_WATCHLIST                 = [t.strip().upper() for t in os.getenv("EU_WATCHLIST", "").split(",") if t.strip()]
US_WATCHLIST                 = [t.strip().upper() for t in os.getenv("US_WATCHLIST", "").split(",") if t.strip()]
EU_FOCUS_TICKER              = os.getenv("EU_FOCUS_TICKER", "").strip().upper().replace("{}", "")
US_FOCUS_TICKER              = os.getenv("US_FOCUS_TICKER", "").strip().upper().replace("{}", "")
EVENT_INTERVAL_THRESHOLD_PCT = float(os.getenv("EVENT_INTERVAL_THRESHOLD_PCT", "1.5"))
EVENT_DAY_THRESHOLD_PCT      = float(os.getenv("EVENT_DAY_THRESHOLD_PCT", "4.0"))
EVENT_COOLDOWN_MINUTES       = int(os.getenv("EVENT_COOLDOWN_MINUTES", "12"))

# GitHub Actions sets CI=true automatically — run headless in the cloud
HEADLESS = os.getenv("CI", "false") == "true"

STATE_FILE   = os.path.join(os.path.dirname(__file__), "state.json")
SESSION_FILE = os.path.join(os.path.dirname(__file__), "twitter_session.json")

DAILY_POST_LIMIT = 30

# ── Slot definitions ───────────────────────────────────────────────────────────

EU_SLOTS = [
    ("09:00", "hook"),
    ("09:33", "analytical"),
    ("10:06", "question"),
    ("10:39", "fomo"),
    ("11:12", "analytical"),
    ("11:45", "question"),
    ("12:18", "fomo"),
    ("12:51", "reaction"),
    ("13:24", "analytical"),
    ("13:57", "question"),
    ("14:30", "fomo"),
    ("15:03", "analytical"),
    ("15:36", "question"),
    ("16:20", "fomo"),
    ("16:55", "wrap"),
]

US_SLOTS = [
    ("15:30", "hook"),
    ("16:00", "analytical"),
    ("16:28", "question"),
    ("16:56", "fomo"),
    ("17:24", "analytical"),
    ("17:52", "question"),
    ("18:20", "fomo"),
    ("18:48", "reaction"),
    ("19:16", "analytical"),
    ("19:44", "question"),
    ("20:12", "fomo"),
    ("20:40", "analytical"),
    ("21:08", "question"),
    ("21:36", "fomo"),
    ("22:00", "wrap"),
]

WEEKEND_SLOTS = [
    ("08:00", "hook"),
    ("08:28", "analytical"),
    ("08:56", "question"),
    ("09:24", "fomo"),
    ("09:52", "analytical"),
    ("10:20", "question"),
    ("10:48", "hook"),
    ("11:16", "analytical"),
    ("11:44", "question"),
    ("12:12", "fomo"),
    ("12:40", "analytical"),
    ("13:08", "question"),
    ("13:36", "fomo"),
    ("14:04", "analytical"),
    ("14:32", "question"),
    ("15:00", "fomo"),
    ("15:28", "analytical"),
    ("15:56", "question"),
    ("16:24", "hook"),
    ("16:52", "analytical"),
    ("17:20", "question"),
    ("17:48", "fomo"),
    ("18:16", "analytical"),
    ("18:44", "question"),
    ("19:12", "fomo"),
    ("19:40", "analytical"),
    ("20:08", "question"),
    ("20:36", "fomo"),
    ("21:04", "analytical"),
    ("21:30", "wrap"),
]


def _build_slots(slot_defs: list[tuple]) -> list[dict]:
    slots = []
    for i, (t, tp) in enumerate(slot_defs):
        h, m = map(int, t.split(":"))
        jitter = random.randint(-SLOT_JITTER_SECONDS, SLOT_JITTER_SECONDS)
        fire_dt = datetime.datetime.now().replace(hour=h, minute=m, second=0, microsecond=0)
        fire_dt += datetime.timedelta(seconds=jitter)
        slots.append({
            "slot":        i,
            "type":        tp,
            "target_time": t,
            "fire_time":   fire_dt.strftime("%H:%M"),
        })
    return slots

# ── State ─────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def today() -> str:
    return datetime.date.today().isoformat()


def now_hhmm() -> str:
    return datetime.datetime.now().strftime("%H:%M")


def now_minutes() -> int:
    n = datetime.datetime.now()
    return n.hour * 60 + n.minute

# ── Data ──────────────────────────────────────────────────────────────────────

def get_ticker_context(symbol: str, max_messages: int = 8) -> list[str]:
    try:
        url = f"https://finance.yahoo.com/rss/headline?s={symbol}"
        resp = requests.get(url, timeout=10, verify=False,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        import xml.etree.ElementTree as ET
        root = ET.fromstring(resp.content)
        items = root.findall(".//item/title")
        return [item.text for item in items[:max_messages] if item.text]
    except Exception as e:
        log.warning("News fetch failed for %s: %s", symbol, e)
        return []


def get_price_context(symbol: str) -> dict:
    try:
        info   = yf.Ticker(symbol).fast_info
        price  = round(info.last_price, 2)
        prev   = round(info.previous_close, 2)
        change = round((price - prev) / prev * 100, 2)
        return {"price": price, "prev_close": prev, "change_pct": change}
    except Exception as e:
        log.warning("yfinance failed for %s: %s", symbol, e)
        return {}

# ── Daily plan ────────────────────────────────────────────────────────────────

PLANNER_SYSTEM_WEEKDAY = """You are planning a day of Twitter content about a single stock ticker.
Return a JSON array. Each object must have:
  - "slot": integer index (0-based)
  - "type": matching the type provided
  - "angle": one sentence — the specific narrative angle for that tweet

Rules:
- Every angle must be tight and punchy — these are 280-character tweets
- The angles must build a coherent story arc. Every angle must be distinct.
- fomo slots: the angle should be a quiet, unsettling observation — something that implies
  more is coming without stating it. Calm, not hyped. Think: pattern recognition, not prediction.
- Start strong, develop with data, drive engagement, react to moves, close with a tease.
Return ONLY the JSON array, no other text."""

PLANNER_SYSTEM_WEEKEND = """You are planning weekend Twitter content about a single stock ticker.
Markets are closed. Return a JSON array. Each object must have:
  - "slot": integer index (0-based)
  - "type": matching the type provided
  - "angle": one sentence — the specific narrative angle for that tweet

Weekend rules — STRICT:
- NEVER reference today's price, daily % moves, or live market activity — markets are closed
- NEVER use phrases like "today", "this session", "the dip", "down X%", "up X%"
- Focus ONLY on: week-in-review themes, structural thesis, long-term fundamentals,
  macro tailwinds, insider activity, what to watch when markets reopen Monday
- question slots: engaging community questions — "what are you buying Monday?",
  "do you agree with this thesis?", "would you add here or wait?", "bull or bear on $TICKER?"
- fomo slots: quiet long-term conviction — "the kind of company that builds the invisible backbone of tomorrow"
- hook slots: open with a striking fact or paradox about the company, not about price
- wrap slots: tease what catalysts or events to watch in the coming week
- Every angle must be distinct. Build a coherent weekend narrative.
Return ONLY the JSON array, no other text."""


def _gemini(system: str, prompt: str) -> str:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    body = json.dumps({
        "contents": [{"parts": [{"text": f"{system}\n\n{prompt}"}]}],
        "generationConfig": {
            "temperature": 1.0,
            "topP": 0.95,
            "thinkingConfig": {"thinkingBudget": 8192}
        }
    }).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data["candidates"][0]["content"]["parts"][-1]["text"].strip()
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < 2:
                wait = 60 * (attempt + 1)
                log.warning("Gemini 429 — waiting %ds before retry %d/2", wait, attempt + 1)
                time.sleep(wait)
            else:
                raise


def generate_daily_plan(symbol: str, slots: list[dict], price_ctx: dict, market: str) -> list[dict]:
    price_str = ""
    if price_ctx:
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        price_str = f"Current price: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% vs yesterday)"

    slot_list = "\n".join(
        f"  slot {s['slot']}: {s['target_time']} — {s['type']}" for s in slots
    )

    prompt = f"""Plan a {market} market day of Twitter content about ${symbol}.
{price_str}

Slots (produce exactly one entry per slot):
{slot_list}

Make every angle distinct. Build a narrative that evolves across the day."""

    planner_system = PLANNER_SYSTEM_WEEKEND if is_weekend() else PLANNER_SYSTEM_WEEKDAY
    try:
        text = _gemini(planner_system, prompt)
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        plan = json.loads(text.strip())
        fire_map = {s["slot"]: s["fire_time"] for s in slots}
        for p in plan:
            p["fire_time"] = fire_map.get(p["slot"], slots[p["slot"]]["target_time"])
        return plan
    except Exception as e:
        log.error("Plan generation failed for %s: %s", symbol, e)

    return [{**s, "angle": f"Cover ${symbol} from the {s['type']} angle"} for s in slots]


def pick_ticker(watchlist: list[str], override: str, label: str) -> str:
    if override:
        return override
    if watchlist:
        day_index = (datetime.date.today() - datetime.date(2026, 1, 1)).days
        return watchlist[day_index % len(watchlist)]
    log.warning("No %s watchlist configured — defaulting to SPY", label)
    return "SPY"


def ensure_storyline(state: dict, key: str, watchlist: list[str], override: str,
                     slot_defs: list[tuple], market: str) -> dict:
    if state.get("date") == today() and state.get(f"{key}_plan"):
        return state

    symbol    = pick_ticker(watchlist, override, key.upper())
    price_ctx = get_price_context(symbol)
    slots     = _build_slots(slot_defs)
    plan      = generate_daily_plan(symbol, slots, price_ctx, market)

    state[f"{key}_ticker"] = symbol
    state[f"{key}_plan"]   = plan
    state[f"{key}_posted"] = []

    log.info("%s story: $%s — %d slots planned", key.upper(), symbol, len(plan))
    for p in plan:
        log.info("  [%s] %s — %s", p["fire_time"], p["type"].upper(), p["angle"])

    return state


def ensure_daily_plans(state: dict) -> dict:
    if state.get("date") != today():
        state = {"date": today(), "daily_posts": 0}

    if is_weekend():
        state = ensure_storyline(state, "eu", EU_WATCHLIST, EU_FOCUS_TICKER, WEEKEND_SLOTS, "weekend")
    else:
        state = ensure_storyline(state, "eu", EU_WATCHLIST, EU_FOCUS_TICKER, EU_SLOTS, "European")
        if now_hhmm() >= "15:00":
            state = ensure_storyline(state, "us", US_WATCHLIST, US_FOCUS_TICKER, US_SLOTS, "US")

    save_state(state)
    return state

# ── Tweet generation ──────────────────────────────────────────────────────────

TWEET_SYSTEM = """You are a sharp financial Twitter personality — punchy, direct, data-driven.
- 2-3 emojis max, placed for visual punch
- 1-2 hashtags max, only if they add signal
- No filler phrases ("hot take", "buckle up", "thread")
- Short sentences. Bold claims. Facts over fluff.
- Sound like a smart trader talking to other smart traders
- MUST be ≤280 characters

Tweet types:
  hook       — stops the scroll. Bold opener that sets the day's narrative.
  analytical — data-driven. Specific numbers, ratios, price levels.
  question   — one sharp genuine question. No fake urgency.
  reaction   — on weekdays: grounded in the actual price move. On weekends: reaction to the week's narrative, NOT daily price moves.
  fomo       — creates anticipation and tension. Vague signal, implied pattern recognition,
               no specifics. Feels like the poster knows something. Short. Calm. Unsettling.
               Example: "$NOK signed a deal last week and the stock didn't move. That's not normal."
  wrap       — closes the day. Teases what to watch tomorrow.
  event      — urgent reaction to a sudden price move. Raw and immediate.

Output ONLY the tweet text. No quotes, no commentary."""


def generate_tweet(symbol: str, slot: dict, price_ctx: dict, community: list[str],
                   event_trigger: str = "") -> str | None:
    price_str = ""
    if price_ctx and not is_weekend():
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        price_str = f"Live: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% today)"

    community_block = "\n".join(f"- {m}" for m in community) if community else "No community messages."
    angle      = event_trigger if event_trigger else slot["angle"]
    tweet_type = "event"      if event_trigger else slot["type"]

    prompt = f"""Ticker: ${symbol}
{price_str}

Angle: {angle}
Tweet type: {tweet_type}

Community sentiment:
{community_block}

Write the tweet."""

    try:
        text = _gemini(TWEET_SYSTEM, prompt).strip('"').strip("'")
        return text[:277].rsplit(" ", 1)[0] + "..." if len(text) > 280 else text
    except Exception as e:
        log.error("Tweet generation failed: %s", e)
    return None

# ── Twitter via Playwright ─────────────────────────────────────────────────────

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


def _apply_stealth(page):
    try:
        from playwright_stealth import stealth_sync
        stealth_sync(page)
    except ImportError:
        pass


def post_tweet(text: str, state: dict) -> bool:
    if state.get("daily_posts", 0) >= DAILY_POST_LIMIT:
        log.info("Daily post limit (%d) reached — skipping", DAILY_POST_LIMIT)
        return False

    if DRY_RUN:
        log.info("[DRY RUN] (%d/%d)\n%s", state.get("daily_posts", 0) + 1, DAILY_POST_LIMIT, text)
        state["daily_posts"] = state.get("daily_posts", 0) + 1
        return True

    if not os.path.exists(SESSION_FILE):
        log.error("twitter_session.json not found. Run login.py on your laptop first.")
        return False

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=HEADLESS,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                storage_state=SESSION_FILE,
                viewport={"width": 1280, "height": 800},
                user_agent=_BROWSER_UA,
            )
            page = context.new_page()
            _apply_stealth(page)

            # Visit home first to establish session, then go to compose
            page.goto("https://x.com/home", wait_until="load", timeout=60000)
            time.sleep(random.uniform(2.0, 3.0))
            page.goto("https://x.com/compose/tweet", wait_until="load", timeout=60000)
            time.sleep(random.uniform(1.5, 3.0))

            # Use primaryColumn to avoid strict mode violation with multiple textareas
            textarea = page.locator("[data-testid='primaryColumn'] [data-testid='tweetTextarea_0']")
            textarea.wait_for(timeout=15000)
            textarea.click(force=True)
            time.sleep(random.uniform(0.5, 1.2))

            for char in text:
                page.keyboard.type(char)
                time.sleep(random.uniform(0.03, 0.11))

            time.sleep(random.uniform(1.5, 3.0))

            post_btn = page.locator("[data-testid='tweetButtonInline']")
            post_btn.wait_for(timeout=5000)
            post_btn.click()
            time.sleep(random.uniform(2.5, 4.0))

            context.storage_state(path=SESSION_FILE)
            browser.close()

        state["daily_posts"] = state.get("daily_posts", 0) + 1
        log.info("Posted (%d/%d): %s", state["daily_posts"], DAILY_POST_LIMIT, text[:80])
        return True

    except Exception as e:
        log.error("Tweet post failed: %s", e)
        return False

# ── Event monitor ─────────────────────────────────────────────────────────────

def check_price_events(state: dict, symbols: list[str]) -> dict:
    snapshots = state.setdefault("price_snapshots", {})
    cooldowns = state.setdefault("event_cooldowns", {})

    for symbol in symbols:
        price_ctx = get_price_context(symbol)
        if not price_ctx:
            continue

        current  = price_ctx["price"]
        day_pct  = abs(price_ctx["change_pct"])

        last_event_min = cooldowns.get(symbol, 0)
        if now_minutes() - last_event_min < EVENT_COOLDOWN_MINUTES:
            snapshots[symbol] = current
            continue

        last_price   = snapshots.get(symbol)
        interval_pct = abs((current - last_price) / last_price * 100) if last_price else 0

        trigger = ""
        if interval_pct >= EVENT_INTERVAL_THRESHOLD_PCT:
            direction = "up" if current > last_price else "down"
            trigger = (
                f"${symbol} just moved {direction} {interval_pct:.1f}% in the last few minutes "
                f"(now ${current}, {price_ctx['change_pct']:+.1f}% on the day). "
                f"React immediately and specifically."
            )
        elif day_pct >= EVENT_DAY_THRESHOLD_PCT:
            direction = "up" if price_ctx["change_pct"] > 0 else "down"
            trigger = (
                f"${symbol} is {direction} {day_pct:.1f}% today (now ${current}). "
                f"This is a significant day move. React with conviction."
            )

        if trigger:
            log.info("EVENT triggered for $%s: %s", symbol, trigger)
            community = get_ticker_context(symbol)
            slot  = {"type": "event", "format": "short", "angle": trigger}
            tweet = generate_tweet(symbol, slot, price_ctx, community, event_trigger=trigger)
            if tweet:
                log.info("Event tweet (%d chars):\n%s", len(tweet), tweet)
                if post_tweet(tweet, state):
                    cooldowns[symbol] = now_minutes()

        snapshots[symbol] = current

    save_state(state)
    return state

# ── Cycle ─────────────────────────────────────────────────────────────────────

def next_due_slot(plan: list[dict], posted: list[int]) -> dict | None:
    now    = now_hhmm()
    now_dt = datetime.datetime.now()
    for slot in plan:
        if slot["slot"] in posted:
            continue
        fire_time = slot.get("fire_time") or slot.get("target_time", "00:00")
        if fire_time > now:
            continue
        h, m = map(int, fire_time.split(":"))
        fire_dt = now_dt.replace(hour=h, minute=m, second=0, microsecond=0)
        if (now_dt - fire_dt).total_seconds() > 2700:
            posted.append(slot["slot"])
            log.info("Skipping stale slot [%s] %s", fire_time, slot["type"].upper())
            continue
        return slot
    return None


def process_storyline(state: dict, key: str) -> dict:
    plan   = state.get(f"{key}_plan", [])
    posted = state.get(f"{key}_posted", [])
    symbol = state.get(f"{key}_ticker", "")

    slot = next_due_slot(plan, posted)
    if not slot:
        return state

    log.info("%s slot due: [%s] %s — %s",
             key.upper(), slot.get("fire_time") or slot.get("target_time"), slot["type"].upper(), slot["angle"])

    price_ctx = get_price_context(symbol)
    community = get_ticker_context(symbol)
    tweet     = generate_tweet(symbol, slot, price_ctx, community)

    if tweet:
        log.info("%s tweet (%d chars):\n%s", key.upper(), len(tweet), tweet)
        if post_tweet(tweet, state):
            state[f"{key}_posted"].append(slot["slot"])
            save_state(state)

    return state


def is_weekend() -> bool:
    return datetime.date.today().weekday() >= 5


def run_cycle(state: dict) -> dict:
    state = ensure_daily_plans(state)
    state = process_storyline(state, "eu")
    state = process_storyline(state, "us")

    if not is_weekend():
        watched = list({state.get("eu_ticker", ""), state.get("us_ticker", "")} - {""})
        if watched:
            state = check_price_events(state, watched)

    return state

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("Bot starting. DRY_RUN=%s  DAILY_LIMIT=%d  HEADLESS=%s",
             DRY_RUN, DAILY_POST_LIMIT, HEADLESS)
    log.info("EU watchlist: %s | US watchlist: %s",
             EU_WATCHLIST or "not set", US_WATCHLIST or "not set")

    state = load_state()
    state = run_cycle(state)
    # Single cycle — GitHub Actions re-triggers on schedule
    # For local continuous run, uncomment the loop below:
    # while True:
    #     time.sleep(120)
    #     state = load_state()
    #     state = run_cycle(state)


if __name__ == "__main__":
    main()
