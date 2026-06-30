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

SLOT_JITTER_SECONDS          = int(os.getenv("SLOT_JITTER_SECONDS", "300"))
DRY_RUN                      = os.getenv("DRY_RUN", "false").lower() == "true"
EU_WATCHLIST                 = [t.strip().upper() for t in os.getenv("EU_WATCHLIST", "").split(",") if t.strip()]
US_WATCHLIST                 = [t.strip().upper() for t in os.getenv("US_WATCHLIST", "").split(",") if t.strip()]
EU_FOCUS_TICKER              = os.getenv("EU_FOCUS_TICKER", "").strip().upper().replace("{}", "")
US_FOCUS_TICKER              = os.getenv("US_FOCUS_TICKER", "").strip().upper().replace("{}", "")
EVENT_INTERVAL_THRESHOLD_PCT = float(os.getenv("EVENT_INTERVAL_THRESHOLD_PCT", "1.5"))
EVENT_DAY_THRESHOLD_PCT      = float(os.getenv("EVENT_DAY_THRESHOLD_PCT", "4.0"))
EVENT_COOLDOWN_MINUTES       = int(os.getenv("EVENT_COOLDOWN_MINUTES", "12"))
NEWS_COOLDOWN_MINUTES        = 60

HEADLESS = os.getenv("CI", "false") == "true"

STATE_FILE   = os.path.join(os.path.dirname(__file__), "state.json")
SESSION_FILE = os.path.join(os.path.dirname(__file__), "twitter_session.json")

DAILY_POST_LIMIT = 32

# ── Slot definitions ──────────────────────────────────────────────────────────

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


def is_weekend() -> bool:
    return datetime.date.today().weekday() >= 5


def market_phase(key: str) -> str:
    """Return 'pre_market', 'open', or 'post_market' for the given storyline."""
    now = now_hhmm()
    if key == "eu":
        if now < "09:00":
            return "pre_market"
        if now <= "17:30":
            return "open"
        return "post_market"
    if key == "us":
        if now < "15:30":
            return "pre_market"
        if now <= "22:00":
            return "open"
        return "post_market"
    return "open"


def active_tickers_sorted() -> list[str]:
    combined = set(EU_WATCHLIST + US_WATCHLIST)
    if EU_FOCUS_TICKER:
        combined.add(EU_FOCUS_TICKER)
    if US_FOCUS_TICKER:
        combined.add(US_FOCUS_TICKER)
    return sorted(combined)

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
        ticker = yf.Ticker(symbol)
        info   = ticker.fast_info
        prev   = round(info.previous_close, 2)
        if prev <= 0:
            return {}

        # Use 1-minute history last close as primary price — more reliable than
        # fast_info.last_price which can return a stale cached value near prev close.
        price = None
        day_high = None
        day_low  = None
        market_open = False
        try:
            hist = ticker.history(period="1d", interval="1m")
            if not hist.empty:
                price    = round(float(hist["Close"].iloc[-1]), 2)
                day_high = round(float(hist["High"].max()), 2)
                day_low  = round(float(hist["Low"].min()), 2)
                last_ts  = hist.index[-1]
                now_utc  = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                market_open = (now_utc - last_ts).total_seconds() < 900
        except Exception:
            pass

        # Fall back to fast_info if history unavailable
        if price is None:
            price = round(info.last_price, 2)

        if price <= 0:
            return {}

        # Cross-check: if fast_info diverges >5% from history close, it's likely stale
        fast_price = round(info.last_price, 2)
        if fast_price > 0 and abs(fast_price - price) / price > 0.05:
            log.warning(
                "fast_info.last_price=%s diverges >5%% from history close=%s for %s — using history",
                fast_price, price, symbol,
            )

        # Reject clearly bad data
        if price > prev * 3 or price < prev * 0.3:
            log.warning("Suspicious price data for %s: price=%s prev=%s — skipping", symbol, price, prev)
            return {}

        change = round((price - prev) / prev * 100, 2)
        result = {"price": price, "prev_close": prev, "change_pct": change, "market_open": market_open}
        if day_high is not None:
            result["day_high"] = day_high
        if day_low is not None:
            result["day_low"] = day_low
        return result
    except Exception as e:
        log.warning("yfinance failed for %s: %s", symbol, e)
        return {}



def get_week_performance(symbols: list[str]) -> dict[str, float]:
    result = {}
    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")
            if len(hist) >= 2:
                start = hist["Close"].iloc[0]
                end   = hist["Close"].iloc[-1]
                result[symbol] = round((end - start) / start * 100, 2)
        except Exception as e:
            log.warning("Week performance failed for %s: %s", symbol, e)
    return result

# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_json_fences(text: str) -> str:
    text = text.strip()
    if "```" in text:
        lines = [l for l in text.splitlines() if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    return text

# ── Gemini ────────────────────────────────────────────────────────────────────

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
                wait = 30 * (attempt + 1)
                log.warning("Gemini %d – waiting %ds before retry %d/2", e.code, wait, attempt + 1)
                time.sleep(wait)
            else:
                raise

# ── Daily plan ────────────────────────────────────────────────────────────────

PLANNER_SYSTEM_WEEKDAY = """You are planning a day of Twitter content about a single stock ticker.
Return a JSON array. Each object must have:
  - "slot": integer index (0-based)
  - "type": matching the type provided
  - "angle": one sentence – the specific narrative angle for that tweet

Each slot is tagged with its market phase. Write angles appropriate to that phase:

PRE_MARKET slots (before market opens):
- Focus on catalysts, news context, and what to watch at the open.
- Do NOT react to price moves as if happening now — market is not open.
- Reference recent headlines to set up the day's narrative.
- Never reference live price figures as if the market is moving.

OPEN slots (during market hours):
- React to live price action and news as they develop.
- Reference specific price levels, % moves, and intraday context from the price data.
- Build the narrative as the session progresses.

POST_MARKET / WRAP slots (after market close):
- Summarise the day factually: how the stock moved intraday, where it closed.
- Reference what drove the move (news, macro, sector) if known from headlines.
- No forward speculation beyond "at tomorrow's open". A statement, not a question.

General rules:
- GROUND every angle in the recent news headlines provided. Reference real events, deals, or data points.
- Every angle must carry a point of view. Never purely descriptive.
- Always frame forward-looking statements as possibility: could, might, may, potentially.
- Never use definitive predictions: no "will", "confirms", "proves", "guarantees".
- Never reference a specific day name. Use "at the open" or "tomorrow's open".
- Never use em dash. Use en dash (–) only.
- analytical slots: use actual numbers, ratios, or price levels.
- fomo slots: quiet, unsettling observation based on a real event being underpriced. No question needed.
  Example: "X signed a deal last week and the stock barely moved – that might not last."
- question slots: only if the question genuinely adds value – not as a reflex.
- If a headline appears to be about a different company that shares part of the ticker's name, ignore it entirely.
- Every angle must be distinct. Build a coherent story arc across the day.
Return ONLY the JSON array, no other text."""

PLANNER_SYSTEM_WEEKEND = """You are planning weekend Twitter content about a single stock ticker.
Markets are closed. Return a JSON array. Each object must have:
  - "slot": integer index (0-based)
  - "type": matching the type provided
  - "angle": one sentence – the specific narrative angle for that tweet

Weekend rules – STRICT:
- GROUND angles in the recent news headlines provided. Reference specific events from the past week.
- NEVER reference today's price, daily % moves, or live market activity – markets are closed.
- NEVER use phrases like "today", "this session", "the dip", "down X%", "up X%".
- Every angle must carry a point of view or forward hook. Never purely descriptive.
- Always frame as possibility: "X happened, which could mean Y" or "X happened – is the market pricing this in?"
- Never use definitive predictions: no "will", "confirms", "proves", "guarantees".
- Never reference a specific day name. Use "at the open" or "tomorrow's open" instead.
- Never use em dash. Use en dash (–) only.
- Focus on: week-in-review, structural thesis grounded in recent news, what to watch at the open.
- question slots: one genuine question rooted in a specific news angle or structural thesis. Only use a question if it genuinely adds value – examples: "Given [specific news], has the market priced this in?" or "Bull or bear into next week given [catalyst]?" Never a reflex ending.
- fomo slots: quiet long-term conviction tied to a specific real development from the week. No question needed.
- hook slots: open with a striking fact, paradox, or overlooked news item from the past week.
- wrap slots: name 2-3 specific catalysts or events to watch at the open. A statement, not a question.
- If a headline appears to be about a different company that shares part of the ticker's name, ignore it entirely.
- Every angle must be distinct. Build a coherent weekend narrative.
Return ONLY the JSON array, no other text."""


_FALLBACK_ANGLES = {
    "hook":       "This ticker is being systematically overlooked – the setup is rare.",
    "analytical": "Let's cut through the noise and look at what the data is actually saying.",
    "question":   "Are you bullish or bearish on this name heading into the open?",
    "fomo":       "The quiet accumulation phase doesn't announce itself. This might be it.",
    "reaction":   "The price action here is telling a story most people are misreading.",
    "wrap":       "That's the session. Tomorrow's open will be the tell – watch it closely.",
    "event":      "Big move. Here's what the market could be pricing in – and what it might be missing.",
}


def generate_daily_plan(symbol: str, slots: list[dict], price_ctx: dict, market: str) -> list[dict]:
    price_str = ""
    if price_ctx:
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        price_str = f"Current price: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% vs yesterday)"

    news = get_ticker_context(symbol, max_messages=8)
    news_block = "\n".join(f"- {h}" for h in news) if news else "No recent news available."

    def _slot_phase(target_time: str, market: str) -> str:
        if market == "European":
            if target_time < "09:00":
                return "PRE_MARKET"
            if target_time >= "16:55":
                return "POST_MARKET"
            return "OPEN"
        if market == "US":
            if target_time < "15:30":
                return "PRE_MARKET"
            if target_time >= "22:00":
                return "POST_MARKET"
            return "OPEN"
        return "OPEN"

    slot_list = "\n".join(
        f"  slot {s['slot']}: {s['target_time']} [{_slot_phase(s['target_time'], market)}] – {s['type']}"
        for s in slots
    )

    prompt = f"""Plan a {market} market day of Twitter content about ${symbol}.
{price_str}

Recent news headlines (ground your angles in these):
{news_block}

Slots (produce exactly one entry per slot — note the market phase tag for each):
{slot_list}

Make every angle distinct. Match the angle to the market phase. Build a narrative that evolves across the day."""

    planner_system = PLANNER_SYSTEM_WEEKEND if is_weekend() else PLANNER_SYSTEM_WEEKDAY
    try:
        text = _strip_json_fences(_gemini(planner_system, prompt))
        plan = json.loads(text)
        fire_map = {s["slot"]: s["fire_time"] for s in slots}
        for p in plan:
            p["fire_time"] = fire_map.get(p["slot"], slots[p["slot"]]["target_time"])
        return plan
    except Exception as e:
        log.error("Plan generation failed for %s: %s", symbol, e)

    return [{**s, "angle": _FALLBACK_ANGLES.get(s["type"], f"Analyze ${symbol} from the {s['type']} angle")} for s in slots]


def pick_ticker(watchlist: list[str], override: str, label: str, exclude: str = "") -> str:
    if override:
        return override
    if watchlist:
        day_index = (datetime.date.today() - datetime.date(2026, 1, 1)).days
        # US gets +1 offset so EU and US never cover the same ticker on the same day
        offset = 1 if label == "US" else 0
        candidates = [t for t in watchlist if t != exclude] if exclude else watchlist
        if not candidates:
            candidates = watchlist
        return candidates[(day_index + offset) % len(candidates)]
    log.warning("No %s watchlist configured – defaulting to SPY", label)
    return "SPY"


def ensure_storyline(state: dict, key: str, watchlist: list[str], override: str,
                     slot_defs: list[tuple], market: str) -> dict:
    if state.get("date") == today() and state.get(f"{key}_plan"):
        return state

    # Ensure EU and US never cover the same ticker on the same day
    exclude = state.get("eu_ticker", "") if key == "us" else ""
    symbol  = pick_ticker(watchlist, override, key.upper(), exclude=exclude)
    price_ctx = get_price_context(symbol)
    slots     = _build_slots(slot_defs)
    plan      = generate_daily_plan(symbol, slots, price_ctx, market)

    state[f"{key}_ticker"] = symbol
    state[f"{key}_plan"]   = plan
    state[f"{key}_posted"] = []

    log.info("%s story: $%s – %d slots planned", key.upper(), symbol, len(plan))
    for p in plan:
        log.info("  [%s] %s – %s", p["fire_time"], p["type"].upper(), p["angle"])

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

TWEET_SYSTEM = """## Role
You are an expert financial X (Twitter) market commentator — sharp, credible, market-native.

## Aim
Write one concise, engaging tweet about the provided stock using only the supplied data.

## Rules
- NEVER invent or infer market conditions, catalysts, or facts not present in the provided data.
- If a headline mentions another company, reference only what that headline actually says — never fabricate what other stocks are doing.
- Reference at least one provided headline specifically. Specific beats vague, always.
- Mention the ticker, current price, and % daily change where relevant.
- Frame all forward-looking statements as possibilities, never certainties.
  Use: could, might, may, potentially, worth watching, raises the question.
  Never: will, confirms, proves, guarantees.
- Every tweet must have a clear point of view. A question or CTA at the end is only used when it flows naturally — never forced.
- Never reference a specific day name. Use "at the open" or "tomorrow's open" instead.
- Never use em dash. Use en dash (–) only.
- Use line breaks to create breathing room – no walls of text.
- Emoji: use sparingly. 🟢🔴 are only for a direct "green or red at the open?" question — place them on their own line immediately before that question. Never use 🟢🔴 inside a sentence or next to unrelated questions. 👇 only for a genuine CTA. When in doubt, use no emoji at all.
- No filler: "hot take", "buckle up", "thread", "building the backbone", "this is huge".
- 1-2 hashtags max, only if they add signal. Omit if they feel forced.
- MUST be under 280 characters.

## Tweet types
  hook       – stops the scroll. Open with a striking fact or news item. End with a hook or implication.
  analytical – use specific numbers, price levels, or data points. State the implication clearly.
  question   – one sharp, genuine question rooted in real news or price action. Only if the question adds real value — not as a reflex ending. Place the question on its own line.
  reaction   – weekdays: ground in the actual price move and what may be driving it.
               Weekends: react to the week's news, not daily moves.
  fomo       – short, calm, unsettling observation based on a real event being underpriced. No question needed.
               Example: "$NOK signed a massive 5G deal last week. Stock barely moved. That might not last."
  wrap       – closes the session. Name specific catalysts to watch at the open. Statement, not a question.
  event      – urgent reaction to a price move or major news. Raw and immediate.
               Include a "so what" framed as possibility.
               If ending with a green/red question, place 🟢🔴 on their own line, then the question on the next line.

## Review before output
Verify: all facts match the input — no unsupported claims — at least one headline referenced — tweet ≤280 characters.

## Output
One tweet only. No quotes, no commentary."""

NEWS_CLASSIFIER_SYSTEM = """You are a financial news classifier. Given a news headline and a stock ticker, determine if the headline represents a major catalyst that could significantly move the stock price.

A headline qualifies as MAJOR if it meets ANY of these criteria:

Earnings:
- EPS beat or miss vs estimate: >5%
- Revenue beat or miss vs estimate: >3%
- Full-year guidance raise or cut: any
- Margin guidance change: >2 percentage points

Contract / Partnership:
- Contract value: >$100M or described as "major", "significant", "multi-year"
- New major customer win or loss: any
- Government or defense contract: any

M&A:
- Acquisition, merger, or buyout: any
- Buyout rumor with named acquirer: any
- Asset sale >$500M: any

Analyst:
- First-ever initiation of coverage by a major firm only (not upgrades/downgrades)

Regulatory / Geopolitical:
- Named company or direct sector ruling: any
- Export control or sanctions affecting supply chain: any
- Government security ban on competitor or own products: any
- CHIPS Act or equivalent government funding: any

Macro:
- Hyperscaler capex revision: >$1B
- Fed rate decision surprise vs expectation: any
- Semiconductor supply/demand shift: >10%

Company-specific:
- Share buyback announcement: >$500M
- Insider buying: >$1M in a single transaction
- Dividend change: any
- Loss of a top 3 customer: any
- Patent win or loss in core business area: any

Ignore: opinion pieces, analysis recaps, general market commentary not specific to the ticker, and any headline that appears to be about a different company that shares part of the ticker's name.

Respond with a JSON object:
{
  "is_major": true or false,
  "category": "earnings|contract|ma|analyst|regulatory|macro|company|none",
  "headline": "the exact headline that triggered this",
  "reason": "one sentence explaining why it qualifies or not"
}

Return ONLY the JSON object, no other text."""

ENGAGEMENT_SYSTEM = """## Role
You are writing a weekly engagement post for a financial Twitter account tracking AI infrastructure stocks – connectivity, memory, networking. The unglamorous but essential picks behind the AI buildout.

## Rules
- Casual, direct, first-person voice – never sounds automated or templated
- Use line breaks – no walls of text
- Always list tickers alphabetically, one per line, with $ prefix
- A closing question or CTA is used when it flows naturally – not as a reflex
- Emoji: sparingly and only where genuinely meaningful. 👇 for a real CTA only. When in doubt, omit.
- No filler, no hype, no em dash – use en dash (–) only
- Never reference a specific day name. Use "at the open" or "tomorrow's open".
- Forward-looking statements as possibilities only: could, might, may – never will or confirms.
- MUST be under 280 characters.

## Output
Post text only. No quotes, no commentary."""


def generate_tweet(symbol: str, slot: dict, price_ctx: dict, community: list[str],
                   event_trigger: str = "", phase: str = "open") -> str | None:

    # Build price context string based on market phase
    price_str = ""
    if price_ctx and not is_weekend():
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        if phase == "pre_market":
            price_str = f"Previous close: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% vs prior day). Market not yet open."
        elif phase == "post_market":
            parts = [f"Closed at: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% on the day)"]
            if "day_high" in price_ctx and "day_low" in price_ctx:
                parts.append(f"Intraday range: low ${price_ctx['day_low']} / high ${price_ctx['day_high']}")
            price_str = ". ".join(parts) + ". Market closed."
        else:
            parts = [f"Live: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% today)"]
            if "day_high" in price_ctx and "day_low" in price_ctx:
                parts.append(f"Intraday range: low ${price_ctx['day_low']} / high ${price_ctx['day_high']}")
            price_str = ". ".join(parts)

    # Phase-specific instruction
    if phase == "pre_market":
        phase_instruction = "Market is NOT yet open. Do NOT react to price moves as if they are happening now. Focus on catalysts, news, and what to watch at the open."
    elif phase == "post_market":
        phase_instruction = "Market is CLOSED. Write a factual day summary: reference how the stock moved intraday (opened, hit high/low, closed). Use the intraday range data provided. No forward speculation beyond 'at tomorrow's open'."
    else:
        phase_instruction = "Market is open. React to live price action and news."

    news_block = "\n".join(f"- {m}" for m in community) if community else ""
    news_section = f"\nRecent news headlines (reference at least one in your tweet):\n{news_block}" if news_block else "\nNo recent news available – use the angle and price context only."
    angle      = event_trigger if event_trigger else slot["angle"]
    tweet_type = "event"      if event_trigger else slot["type"]

    prompt = f"""Ticker: ${symbol}
{price_str}

Market phase: {phase_instruction}

Angle: {angle}
Tweet type: {tweet_type}
{news_section}

Write the tweet. Keep it under 280 characters. Be specific – name real events, numbers, or catalysts. Use line breaks for breathing room. End with a clear point of view. A question or CTA only if it flows naturally."""

    try:
        text = _gemini(TWEET_SYSTEM, prompt).strip('"').strip("'")
        if len(text) > 280:
            # Trim to last complete sentence within limit
            trimmed = text[:280]
            for sep in (". ", ".\n", "? ", "?\n", "! ", "!\n"):
                idx = trimmed.rfind(sep)
                if idx > 100:
                    return trimmed[:idx + 1]
            # Fall back to last word boundary
            return trimmed.rsplit(" ", 1)[0]
        return text
    except Exception as e:
        log.error("Tweet generation failed: %s", e)
    return None

# ── News event classifier ─────────────────────────────────────────────────────

def classify_news(symbol: str, headlines: list[str]) -> dict | None:
    if not headlines:
        return None

    headlines_block = "\n".join(f"- {h}" for h in headlines)
    prompt = f"""Ticker: ${symbol}

Headlines:
{headlines_block}

Classify whether any of these headlines is a major catalyst for ${symbol}."""

    try:
        text = _strip_json_fences(_gemini(NEWS_CLASSIFIER_SYSTEM, prompt))
        result = json.loads(text)
        if result.get("is_major"):
            return result
    except Exception as e:
        log.warning("News classification failed for %s: %s", symbol, e)
    return None


NEWS_EVENT_SYSTEM = """## Role
You are an expert financial X (Twitter) market commentator reacting to a major news event.

## Aim
Write one immediate, specific reaction tweet using only the supplied headline and price data.

## Rules
- Reference the specific headline directly — never generic reactions
- NEVER invent or infer facts beyond what the headline and price data state
- Include a "so what": what this could mean for price, margins, market share, or competitive position
- Frame all implications as possibilities, never certainties
  Use: could, might, may, potentially, raises the question, worth watching
  Never: will, confirms, proves, guarantees
- Never restate the headline without adding meaning or context
- A closing question or CTA is only used when it flows naturally — never forced
- No filler: "this is huge", "big news", "buckle up"
- Use line breaks – no walls of text
- Emoji: sparingly. 🟢🔴 only for a direct "green or red at the open?" question — place them on their own line immediately before that question. Never inside a sentence. When in doubt, omit.
- Never use em dash. Use en dash (–) only.
- Never reference a specific day name. Use "at the open" or "tomorrow's open".
- MUST be under 280 characters.

## Review before output
Verify: facts match the input — no unsupported claims — tweet ≤280 characters.

## Output
One tweet only. No quotes, no commentary."""


def generate_news_event_tweet(symbol: str, classification: dict, price_ctx: dict) -> str | None:
    price_str = ""
    if price_ctx and not is_weekend():
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        price_str = f"Current: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% today)"

    prompt = f"""Ticker: ${symbol}
{price_str}

Major news headline: {classification['headline']}
Category: {classification['category']}
Why it qualifies: {classification['reason']}

Write a reaction tweet. Be specific. Add a "so what" framed as possibility. End with a clear point of view. A 🟢🔴 question or CTA only if it flows naturally."""

    try:
        text = _gemini(NEWS_EVENT_SYSTEM, prompt).strip('"').strip("'")
        if len(text) > 280:
            trimmed = text[:280]
            for sep in (". ", ".\n", "? ", "?\n", "! ", "!\n"):
                idx = trimmed.rfind(sep)
                if idx > 100:
                    return trimmed[:idx + 1]
            return trimmed.rsplit(" ", 1)[0]
        return text
    except Exception as e:
        log.error("News event tweet generation failed: %s", e)
    return None

# ── Weekly engagement posts ───────────────────────────────────────────────────

def check_weekly_engagement(state: dict) -> dict:
    today_str  = today()
    weekday    = datetime.date.today().weekday()
    now        = now_hhmm()
    tickers    = active_tickers_sorted()
    ticker_str = "\n".join(f"${t}" for t in tickers)
    engagement = state.setdefault("engagement", {})

    posts = []

    # All time windows are CET (TZ=Europe/Amsterdam set on runner)
    # Monday open:      07:00–08:30 CET (pre-market, before EU open at 09:00)
    # Wednesday midday: 12:00–13:30 CET
    # Friday close:     17:00–18:30 CET (after EU market close)
    # Saturday recap:   10:00–11:30 CET

    if weekday == 0 and "07:00" <= now <= "08:30" and engagement.get("monday") != today_str:
        posts.append(("monday", f"""Write a Monday opening post for a financial Twitter account.

This week's tickers (already sorted alphabetically, list them exactly as given):
{ticker_str}

Format:
- Open with a short intro line about tracking these this week
- List the tickers on separate lines
- End with: "$1000 to allocate across these – how do you split it? 👇"

Keep it casual, direct, under 280 characters."""))

    if weekday == 2 and "12:00" <= now <= "13:30" and engagement.get("wednesday") != today_str:
        posts.append(("wednesday", f"""Write a midweek engagement post for a financial Twitter account.

This week's tickers (already sorted alphabetically, list them exactly as given):
{ticker_str}

Format:
- Open with a short midweek check-in line
- List the tickers on separate lines
- End with: "Which one did you add this week? 👇"

Keep it casual, direct, under 280 characters."""))

    if weekday == 4 and "17:00" <= now <= "18:30" and engagement.get("friday") != today_str:
        posts.append(("friday", f"""Write a Friday closing post for a financial Twitter account.

This week's tickers (already sorted alphabetically, list them exactly as given):
{ticker_str}

Format:
- Open with a short week wrap line
- List the tickers on separate lines
- End with what to watch at the open next week and a CTA

Keep it casual, direct, under 280 characters."""))

    if weekday == 6 and "19:00" <= now <= "20:00" and engagement.get("sunday") != today_str:
        posts.append(("sunday", f"""Write a Sunday evening weekly outlook post for a financial Twitter account focused on AI infrastructure stocks.

This week's tickers (already sorted alphabetically, list them exactly as given):
{ticker_str}

Format:
- Open with a short line about the week ahead
- List the tickers on separate lines
- For each ticker hint at what could matter this week (catalysts, earnings, macro events) framed as possibilities – could, might, worth watching
- End with a forward-looking question or CTA about the open

Keep it calm, considered, under 280 characters. No hype. No em dash – use en dash only."""))

    if weekday == 5 and "10:00" <= now <= "11:30" and engagement.get("saturday") != today_str:
        perf = get_week_performance(tickers)
        if perf:
            perf_lines = "\n".join(
                f"${t}  {'+' if v >= 0 else ''}{v}%" for t, v in sorted(perf.items())
            )
            posts.append(("saturday", f"""Write a Saturday weekly performance post for a financial Twitter account.

Weekly performance (use exactly these numbers, tickers already sorted alphabetically):
{perf_lines}

Format:
- Open with "Week in numbers:" or similar
- List each ticker and its performance on a separate line exactly as given
- End with an engaging question like "Which one surprised you most? 👇"

Keep it casual, direct, under 280 characters."""))

    for key, prompt in posts:
        try:
            text = _gemini(ENGAGEMENT_SYSTEM, prompt).strip('"').strip("'")
            if len(text) > 280:
                trimmed = text[:280]
                for sep in (". ", ".\n", "? ", "?\n", "! ", "!\n"):
                    idx = trimmed.rfind(sep)
                    if idx > 100:
                        text = trimmed[:idx + 1]
                        break
                else:
                    text = trimmed.rsplit(" ", 1)[0]
            tweet = text
            log.info("Engagement [%s] (%d chars):\n%s", key, len(tweet), tweet)
            if post_tweet(tweet, state):
                engagement[key] = today_str
                save_state(state)
        except Exception as e:
            log.error("Engagement post [%s] failed: %s", key, e)

    return state

# ── Twitter via Playwright ────────────────────────────────────────────────────

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
        log.info("Daily post limit (%d) reached – skipping", DAILY_POST_LIMIT)
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

            page.goto("https://x.com/home", wait_until="load", timeout=60000)
            time.sleep(random.uniform(2.0, 3.0))

            if "login" in page.url or "flow/login" in page.url:
                log.error("Twitter session expired — re-run login.py and update TWITTER_SESSION secret. URL: %s", page.url)
                browser.close()
                return False

            textarea = page.locator("[data-testid='tweetTextarea_0']").first
            textarea.wait_for(timeout=15000)
            textarea.click()
            time.sleep(random.uniform(0.5, 1.2))

            for char in text:
                page.keyboard.type(char)
                time.sleep(random.uniform(0.03, 0.11))

            time.sleep(random.uniform(1.5, 3.0))

            post_btn = page.locator("[data-testid='tweetButtonInline']")
            post_btn.wait_for(timeout=10000)
            post_btn.dispatch_event("click")
            time.sleep(random.uniform(2.5, 4.0))

            context.storage_state(path=SESSION_FILE)
            browser.close()

        state["daily_posts"] = state.get("daily_posts", 0) + 1
        log.info("Posted (%d/%d): %s", state["daily_posts"], DAILY_POST_LIMIT, text[:80])
        return True

    except Exception as e:
        log.error("Tweet post failed: %s", e)
        try:
            log.error("Page URL at failure: %s", page.url)
        except Exception:
            pass
        return False

# ── Event monitor ─────────────────────────────────────────────────────────────

def check_price_events(state: dict, symbols: list[str]) -> dict:
    snapshots = state.setdefault("price_snapshots", {})
    cooldowns = state.setdefault("event_cooldowns", {})

    candidates = []

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

        last_price = snapshots.get(symbol)

        # Skip if no baseline yet — don't trigger on first run
        if last_price is None:
            snapshots[symbol] = current
            continue

        interval_pct = abs((current - last_price) / last_price * 100)

        intraday = ""
        if "day_high" in price_ctx and "day_low" in price_ctx:
            intraday = (
                f" Intraday range: low ${price_ctx['day_low']} / high ${price_ctx['day_high']}."
                f" Use this context — if the stock dropped sharply and is now rebounding, say so."
            )

        if interval_pct >= EVENT_INTERVAL_THRESHOLD_PCT:
            direction = "up" if current > last_price else "down"
            candidates.append((interval_pct, symbol, price_ctx, (
                f"${symbol} just moved {direction} {interval_pct:.1f}% in the last few minutes "
                f"(now ${current}, {price_ctx['change_pct']:+.1f}% on the day).{intraday} "
                f"React immediately and specifically to what is actually happening — "
                f"a rebound is different from a continuation move."
            )))
        elif day_pct >= EVENT_DAY_THRESHOLD_PCT:
            direction = "up" if price_ctx["change_pct"] > 0 else "down"
            candidates.append((day_pct, symbol, price_ctx, (
                f"${symbol} is {direction} {day_pct:.1f}% today (now ${current}).{intraday} "
                f"This is a significant day move. React with conviction."
            )))

        snapshots[symbol] = current

    # Only fire the single biggest mover per cycle to avoid Gemini rate limit spiral
    if candidates:
        candidates.sort(reverse=True)
        _, symbol, price_ctx, trigger = candidates[0]
        log.info("PRICE EVENT triggered for $%s: %s", symbol, trigger)
        community = get_ticker_context(symbol)
        slot  = {"type": "event", "format": "short", "angle": trigger}
        tweet = generate_tweet(symbol, slot, price_ctx, community, event_trigger=trigger)
        if tweet:
            log.info("Price event tweet (%d chars):\n%s", len(tweet), tweet)
            if post_tweet(tweet, state):
                cooldowns[symbol] = now_minutes()

    save_state(state)
    return state


def get_ticker_context_with_dates(symbol: str, max_messages: int = 10) -> list[dict]:
    try:
        url = f"https://finance.yahoo.com/rss/headline?s={symbol}"
        resp = requests.get(url, timeout=10, verify=False,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        import xml.etree.ElementTree as ET
        from email.utils import parsedate_to_datetime
        root = ET.fromstring(resp.content)
        items = root.findall(".//item")
        results = []
        for item in items[:max_messages]:
            title = item.findtext("title")
            pub_date = item.findtext("pubDate")
            if not title:
                continue
            try:
                pub_dt = parsedate_to_datetime(pub_date).replace(tzinfo=None) if pub_date else None
            except Exception:
                pub_dt = None
            results.append({"headline": title, "published": pub_dt})
        return results
    except Exception as e:
        log.warning("News fetch with dates failed for %s: %s", symbol, e)
        return []


def check_news_events(state: dict, symbols: list[str]) -> dict:
    # Only run during market hours CET to avoid off-hours noise
    if not ("09:00" <= now_hhmm() <= "22:00"):
        return state

    cooldowns = state.setdefault("event_cooldowns", {})
    news_seen = state.setdefault("news_seen", {})
    now_dt    = datetime.datetime.utcnow()

    for symbol in symbols:
        last_event_min = cooldowns.get(f"news_{symbol}", 0)
        if now_minutes() - last_event_min < NEWS_COOLDOWN_MINUTES:
            continue

        articles = get_ticker_context_with_dates(symbol, max_messages=10)

        # Only consider headlines published within the last 24 hours
        recent = [
            a["headline"] for a in articles
            if a["published"] and (now_dt - a["published"]).total_seconds() < 86400
        ]
        new_headlines = [h for h in recent if h not in news_seen.get(symbol, [])]
        if not new_headlines:
            news_seen[symbol] = [a["headline"] for a in articles]
            continue

        classification = classify_news(symbol, new_headlines)
        if classification:
            log.info("NEWS EVENT for $%s [%s]: %s", symbol, classification["category"], classification["headline"])
            price_ctx = get_price_context(symbol)
            tweet = generate_news_event_tweet(symbol, classification, price_ctx)
            if tweet:
                log.info("News event tweet (%d chars):\n%s", len(tweet), tweet)
                if post_tweet(tweet, state):
                    cooldowns[f"news_{symbol}"] = now_minutes()

        news_seen[symbol] = [a["headline"] for a in articles]
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

    price_ctx = get_price_context(symbol)
    phase     = market_phase(key)

    # If market is still open, don't fire wrap/reaction — override to analytical
    if slot["type"] in ("wrap", "reaction") and price_ctx.get("market_open", False):
        log.info("Market still open for $%s – overriding %s slot to analytical", symbol, slot["type"])
        slot = {**slot, "type": "analytical"}

    log.info("%s slot due: [%s] %s (phase=%s) – %s",
             key.upper(), slot.get("fire_time") or slot.get("target_time"),
             slot["type"].upper(), phase, slot["angle"])

    community = get_ticker_context(symbol)
    tweet     = generate_tweet(symbol, slot, price_ctx, community, phase=phase)

    if tweet:
        log.info("%s tweet (%d chars):\n%s", key.upper(), len(tweet), tweet)
        if post_tweet(tweet, state):
            state[f"{key}_posted"].append(slot["slot"])
            save_state(state)

    return state


def run_cycle(state: dict) -> dict:
    state = ensure_daily_plans(state)
    state = check_weekly_engagement(state)
    state = process_storyline(state, "eu")
    state = process_storyline(state, "us")

    all_tickers = active_tickers_sorted()
    # Price events watch all tickers but only fire the biggest mover per cycle
    if not is_weekend() and all_tickers:
        state = check_price_events(state, all_tickers)

    # News events only check the day's story ticker to avoid Gemini rate limit spiral
    story_tickers = list({state.get("eu_ticker", ""), state.get("us_ticker", "")} - {""})
    if not is_weekend() and story_tickers:
        state = check_news_events(state, story_tickers)

    return state

# ── Main ──────────────────────────────────────────────────────────────────────

LOCK_FILE = os.path.join(os.path.dirname(__file__), "bot.lock")


def main():
    if os.path.exists(LOCK_FILE):
        log.warning("Another instance is already running (bot.lock exists) — exiting.")
        return

    try:
        with open(LOCK_FILE, "w") as f:
            f.write(str(os.getpid()))

        log.info("Bot starting. DRY_RUN=%s  DAILY_LIMIT=%d  HEADLESS=%s",
                 DRY_RUN, DAILY_POST_LIMIT, HEADLESS)
        log.info("EU watchlist: %s | US watchlist: %s",
                 EU_WATCHLIST or "not set", US_WATCHLIST or "not set")

        state = load_state()
        state = run_cycle(state)

    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)


if __name__ == "__main__":
    main()
