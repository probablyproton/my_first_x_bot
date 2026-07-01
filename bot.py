"""
Ticker Twitter Bot — dual storyline + event-driven edition
EU story:  09:00–17:00 local time  [15 slots + 1 close summary]
US story:  15:15–22:00 local time  [15 slots, pre-market then open]
Weekend:   08:00–21:30             [30 slots]

Overlap window: 15:30–17:00 CET
- EU posts close-outs and wrap (market closing)
- US posts pre-market context and opening reactions (market opening)

Local run:  python bot.py          (browser window visible)
GitHub:     triggered every 15 min by Actions cron (headless, TZ=Europe/Amsterdam)
"""

import os
import re
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

# ── Config ────────────────────────────────────────────────────────────[...[...]

GEMINI_API_KEY               = os.environ["GEMINI_API_KEY"]

SLOT_JITTER_SECONDS          = int(os.getenv("SLOT_JITTER_SECONDS", "300"))
DRY_RUN                      = os.getenv("DRY_RUN", "false").lower() == "true"
EU_WATCHLIST                 = [t.strip().upper() for t in os.getenv("EU_WATCHLIST", "").split(",") if t.strip()]
US_WATCHLIST                 = [t.strip().upper() for t in os.getenv("US_WATCHLIST", "").split(",") if t.strip()]
EVENT_INTERVAL_THRESHOLD_PCT = float(os.getenv("EVENT_INTERVAL_THRESHOLD_PCT", "1.5"))
EVENT_DAY_THRESHOLD_PCT      = float(os.getenv("EVENT_DAY_THRESHOLD_PCT", "4.0"))
EVENT_COOLDOWN_MINUTES       = int(os.getenv("EVENT_COOLDOWN_MINUTES", "60"))
NEWS_COOLDOWN_MINUTES        = 60
TICKER_POST_COOLDOWN_MINUTES = 120  # minimum gap between any two posts about the same ticker
NEWS_POST_MIN_GAP_MINUTES    = int(os.getenv("NEWS_POST_MIN_GAP_MINUTES", "10"))    # base minimum gap between any two news-event posts, across all tickers
NEWS_POST_GAP_JITTER_MINUTES = int(os.getenv("NEWS_POST_GAP_JITTER_MINUTES", "2"))  # +/- randomness applied to that gap each time (e.g. 10+/-2 -> 8-12min)
NEWS_QUEUE_MAX_AGE_MINUTES   = int(os.getenv("NEWS_QUEUE_MAX_AGE_MINUTES", "360"))  # drop a held news event if it's waited this long unreleased (6h – too stale)

HEADLESS = os.getenv("CI", "false") == "true"

STATE_FILE   = os.path.join(os.path.dirname(__file__), "state.json")
SESSION_FILE = os.path.join(os.path.dirname(__file__), "twitter_session.json")

DAILY_POST_LIMIT = 32

# ── Slot definitions ────────────────────────────────────────────────────────[.[...]

EU_SLOTS = [
    ("08:45", "hook"),
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
    ("17:00", "close_summary"),
]

US_SLOTS = [
    ("15:10", "hook"),
    ("15:45", "analytical"),
    ("16:00", "reaction"),
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

# ── State ────────────────────────────────────────────────────────────[...]

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


def _epoch_minutes() -> int:
    """Absolute minute counter (unlike now_minutes, doesn't reset at midnight) —
    used for spacing that must hold correctly across a day boundary."""
    return int(time.time() // 60)


def is_weekend() -> bool:
    return datetime.date.today().weekday() >= 5


def _is_market_open_for(symbol: str) -> bool:
    """Whether the ticker's home market (EU or US) is currently in its trading session."""
    if is_weekend():
        return False
    now = now_hhmm()
    if symbol in EU_WATCHLIST:
        return "09:00" <= now <= "17:30"
    if symbol in US_WATCHLIST:
        return "15:30" <= now <= "22:00"
    # Unknown / off-watchlist ticker — fall back to the broader combined window
    return "09:00" <= now <= "22:00"


def market_phase(key: str) -> str:
    """Return 'weekend', 'pre_market', 'open', or 'post_market' for the given storyline."""
    if is_weekend():
        return "weekend"
    now = now_hhmm()
    if key == "eu":
        if now < "09:00":
            return "pre_market"
        if now <= "17:00":
            return "open"
        return "post_market"
    if key == "us":
        if now < "15:30":
            return "pre_market"
        if now <= "22:00":
            return "open"
        return "post_market"
    return "open"


def in_overlap_window() -> bool:
    """Check if current time is in EU/US overlap window (15:30-17:00 CET)."""
    now = now_hhmm()
    return "15:30" <= now <= "17:00"


def active_tickers_sorted() -> list[str]:
    return sorted(set(EU_WATCHLIST + US_WATCHLIST))

# ── Data ────────────────────────────────────────────────────────────[[...]

_COMPANY_NAME_CACHE: dict[str, str] = {}
_LEGAL_SUFFIX_RE = re.compile(
    r"\s+(Inc\.?|Corp\.?|Holdings?|SA|AG|SE|NV|PLC|Ltd\.?|Oyj|AB|GmbH|Co\.?|LLC)$",
    re.IGNORECASE,
)

def _company_name(symbol: str) -> str:
    if symbol not in _COMPANY_NAME_CACHE:
        try:
            raw = yf.Ticker(symbol).info.get("shortName", "") or ""
            name = _LEGAL_SUFFIX_RE.sub("", raw).strip()
            _COMPANY_NAME_CACHE[symbol] = name or symbol.split(".")[0].split("-")[0]
        except Exception:
            _COMPANY_NAME_CACHE[symbol] = symbol.split(".")[0].split("-")[0]
    return _COMPANY_NAME_CACHE[symbol]


def get_ticker_context(symbol: str, max_messages: int = 8) -> list[str]:
    return [a["headline"] for a in get_ticker_context_with_dates(symbol, max_messages)]


def get_price_context(symbol: str) -> dict:
    try:
        ticker = yf.Ticker(symbol)
        info   = ticker.fast_info
        prev   = round(info.previous_close, 2)
        if prev <= 0:
            return {}

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

        if price is None:
            price = round(info.last_price, 2)

        if price <= 0:
            return {}

        fast_price = round(info.last_price, 2)
        if fast_price > 0 and abs(fast_price - price) / price > 0.05:
            log.warning(
                "fast_info.last_price=%s diverges >5%% from history close=%s for %s — using history",
                fast_price, price, symbol,
            )

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

# ── Helpers ───────────────────────────────────────────────────────────[[...]

def _strip_json_fences(text: str) -> str:
    text = text.strip()
    if "```" in text:
        lines = [l for l in text.splitlines() if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    return text

# ── Gemini ───────────────────────────────────────────────────────────[.[...]

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

def ensure_storyline(state: dict, key: str) -> dict:
    if state.get("date") == today() and state.get(f"{key}_slots"):
        return state
    if is_weekend():
        slot_defs = WEEKEND_SLOTS
    else:
        slot_defs = EU_SLOTS if key == "eu" else US_SLOTS
    slots = _build_slots(slot_defs)
    state[f"{key}_slots"]  = slots
    state[f"{key}_posted"] = []
    log.info("%s: %d slots planned", key.upper(), len(slots))
    return state


def ensure_daily_plans(state: dict) -> dict:
    if state.get("date") != today():
        state = {"date": today(), "daily_posts": 0}

    state = ensure_storyline(state, "eu")
    state = ensure_storyline(state, "us")
    if not is_weekend() and "15:30" <= now_hhmm() <= "17:00":
        log.info("▶ OVERLAP WINDOW 15:30–17:00 CET: EU closing + US opening")

    save_state(state)
    return state

# ── Tweet generation ────────────────────────────────────────────────────────[[...]

TWEET_SYSTEM = """## Role
You are an expert financial X (Twitter) market commentator — sharp, credible, market-native.

## Aim
Write one concise, engaging tweet about the provided stock using only the supplied data.

## Rules
- NEVER invent or infer market conditions, catalysts, or facts not present in the provided data.
- If a headline mentions another company, reference only what that headline actually says — never fabricate what other stocks are doing.
- Reference at least one provided headline specifically. Specific beats vague, always.
- Mention the ticker, current price, and % daily change where relevant.
- Reference the stock only via its bare $TICKER symbol (e.g. $VRT). Never spell out the company
  name in place of, or alongside in parentheses, the ticker.
- Frame all forward-looking statements as possibilities, never certainties.
  Use: could, might, may, potentially, worth watching, raises the question.
  Never: will, confirms, proves, guarantees.
- Every tweet must have a clear point of view. A question or CTA at the end is only used when it flows naturally — never forced.
- Never reference a specific day name. Use "at the open" or "tomorrow's open" instead.
- Never use em dash. Use en dash (–) only.
- Use line breaks to create breathing room – no walls of text.
- Emoji: use sparingly. 🟢🔴 are only for a direct "green or red at the open?" question — place them on their own line immediately before that question.
- No filler: "hot take", "buckle up", "thread", "building the backbone", "this is huge".
- 1-2 hashtags max, only if they add signal. Omit if they feel forced.
- MUST be under 280 characters.

## Tweet types
  hook       – stops the scroll. Open with a striking fact or news item. End with a hook or implication.
  analytical – use specific numbers, price levels, or data points. State the implication clearly.
  question   – one sharp, genuine question rooted in real news or price action. Only if the question adds real value — not as a reflex ending.
  reaction   – ground in the actual price move and what may be driving it.
  fomo       – short, calm, unsettling observation based on a real event being underpriced. No question needed.
  wrap       – closes the session. Name specific catalysts to watch at the open. Statement, not a question.
  close_summary – EU close-out: concise recap of final price and key driver. Factual, no speculation.
  event      – urgent reaction to a price move or major news. Raw and immediate.

## Review before output
Verify: all facts match the input — no unsupported claims — at least one headline referenced — tweet ≤280 characters.

## Output
One tweet only. No quotes, no commentary."""

NEWS_CLASSIFIER_SYSTEM = """You are a financial news classifier. Given a news headline and a stock ticker, determine if the headline represents a major catalyst that could significantly move the stock.

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
You are writing a weekly engagement post for a financial Twitter account tracking AI infrastructure stocks – connectivity, memory, networking.

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

    # FIXED: Only include price for OPEN/REACTION/CLOSE_SUMMARY/POST_MARKET phases
    price_str = ""
    if price_ctx and not is_weekend() and phase in ("open", "reaction", "close_summary", "post_market"):
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        if phase == "close_summary":
            price_str = f"Closed: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% today)"
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
    elif phase == "close_summary":
        phase_instruction = "Market is CLOSED (EU close at 17:00 CET). This is a factual daily summary: focus on how the stock moved intraday and where it closed. Reference what drove today's move based on news and price action. One sentence only."
    elif phase == "post_market":
        phase_instruction = "Market is CLOSED. Write a factual day summary: reference how the stock moved intraday (opened, hit high/low, closed). Use the intraday range data provided. No forward speculation."
    else:
        phase_instruction = "Market is open. React to live price action and news."

    news_block = "\n".join(f"- {m}" for m in community) if community else ""
    news_section = f"\nRecent news headlines (reference at least one in your tweet):\n{news_block}" if news_block else "\nNo recent news available – use the angle and price context only."
    angle      = event_trigger if event_trigger else slot["angle"]
    tweet_type = "event"      if event_trigger else slot["type"]

    prompt = f"""Ticker: ${_base_symbol(symbol)}
{price_str}

Market phase: {phase_instruction}

Angle: {angle}
Tweet type: {tweet_type}
{news_section}

Write the tweet. Keep it under 280 characters. Be specific – name real events, numbers, or catalysts. Use line breaks for breathing room. End with a clear point of view. A question or CTA only if it flows naturally."""

    try:
        text = _gemini(TWEET_SYSTEM, prompt).strip('"').strip("'")
        if _has_unknown_ticker(text, [symbol]):
            return None
        if len(text) > 280:
            trimmed = text[:280]
            for sep in (". ", ".\n", "? ", "?\n", "! ", "!\n"):
                idx = trimmed.rfind(sep)
                if idx > 100:
                    return trimmed[:idx + 1]
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
- Reference the stock only via its bare $TICKER symbol (e.g. $VRT). Never spell out the company
  name in place of, or alongside in parentheses, the ticker.
- Use line breaks – no walls of text
- Emoji: sparingly. 🟢🔴 only for a direct "green or red at the open?" question — place them on their own line immediately before that question.
- Never use em dash. Use en dash (–) only.
- Never reference a specific day name. Use "at the open" or "tomorrow's open".
- MUST be under 280 characters.

## Review before output
Verify: facts match the input — no unsupported claims — tweet ≤280 characters.

## Output
One tweet only. No quotes, no commentary."""


def generate_news_event_tweet(symbol: str, classification: dict, price_ctx: dict) -> str | None:
    price_str = ""
    if price_ctx and _is_market_open_for(symbol):
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        price_str = f"Current: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% today)"

    prompt = f"""Ticker: ${_base_symbol(symbol)}
{price_str}

Major news headline: {classification['headline']}
Category: {classification['category']}
Why it qualifies: {classification['reason']}

Write a reaction tweet. Be specific. Add a "so what" framed as possibility. End with a clear point of view. A 🟢🔴 question or CTA only if it flows naturally."""

    try:
        text = _gemini(NEWS_EVENT_SYSTEM, prompt).strip('"').strip("'")
        if _has_unknown_ticker(text, [symbol]):
            return None
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

MARKET_UPDATE_SYSTEM = """## Role
You are an expert financial X (Twitter) market commentator — sharp, credible, market-native.

## Aim
Write one tweet about the current market session using the stock data provided.
You may focus on one stock or reference multiple — choose based on what's most interesting.
The biggest movers, clearest news catalyst, or a cross-stock pattern are all valid angles.

## Rules
- NEVER invent or infer market conditions, catalysts, or facts not present in the provided data.
- ONLY give a $TICKER + specific price or % move for a stock that appears in the "Market data" block below.
  If a headline mentions some OTHER company (a peer, competitor, or supplier), you may reference what that
  headline literally says about it in prose — but NEVER invent a price, a % move, or a $TICKER for it.
  That other company has no data block here; anything you'd write about its price would be fabricated.
- Reference specific prices, % moves, and headlines from the data — specific beats vague, always.
- ALWAYS reference a stock by its bare ticker symbol with a $ prefix (e.g. $VRT, $SU). Never write out
  the company name in place of, or alongside in parentheses, the ticker — traders recognize the symbol,
  spelling out the name wastes space.
- Frame all forward-looking statements as possibilities, never certainties.
  Use: could, might, may, potentially, worth watching, raises the question.
  Never: will, confirms, proves, guarantees.
- Every tweet must have a clear point of view.
- Never reference a specific day name. Use "at the open" or "tomorrow's open" instead.
- Never use em dash. Use en dash (–) only.
- Use line breaks to create breathing room – no walls of text.
- Emoji: use sparingly. 🟢🔴 are only for a direct "green or red at the open?" question.
- No filler: "hot take", "buckle up", "thread", "this is huge".
- 1-2 hashtags max, only if they add signal. Omit if forced.
- MUST be under 280 characters.

## Tweet types
  hook          – stops the scroll. Open with a striking fact or overlooked news item.
  analytical    – specific numbers, price levels, or data points. State the implication clearly.
  question      – one sharp, genuine question rooted in real news or price action. Only if it adds value.
  reaction      – ground in actual price moves and what may be driving them.
  fomo          – short, calm, unsettling observation based on a real event being underpriced.
  wrap          – closes the session. Name specific catalysts to watch at the open. Statement, not a question.
  close_summary – concise recap of key moves and drivers today. Factual, no speculation.
  event         – urgent reaction to a price move or major news. Raw and immediate.

## Weekend rules (only apply when phase = WEEKEND)
- Markets are closed. NEVER reference today's price, daily % moves, or live market activity.
- NEVER use phrases like "today", "this session", "up X%", "down X%".
- Ground every angle in the recent news headlines provided — reference specific events from the past week.
- Frame as possibility: "X happened, which could mean Y" or "X happened – is the market pricing this in?"
- Focus on: week-in-review, structural thesis grounded in news, what to watch at the open.

## Output
One tweet only. No quotes, no commentary."""


def _base_symbol(t: str) -> str:
    return t.split(".")[0].split("-")[0]


def _has_unknown_ticker(tweet: str, allowed_symbols) -> bool:
    """True if the tweet $-mentions a ticker with no real supplied price data —
    guards against the model fabricating a price/% move for a company it only
    saw referenced inside a headline (e.g. a peer or competitor)."""
    allowed = {_base_symbol(t) for t in allowed_symbols}
    mentioned = set(re.findall(r"\$([A-Z]{1,6})\b", tweet))
    unknown = mentioned - allowed
    if unknown:
        log.warning("Tweet references untracked ticker(s) %s with no supplied data — rejecting", unknown)
    return bool(unknown)


def generate_market_update_tweet(key: str, ranked: list[str], ticker_data: dict,
                                  news_data: dict, slot: dict, phase: str) -> str | None:
    if phase == "weekend":
        phase_instruction = (
            "WEEKEND. Markets are closed. Do NOT reference today's price, daily % moves, or live "
            "activity. Ground the tweet in the news headlines below — week-in-review, structural "
            "thesis, or what to watch at the open."
        )
    elif phase == "pre_market":
        phase_instruction = (
            "Market is NOT yet open. Focus on overnight moves, catalysts, and what to watch at the open. "
            "Do NOT react to moves as if they are happening live."
        )
    elif phase == "close_summary":
        phase_instruction = "Market is CLOSED. Write a factual recap of today's key moves and drivers. One concise statement."
    elif phase == "post_market":
        phase_instruction = "Market is CLOSED. Summarise how these stocks moved today. Reference the intraday range where relevant."
    else:
        phase_instruction = "Market is open. React to live price action and news as they develop."

    lines = []
    for t in ranked:
        ctx  = ticker_data.get(t, {})
        base = _base_symbol(t)
        if phase == "weekend":
            line = f"${base}: last close ${ctx['price']}" if ctx else f"${base}:"
        else:
            sign = "+" if ctx["change_pct"] >= 0 else ""
            line = f"${base}: ${ctx['price']} ({sign}{ctx['change_pct']}%)"
            if "day_high" in ctx and "day_low" in ctx:
                line += f"  |  range ${ctx['day_low']}–${ctx['day_high']}"
        headlines = news_data.get(t, [])
        if headlines:
            line += "\n  News: " + " | ".join(headlines[:3] if phase == "weekend" else headlines[:1])
        lines.append(line)

    prompt = f"""Market data ({key.upper()} session):
{chr(10).join(lines)}

Phase: {phase_instruction}
Slot type: {slot['type']}

Write one tweet. Focus on what's most interesting — biggest mover, news catalyst, or a cross-stock pattern. \
Can reference one stock or multiple. Always use $TICKER (bare symbol, no company name). Under 280 characters."""

    try:
        text = _gemini(MARKET_UPDATE_SYSTEM, prompt).strip('"').strip("'")
        if _has_unknown_ticker(text, ranked):
            return None
        if len(text) > 280:
            trimmed = text[:280]
            for sep in (". ", ".\n", "? ", "?\n", "! ", "!\n"):
                idx = trimmed.rfind(sep)
                if idx > 100:
                    return trimmed[:idx + 1]
            return trimmed.rsplit(" ", 1)[0]
        return text
    except Exception as e:
        log.error("Market update tweet failed: %s", e)
    return None


def _extract_tickers(tweet: str, watchlist: list[str]) -> list[str]:
    """Return watchlist tickers whose base symbol appears as $TICKER in the tweet."""
    return [t for t in watchlist if f"${_base_symbol(t)}" in tweet]


# ── Weekly engagement posts ───────────────────────────────────────────────────

def check_weekly_engagement(state: dict) -> dict:
    today_str  = today()
    weekday    = datetime.date.today().weekday()
    now        = now_hhmm()
    tickers    = active_tickers_sorted()
    ticker_str = "\n".join(f"${t}" for t in tickers)
    engagement = state.setdefault("engagement", {})

    posts = []

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

# ── Event monitor ─────────────────────────────────────────────────────────[[...]

def check_price_events(state: dict, symbols: list[str]) -> dict:
    """Check for price movements and post event tweets. Only runs during market hours."""
    snapshots        = state.setdefault("price_snapshots", {})
    cooldowns        = state.setdefault("event_cooldowns", {})
    day_event_fired  = state.setdefault("day_event_fired", {})

    candidates = []

    for symbol in symbols:
        # CRITICAL: Only check price events when market is open for this ticker
        if not _is_market_open_for(symbol):
            log.debug("Price event check skipped for %s – market closed", symbol)
            continue

        price_ctx = get_price_context(symbol)
        if not price_ctx:
            continue

        current  = price_ctx["price"]
        day_pct  = abs(price_ctx["change_pct"])

        # Per-event cooldown (interval moves)
        last_event_min = cooldowns.get(symbol, 0)
        if now_minutes() - last_event_min < EVENT_COOLDOWN_MINUTES:
            snapshots[symbol] = current
            continue

        last_price = snapshots.get(symbol)

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
            candidates.append((interval_pct, symbol, price_ctx, "interval", (
                f"${_base_symbol(symbol)} just moved {direction} {interval_pct:.1f}% in the last few minutes "
                f"(now ${current}, {price_ctx['change_pct']:+.1f}% on the day).{intraday} "
                f"React immediately and specifically to what is actually happening — "
                f"a rebound is different from a continuation move."
            )))
        elif day_pct >= EVENT_DAY_THRESHOLD_PCT:
            # Day-move events fire at most once per ticker per day
            if day_event_fired.get(symbol) == today():
                snapshots[symbol] = current
                continue
            direction = "up" if price_ctx["change_pct"] > 0 else "down"
            candidates.append((day_pct, symbol, price_ctx, "day", (
                f"${_base_symbol(symbol)} is {direction} {day_pct:.1f}% today (now ${current}).{intraday} "
                f"This is a significant day move. React with conviction."
            )))

        snapshots[symbol] = current

    if candidates:
        candidates.sort(reverse=True)
        _, symbol, price_ctx, event_type, trigger = candidates[0]
        if _ticker_on_cooldown(state, symbol):
            log.info("PRICE EVENT suppressed — $%s posted within last %d min", symbol, TICKER_POST_COOLDOWN_MINUTES)
        else:
            log.info("PRICE EVENT [%s] triggered for $%s: %s", event_type, symbol, trigger)
            community = get_ticker_context(symbol)
            slot  = {"type": "event", "format": "short", "angle": trigger}
            tweet = generate_tweet(symbol, slot, price_ctx, community, event_trigger=trigger)
            if tweet:
                log.info("Price event tweet (%d chars):\n%s", len(tweet), tweet)
                if post_tweet(tweet, state):
                    cooldowns[symbol] = now_minutes()
                    _record_ticker_post(state, symbol)
                    if event_type == "day":
                        day_event_fired[symbol] = today()

    save_state(state)
    return state


def _fetch_rss_with_dates(url: str, max_messages: int) -> list[dict]:
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    resp = requests.get(url, timeout=10, verify=False, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    results = []
    for item in root.findall(".//item")[:max_messages]:
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


def get_ticker_context_with_dates(symbol: str, max_messages: int = 10) -> list[dict]:
    q = _company_name(symbol).replace(" ", "+")
    sources = [
        f"https://finance.yahoo.com/rss/headline?s={symbol}",
        f"https://news.google.com/rss/search?q={q}&hl=en&gl=US&ceid=US:en",
    ]
    seen_titles: set[str] = set()
    results: list[dict] = []
    for url in sources:
        try:
            for item in _fetch_rss_with_dates(url, max_messages):
                if item["headline"] not in seen_titles:
                    seen_titles.add(item["headline"])
                    results.append(item)
        except Exception as e:
            log.warning("News fetch failed (%s): %s", url, e)
    return results[:max_messages * 2]


def check_news_events(state: dict, symbols: list[str]) -> dict:
    """Check for major news catalysts. Runs around the clock — news can break outside
    market hours and should be covered in the next cycle, not held until the market
    reopens. Detected events are queued rather than posted immediately: at most one
    news-event tweet is released per cycle, at least NEWS_POST_MIN_GAP_MINUTES apart
    from the last one, so a burst of simultaneous headlines doesn't post back-to-back
    like a bot firing instantly — it reads as paced, human-like coverage instead."""
    cooldowns = state.setdefault("event_cooldowns", {})
    news_seen = state.setdefault("news_seen", {})
    queue     = state.setdefault("pending_news_posts", [])
    now_dt    = datetime.datetime.utcnow()
    queued_symbols = {item["symbol"] for item in queue}

    # ── Detect: classify new headlines per ticker, hold qualifying ones in the queue ──
    for symbol in symbols:
        if symbol in queued_symbols:
            continue  # already holding a news event for this ticker — don't pile on

        last_event_min = cooldowns.get(f"news_{symbol}", 0)
        if now_minutes() - last_event_min < NEWS_COOLDOWN_MINUTES:
            continue

        articles = get_ticker_context_with_dates(symbol, max_messages=10)

        recent = [
            a["headline"] for a in articles
            if a["published"] and (now_dt - a["published"]).total_seconds() < 86400
        ]
        new_headlines = [h for h in recent if h not in news_seen.get(symbol, [])]
        news_seen[symbol] = [a["headline"] for a in articles]

        if not new_headlines:
            continue

        classification = classify_news(symbol, new_headlines)
        if classification:
            log.info("NEWS EVENT queued for $%s [%s]: %s", symbol, classification["category"], classification["headline"])
            queue.append({
                "symbol": symbol,
                "classification": classification,
                "queued_min": _epoch_minutes(),
            })

    # ── Release: post at most one held event this cycle, respecting the global gap ──
    last_post_min = state.get("last_news_post_min", 0)
    required_gap  = NEWS_POST_MIN_GAP_MINUTES + random.randint(-NEWS_POST_GAP_JITTER_MINUTES, NEWS_POST_GAP_JITTER_MINUTES)
    if queue and _epoch_minutes() - last_post_min >= required_gap:
        remaining = []
        released = False
        for item in queue:
            age = _epoch_minutes() - item["queued_min"]
            symbol, classification = item["symbol"], item["classification"]

            if released or age > NEWS_QUEUE_MAX_AGE_MINUTES:
                if age > NEWS_QUEUE_MAX_AGE_MINUTES:
                    log.warning("NEWS EVENT dropped (stale, held %d min) — $%s: %s",
                                age, symbol, classification["headline"])
                else:
                    remaining.append(item)
                continue

            if _ticker_on_cooldown(state, symbol):
                remaining.append(item)  # ticker itself busy — try again next cycle
                continue

            price_ctx = get_price_context(symbol)
            tweet = generate_news_event_tweet(symbol, classification, price_ctx)
            if tweet:
                log.info("News event tweet (%d chars):\n%s", len(tweet), tweet)
                if post_tweet(tweet, state):
                    cooldowns[f"news_{symbol}"] = now_minutes()
                    _record_ticker_post(state, symbol)
                    state["last_news_post_min"] = _epoch_minutes()
                    released = True
                    continue
            remaining.append(item)

        queue[:] = remaining

    save_state(state)
    return state

# ── CONFIG ────────────────────────────────────────────────────────────

SLOT_FIRE_WINDOW_SECONDS = 1200  # 20 minutes

# ── CYCLE ────────────────────────────────────────────────────────────

def next_due_slot(plan: list[dict], posted: list[int]) -> dict | None:
    """
    Return the next slot that is:
    1. Not already posted
    2. Within SLOT_FIRE_WINDOW_SECONDS of its scheduled fire time
    
    This prevents slots from firing long after their scheduled time or multiple times.
    """
    now_dt = datetime.datetime.now()
    now    = now_hhmm()
    
    for slot in plan:
        if slot["slot"] in posted:
            continue
        
        fire_time = slot.get("fire_time") or slot.get("target_time", "00:00")
        
        # Slot hasn't reached its fire time yet
        if fire_time > now:
            continue
        
        # Parse fire time and calculate how long ago it was
        h, m = map(int, fire_time.split(":"))
        fire_dt = now_dt.replace(hour=h, minute=m, second=0, microsecond=0)
        seconds_since_fire = (now_dt - fire_dt).total_seconds()
        
        # If slot is older than the fire window, mark it as posted and skip
        if seconds_since_fire > SLOT_FIRE_WINDOW_SECONDS:
            posted.append(slot["slot"])
            log.info("Slot [%s] %s is stale (%.0f sec old, window=%d sec) – skipping",
                     fire_time, slot["type"].upper(), seconds_since_fire, SLOT_FIRE_WINDOW_SECONDS)
            continue
        
        # Slot is within the fire window – return it
        return slot
    
    return None

def _ticker_on_cooldown(state: dict, symbol: str) -> bool:
    last_posted = state.get("last_posted", {})
    last_min = last_posted.get(symbol, 0)
    return now_minutes() - last_min < TICKER_POST_COOLDOWN_MINUTES


def _record_ticker_post(state: dict, symbol: str):
    state.setdefault("last_posted", {})[symbol] = now_minutes()


def process_storyline(state: dict, key: str) -> dict:
    """
    Process one posting cycle for EU or US storyline.
    
    Workflow:
    1. Find the next due slot (not posted, within fire window)
    2. Skip if close_summary and not in overlap window
    3. Collect eligible tickers (not on 120-min cooldown)
    4. Fetch price data for all eligible tickers
    5. Rank by absolute daily move (or news on weekends)
    6. Override wrap/reaction to analytical if market is still open
    7. Generate tweet and post
    8. Record all context_tickers with cooldown (not just those in final tweet text)
    """
    slots  = state.get(f"{key}_slots", [])
    posted = state.get(f"{key}_posted", [])

    slot = next_due_slot(slots, posted)
    if not slot:
        return state

    watchlist = EU_WATCHLIST if key == "eu" else US_WATCHLIST
    # Fall back to US watchlist when EU list is empty (posts during EU hours about US stocks)
    if not watchlist:
        watchlist = US_WATCHLIST
    if not watchlist:
        return state

    phase   = market_phase(key)
    overlap = in_overlap_window()

    # EU close_summary only fires during overlap window
    if slot["type"] == "close_summary" and key == "eu" and not overlap:
        return state

    # Collect eligible tickers (not on 120-min cooldown)
    eligible = [t for t in watchlist if not _ticker_on_cooldown(state, t)]
    if not eligible:
        log.info("%s slot skipped — all tickers on cooldown", key.upper())
        return state

    # Fetch price for all eligible tickers, rank by absolute daily move
    ticker_data: dict = {}
    for t in eligible:
        ctx = get_price_context(t)
        if ctx:
            ticker_data[t] = ctx

    if not ticker_data:
        return state

    if phase == "weekend":
        # Stale Fri-close % moves are meaningless — rank by news coverage instead
        news_data = {t: get_ticker_context(t, max_messages=5) for t in ticker_data}
        ranked = sorted(ticker_data, key=lambda t: len(news_data.get(t, [])), reverse=True)
        # If no tickers have news, keep the original ranking
        ranked = [t for t in ranked if news_data.get(t)] or ranked
    else:
        ranked = sorted(ticker_data, key=lambda t: abs(ticker_data[t].get("change_pct", 0)), reverse=True)
        # Override wrap/reaction to analytical if market is still open (only for the current key)
        market_is_open = any(ticker_data[t].get("market_open") for t in ranked)
        if market_is_open and slot["type"] in ("wrap", "reaction"):
            slot = {**slot, "type": "analytical"}
            log.info("Overriding %s slot type to analytical — market still open", slot["type"])
        # Fetch news headlines for top 3 movers only
        news_data = {t: get_ticker_context(t, max_messages=3) for t in ranked[:3]}

    # Pass top 6 to Gemini (keeps prompt tight)
    context_tickers = ranked[:6]

    log.info("%s slot due: [%s] %s (phase=%s)%s — candidates: %s",
             key.upper(),
             slot.get("fire_time") or slot.get("target_time"),
             slot["type"].upper(),
             phase,
             " [OVERLAP]" if overlap else "",
             ", ".join(f"${_base_symbol(t)}" for t in context_tickers))

    tweet = generate_market_update_tweet(key, context_tickers, ticker_data, news_data, slot, phase)

    if tweet:
        log.info("%s tweet (%d chars):\n%s", key.upper(), len(tweet), tweet)
        # FIXED: Mark slot as posted BEFORE attempting to post, ensure all context_tickers get cooldown
        state[f"{key}_posted"].append(slot["slot"])
        for t in context_tickers:
            _record_ticker_post(state, t)
        save_state(state)
        # Post the tweet (will update daily_posts counter)
        if not post_tweet(tweet, state):
            log.warning("Post failed; slot already marked. Will retry on next cycle if still in window.")
        else:
            save_state(state)
    else:
        # Gemini failed to generate tweet, unmark the slot so it can retry
        state[f"{key}_posted"].remove(slot["slot"])
        save_state(state)

    return state


def run_cycle(state: dict) -> dict:
    state = ensure_daily_plans(state)
    state = check_weekly_engagement(state)
    state = process_storyline(state, "eu")
    state = process_storyline(state, "us")

    all_tickers = list(set(EU_WATCHLIST + US_WATCHLIST))
    if all_tickers:
        if not is_weekend():
            state = check_price_events(state, all_tickers)
        state = check_news_events(state, all_tickers)

    return state

# ── Main ────────────────────────────────────────────────────────────[...]

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
