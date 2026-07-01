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
EVENT_DAY_THRESHOLD_PCT      = float(os.getenv("EVENT_DAY_THRESHOLD_PCT", "5.0"))
EVENT_COOLDOWN_MINUTES       = int(os.getenv("EVENT_COOLDOWN_MINUTES", "60"))
FLEXIBLE_SLOTS_PER_STORYLINE = int(os.getenv("FLEXIBLE_SLOTS_PER_STORYLINE", "6"))  # shared by price + news events
BUFFER_SLOTS_PER_STORYLINE   = int(os.getenv("BUFFER_SLOTS_PER_STORYLINE", "2"))    # news-only, used once flexible is exhausted
NEWS_COOLDOWN_MINUTES        = 60
TICKER_POST_COOLDOWN_MINUTES = 120  # minimum gap between any two posts about the same ticker
NEWS_POST_MIN_GAP_MINUTES    = int(os.getenv("NEWS_POST_MIN_GAP_MINUTES", "10"))    # base minimum gap between any two news-event posts, across all tickers
NEWS_POST_GAP_JITTER_MINUTES = int(os.getenv("NEWS_POST_GAP_JITTER_MINUTES", "2"))  # +/- randomness applied to that gap each time (e.g. 10+/-2 -> 8-12min)
NEWS_QUEUE_MAX_AGE_MINUTES   = int(os.getenv("NEWS_QUEUE_MAX_AGE_MINUTES", "360"))  # drop a held news event if it's waited this long unreleased (6h – too stale)
NEWS_CLASSIFY_BATCH_SIZE     = int(os.getenv("NEWS_CLASSIFY_BATCH_SIZE", "5"))      # tickers per news-classification Gemini call

HEADLESS = os.getenv("CI", "false") == "true"

STATE_FILE   = os.path.join(os.path.dirname(__file__), "state.json")
SESSION_FILE = os.path.join(os.path.dirname(__file__), "twitter_session.json")

DAILY_POST_LIMIT = 20

# ── Slot definitions ────────────────────────────────────────────────────────[.[...]

# Each storyline gets exactly 2 scheduled posts/day (pre-market + close). The rest of that
# storyline's 10-post daily budget is event-driven: 6 slots shared between price moves (>=5%)
# and major news, plus 2 buffer slots reserved for news once the shared 6 are used up.
EU_SLOTS = [
    ("08:45", "hook"),           # pre-market: expectations for the day
    ("17:00", "close_summary"),  # close: top 5 movers + major announcements
]

US_SLOTS = [
    ("15:10", "hook"),   # pre-market: expectations for the day
    ("22:00", "wrap"),   # close: top 5 movers + major announcements
]

WEEKEND_SLOTS = [
    ("10:00", "hook"),   # morning: week-in-review framing, news-grounded
    ("18:00", "wrap"),   # evening: recap + what to watch at Monday's open
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


def get_recent_volatility(symbols: list[str], sessions: int = 3) -> dict[str, float]:
    """Rough volatility score per ticker: largest single-day |% move| over the
    last `sessions` trading sessions (including today, if the market's open)."""
    result = {}
    for symbol in symbols:
        try:
            hist = yf.Ticker(symbol).history(period=f"{sessions + 2}d")
            closes = hist["Close"].tail(sessions + 1)
            moves = closes.pct_change().dropna().abs() * 100
            if len(moves) > 0:
                result[symbol] = round(float(moves.max()), 2)
        except Exception as e:
            log.warning("Volatility fetch failed for %s: %s", symbol, e)
    return result

# ── Helpers ───────────────────────────────────────────────────────────[[...]

def _strip_json_fences(text: str) -> str:
    text = text.strip()
    if "```" in text:
        lines = [l for l in text.splitlines() if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()
    return text

# ── Gemini ───────────────────────────────────────────────────────────[.[...]

# Circuit breaker: once any Gemini call fails in this process (= this cron run),
# stop attempting further Gemini calls for the rest of the cycle. Each run is a
# fresh `python bot.py` invocation, so this always starts False on every cron trigger —
# no manual reset needed. Prevents one outage from cascading into dozens of doomed
# retry-and-fail attempts (classify batches, queued news releases, etc.) that just
# burn more of an already-exhausted quota without any chance of succeeding.
_gemini_unavailable = False


def _gemini(system: str, prompt: str) -> str:
    global _gemini_unavailable
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
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data["candidates"][0]["content"]["parts"][-1]["text"].strip()
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt == 0:
                log.warning("Gemini %d – waiting 30s before one retry", e.code)
                time.sleep(30)
            else:
                _gemini_unavailable = True
                raise
        except Exception:
            _gemini_unavailable = True
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

NEWS_CLASSIFIER_SYSTEM = """You are a financial news classifier. You'll be given several tickers, each with its own
set of recent headlines. For EACH ticker independently, determine if any of its headlines represents a major
catalyst that could significantly move that stock. Treat every ticker as a separate, fully independent judgment —
do not let one ticker's news influence another's classification.

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

Respond with a JSON array, exactly one object per ticker given, in the same order:
[
  {
    "symbol": "the ticker this judgment is about",
    "is_major": true or false,
    "category": "earnings|contract|ma|analyst|regulatory|macro|company|none",
    "headline": "the exact headline that triggered this (omit or empty if is_major is false)",
    "reason": "one sentence explaining why it qualifies or not"
  }
]

Return ONLY the JSON array, no other text."""

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

def classify_news_batch(ticker_headlines: dict[str, list[str]]) -> dict[str, dict]:
    """Classify multiple tickers' fresh headlines in a single Gemini call.
    Returns {symbol: classification} only for tickers judged major."""
    if not ticker_headlines:
        return {}

    blocks = []
    for symbol, headlines in ticker_headlines.items():
        headlines_block = "\n".join(f"- {h}" for h in headlines)
        blocks.append(f"Ticker: ${symbol}\nHeadlines:\n{headlines_block}")
    prompt = "\n\n".join(blocks) + "\n\nClassify each ticker independently per the rules above."

    try:
        text = _strip_json_fences(_gemini(NEWS_CLASSIFIER_SYSTEM, prompt))
        results = json.loads(text)
        major = {}
        for r in results:
            symbol = r.get("symbol", "")
            if symbol in ticker_headlines and r.get("is_major"):
                major[symbol] = r
        return major
    except Exception as e:
        log.warning("Batch news classification failed for %s: %s", list(ticker_headlines), e)
    return {}


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


def generate_news_event_tweet(symbol: str, classification: dict, price_ctx: dict,
                               link: str = "", source: str = "") -> str | None:
    price_str = ""
    if price_ctx and _is_market_open_for(symbol):
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        price_str = f"Current: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% today)"

    source_line = f"\nReported by: {source}" if source else ""
    link_instruction = (
        "Do NOT include a URL yourself — the article link is appended separately after your text. "
        "You may naturally name the reporting outlet in your commentary if it adds credibility."
        if link else ""
    )

    prompt = f"""Ticker: ${_base_symbol(symbol)}
{price_str}

Major news headline: {classification['headline']}
Category: {classification['category']}
Why it qualifies: {classification['reason']}{source_line}

Write a reaction tweet. Be specific. Add a "so what" framed as possibility. End with a clear point of view. A 🟢🔴 question or CTA only if it flows naturally. {link_instruction}"""

    # X counts any URL as a fixed ~23 characters (t.co shortening) regardless of its real length.
    suffix = f"\n\n{link}" if link else ""
    budget = 280 - 25 if link else 280  # 23 for the shortened link + 2 for the newlines

    try:
        text = _gemini(NEWS_EVENT_SYSTEM, prompt).strip('"').strip("'")
        if _has_unknown_ticker(text, [symbol]):
            return None
        if len(text) > budget:
            trimmed = text[:budget]
            for sep in (". ", ".\n", "? ", "?\n", "! ", "!\n"):
                idx = trimmed.rfind(sep)
                if idx > 100:
                    text = trimmed[:idx + 1]
                    break
            else:
                text = trimmed.rsplit(" ", 1)[0]
        return text + suffix
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
  hook          – the day's pre-market post. Set expectations for the session ahead: what to watch,
                  overnight moves, upcoming catalysts. Do not react to live price action — market isn't open yet.
  analytical    – specific numbers, price levels, or data points. State the implication clearly.
  question      – one sharp, genuine question rooted in real news or price action. Only if it adds value.
  reaction      – ground in actual price moves and what may be driving them.
  fomo          – short, calm, unsettling observation based on a real event being underpriced.
  wrap          – the day's close post. Recap the top movers from the data provided and any major news
                  from today. Factual, no speculation. This is the day's one closing summary.
  close_summary – same role as wrap (EU's version): recap the top movers and today's major news at the close.
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
        vol = get_recent_volatility(tickers, sessions=3)
        top5 = sorted(sorted(vol, key=vol.get, reverse=True)[:5], key=_base_symbol)
        if top5:
            vol_lines = "\n".join(f"${_base_symbol(t)}" for t in top5)
            posts.append(("wednesday", f"""Write a midweek engagement post for a financial Twitter account.

The 5 most volatile tickers this week so far (biggest single-day moves over the last 3 sessions):
{vol_lines}

Format:
- One brief line noting these specific names have seen real volatility across the first few sessions this week
- List the 5 tickers on separate lines, nothing else attached to them
- End with a simple, clear hypothetical: if you had $1000 to invest right now, how would you split it across
  these 5? Phrase this fresh each time in your own words – don't reuse the same wording as a template.

Keep it simple, clear, casual, engaging. Under 280 characters."""))

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
        if _gemini_unavailable:
            log.warning("Engagement post [%s] skipped — Gemini unavailable this cycle", key)
            continue
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
    """Check for significant daily price moves (>= EVENT_DAY_THRESHOLD_PCT, up or down) and
    post event tweets. Only runs during market hours, and draws from each storyline's shared
    flexible budget (FLEXIBLE_SLOTS_PER_STORYLINE/day) — never the news-only buffer."""
    snapshots        = state.setdefault("price_snapshots", {})
    cooldowns        = state.setdefault("event_cooldowns", {})
    day_event_fired  = state.setdefault("day_event_fired", {})

    candidates = []

    for symbol in symbols:
        if not _is_market_open_for(symbol):
            continue

        price_ctx = get_price_context(symbol)
        if not price_ctx:
            continue

        current  = price_ctx["price"]
        day_pct  = abs(price_ctx["change_pct"])
        snapshots[symbol] = current

        last_event_min = cooldowns.get(symbol, 0)
        if now_minutes() - last_event_min < EVENT_COOLDOWN_MINUTES:
            continue

        if day_pct < EVENT_DAY_THRESHOLD_PCT:
            continue

        # Day-move events fire at most once per ticker per day
        if day_event_fired.get(symbol) == today():
            continue

        intraday = ""
        if "day_high" in price_ctx and "day_low" in price_ctx:
            intraday = (
                f" Intraday range: low ${price_ctx['day_low']} / high ${price_ctx['day_high']}."
                f" Use this context — if the stock dropped sharply and is now rebounding, say so."
            )
        direction = "up" if price_ctx["change_pct"] > 0 else "down"
        trigger = (
            f"${_base_symbol(symbol)} is {direction} {day_pct:.1f}% today (now ${current}).{intraday} "
            f"This is a significant day move. React with conviction."
        )
        candidates.append((day_pct, symbol, price_ctx, trigger))

    candidates.sort(reverse=True)
    for day_pct, symbol, price_ctx, trigger in candidates:
        if _gemini_unavailable:
            log.warning("PRICE EVENT skipped for $%s — Gemini unavailable this cycle", symbol)
            break
        if _ticker_on_cooldown(state, symbol):
            log.info("PRICE EVENT suppressed — $%s posted within last %d min", symbol, TICKER_POST_COOLDOWN_MINUTES)
            continue
        pool = _consume_price_slot(state, symbol)
        if not pool:
            log.info("PRICE EVENT suppressed — $%s's storyline flexible budget exhausted today", symbol)
            continue

        log.info("PRICE EVENT triggered for $%s: %s", symbol, trigger)
        community = get_ticker_context(symbol)
        slot  = {"type": "event", "format": "short", "angle": trigger}
        tweet = generate_tweet(symbol, slot, price_ctx, community, event_trigger=trigger)
        if tweet:
            log.info("Price event tweet (%d chars):\n%s", len(tweet), tweet)
        if tweet and post_tweet(tweet, state):
            cooldowns[symbol] = now_minutes()
            _record_ticker_post(state, symbol)
            day_event_fired[symbol] = today()
        else:
            _refund_slot(state, symbol, pool)
        break  # at most one price-event attempt per cycle, success or not

    save_state(state)
    return state


_DC_CREATOR = "{http://purl.org/dc/elements/1.1/}creator"


def _fetch_rss_with_dates(url: str, max_messages: int) -> list[dict]:
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    from urllib.parse import urlparse
    resp = requests.get(url, timeout=10, verify=False, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    results = []
    for item in root.findall(".//item")[:max_messages]:
        title = item.findtext("title")
        link = item.findtext("link")
        pub_date = item.findtext("pubDate")
        if not title:
            continue
        try:
            pub_dt = parsedate_to_datetime(pub_date).replace(tzinfo=None) if pub_date else None
        except Exception:
            pub_dt = None

        # Attribution: Google News <source>, Nasdaq <dc:creator>, else the link's own domain
        # (e.g. a Yahoo item that links straight to fool.com) — never "Yahoo"/"Google" themselves.
        source_el = item.find("source")
        creator = item.findtext(_DC_CREATOR)
        if source_el is not None and source_el.text:
            source = source_el.text.strip()
        elif creator:
            source = creator.strip()
        elif link:
            domain = urlparse(link).netloc.replace("www.", "")
            source = None if domain in ("finance.yahoo.com", "news.google.com") else domain
        else:
            source = None

        results.append({"headline": title, "link": link, "source": source, "published": pub_dt})
    return results


def get_ticker_context_with_dates(symbol: str, max_messages: int = 10) -> list[dict]:
    q = _company_name(symbol).replace(" ", "+")
    sources = [
        f"https://finance.yahoo.com/rss/headline?s={symbol}",
        f"https://news.google.com/rss/search?q={q}&hl=en&gl=US&ceid=US:en",
    ]
    if symbol in US_WATCHLIST:
        # Nasdaq's per-symbol feed only understands US-listed tickers — querying it with an
        # EU-suffixed symbol (e.g. SU.PA) silently returns an unrelated generic news feed.
        sources.append(f"https://www.nasdaq.com/feed/rssoutbound?symbol={_base_symbol(symbol)}")
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

    # ── Gather: fetch fresh headlines per ticker first, no Gemini calls yet ──
    candidates: dict[str, list[str]] = {}
    article_meta: dict[str, dict[str, dict]] = {}  # symbol -> {headline: {"link":, "source":}}
    for symbol in symbols:
        if symbol in queued_symbols:
            continue  # already holding a news event for this ticker — don't pile on

        if not _has_news_budget(state, symbol):
            continue  # storyline's flexible+buffer budget exhausted — not worth spending a
                       # Gemini call classifying news we couldn't post about anyway today

        last_event_min = cooldowns.get(f"news_{symbol}", 0)
        if now_minutes() - last_event_min < NEWS_COOLDOWN_MINUTES:
            continue

        articles = get_ticker_context_with_dates(symbol, max_messages=10)

        recent = [
            a for a in articles
            if a["published"] and (now_dt - a["published"]).total_seconds() < 86400
        ]
        new_headlines = [a["headline"] for a in recent if a["headline"] not in news_seen.get(symbol, [])]
        news_seen[symbol] = [a["headline"] for a in articles]

        if new_headlines:
            candidates[symbol] = new_headlines
            article_meta[symbol] = {a["headline"]: {"link": a["link"], "source": a["source"]} for a in recent}

    # ── Classify: batch tickers with fresh headlines into groups of NEWS_CLASSIFY_BATCH_SIZE,
    # so a busy day costs a handful of Gemini calls instead of one call per ticker ──
    candidate_symbols = list(candidates)
    for i in range(0, len(candidate_symbols), NEWS_CLASSIFY_BATCH_SIZE):
        if _gemini_unavailable:
            log.warning("Gemini unavailable this cycle — skipping remaining news classification batches (%d tickers left)",
                        len(candidate_symbols) - i)
            break
        batch = candidate_symbols[i:i + NEWS_CLASSIFY_BATCH_SIZE]
        major = classify_news_batch({s: candidates[s] for s in batch})
        for symbol, classification in major.items():
            meta = article_meta.get(symbol, {}).get(classification["headline"], {})
            # Only attach a link if we could identify a real, named, non-aggregator source —
            # never cite/link Yahoo's or Google's own domain as if it were "the source".
            source = meta.get("source")
            link = meta.get("link") if source else None
            log.info("NEWS EVENT queued for $%s [%s]: %s (source: %s)",
                     symbol, classification["category"], classification["headline"], source or "unknown")
            queue.append({
                "symbol": symbol,
                "classification": classification,
                "link": link,
                "source": source,
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

            if age > NEWS_QUEUE_MAX_AGE_MINUTES:
                log.warning("NEWS EVENT dropped (stale, held %d min) — $%s: %s",
                            age, symbol, classification["headline"])
                continue

            if released or _gemini_unavailable:
                remaining.append(item)  # already posted one this cycle, or Gemini's down — retry next cycle
                continue

            if _ticker_on_cooldown(state, symbol):
                remaining.append(item)  # ticker itself busy — try again next cycle
                continue

            pool = _consume_news_slot(state, symbol)
            if not pool:
                remaining.append(item)  # storyline's flexible+buffer budget exhausted for today
                continue

            price_ctx = get_price_context(symbol)
            tweet = generate_news_event_tweet(symbol, classification, price_ctx,
                                               link=item.get("link"), source=item.get("source"))
            if tweet:
                log.info("News event tweet (%d chars):\n%s", len(tweet), tweet)
                if post_tweet(tweet, state):
                    cooldowns[f"news_{symbol}"] = now_minutes()
                    _record_ticker_post(state, symbol)
                    state["last_news_post_min"] = _epoch_minutes()
                    released = True
                    continue
            _refund_slot(state, symbol, pool)
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


def _storyline_key_for(symbol: str) -> str:
    return "eu" if symbol in EU_WATCHLIST else "us"


def _flexible_remaining(state: dict, key: str) -> int:
    return max(0, FLEXIBLE_SLOTS_PER_STORYLINE - state.get(f"{key}_flexible_used", 0))


def _buffer_remaining(state: dict, key: str) -> int:
    return max(0, BUFFER_SLOTS_PER_STORYLINE - state.get(f"{key}_buffer_used", 0))


def _has_news_budget(state: dict, symbol: str) -> bool:
    key = _storyline_key_for(symbol)
    return _flexible_remaining(state, key) > 0 or _buffer_remaining(state, key) > 0


def _consume_price_slot(state: dict, symbol: str) -> str | None:
    """Price events only ever draw from the shared flexible pool, never the buffer.
    Returns the pool name consumed ('flexible'), or None if no budget remains."""
    key = _storyline_key_for(symbol)
    if _flexible_remaining(state, key) <= 0:
        return None
    state[f"{key}_flexible_used"] = state.get(f"{key}_flexible_used", 0) + 1
    return "flexible"


def _consume_news_slot(state: dict, symbol: str) -> str | None:
    """News events draw from the shared flexible pool first, then the reserved buffer.
    Returns the pool name actually consumed, or None if no budget remains."""
    key = _storyline_key_for(symbol)
    if _flexible_remaining(state, key) > 0:
        state[f"{key}_flexible_used"] = state.get(f"{key}_flexible_used", 0) + 1
        return "flexible"
    if _buffer_remaining(state, key) > 0:
        state[f"{key}_buffer_used"] = state.get(f"{key}_buffer_used", 0) + 1
        return "buffer"
    return None


def _refund_slot(state: dict, symbol: str, pool: str):
    """Give back a tentatively-consumed slot when generation or posting ends up failing."""
    key = _storyline_key_for(symbol)
    field = f"{key}_{pool}_used"
    state[field] = max(0, state.get(field, 0) - 1)


def process_storyline(state: dict, key: str) -> dict:
    """
    Process one posting cycle for EU or US storyline.
    
    Workflow:
    1. Find the next due slot (not posted, within fire window) — pre-market or close, 2/day
    2. Collect eligible tickers (not on 120-min cooldown)
    3. Fetch price data for all eligible tickers
    4. Rank by absolute daily move (or news on weekends)
    5. Override wrap/reaction to analytical if market is still open
    6. Generate tweet and post
    7. Record all context_tickers with cooldown (not just those in final tweet text)
    """
    slots  = state.get(f"{key}_slots", [])
    posted = state.get(f"{key}_posted", [])

    slot = next_due_slot(slots, posted)
    if not slot:
        return state

    if _gemini_unavailable:
        log.warning("%s slot skipped — Gemini unavailable this cycle, will retry next cycle", key.upper())
        return state

    watchlist = EU_WATCHLIST if key == "eu" else US_WATCHLIST
    # Fall back to US watchlist when EU list is empty (posts during EU hours about US stocks)
    if not watchlist:
        watchlist = US_WATCHLIST
    if not watchlist:
        return state

    phase   = market_phase(key)
    overlap = in_overlap_window()

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

    # Pass top 5 to Gemini (keeps prompt tight, matches the "top 5 movers" framing for the close post)
    context_tickers = ranked[:5]

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
        # Generation failed — slot was never marked posted, so it naturally retries
        # next cycle (as long as it's still within the slot's fire window).
        log.warning("%s tweet generation failed for slot [%s] — will retry next cycle if still in window.",
                    key.upper(), slot["type"])

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
