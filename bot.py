"""
Ticker Twitter Bot — dual storyline + event-driven edition
EU story:  08:50 pre-market hook, 18:00 close_summary   [2 scheduled slots/day]
US story:  15:10 pre-market hook, 22:30 wrap            [2 scheduled slots/day]
Weekend:   10:00-19:00, 7 slots (hook/analytical/question mix, wrap to close),
           PER storyline (EU + US) — see WEEKEND_SLOTS  [7 scheduled slots/day each]
Each storyline's remaining daily budget is event-driven (price moves + news) — see
FLEXIBLE_SLOTS_PER_STORYLINE / BUFFER_SLOTS_PER_STORYLINE. Separately, a zero-LLM
"pulse" heartbeat (see check_zero_llm_pulse) posts every 30-45min on weekdays,
independent of both the Gemini budget and real news/price activity.

Real market hours: EU 09:00-17:30 CET, US 15:30-22:00 CET (see _is_market_open_for).
Scheduled slots deliberately sit just before the open and just after the close, so
pre-market posts have real pre-open data to reference and close-out posts have real
settled closing data, rather than firing at the exact open/close boundary.

Local run:  python bot.py          (browser window visible)
GitHub:     runs on workflow_dispatch (headless, TZ=Europe/Amsterdam), dispatched
            externally on a confirmed ~15min cadence — this repo's own bot.yml has
            no schedule: trigger itself, so that external caller is load-bearing for
            every cadence assumption in this file (SLOT_FIRE_WINDOW_SECONDS, pulse
            interval, news queue pacing).
"""

import os
import re
import tempfile
import csv
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
import pandas_market_calendars as mcal
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
NEWS_CATEGORY_DEDUP_MINUTES  = int(os.getenv("NEWS_CATEGORY_DEDUP_MINUTES", "1440"))  # rolling window (24h), not calendar-day
NEWS_FRESHNESS_HOURS         = int(os.getenv("NEWS_FRESHNESS_HOURS", "24"))  # shared by the RSS-pubDate filter and the
                                                                              # destination-page cross-check — must match,
                                                                              # or the cross-check becomes a weaker backstop
                                                                              # than the filter it's supposed to be backing up
NEWS_POST_MIN_GAP_MINUTES    = int(os.getenv("NEWS_POST_MIN_GAP_MINUTES", "10"))    # base minimum gap between any two news-event posts, across all tickers
NEWS_POST_GAP_JITTER_MINUTES = int(os.getenv("NEWS_POST_GAP_JITTER_MINUTES", "2"))  # +/- randomness applied to that gap each time (e.g. 10+/-2 -> 8-12min)
NEWS_QUEUE_MAX_AGE_MINUTES   = int(os.getenv("NEWS_QUEUE_MAX_AGE_MINUTES", "360"))  # drop a held news event if it's waited this long unreleased (6h – too stale)
NEWS_CLASSIFY_BATCH_SIZE     = int(os.getenv("NEWS_CLASSIFY_BATCH_SIZE", "5"))      # tickers per news-classification Gemini call
# How many recent posts' sources to remember, to keep the feed from looking like a single-source
# reposter. When a story's source was used within this window, the bot tries to attach a different
# outlet's version of the same story instead (see _diversify_source).
RECENT_SOURCE_MEMORY         = int(os.getenv("RECENT_SOURCE_MEMORY", "6"))

# Feature toggle: post "large_share_purchases" — a named institution buying a sizeable number of
# SHARES of a tracked company (distinct from the company itself being acquired, which is `ma`).
# Its own category AND its own on/off switch, so it can be paused independently without touching
# any other news handling. Set the env var to "false" to mute this whole class of post.
ENABLE_LARGE_SHARE_PURCHASES = os.getenv("ENABLE_LARGE_SHARE_PURCHASES", "true").lower() == "true"

# Feature toggle: "evergreen opinion" — thematic think-pieces on AI infrastructure (the sector,
# not a single stock: policy essays, consultancy/bank sector notes, "what it means" analysis).
# These don't lose value in a day, so they're used as FILLER on weekends and slow weekdays to keep
# the feed alive when there's little live news. Sourced from topic (not ticker) RSS searches.
ENABLE_EVERGREEN_OPINION            = os.getenv("ENABLE_EVERGREEN_OPINION", "true").lower() == "true"
EVERGREEN_OPINION_LOOKBACK_DAYS     = int(os.getenv("EVERGREEN_OPINION_LOOKBACK_DAYS", "30"))   # evergreen: older is fine
EVERGREEN_OPINION_MEMORY_DAYS       = 60   # don't re-post the same piece within this window
EVERGREEN_OPINION_DAILY_LIMIT       = int(os.getenv("EVERGREEN_OPINION_DAILY_LIMIT", "2"))
EVERGREEN_OPINION_MIN_GAP_MINUTES   = int(os.getenv("EVERGREEN_OPINION_MIN_GAP_MINUTES", "180"))
# "Slow day" = past the midpoint of the combined trading day AND fewer than this many substantive
# (non-pulse) posts have gone out — i.e. a genuinely quiet news day worth filling.
SLOW_DAY_SUBSTANTIVE_POST_THRESHOLD = int(os.getenv("SLOW_DAY_SUBSTANTIVE_POST_THRESHOLD", "8"))

HEADLESS = os.getenv("CI", "false") == "true"

STATE_FILE   = os.path.join(os.path.dirname(__file__), "state.json")
SESSION_FILE = os.path.join(os.path.dirname(__file__), "twitter_session.json")

# ── Events log (CSV) ─────────────────────────────────────────────────────────
# One row per candidate decision the bot makes — posted AND not-posted alike — so this is
# usable for real data analysis later (hit rate by mechanism, Gemini vs keyword split, time-of-
# day patterns, why things get dropped), not just an audit trail of what went out.
EVENTS_LOG_FILE = os.path.join(os.path.dirname(__file__), "events_log.csv")

EVENTS_LOG_COLUMNS = [
    "event_id", "date", "time", "weekday",
    "mechanism", "storyline", "slot_type", "generation_method",
    "symbol", "base_symbol", "related_tickers",
    "exchange", "exchange_open_today", "market_phase",
    "price", "change_pct", "day_high", "day_low",
    "headline", "headline_source", "headline_link", "headline_published_utc",
    "news_category", "holding_disclosure_fingerprint",
    "day_move_pct", "catalyst_found", "catalyst_headline",
    "gemini_call_used", "gemini_calls_today_after", "pool_used",
    "posted", "skip_reason",
    "tweet_text", "tweet_char_count",
    # Reserved for a future engagement-backfill step — always blank for now.
    "tweet_url", "likes", "retweets", "replies", "engagement_checked_at",
]


def _log_event(state: dict, **fields) -> None:
    unknown = set(fields) - set(EVENTS_LOG_COLUMNS)
    if unknown:
        raise ValueError(f"_log_event got unknown column(s): {unknown}")

    now = datetime.datetime.now()
    event_id = state.get("event_log_next_id", 1)
    state["event_log_next_id"] = event_id + 1

    row = {col: "" for col in EVENTS_LOG_COLUMNS}
    row.update({
        "event_id": event_id,
        # ISO 8601 (yyyy-mm-dd) is the one text date format Excel/Sheets auto-recognizes as a
        # real, sortable date in EVERY locale. A literal dd/mm/yyyy string only parses correctly
        # on a day-first system — on a US-locale machine it's silently misread as mm/dd for any
        # day <=12 (e.g. 03/07/2026 becomes March 7th instead of July 3rd) while days >12 fall
        # back to plain text, so the same column ends up part-genuine-date, part-text. Once this
        # is recognized as a real date, reformatting the column's display to dd/mm/yyyy is a
        # one-time Format Cells step and stays correct.
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "weekday": now.strftime("%A"),
    })
    row.update(fields)

    file_exists = os.path.exists(EVENTS_LOG_FILE)
    with open(EVENTS_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=EVENTS_LOG_COLUMNS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

DAILY_POST_LIMIT = 20  # LLM-driven posts only: scheduled updates, price events, Gemini-classified news, engagement

# The real ceiling Gemini enforces (20 RPD) counts every actual HTTP request — including the
# classify+generate split for news events (2 calls per post, not 1) and every 429-triggered
# retry — none of which the post-count budgets above track. This is the one limit that maps
# directly onto what Google is actually counting. Set below 20 for a safety margin.
GEMINI_DAILY_CALL_LIMIT = int(os.getenv("GEMINI_DAILY_CALL_LIMIT", "18"))

# Zero-LLM content (keyword-classified news, weekend caption/poll) doesn't touch the Gemini quota,
# so it gets its own separate, more generous ceiling — a backstop against runaway volume on a heavy
# news day, not a budget tied to any API limit. Worst case combined with DAILY_POST_LIMIT: ~35/day.
DAILY_KEYWORD_POST_LIMIT = int(os.getenv("DAILY_KEYWORD_POST_LIMIT", "15"))

# Zero-LLM cadence heartbeat, independent of both Gemini's budget and real news volume — a
# templated "biggest mover right now" snapshot on its own clock, so posting frequency doesn't
# depend on whether anything "major" happened. Kept in a separate pool from DAILY_KEYWORD_POST_LIMIT
# on purpose: sharing that pool would mean either a busy news day starves the heartbeat, or the
# heartbeat crowds out real news — both defeat the point of one or the other.
# The gap itself is randomized fresh within this range each time (not a fixed base +/- jitter) —
# a hard fixed interval would make the account's automation trivially obvious from the outside.
PULSE_INTERVAL_MIN_MINUTES = int(os.getenv("PULSE_INTERVAL_MIN_MINUTES", "30"))
PULSE_INTERVAL_MAX_MINUTES = int(os.getenv("PULSE_INTERVAL_MAX_MINUTES", "45"))
# Tightened from 45-60min: over the 09:00-22:00 window this averages ~21 posts/day instead of
# ~15, pushing weekday volume closer to the 30/day objective. The limit below is raised to match
# — it used to sit right at the old interval's natural output (~18), which would now silently cap
# the tighter interval below what it can actually produce.
DAILY_PULSE_POST_LIMIT = int(os.getenv("DAILY_PULSE_POST_LIMIT", "24"))

# Maps a post's pool to its (state counter key, daily limit) — one shared lookup so post_tweet/
# post_poll don't duplicate this logic, and adding a new pool later is a one-line change here.
_POST_POOLS = {
    "llm":     ("daily_posts", DAILY_POST_LIMIT),
    "keyword": ("daily_keyword_posts", DAILY_KEYWORD_POST_LIMIT),
    "pulse":   ("daily_pulse_posts", DAILY_PULSE_POST_LIMIT),
}

# ── Slot definitions ────────────────────────────────────────────────────────[.[...]

# Each storyline gets exactly 2 scheduled posts/day (pre-market + close). The rest of that
# storyline's 10-post daily budget is event-driven: 6 slots shared between price moves (>=5%)
# and major news, plus 2 buffer slots reserved for news once the shared 6 are used up.
EU_SLOTS = [
    ("08:50", "hook"),           # pre-market: 10min before the real 09:00 open — pre-open
                                 # price discovery has mostly settled by now
    ("18:00", "close_summary"),  # close: 30min after the real 17:30 close — a 10min buffer
                                 # wasn't enough; yfinance's official closing print (especially
                                 # for anything settled via closing auction) can lag by more than
                                 # that, so the reported number was consistently a bit off from
                                 # the true final close. Doesn't need to be immediate — the first
                                 # post-close cycle catching this slot is fine.
]

US_SLOTS = [
    ("15:10", "hook"),   # pre-market: 20min before the real 15:30 open (9:10am ET)
    ("22:30", "wrap"),   # close: 30min after the real 22:00 close (4:00pm ET) — same settlement-
                         # lag reasoning as EU's close_summary above.
]

WEEKEND_SLOTS = [
    # Weekends have neither price events nor much real news (confirmed empirically — far less
    # than a weekday), so the Gemini budget goes mostly unused unless the SCHEDULE itself fills
    # more of the day. hook/wrap still cover the full watchlist; the analytical/question slots in
    # between use single-ticker focus (see MARKET_UPDATE_SYSTEM) rather than forcing a repetitive
    # full-watchlist recap seven times a day. 7 slots x 2 storylines = 14 Gemini calls, + up to 1
    # weekly-engagement call = 15, leaving a few calls in reserve out of the 18/day ceiling for any
    # genuine weekend news rather than committing the entire budget to the fixed schedule.
    ("10:00", "hook"),        # morning: week-in-review framing, news-grounded
    ("11:30", "analytical"),  # single sharpest angle (a research spotlight, if one exists)
    ("13:00", "analytical"),
    ("14:30", "question"),    # one genuine question to keep engagement going
    ("16:00", "analytical"),
    ("17:30", "question"),
    ("19:00", "wrap"),        # evening: recap + what to watch at Monday's open
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
            with open(STATE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.warning("State load failed, starting from empty state: %s", e)
    return {}


def _parse_datetime_value(value):
    """Best-effort parser for datetimes from RSS feeds, page metadata, and saved state.

    The bot stores state as JSON, so datetimes may come back as strings on the next
    run. This helper accepts both live datetime objects and persisted strings.
    """
    if value is None or value == "":
        return None

    if isinstance(value, datetime.datetime):
        return value

    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min)

    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    # ISO-8601 / JSON-LD / state.json strings.
    try:
        return datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        pass

    # RFC-2822 style feed dates, where available.
    try:
        from email.utils import parsedate_to_datetime
        parsed = parsedate_to_datetime(raw)
        if parsed is not None:
            return parsed
    except Exception:
        pass

    # Some destination pages use strings such as: "Sat Jul 4, 5:42AM CDT".
    # They are not ISO strings and often omit the year, so infer the current year
    # and normalize common US timezone abbreviations.
    m = re.match(
        r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+"
        r"(?P<mon>Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
        r"(?P<day>\d{1,2}),\s*"
        r"(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*"
        r"(?P<ampm>AM|PM)\s*(?P<tz>[A-Z]{2,4})?$",
        raw,
        re.I,
    )
    if m:
        months = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        hour = int(m.group("hour"))
        if m.group("ampm").upper() == "PM" and hour != 12:
            hour += 12
        if m.group("ampm").upper() == "AM" and hour == 12:
            hour = 0

        tz_offsets = {
            "UTC": 0, "GMT": 0,
            "EST": -5, "EDT": -4,
            "CST": -6, "CDT": -5,
            "MST": -7, "MDT": -6,
            "PST": -8, "PDT": -7,
        }
        tz_name = (m.group("tz") or "UTC").upper()
        tzinfo = datetime.timezone(datetime.timedelta(hours=tz_offsets.get(tz_name, 0)))
        candidate = datetime.datetime(
            datetime.datetime.utcnow().year,
            months[m.group("mon").lower()],
            int(m.group("day")),
            hour,
            int(m.group("minute")),
            tzinfo=tzinfo,
        )
        # Around New Year, a no-year date from late December can otherwise look
        # almost a full year in the future. Pull implausible future dates back.
        if candidate.astimezone(datetime.timezone.utc) > datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1):
            candidate = candidate.replace(year=candidate.year - 1)
        return candidate

    return None


def _isoformat_or_empty(value) -> str:
    parsed = _parse_datetime_value(value)
    if parsed is None:
        return value if isinstance(value, str) else ""
    return parsed.isoformat()


def _make_json_safe(obj):
    """Recursively convert state values into JSON-serializable primitives."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()

    if isinstance(obj, datetime.date):
        return obj.isoformat()

    if isinstance(obj, dict):
        return {str(k): _make_json_safe(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_make_json_safe(v) for v in obj]

    if isinstance(obj, tuple):
        return [_make_json_safe(v) for v in obj]

    if isinstance(obj, set):
        return [_make_json_safe(v) for v in obj]

    # Handles numpy/pandas scalar values without adding a hard dependency here.
    item = getattr(obj, "item", None)
    if callable(item):
        try:
            return _make_json_safe(item())
        except Exception:
            pass

    return obj


def save_state(state: dict):
    """Persist state atomically and tolerate datetime/date values in nested structures."""
    safe_state = _make_json_safe(state)
    state_dir = os.path.dirname(STATE_FILE) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".state.", suffix=".json.tmp", dir=state_dir)

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(safe_state, f, indent=2, ensure_ascii=False)
            f.write("\n")
        os.replace(tmp_path, STATE_FILE)
    except Exception:
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        raise


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


# Derived from the Yahoo Finance ticker suffix, not a per-ticker table — works automatically
# for any future watchlist addition using the same suffix convention. The EU watchlist spans
# four distinct national exchanges with four distinct holiday calendars (a French-only holiday
# doesn't close Xetra, and vice versa), so "EU" can't be treated as a single calendar the way the
# rest of this file treats it as a single storyline for scheduling/budget purposes.
_SUFFIX_TO_EXCHANGE = {
    ".PA": "XPAR",  # Euronext Paris
    ".DE": "XETR",  # Deutsche Börse / Xetra
    ".SW": "XSWX",  # SIX Swiss Exchange
    ".MC": "XMAD",  # Bolsa de Madrid
}


def _exchange_for(symbol: str) -> str:
    for suffix, code in _SUFFIX_TO_EXCHANGE.items():
        if symbol.endswith(suffix):
            return code
    return "NYSE"  # no-suffix tickers are US-listed; NASDAQ observes the same holiday schedule


_exchange_open_cache: dict[str, bool] = {}


def _exchange_open_today(exchange_code: str) -> bool:
    """Forward-looking holiday check via pandas_market_calendars, not a hand-maintained date
    list that silently goes stale every year, and not a data-absence check either — this works
    just as well before the open as after, so it also catches a holiday morning's pre-market
    slot (a data-absence check can't, since there's no session yet to be missing from either way).
    Cached per-process since it's the same answer all day for a given exchange."""
    if exchange_code in _exchange_open_cache:
        return _exchange_open_cache[exchange_code]
    try:
        today = datetime.date.today()
        sched = mcal.get_calendar(exchange_code).schedule(start_date=today, end_date=today)
        is_open = not sched.empty
    except Exception as e:
        log.warning("Exchange calendar check failed for %s, assuming open: %s", exchange_code, e)
        is_open = True
    _exchange_open_cache[exchange_code] = is_open
    return is_open


def _ticker_exchange_open_today(symbol: str) -> bool:
    return _exchange_open_today(_exchange_for(symbol))


def _is_market_open_for(symbol: str) -> bool:
    """Whether the ticker's home exchange is currently in its trading session — per-exchange,
    not per-storyline, so a Xetra-only holiday correctly leaves Euronext Paris/SIX/Madrid tickers
    open (and vice versa)."""
    if is_weekend():
        return False
    if not _ticker_exchange_open_today(symbol):
        return False
    now = now_hhmm()
    if symbol in EU_WATCHLIST:
        return "09:00" <= now <= "17:30"
    if symbol in US_WATCHLIST:
        return "15:30" <= now <= "22:00"
    # Unknown / off-watchlist ticker — fall back to the broader combined window
    return "09:00" <= now <= "22:00"


def _gemini_news_hours_active() -> bool:
    """News classification only uses Gemini between 08:30 and US close (22:00) CET. The
    unlock sits 15min before the EU pre-market slot's own 08:45 target (which can itself
    fire as early as 08:40 with jitter) — giving news its own earlier cron cycle to make
    any Gemini attempt, rather than competing for the 5-RPM ceiling in the same cycle the
    pre-market post needs to succeed in. Outside this window it always routes to the
    keyword fallback, regardless of remaining budget or Gemini's availability — so
    overnight activity never quietly spends the day's Gemini allowance before it's needed."""
    return "08:30" <= now_hhmm() <= "22:00"


def market_phase(key: str) -> str:
    """Return 'weekend', 'pre_market', 'open', or 'post_market' for the given storyline.

    On a day where EVERY exchange in this storyline's watchlist is closed for a holiday, returns
    'weekend' regardless of time of day — reusing 'weekend' rather than a distinct phase since the
    correct framing (no live price references, news-grounded content) is identical either way. If
    only SOME exchanges are closed (e.g. Xetra closed, Euronext Paris/SIX/Madrid open), the
    storyline stays in its normal phase — the ranking logic in process_storyline separately
    excludes just the closed-exchange tickers, so coverage reallocates to whichever exchanges are
    actually open rather than the whole EU post going quiet. Calendar-based (via
    _ticker_exchange_open_today), so unlike a data-absence check, this works before the open too —
    a holiday morning's pre-market slot is caught, not just the after-open phases."""
    if is_weekend():
        return "weekend"

    watchlist = EU_WATCHLIST if key == "eu" else US_WATCHLIST if key == "us" else []
    if watchlist and not any(_ticker_exchange_open_today(t) for t in watchlist):
        return "weekend"

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
            name = _LEGAL_SUFFIX_RE.sub("", raw).strip().rstrip(",.").strip()
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
        try:
            # prepost=True is required to see pre-market/after-hours prints — without it,
            # yfinance only returns regular-session bars, which is empty before the open
            # and silently falls back to a stale-looking price with no range data at all.
            hist = ticker.history(period="1d", interval="1m", prepost=True)
            if not hist.empty:
                price    = round(float(hist["Close"].iloc[-1]), 2)
                day_high = round(float(hist["High"].max()), 2)
                day_low  = round(float(hist["Low"].min()), 2)
        except Exception:
            pass

        if price is None:
            price = round(info.last_price, 2)

        market_open = _is_market_open_for(symbol)

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
        if not market_open:
            # Only needed for the "last close" framing used when the market's shut — lets a
            # news-event tweet say WHICH session that price is from instead of an undated "last
            # close" that reads as current even after a multi-day holiday/weekend closure.
            try:
                daily = ticker.history(period="5d", interval="1d")
                if not daily.empty:
                    result["last_close_date"] = daily.index[-1].date().isoformat()
            except Exception:
                pass
        return result
    except Exception as e:
        log.warning("yfinance failed for %s: %s", symbol, e)
        return {}


def _find_move_timestamp(symbol: str) -> datetime.datetime | None:
    """Identifies the approximate UTC moment of the day's sharpest price move, if the day had a
    genuinely catalyst-shaped one — used to correlate against news publish times so a price-event
    tweet can be grounded in the headline whose timing actually lines up with the move, instead of
    handing the model an undifferentiated list of "recent headlines" and letting it guess (or
    invent) a connection. Returns None when the day's move looks like gradual drift rather than a
    single sharp jump — no one window accounts for a meaningful share of the day's total change —
    since there's no honest "moment" to correlate news against in that case."""
    try:
        hist = yf.Ticker(symbol).history(period="1d", interval="5m", prepost=True)
        if hist.empty or len(hist) < 2:
            return None
        closes = hist["Close"]
        total_move_pct = abs((closes.iloc[-1] - closes.iloc[0]) / closes.iloc[0] * 100)
        if total_move_pct < 1:
            return None  # too small a day to meaningfully attribute to anything
        step_pct = closes.pct_change().abs() * 100
        biggest_idx = step_pct.idxmax()
        biggest_step_pct = step_pct.loc[biggest_idx]
        if biggest_step_pct < 1 or biggest_step_pct < 0.4 * total_move_pct:
            return None  # gradual drift, no single sharp moment to point to
        ts = biggest_idx.to_pydatetime()
        if ts.tzinfo is not None:
            ts = ts.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return ts
    except Exception as e:
        log.warning("Move-timestamp detection failed for %s: %s", symbol, e)
        return None


def _closest_headline_to_move(move_time: datetime.datetime, articles: list[dict]) -> dict | None:
    """Among fresh articles, find the one published closest to the move timestamp — a generous
    lookback (reporting/data lag is common) but only a short lookahead (a headline can't cause a
    move that already happened well before it existed). Returns None if nothing lines up within a
    plausible window, which the caller should treat as "no confirmed catalyst," not license to
    guess."""
    MAX_LOOKBACK  = datetime.timedelta(hours=4)
    MAX_LOOKAHEAD = datetime.timedelta(minutes=30)
    best, best_delta = None, None
    for a in articles:
        if not a.get("published"):
            continue
        if _is_generic_analysis_piece(a["headline"]):
            continue  # a coincidentally-timed analyst-roundup/opinion piece isn't a real catalyst
        delta = move_time - a["published"]
        if -MAX_LOOKAHEAD <= delta <= MAX_LOOKBACK:
            abs_delta = abs(delta)
            if best_delta is None or abs_delta < best_delta:
                best, best_delta = a, abs_delta
    return best


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


def get_week_to_date_change(symbols: list[str]) -> dict[str, float]:
    """Signed net % change since this week's Monday open — current price vs. Monday's
    opening price. Signed so direction (+xx% / -xx%) is unambiguous at a glance."""
    monday = datetime.date.today() - datetime.timedelta(days=datetime.date.today().weekday())
    result = {}
    for symbol in symbols:
        try:
            hist = yf.Ticker(symbol).history(period="5d")
            hist = hist[hist.index.date >= monday]
            if hist.empty:
                continue
            monday_open = float(hist["Open"].iloc[0])
            current = float(hist["Close"].iloc[-1])
            if monday_open > 0:
                result[symbol] = round((current - monday_open) / monday_open * 100, 2)
        except Exception as e:
            log.warning("Week-to-date change failed for %s: %s", symbol, e)
    return result

# ── Zero-LLM content (weekend recaps + polls) ─────────────────────────────────
# Everything below fires with zero Gemini calls — pure yfinance data + string
# templates. Meant to run as an ADDITIONAL weekend post type, outside the
# per-storyline flexible/buffer budget entirely, since it costs nothing to fire.

WEEKEND_CAPTION_TEMPLATES = [
    "Week in numbers:\n{top5}\n\nWhich of these would you add to your portfolio?",
    "$1000, one week, these names:\n{top5}\n\nWhere's it going?",
    "Biggest mover this week: {best} {best_pct}.\nBiggest laggard: {worst} {worst_pct}.\n\nSpread like that raises the question of what the market's pricing in.",
    "{best} led the group this week at {best_pct}. {worst} brought up the rear at {worst_pct}.\n\nWhich one's the better setup going into next week?",
    "This week's board:\n{top5}\n\nOne of these could be quietly setting up. Which one are you watching?",
    "{spread} points separated the best and worst performer this week ({best} vs {worst}).\n\nDispersion like that doesn't happen without a reason.",
    "Closing out the week with these names on watch:\n{top5}\n\nWhat's your read heading into Monday's open?",
    "Top movers this week:\n{top3}\n\nMomentum like that could carry into next week – or stall right at the open.",
    "Wildly different outcomes on the board this week:\n{top3}\n\nThe dispersion alone is worth watching.",
    "$1000. These tickers. One week to hold:\n{top5}\n\nWhich one are you picking up?",
    "{best} quietly put up {best_pct} this week while most eyes were elsewhere.\n\nWorth asking what's still underpriced here.",
    "Weekly scoreboard:\n{top3}\n\nWhich of these keeps the momentum into next week?",
    "Watching these into next week:\n{top5}\n\nNo action needed this weekend – just setting the board.",
    "{worst} lagged the group this week at {worst_pct}. Could be a gap that closes fast, or a warning sign.\n\nWhich is it?",
    "This week's spread between {best} and {worst} was {spread} points.\n\nThat's not noise – that's a market making a decision.",
    "Sunday check-in:\n{top5}\n\nAny of these you're adding before Monday's open?",
    "{best} was the standout this week at {best_pct}.\n\nThe question now is whether that continues or whether it's already priced in.",
    "Full board heading into next week:\n{top5}\n\nWhich one moves first?",
    "Weekly standouts:\n{top3}\n\nWhich of these three would you add?",
    "A lot of divergence on the board this week:\n{top3}\n\nThat kind of spread tends to resolve one way or another.",
    "Heading into next week still watching:\n{top5}\n\nNothing's changed thesis-wise – just tracking the setup.",
    "{best} up {best_pct}, {worst} down {worst_pct} – same watchlist, opposite outcomes.\n\nWorth asking why.",
    "This week's names, ranked:\n{top3}\n\nMonday's open will be the first real test of whether this holds.",
    "Quiet week for headlines, loud week for price action:\n{top3}\n\nSometimes the moves happen before the news does.",
    "$1000 to deploy, this week's board to choose from:\n{top5}\n\nWhat are you picking up?",
    "{best} led, {worst} lagged, and the rest sat in between:\n{top5}\n\nWhere do you see the most room left?",
]

POLL_QUESTION_TEMPLATES = [
    "If you had $1000 to invest right now, which of these gets it?",
    "Which of these is the best setup heading into next week?",
    "One of these outperforms the rest by Friday. Which one?",
    "Forced to hold just one of these for a month – which do you pick?",
    "Which of these has the most room left to run?",
]


def generate_zero_llm_weekend_post(tickers: list[str]) -> str | None:
    """Fill a random template with this week's real performance numbers. No Gemini call.
    Uses get_week_to_date_change (Monday's OPEN vs. latest close) rather than get_week_performance
    (a 5-trading-day rolling window) — the same metric the Wednesday midweek post already uses, so
    a reader doesn't see two differently-computed "this week" numbers for the same tickers on the
    same weekend. Every figure is explicitly labeled "WTD" so it's unambiguous which window it
    covers, rather than a bare % that could be read as a single day's move."""
    perf = get_week_to_date_change(tickers)
    if len(perf) < 2:
        return None

    ranked = sorted(perf.items(), key=lambda kv: kv[1], reverse=True)
    best_t, best_v = ranked[0]
    worst_t, worst_v = ranked[-1]

    def fmt(t, v):
        return f"${_base_symbol(t)} {'+' if v >= 0 else ''}{v}% WTD"

    values = {
        "best": f"${_base_symbol(best_t)}",
        "best_pct": f"{'+' if best_v >= 0 else ''}{best_v}% WTD",
        "worst": f"${_base_symbol(worst_t)}",
        "worst_pct": f"{'+' if worst_v >= 0 else ''}{worst_v}% WTD",
        "spread": round(best_v - worst_v, 1),
        "top3": "\n".join(fmt(t, v) for t, v in ranked[:3]),
        "top5": "\n".join(fmt(t, v) for t, v in ranked[:5]),
    }

    template = random.choice(WEEKEND_CAPTION_TEMPLATES)
    text = template.format(**values)
    return text if len(text) <= 280 else None


# Weekday cadence heartbeat — a templated "biggest mover right now" snapshot, zero Gemini calls,
# fired on its own clock (see check_zero_llm_pulse) rather than triggered by news or a move
# threshold. Two pools so a quiet market isn't dressed up as exciting — matches the same
# anti-hallucination principle used for pre-market framing elsewhere in this file.
PULSE_MOVER_TEMPLATES = [
    "${base} leading the tape right now: {sign}{pct}% to ${price}.",
    "Biggest mover this hour: ${base} {sign}{pct}% at ${price}.",
    "${base} standing out at {sign}{pct}%, now ${price}.",
    "Live check: ${base} {sign}{pct}% to ${price} – the standout so far.",
    "${base} is the one to watch right now – {sign}{pct}% at ${price}.",
    "Hourly check-in: ${base} {sign}{pct}% to ${price}, well ahead of the rest of the board.",
    "${base} pulling away at {sign}{pct}%, now trading at ${price}.",
]

PULSE_QUIET_TEMPLATES = [
    "Quiet stretch across the board – ${base} holding near flat at ${price}.",
    "Nothing dramatic this hour. ${base} sits at ${price} ({sign}{pct}%).",
    "Calm session so far – ${base} at ${price}, barely moved ({sign}{pct}%).",
    "${base} holding steady at ${price} ({sign}{pct}%) – a quiet stretch for the group.",
    "Low-key hour across the watchlist. ${base} at ${price}, {sign}{pct}%.",
]

# Fallback for a genuine >= EVENT_DAY_THRESHOLD_PCT day move when Gemini's daily budget is
# already exhausted. No LLM call, so – same discipline as generate_tweet's no-catalyst branch –
# this only ever describes the move itself, never speculates about a cause.
PRICE_EVENT_ZERO_LLM_TEMPLATES = [
    "${base} {sign}{pct}% today at ${price} – one of the bigger moves on the board right now.{range}",
    "Sharp move: ${base} {sign}{pct}% to ${price} today.{range}",
    "${base} swinging {sign}{pct}% on the day, now ${price}.{range}",
    "Big print: ${base} {sign}{pct}% at ${price} today – a real standout.{range}",
    "${base} on the move – {sign}{pct}% to ${price} today.{range}",
]


def _draw_template(state: dict, pool_key: str, templates: list[str]) -> str:
    """Draw without replacement from a shuffled deck of template indices, reshuffling only once
    the deck is empty. Firing up to ~18x/day from a pool of 5-7 templates, plain random.choice()
    would visibly repeat within just a few posts (and could repeat back-to-back) — this
    guarantees every template gets used once before any of them repeat."""
    deck = state.setdefault("pulse_template_decks", {}).setdefault(pool_key, [])
    if not deck:
        deck.extend(range(len(templates)))
        random.shuffle(deck)
    return templates[deck.pop()]


def generate_zero_llm_pulse(tickers: list[str], state: dict) -> str | None:
    """Fill a template with the biggest live mover right now. No Gemini call. Only considers
    tickers whose market is actually open, so the number shown is always live, not a stale
    closed-market print."""
    open_tickers = [t for t in tickers if _is_market_open_for(t)]
    if not open_tickers:
        return None

    contexts = {}
    for t in open_tickers:
        ctx = get_price_context(t)
        if ctx:
            contexts[t] = ctx
    if not contexts:
        return None

    base_t = max(contexts, key=lambda t: abs(contexts[t]["change_pct"]))
    ctx = contexts[base_t]
    pct = ctx["change_pct"]
    values = {
        "base": _base_symbol(base_t),
        "price": ctx["price"],
        "pct": pct,
        "sign": "+" if pct >= 0 else "",
    }

    if abs(pct) >= 1.5:
        text = _draw_template(state, "mover", PULSE_MOVER_TEMPLATES).format(**values)
    else:
        text = _draw_template(state, "quiet", PULSE_QUIET_TEMPLATES).format(**values)
    return text if len(text) <= 280 else None


def generate_zero_llm_price_event(symbol: str, price_ctx: dict, state: dict) -> str | None:
    """No-LLM fallback for check_price_events when Gemini's daily call budget is already
    exhausted. A significant move is still worth posting – it just can't be attributed to
    anything, since there's no LLM call left to weigh a headline's timing against it."""
    pct = price_ctx["change_pct"]
    range_str = ""
    if "day_high" in price_ctx and "day_low" in price_ctx:
        range_str = f" Intraday range: low ${price_ctx['day_low']} / high ${price_ctx['day_high']}."
    values = {
        "base": _base_symbol(symbol),
        "price": price_ctx["price"],
        "pct": pct,
        "sign": "+" if pct >= 0 else "",
        "range": range_str,
    }
    text = _draw_template(state, "price_event", PRICE_EVENT_ZERO_LLM_TEMPLATES).format(**values)
    return text if len(text) <= 280 else None


def generate_zero_llm_poll(tickers: list[str]) -> tuple[str, list[str]] | None:
    """Pick this week's top-volatility tickers as poll options. No Gemini call."""
    vol = get_recent_volatility(tickers, sessions=5)
    if len(vol) < 2:
        return None
    top = sorted(vol, key=vol.get, reverse=True)[:4]
    if len(top) < 2:
        return None
    question = random.choice(POLL_QUESTION_TEMPLATES)
    options = [f"${_base_symbol(t)}" for t in top]
    return question, options

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


def _gemini(system: str, prompt: str, state: dict) -> str:
    global _gemini_unavailable
    # Refuse to even attempt once at the real-call ceiling — a post-count budget doesn't catch
    # this because a single logical "call" can cost 2+ real requests (see GEMINI_DAILY_CALL_LIMIT).
    if state.get("gemini_calls_today", 0) >= GEMINI_DAILY_CALL_LIMIT:
        _gemini_unavailable = True
        raise RuntimeError(f"Gemini daily call limit ({GEMINI_DAILY_CALL_LIMIT}) reached — refusing further calls")

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
        # Every attempt here is a real HTTP request to Google, counted against RPD whether it
        # succeeds, fails, or gets retried — so count it here, not just on success.
        state["gemini_calls_today"] = state.get("gemini_calls_today", 0) + 1
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
        # Only reset what's genuinely tied to the calendar day: post counters (mirrors
        # Gemini's own daily quota reset) and each storyline's flexible/buffer budget pools
        # and scheduled-slot plan. Everything else — news_seen, pending_news_posts,
        # news_category_posted, event_cooldowns, last_posted/ticker cooldown — is a
        # rolling-window concept, not a calendar-day one. A full wipe here used to drop
        # queued-but-unposted news the instant the clock crossed midnight (regardless of how
        # fresh it still was) and let a story dodge category-dedup just by reposting a few
        # minutes into the new day.
        state["date"] = today()
        state["daily_posts"] = 0
        state["daily_keyword_posts"] = 0
        state["daily_pulse_posts"] = 0
        state["gemini_calls_today"] = 0
        state["evergreen_opinion_posts_today"] = 0
        for key in ("eu", "us"):
            state.pop(f"{key}_flexible_used", None)
            state.pop(f"{key}_buffer_used", None)
            state.pop(f"{key}_slots", None)
            state.pop(f"{key}_posted", None)

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
- If the data marks a headline as a CONFIRMED, timing-matched catalyst, ground your reaction in
  it specifically — that's the strongest, most credible claim available.
- Otherwise, you may reference the provided headlines and offer your own read on what's likely
  driving the move. Frame it as a possibility ("could be", "may reflect"), never as a confirmed
  fact — opinions are fine on Twitter, fabricated facts are not.
- Mention the ticker, current price, and % daily change where relevant.
- Reference the stock only via its bare $TICKER symbol (e.g. $VRT). Never spell out the company
  name in place of the ticker. Never put the ticker inside brackets or parentheses — e.g. NOT
  "(NYSE: VRT)" or "($VRT)" — a bracketed ticker won't render as a clickable cashtag on X. If you
  ever need the company name for clarity, write it plainly followed by the bare ticker with no
  brackets between them: "Vertiv $VRT", not "Vertiv ($VRT)".
- Frame all forward-looking statements as possibilities, never certainties.
  Use: could, might, may, potentially, worth watching, raises the question.
  Never: will, confirms, proves, guarantees.
- Every tweet must have a clear point of view. A question or CTA at the end is only used when it
  flows naturally — never forced. When used, it goes on its OWN line with a blank line before it —
  never tacked onto the end of the preceding sentence.
- Never reference a specific day name. Use "at the open" or "tomorrow's open" instead.
- Never use em dash. Use en dash (–) only.
- Use line breaks to create breathing room – no walls of text.
- Emoji: use sparingly. 🟢🔴 are only for a direct "green or red at the open?" question — place them on their own line immediately before that question.
- No filler: "hot take", "buckle up", "thread", "building the backbone", "this is huge".
- 1-2 hashtags max, only if they add signal. Omit if they feel forced.
- Write in clear, complete sentences a reader parses in one pass — not clipped telegraphic
  fragments strung together with semicolons ("AI narrative vs. CEO award" reads as a puzzle, not
  a sentence). Keep real financial terms and abbreviations (PT, JV, upgrade, buyback) — this is a
  finance audience — the fix is sentence structure, not vocabulary.
- MUST be under 280 characters.

## Tweet type
  event – urgent reaction to a price move. Raw and immediate.

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
- Acquisition, merger, or buyout of the COMPANY ITSELF (the ticker being taken over, or the
  ticker acquiring another whole company): any
- Buyout rumor with named acquirer: any
- Asset sale >$500M: any
- NOTE: a fund or institution buying a block of SHARES of the company is NOT M&A — the company
  itself isn't being acquired. Classify that as "large_share_purchases" instead (see below).

Large share purchase (institutional):
- A named institution, fund, or investor buying a specific, sizeable number of SHARES of the
  company (e.g. "HSBC Holdings PLC Acquires 25,259 Shares of Equinix"): any. Use category
  "large_share_purchases". This is distinct from M&A above (the company is the target of a
  purchase of its stock, not being acquired outright).

Analyst note (category "analyst_note"):
- A named firm's research action on the company — price-target change, rating change (upgrade/
  downgrade), initiation of coverage, or a specific forward call ("BofA expects strong Q2 AI
  orders"): any. Must name the firm and say what it concluded — a generic "is this stock a buy?"
  opinion piece with no named firm is NOT this; ignore those.

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
    "category": "earnings|contract|ma|large_share_purchases|analyst_note|regulatory|macro|company|none",
    "headline": "the exact headline that triggered this (omit or empty if is_major is false)",
    "reason": "one sentence explaining why it qualifies or not"
  }
]

Return ONLY the JSON array, no other text."""

ENGAGEMENT_SYSTEM = """## Role
You are writing a weekly engagement post for a financial Twitter account tracking AI infrastructure stocks – connectivity, memory, networking.

## Rules
- NEVER invent or infer a catalyst, theme, or driver not supported by the data/headlines provided.
  If nothing in the data supports a real narrative, describe the numbers themselves rather than
  fabricating a reason behind them.
- Casual, direct, first-person voice – never sounds automated or templated
- Use line breaks – no walls of text
- Always list tickers alphabetically, one per line, with $ prefix, UNLESS the prompt explicitly
  specifies a different order (e.g. sorted by performance) — follow the prompt's order in that case.
- A closing question or CTA is used when it flows naturally – not as a reflex. When used, put it
  on its own line with a blank line before it – never run on from the previous sentence.
- Emoji: sparingly and only where genuinely meaningful. 👇 for a real CTA only. When in doubt, omit.
- No filler, no hype, no em dash – use en dash (–) only
- Never reference a specific day name. Use "at the open" or "tomorrow's open".
- Forward-looking statements as possibilities only: could, might, may – never will or confirms.
- MUST be under 280 characters.

## Output
Post text only. No quotes, no commentary."""


def generate_tweet(symbol: str, slot: dict, price_ctx: dict,
                   state: dict, event_trigger: str = "") -> str | None:
    # Only ever called by check_price_events, which is itself gated to a ticker's real
    # market hours — so this is always a live, in-session reaction. No other phase is
    # reachable here (that's what generate_market_update_tweet handles).
    price_str = ""
    if price_ctx and not is_weekend():
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        parts = [f"Live: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% today)"]
        if "day_high" in price_ctx and "day_low" in price_ctx:
            parts.append(f"Intraday range: low ${price_ctx['day_low']} / high ${price_ctx['day_high']}")
        price_str = ". ".join(parts)

    phase_instruction = "Market is open. React to live price action and news."

    # Prefer grounding the reaction in a headline whose PUBLISH TIME actually lines up with when
    # this move happened. If no such timing-confirmed catalyst exists, fall back to the original
    # behavior: hand the model the general headline list and let it form its own read — this is a
    # Twitter account, not a compliance filing, so a clearly-framed guess beats a flat "no comment."
    move_time = _find_move_timestamp(symbol)
    articles = get_ticker_context_with_dates(symbol, max_messages=10)
    catalyst = _closest_headline_to_move(move_time, articles) if move_time else None

    if catalyst:
        news_section = (
            f"\n\nLIKELY CATALYST — the only headline whose publish time actually lines up with "
            f"this move: \"{catalyst['headline']}\". This is a confirmed timing match (published "
            f"within a few hours before the move), not just a recent story — ground your "
            f"reaction in it specifically."
        )
    else:
        headlines = [a["headline"] for a in articles if a.get("headline")]
        if headlines:
            headline_list = "\n".join(f"- {h}" for h in headlines)
            news_section = (
                "\n\nNo headline's publish time lines up precisely with this specific move, so "
                "none of these is a confirmed cause. Recent headlines – reference at least one, "
                "and you're welcome to offer your own read on what's likely driving the move, "
                f"framed as a possibility, not a fact:\n{headline_list}"
            )
        else:
            news_section = (
                "\n\nNo recent headlines available. Describe the price action itself (the move, "
                "the intraday range) with conviction."
            )

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
        text = _gemini(TWEET_SYSTEM, prompt, state).strip('"').strip("'")
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

def classify_news_batch(ticker_articles: dict[str, list[dict]], state: dict) -> dict[str, dict] | None:
    """Classify multiple tickers' fresh headlines (+ summary excerpt where available) in a
    single Gemini call. Returns {symbol: classification} for tickers judged major (empty dict
    if the call succeeded but found nothing major), or None if the call itself failed — the
    caller needs to tell these apart to fall back to keyword classification only on a genuine
    failure, not silently treat a failure the same as 'nothing major found'."""
    if not ticker_articles:
        return {}

    blocks = []
    for symbol, articles in ticker_articles.items():
        lines = []
        for a in articles:
            lines.append(f"- {a['headline']}")
            if a.get("summary"):
                lines.append(f"  Summary: {a['summary']}")
        blocks.append(f"Ticker: ${symbol}\nHeadlines:\n" + "\n".join(lines))
    prompt = "\n\n".join(blocks) + "\n\nClassify each ticker independently per the rules above."

    try:
        text = _strip_json_fences(_gemini(NEWS_CLASSIFIER_SYSTEM, prompt, state))
        results = json.loads(text)
        major = {}
        for r in results:
            symbol = r.get("symbol", "")
            if symbol in ticker_articles and r.get("is_major"):
                major[symbol] = r
        return major
    except Exception as e:
        log.warning("Batch news classification failed for %s: %s", list(ticker_articles), e)
    return None


# ── Zero-LLM news fallback ──────────────────────────────────────────────────
# Only used when Gemini classification isn't available for a ticker this cycle
# (storyline's news budget already spent, or Gemini itself is down). Compound
# pattern matching, not bare keyword presence — mirrors Gemini's own category
# list but can't verify exact thresholds (a headline rarely states "beat by
# 6.2%"), so it's deliberately narrower: only categorically unambiguous events.
_KEYWORD_RULES = [
    ("ma",         re.compile(r"\b(acquir\w*|merger|buyout|takeover)\b", re.I), None),
    ("contract",   re.compile(r"\b(contract|partnership|deal)\b", re.I),
                   re.compile(r"\$[\d,.]+\s?(million|billion|M|B)\b", re.I)),
    ("earnings",   re.compile(r"\b(beats?|misses?)\s+(Q\d\s+)?estimates?\b|\btops? estimates?\b|"
                               r"\bguidance (raised|cut|lowered)\b", re.I), None),
    ("buyback",    re.compile(r"\b(buyback|share repurchase)\b", re.I),
                   re.compile(r"\$[\d,.]+\s?(million|billion|M|B)\b", re.I)),
    ("regulatory", re.compile(r"\b(sanctions|export (ban|control)|security ban|CHIPS Act)\b", re.I), None),
    # Analyst research notes (initiations, ratings, price-target changes, forward expectations) are
    # handled by the dedicated `analyst_note` check in classify_news_keyword (via _RESEARCH_NOTE_RE),
    # not a keyword rule here — that check catches the full range, not just "initiates coverage",
    # and runs before this loop. No separate `analyst` rule / routine-exclusion needed anymore.
]

# "Acquired" fires on genuine M&A ("Company X acquires Company Y") but just as readily on routine
# institutional 13F-style disclosures ("Shares Acquired by Leonteq Securities AG") — the latter
# happens for every stock, constantly, and isn't remotely "major" in the sense this rule means.
_MA_ROUTINE_RE = re.compile(
    r"\bshares?\s+acquired\b|\bacquires?\s+(shares|stake|position)\b|"
    r"\bacquired\s+by\s+[A-Z][\w.&]*(\s+[A-Z][\w.&]*){0,3}\s+"
    r"(Securities|Capital|Advisors?|Advisers?|Management|Partners|Wealth|Financial|Investment"
    r"|Asset|Trust|Group|LLC|Inc\.?)\b",
    re.I,
)

# A named institution buying a specific, sizeable number of SHARES of a tracked company ("HSBC
# Holdings PLC Acquires 25,259 Shares of Equinix"; "40,000 Shares in Vistra Corp. $VST Acquired
# by ... PZU ...") is real, price-relevant signal — but it is NOT M&A (the company itself isn't
# being bought), so it gets its own `large_share_purchases` category rather than muddying `ma`.
# The tell is a share COUNT + a purchase verb, in either order; genuine M&A names a target
# company and a deal value ("$4 billion deal"), not a share count of the tracked ticker.
#   - Share count requires a comma group or 4+ digits (>= ~1,000 shares) so a trivial "acquires
#     500 shares" doesn't qualify as "large". True dollar-size can't be judged from the headline
#     alone (no price), so this nominal-count floor is the practical proxy.
#   - A strategic stake buildup toward a takeover is technically pre-M&A, but telling a passive
#     pension fund from an activist accumulator isn't reliable via regex — the Gemini path may
#     still call a genuine strategic stake `ma`, which is fine.
_SHARE_COUNT_ANY_RE   = re.compile(r"\b\d[\d,]*\s+(?:new\s+)?shares?\b", re.I)
_SHARE_COUNT_LARGE_RE = re.compile(r"\b(\d{1,3}(?:,\d{3})+|\d{4,})\s+(?:new\s+)?shares?\b", re.I)
_PURCHASE_VERB_RE     = re.compile(r"\b(acquir\w*|purchas\w*|buys?|bought|boosts?|increas\w*|raises?|grows?|lifts?)\b", re.I)


def _is_share_purchase(text: str) -> bool:
    """Any institutional share purchase (a purchase verb + a share COUNT, any size). Used to
    recognize the whole CLASS so a small one can be dropped rather than leaking into `ma`."""
    return bool(_SHARE_COUNT_ANY_RE.search(text) and _PURCHASE_VERB_RE.search(text))


def _is_large_share_purchase(text: str) -> bool:
    """The subset of _is_share_purchase that clears the size floor (comma group or 4+ digits,
    i.e. >= ~1,000 shares) — only these are worth posting as `large_share_purchases`."""
    return bool(_SHARE_COUNT_LARGE_RE.search(text) and _PURCHASE_VERB_RE.search(text))

# MarketBeat-style "instant alert" institutional-holdings headlines ("HSBC Holdings PLC Acquires
# 25,259 Shares of Equinix, Inc.") are genuinely worth posting — a major bank taking a real
# position is real signal. But these alert mills are known to re-issue the SAME underlying filing
# (identical filer + identical share count) weeks or months apart under a brand-new URL/date, with
# every layer of metadata (RSS pubDate, URL slug, on-page datePublished) uniformly reporting the
# NEW generation date — there's no metadata mismatch for a staleness check to catch, because the
# page genuinely was (re-)generated that day. The only way to catch the source repeating itself is
# to fingerprint the actual disclosed facts (filer + share count, per ticker) and remember them
# well past the 24h category-dedup window, since a re-issue can land weeks later.
_HOLDING_DISCLOSURE_RE = re.compile(
    r"^(?P<institution>[\w.,&'-]+(?:\s+[\w.,&'-]+){0,4}?)\s+"
    r"(?:Acquires|Purchases|Buys|Sells|Increases|Decreases|Boosts|Trims|Cuts|Raises|Lowers|Grows|Reduces)"
    r"\b.*?(?P<shares>[\d,]{3,})\s+Shares?\b",
    re.I,
)


def _holding_disclosure_fingerprint(symbol: str, headline: str) -> str | None:
    m = _HOLDING_DISCLOSURE_RE.search(headline)
    if not m:
        return None
    institution = re.sub(r"[^\w]", "", m.group("institution")).lower()
    shares = m.group("shares").replace(",", "")
    return f"{_base_symbol(symbol).lower()}:{institution}:{shares}"


# Comfortably longer than a 13F's quarterly (~90 day) filing cadence, so a delayed reprocessing
# re-issue is still caught — but bounded, so state.json doesn't carry this dict forever.
HOLDING_DISCLOSURE_MEMORY_DAYS = 400


def _prune_date_keyed_dict(d: dict, max_age_days: int):
    """Drops entries whose ISO-date value is older than max_age_days. Shared by any fingerprint
    dict that maps 'thing we've seen' -> 'date we saw it', so state.json doesn't carry them
    forever once they're too old to plausibly matter again."""
    cutoff = (datetime.date.today() - datetime.timedelta(days=max_age_days)).isoformat()
    for key in [key for key, seen_date in d.items() if seen_date < cutoff]:
        del d[key]


# "Beat/miss estimates" fires just as readily inside a speculative preview question ("Will X Beat
# Estimates Again in Its Next Earnings Report?") as inside an actual report of one — a real beat/
# miss headline states it as a fact, never as a question.

# Generic Zacks/Motley Fool-style opinion pieces that reference real events (an old
# acquisition, a past guidance change) as supporting color in a valuation thesis — not
# reporting news. A bare category-keyword match can't tell "reporting X happened" from
# "mentioning X happened as background", so these title templates are excluded outright.
_ANALYSIS_PIECE_PATTERNS = [
    re.compile(r"\bis\s+.+\s+a\s+good\s+(investment|stock|buy)\b", re.I),
    re.compile(r"\bstock\s+look(s)?\s+(cheap|expensive|undervalued|overvalued)\b", re.I),
    re.compile(r"\bwhat\s+to\s+expect\s+from\b", re.I),
    re.compile(r"\bshould\s+you\s+buy\b", re.I),
    re.compile(r"\bbuy\s+or\s+sell\b", re.I),
    re.compile(r"\b(the\s+)?better\s+buy\b", re.I),
    re.compile(r"\btop\s+\d*\s*stocks?\s+to\b", re.I),
    re.compile(r"\bwhy\s+.+\s+is(n'?t)?\s+a\s+good\s+investment\b", re.I),
    re.compile(r"\bis\s+it\s+time\s+to\s+buy\b", re.I),
    re.compile(r"\bworth\s+(buying|investing)\b", re.I),
    re.compile(r"\bwhat\s+analysts?\s+think\b", re.I),
    # SimplyWall.St-style automated "intrinsic value" pieces ("Digital Realty (DLR) Stock Could
    # Be 35% Undervalued Despite $3.5b Data Center Deal") — a real dollar figure or real deal
    # name in the headline is just background color for the valuation model's opinion, not fresh
    # reporting of that deal. "stock look(s) undervalued" above only caught "looks undervalued";
    # this catches the more common "could be X% undervalued" template separately.
    re.compile(r"\b(could|might|may)\s+be\s+(\d+%\s+)?(undervalued|overvalued)\b", re.I),
    # Urgency/FOMO clickbait templates — confirmed live on the real watchlist ("After
    # Skyrocketing Nearly 200%, Is It Too Late to Buy Bloom Energy?", "All You Need to Know
    # About ENGIE - Sponsored ADR (ENGIY) Rating Upgrade to Strong Buy"). These are engagement
    # bait regardless of whether a real event is buried inside — the phrasing itself is the
    # signal, not the content. Deliberately NOT matching on bare words like "secret" (a Barron's
    # piece on Meta's "Secret Cloud Move" and a routine "Corporate Secretary" hire both showed up
    # in the same real data pull — a loose keyword would have wrongly excluded one and matched
    # the other by accident) — every pattern here is a full phrase, tested against 741 real
    # headlines from the actual watchlist with zero false positives.
    re.compile(r"\bis\s+it\s+too\s+late\s+to\s+buy\b", re.I),
    re.compile(r"\ball\s+you\s+need\s+to\s+know\s+about\b", re.I),
    re.compile(r"\byou\s+(need|have)\s+to\s+see\s+this\b", re.I),
    re.compile(r"\byou'?(ll| will)\s+(only\s+)?(read|know|realize|find\s+out|regret)\b.*\btoo\s+late\b", re.I),
    re.compile(r"\bbefore\s+it'?s\s+too\s+late\b", re.I),
    re.compile(r"\bdon'?t\s+miss\s+(this|out)\b", re.I),
    re.compile(r"\byou\s+won'?t\s+believe\b", re.I),
    re.compile(r"\byour\s+last\s+chance\b", re.I),
    re.compile(r"\bact\s+(now|fast)\b", re.I),
    re.compile(r"\bwake[- ]up\s+call\b", re.I),
    re.compile(r"^warning:", re.I),
    re.compile(r"\bwish\s+you\s+(had\s+)?(bought|sold)\b", re.I),
]


def _is_generic_analysis_piece(headline: str) -> bool:
    return any(p.search(headline) for p in _ANALYSIS_PIECE_PATTERNS)


def _mentions_company(symbol: str, text: str) -> bool:
    """Loose check that an article is actually about this company, not just topically
    adjacent (e.g. a Bloom Energy story showing up in Equinix's feed). Requires either
    standard ticker notation — (VRT), (NYSE: VRT), $VRT — or the company's distinguishing
    name to appear. Uses the first word of the resolved name rather than the full name,
    since headlines routinely abbreviate ("NextEra-Dominion merger", not "NextEra Energy")."""
    text_lower = text.lower()
    base = _base_symbol(symbol).lower()
    if re.search(rf"[(\$]\s*(nyse|nasdaq)?\s*:?\s*{re.escape(base)}\b", text_lower):
        return True
    name = _company_name(symbol)
    first_word = re.sub(r"[^\w]", "", name.split()[0]).lower() if name else ""
    return bool(first_word) and first_word in text_lower


# Genuine bank/research-house activity — a price target change, a rating, a coverage call, a
# named firm's forward expectation — shows up regularly in the Yahoo/Google News/Nasdaq feeds
# already fetched ("Morgan Stanley Cuts Price Target on Vistra", "BofA Lifts Nokia Price Objective
# on Expectations of Strong Q2 AI Orders", "BofA Expects Nokia AI Orders to Stay Strong in Q2",
# "Vistra Corp Gets a Buy from Wells Fargo"). This is real, price-relevant substance — competitor
# accounts post it — so it's caught both in the real-time pipeline (category `analyst_note`, see
# classify_news_keyword) AND surfaced in the weekend research spotlight.
# Verb list stays broad on purpose (Lifts/Hikes/Bumps, Expects/Anticipates, etc.) since these
# headlines are phrased a dozen different ways; "Price Objective"/"Target Price" are the same
# thing as "Price Target" reworded, and both bare phrases match regardless of the verb used.
_RESEARCH_NOTE_RE = re.compile(
    r"^[A-Z][\w.&'-]*(?:\s+[A-Z][\w.&'-]*){0,4}\s+"
    r"(?:Backs|Raises|Cuts|Lowers|Lifts|Hikes|Bumps|Maintains|Reiterates|Initiates|Sees|Sets|Gives|"
    r"Upgrades|Downgrades|Sends|Starts|Boosts|Trims|Expects|Anticipates|Flags)\b"
    r"|\bGets?\s+an?\s+(?:Buy|Sell|Hold|Overweight|Underweight|Outperform|Underperform)\s+(?:from|Rating)\b"
    r"|\bSees?\s+\d+%\s+(?:Upside|Downside)\b"
    r"|\b(Price\s+Target|Price\s+Objective|Target\s+Price)\b"
    r"|\bInitiates?\s+Coverage\b",
    re.I,
)


def _is_research_note_headline(headline: str) -> bool:
    return not _is_generic_analysis_piece(headline) and bool(_RESEARCH_NOTE_RE.search(headline))


WEEKEND_RESEARCH_LOOKBACK_DAYS = int(os.getenv("WEEKEND_RESEARCH_LOOKBACK_DAYS", "6"))
# A note reused across weekends would just be a duplicate the following week — this doesn't need
# to be nearly as long-lived as the holding-disclosure fingerprint, since the failure mode here is
# "repeated within a couple weekends", not "resurfaced months later".
RESEARCH_SPOTLIGHT_MEMORY_DAYS = 21


def _find_research_spotlight(watchlist: list[str], state: dict) -> dict | None:
    """Scans the whole watchlist over WEEKEND_RESEARCH_LOOKBACK_DAYS (not just the usual 24h
    freshness window) for a genuine research-note headline not already spotlighted in a prior
    weekend, richest-first: most recent, but skipping any candidate whose link can't be verified
    (same staleness/redirect-resolution discipline as the real-time news pipeline) in favor of the
    next-best one instead of just giving up. Returns the article (plus its symbol and a resolved,
    verified link) or None if nothing qualifies this week."""
    used = state.setdefault("research_spotlights_used", {})
    _prune_date_keyed_dict(used, RESEARCH_SPOTLIGHT_MEMORY_DAYS)

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=WEEKEND_RESEARCH_LOOKBACK_DAYS)
    candidates = []
    for symbol in watchlist:
        for a in get_ticker_context_with_dates(symbol, max_messages=20):
            if not a.get("published") or a["published"] < cutoff:
                continue
            if not _mentions_company(symbol, a["headline"]):
                continue
            if not _is_research_note_headline(a["headline"]):
                continue
            fp = f"{_base_symbol(symbol).lower()}:{a['headline'].strip().lower()}"
            if fp in used:
                continue
            candidates.append({**a, "symbol": symbol, "fingerprint": fp})

    candidates.sort(key=lambda a: a["published"], reverse=True)
    for a in candidates:
        link = _resolve_google_news_url(a.get("link"))
        if link and _is_confirmed_stale(link):
            continue
        a["link"] = link
        return a
    return None


def classify_news_keyword(symbol: str, articles: list[dict]) -> dict | None:
    """Zero-LLM classifier for a single ticker's fresh articles. Returns the first article
    matching a compound rule AND actually being about this company, in the same shape
    classify_news_batch's Gemini path returns."""
    for a in articles:
        if _is_generic_analysis_piece(a["headline"]):
            continue  # opinion/valuation piece, not a news report — skip regardless of keywords
        text = f"{a['headline']} {a.get('summary') or ''}"
        if not _mentions_company(symbol, text):
            continue  # topically adjacent but not actually about this ticker — skip
        # Institutional share purchase gets its own handling, checked BEFORE the M&A rule so
        # "[Institution] acquires N shares of [company]" isn't miscategorized as a corporate
        # takeover. A large one → its own category (toggle enforced downstream at queue time, so
        # this stays a pure "what is it" classifier). A small one → skipped entirely, since it's
        # neither sizeable enough to be interesting NOR M&A (letting it fall through would leak it
        # into `ma`, because the active-voice "Acquires N Shares" form evades _MA_ROUTINE_RE).
        if _is_share_purchase(text):
            if _is_large_share_purchase(text):
                return {
                    "is_major": True,
                    "category": "large_share_purchases",
                    "headline": a["headline"],
                    "reason": "Sizeable institutional share purchase",
                }
            continue
        # Named-firm research note (price-target change, rating, initiation, forward call) — a
        # real, price-relevant view competitor accounts post too. Checked before the keyword loop
        # so e.g. "BofA Lifts Nokia Price Objective ..." is caught as its own category rather than
        # slipping through uncategorized. _is_research_note_headline already excludes generic
        # valuation/opinion clickbait, so this stays tight.
        if _is_research_note_headline(a["headline"]):
            return {
                "is_major": True,
                "category": "analyst_note",
                "headline": a["headline"],
                "reason": "Named-firm analyst research note",
            }
        for category, primary, secondary in _KEYWORD_RULES:
            if not primary.search(text):
                continue
            if category == "ma" and _MA_ROUTINE_RE.search(text):
                continue  # a fund buying shares, not the company itself being acquired
            if category == "earnings" and a["headline"].rstrip().endswith("?"):
                continue  # speculative preview ("Will X Beat Estimates?"), not a reported beat/miss
            if secondary and not secondary.search(text):
                continue  # e.g. "partnership" without a dollar figure is too vague to act on
            return {
                "is_major": True,
                "category": category,
                "headline": a["headline"],
                "reason": f"Keyword rule matched (category: {category})",
            }
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
- A closing question or CTA is only used when it flows naturally — never forced. When used, it
  goes on its OWN line with a blank line before it — never appended straight onto the sentence
  before it (e.g. NOT "...for incumbents. How might this play out?" on the same line/run-on —
  put a blank line, then the question, on its own).
- No filler: "this is huge", "big news", "buckle up"
- Write in clear, complete sentences a reader parses in one pass — not clipped telegraphic
  fragments. Keep real financial terms (PT, JV, upgrade, buyback) — this is a finance audience —
  the fix is sentence structure, not vocabulary.
- Reference the stock only via its bare $TICKER symbol (e.g. $VRT). Never spell out the company
  name in place of the ticker. Never put the ticker inside brackets or parentheses — e.g. NOT
  "(NYSE: VRT)" or "($VRT)" — a bracketed ticker won't render as a clickable cashtag on X. If you
  ever need the company name for clarity, write it plainly followed by the bare ticker with no
  brackets between them: "Vertiv $VRT", not "Vertiv ($VRT)".
- Use line breaks – no walls of text
- Emoji: sparingly. 🟢🔴 only for a direct "green or red at the open?" question — place them on their own line immediately before that question.
- Never use em dash. Use en dash (–) only.
- Never reference a specific day name. Use "at the open" or "tomorrow's open".
- MUST be under 280 characters.

## Review before output
Verify: facts match the input — no unsupported claims — tweet ≤280 characters.

## Output
One tweet only. No quotes, no commentary."""


def _format_close_date(iso_date: str) -> str:
    """Compact, unambiguous 'as of' qualifier for a last-close price. A bare weekday name would
    violate the existing 'never reference a specific day name' style rule, and a slash-separated
    date (dd/mm vs mm/dd) is ambiguous to a global audience — 'Jul 2' has neither problem."""
    try:
        d = datetime.date.fromisoformat(iso_date)
        return f"{d.strftime('%b')} {d.day}"
    except Exception:
        return ""


def generate_news_event_tweet(symbol: str, classification: dict, price_ctx: dict, state: dict,
                               link: str = "", source: str = "") -> str | None:
    price_str = ""
    if price_ctx:
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        # Same number either way (move vs. prior close) — just be honest about whether it's
        # live or last session's, rather than dropping a genuinely newsworthy move (e.g. a
        # post-news selloff) just because the post happens to fire outside market hours.
        if _is_market_open_for(symbol):
            price_str = f"Current: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}% today)"
        else:
            close_date = _format_close_date(price_ctx.get("last_close_date", ""))
            date_suffix = f" as of {close_date}" if close_date else ""
            price_str = f"Last close{date_suffix}: ${price_ctx['price']} ({sign}{price_ctx['change_pct']}%)"

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
        text = _gemini(NEWS_EVENT_SYSTEM, prompt, state).strip('"').strip("'")
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


def generate_keyword_event_tweet(symbol: str, classification: dict, price_ctx: dict,
                                  link: str = "") -> str | None:
    """Zero-LLM counterpart to generate_news_event_tweet — pure template, no Gemini call.
    Used only when Gemini classification wasn't available for this ticker this cycle.
    No explicit source attribution line — the link itself (and its preview card) already
    makes the source obvious."""
    base = _base_symbol(symbol)
    price_str = ""
    if price_ctx:
        sign = "+" if price_ctx["change_pct"] >= 0 else ""
        # change_pct is always the move vs. the prior close, live or not — while the market's
        # open that's "today"'s move; once it's closed, the same number is last session's move,
        # so say so rather than dropping a genuinely newsworthy number (e.g. a post-news selloff)
        # just because the post happens to fire outside market hours.
        if _is_market_open_for(symbol):
            price_str = f" (${price_ctx['price']}, {sign}{price_ctx['change_pct']}% today)"
        else:
            close_date = _format_close_date(price_ctx.get("last_close_date", ""))
            date_suffix = f" as of {close_date}" if close_date else ""
            price_str = f" (${price_ctx['price']}, {sign}{price_ctx['change_pct']}% at last close{date_suffix})"

    prefix = f"${base}{price_str}: "
    suffix = f"\n\n{link}" if link else ""
    # X counts any URL as a fixed ~23 characters (t.co shortening) regardless of its real length.
    suffix_budget = 25 if link else 0  # 2 newlines + 23-char shortened link

    headline = classification["headline"]
    max_headline_len = 280 - len(prefix) - suffix_budget
    if max_headline_len < 20:
        return None
    if len(headline) > max_headline_len:
        headline = headline[:max_headline_len].rsplit(" ", 1)[0] + "…"

    return prefix + headline + suffix


MARKET_UPDATE_SYSTEM = """## Role
You are an expert financial X (Twitter) market commentator — sharp, credible, market-native.

## Aim
Write one tweet about the current market session using the stock data provided.
For analytical/question types, you may focus on one stock or reference multiple — choose based on
what's most interesting. For the scheduled session posts (hook, wrap, close_summary), the type
instructions below REQUIRE covering every ticker in the data block — that requirement overrides
this general discretion.

## Rules
- NEVER invent or infer market conditions, catalysts, or facts not present in the provided data.
- ONLY give a $TICKER + specific price or % move for a stock that appears in the "Market data" block below.
  If a headline mentions some OTHER company (a peer, competitor, or supplier), you may reference what that
  headline literally says about it in prose — but NEVER invent a price, a % move, or a $TICKER for it.
  That other company has no data block here; anything you'd write about its price would be fabricated.
- Reference specific prices, % moves, and headlines from the data — specific beats vague, always.
- ALWAYS reference a stock by its bare ticker symbol with a $ prefix (e.g. $VRT, $SU). Never write out
  the company name in place of the ticker — traders recognize the symbol, spelling out the name wastes
  space. Never put the ticker inside brackets or parentheses — e.g. NOT "(NYSE: VRT)" or "($VRT)" — a
  bracketed ticker won't render as a clickable cashtag on X. If the company name is ever needed for
  clarity, write it plainly followed by the bare ticker with no brackets: "Vertiv $VRT", not
  "Vertiv ($VRT)".
- When the post covers more than one ticker, give EACH its own line — a short, CLEAR, complete
  phrase (e.g. "$VRT -6.6% at $314.26, off its $335.66 high"), not multiple tickers woven into one
  flowing sentence, and not a clipped fragment either (NOT "$IREN: AI narrative vs. CEO award." —
  that's a puzzle, not a sentence; write "$IREN: AI narrative debate overshadowed by its CEO's
  industry award" instead). Keep real financial terms and abbreviations (PT, JV, upgrade,
  buyback) — this is a finance audience — the fix is sentence structure, not vocabulary.
  Skimmable beats prose when there's more than one name to cover. Structure the whole tweet as
  THREE visually separated blocks with a full BLANK line between each: the opening sentence, then
  the ticker list, then the closing takeaway.
- The closing takeaway is MANDATORY, not optional — a multi-ticker tweet must never end on the
  last ticker's line. It must add a genuine "so what": what connects these moves, what it implies,
  or what to watch for — never a generic label that just restates the category (NOT "AI
  infrastructure is a core theme." — that says nothing a reader didn't already know from the
  ticker list above it). Ask: after reading the ticker lines, what's the ONE insight a sharp
  trader would take away? Write that.
- For any post covering multiple tickers, the opening sentence must be a GENERAL summary or shared
  theme across the group (e.g. "AI/data center demand led this week") — never a specific single
  ticker's specific detail (e.g. NOT "$VRT eyed for earnings growth"). That specific detail belongs
  on $VRT's own line below, not the opener — naming it in both places is pure duplication and wastes
  space that could cover another ticker's detail instead. Save each ticker's specific fact — a
  number, a named catalyst, a concrete event, not a vague tag like "Analyst fav" — for its own line.
- Frame all forward-looking statements as possibilities, never certainties.
  Use: could, might, may, potentially, worth watching, raises the question.
  Never: will, confirms, proves, guarantees.
- Every tweet must have a clear point of view.
- If asking a question, ask exactly ONE for the whole tweet — never one question per ticker
  when covering multiple stocks. Pick the single most interesting angle and ask about that.
  Put it on its OWN line with a blank line before it — never run on from the sentence before it.
- Never reference a specific day name. Use "at the open" or "tomorrow's open" instead.
- Never use em dash. Use en dash (–) only.
- Use line breaks to create breathing room – no walls of text.
- Emoji: use sparingly. 🟢🔴 are only for a direct "green or red at the open?" question.
- No filler: "hot take", "buckle up", "thread", "this is huge".
- 1-2 hashtags max, only if they add signal. Omit if forced.
- MUST be under 280 characters.

## Tweet types
  hook          – the day's pre-market post. Cover EVERY ticker in the data block below, one line
                  each (same skimmable format the multi-ticker rule above describes) — never narrow
                  the whole tweet to a single name, even if one stock moved the most. Open with one
                  sentence naming that biggest mover or the clearest catalyst, then list every
                  ticker's line underneath. Real pre-market prints, not yesterday's close — if a
                  stock has genuinely moved, name it and tie it to a catalyst. Otherwise frame around
                  what to watch at the open. Do not write as if the regular session is already live —
                  never phrase it as "early session", which reads as if regular trading has already
                  started. The tweet is prefixed with "Pre-market update: " automatically (see the
                  phase instructions below) — don't write that label yourself.
  analytical    – specific numbers, price levels, or data points. State the implication clearly.
                  May focus on a single ticker (e.g. a research spotlight) rather than the full
                  watchlist — see the general Aim section above.
  question      – one sharp, genuine question rooted in real news or price action. Only if it adds value.
                  May focus on a single ticker rather than the full watchlist.
  wrap          – the day's close post (US). Cover EVERY ticker in the data block, one line each —
                  never narrow to a single name. Open with one sentence naming the biggest mover,
                  then list where each stock closed and how volatile its session was (use the
                  intraday range — near-high close vs. near-low close tell different stories). Name
                  what drove the biggest move if the news supports it. This is the day's one recap.
                  The tweet is prefixed with "Market wrap-up: " automatically (see the phase
                  instructions below) — don't write that label yourself.
  close_summary – same role as wrap (EU's version): cover every ticker, biggest mover named up
                  front, closing price, session volatility, and driver for each. Also prefixed
                  with "Market wrap-up: " automatically.

## Weekend rules (only apply when phase = WEEKEND)
- Markets are closed. NEVER reference today's price, daily % moves, or live market activity.
- NEVER use phrases like "today", "this session", "up X%", "down X%".
- Ground every angle in the recent news headlines provided — reference specific events from the past week.
- Frame as possibility: "X happened, which could mean Y" or "X happened – is the market pricing this in?"
- Focus on: week-in-review, structural thesis grounded in news, what to watch at the open.
- The hook/wrap "automatic prefix" described under Tweet types ("Pre-market update: " / "Market
  wrap-up: ") is a WEEKDAY-only mechanic tied to a real market open/close — it does NOT apply on
  weekends, and nothing gets prepended for you here. Do NOT write "Pre-market update:" or "Market
  wrap-up:" yourself as an opener; write a plain, natural opening sentence per the general-summary
  rule above instead.

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
                                  news_data: dict, slot: dict, phase: str, state: dict,
                                  research_spotlight: dict | None = None) -> str | None:
    if phase == "weekend":
        phase_instruction = (
            "WEEKEND. Markets are closed. Do NOT reference today's price, daily % moves, or live "
            "activity. Ground the tweet in the news headlines below — week-in-review, structural "
            "thesis, or what to watch at the open."
        )
        if research_spotlight:
            source_note  = f" ({research_spotlight['source']})" if research_spotlight.get("source") else ""
            summary_note = f"\nReport summary: {research_spotlight['summary']}" if research_spotlight.get("summary") else ""
            phase_instruction += (
                f"\n\nRESEARCH SPOTLIGHT this week on ${_base_symbol(research_spotlight['symbol'])}: "
                f"\"{research_spotlight['headline']}\"{source_note}.{summary_note}\n"
                "This is a genuine named-firm research view, not routine chatter. Distill it into a "
                "tight, high-level executive-summary callout — the specific number or thesis that "
                "matters (the price target, the rating, the stated reasoning) — not a restatement of "
                "the headline. Build the tweet's main angle around it. Frame implications as "
                "possibilities, never certainties. Do NOT include a URL yourself — the report link is "
                "appended separately after your text."
            )
    elif phase == "pre_market":
        phase_instruction = (
            "Market is NOT yet open. Cover EVERY ticker in the data block below, one line each — do "
            "NOT narrow the whole tweet to a single name even if one stock is the biggest mover. Open "
            "with one sentence naming that biggest mover or the clearest catalyst, then list every "
            "ticker's line same as any other multi-ticker post. The price/% figures below are real "
            "pre-market prints, not yesterday's close — if a stock has actually moved pre-market, "
            "name the specific move and treat it as the day's setup, not background noise. Tie it to "
            "a catalyst from the news provided if one exists. If nothing has genuinely moved "
            "pre-market, don't invent tension — frame the post around what to watch at the open "
            "instead. Do NOT write as if the regular session is already underway — never phrase it "
            "as 'early session', which reads as if regular trading has already started."
        )
    elif phase == "post_market":
        phase_instruction = (
            "Market is CLOSED — this is the day's one closing summary, so make it count. Cover EVERY "
            "ticker in the data block below, one line each — do NOT narrow the whole tweet to a "
            "single name even if one stock is the biggest mover. Open with one sentence naming that "
            "biggest mover, then list where each stock actually closed, using the intraday range "
            "(low-to-high spread) to characterize how volatile each session was — a stock that closed "
            "near its low after a much higher open tells a different story than one that closed at "
            "its high. Name what likely drove the biggest move if the news data supports it. Factual "
            "and specific, no forward speculation."
        )
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

    # Every pre-market/close post starts with this exact label, guaranteed in code rather than
    # left to the model to remember every time — a prompt instruction alone drifted in practice.
    prefix = {"pre_market": "Pre-market update: ", "post_market": "Market wrap-up: "}.get(phase, "")
    prefix_note = (
        f'\nThe tweet will be prefixed with "{prefix}" automatically — do NOT write that label '
        "yourself. Write your opening sentence (naming the biggest mover) as a natural continuation "
        "of that phrase." if prefix else ""
    )

    prompt = f"""Market data ({key.upper()} session):
{chr(10).join(lines)}

Phase: {phase_instruction}
Slot type: {slot['type']}
{prefix_note}
Write one tweet. Focus on what's most interesting — biggest mover, news catalyst, or a cross-stock pattern. \
Can reference one stock or multiple. Always use $TICKER (bare symbol, no company name). Under 280 characters."""

    link = research_spotlight.get("link") if research_spotlight else None
    # X counts any URL as a fixed ~23 characters (t.co shortening) regardless of its real length.
    budget = 280 - len(prefix) - (25 if link else 0)  # 2 newlines + 23-char shortened link

    try:
        text = _gemini(MARKET_UPDATE_SYSTEM, prompt, state).strip('"').strip("'")
        if _has_unknown_ticker(text, ranked):
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
        result = prefix + text
        if link:
            result += f"\n\n{link}"
        return result
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
        last_week_perf = get_week_performance(tickers)
        if last_week_perf:
            monday_top5 = sorted(last_week_perf, key=last_week_perf.get, reverse=True)[:5]
            monday_lines = "\n".join(
                f"${t}  {'+' if last_week_perf[t] >= 0 else ''}{last_week_perf[t]}%" for t in monday_top5
            )
            ranked_note = "already ranked best to worst by last week's % change, "
        else:
            monday_top5 = tickers[:5]
            monday_lines = "\n".join(f"${t}" for t in monday_top5)
            ranked_note = ""
        n = len(monday_top5)
        posts.append(("monday", f"""Write a Monday opening post for a financial Twitter account.

Last week's top {n} performers from the watchlist, worth a fresh look this week ({ranked_note}use
exactly these {n}, none added or dropped):
{monday_lines}

Format:
- Open with a short intro line noting these are last week's standout names
- List the tickers on separate lines
- End with: "$1000 to deploy this week – which one are you picking up? 👇"

Keep it casual, direct, simple everyday English, under 280 characters."""))

    if weekday == 2 and "12:00" <= now <= "13:30" and engagement.get("wednesday") != today_str:
        vol = get_recent_volatility(tickers, sessions=3)
        top5 = sorted(vol, key=vol.get, reverse=True)[:5]
        if top5:
            wtd = get_week_to_date_change(top5)
            # Biggest gain to biggest loss — NOT alphabetical, overriding the general
            # engagement-post rule for this one post since the ranking itself is the point.
            top5_sorted = sorted(top5, key=lambda t: wtd.get(t, float("-inf")), reverse=True)
            vol_lines = "\n".join(
                f"${_base_symbol(t)}: {'+' if wtd[t] >= 0 else ''}{wtd[t]}%" if t in wtd else f"${_base_symbol(t)}"
                for t in top5_sorted
            )
            news_by_ticker = {t: get_ticker_context(t, max_messages=1) for t in top5_sorted}
            news_lines = "\n".join(
                f"${_base_symbol(t)}: {headlines[0]}" if (headlines := news_by_ticker.get(t)) else f"${_base_symbol(t)}: no notable headline this week"
                for t in top5_sorted
            )
            n = len(top5_sorted)
            posts.append(("wednesday", f"""Write a midweek engagement post for a financial Twitter account.

This week's biggest movers, already sorted from biggest gain to biggest loss (net % change since
Monday's open) — {n} ticker{'s' if n != 1 else ''} total, exactly as many as listed below, none missing:
{vol_lines}

A recent headline per ticker, for context on what may be driving the move:
{news_lines}

Format:
- Open with "Midweek check-in:" followed by ONE specific observation grounded in the actual headlines
  above — name a real theme or catalyst (a specific deal, sector trend, earnings reaction) tying
  together why these stocks moved. If the headlines don't genuinely support a shared driver, name the
  single standout mover and its move instead of inventing a connection. Never a generic line like
  "the week has seen volatility" that could apply to any week.
- List {'the ticker' if n == 1 else f'all {n} tickers'} on separate lines with their % figure, in the
  EXACT order given above — do NOT resort alphabetically (the gain-to-loss order is the point) and do
  NOT add or invent any ticker not in that list.
- End with a simple, clear hypothetical: {"if you had $1000 to invest right now, would you put it into this one or hold off?" if n == 1 else f"if you had $1000 to invest right now, which of these {n} would you pick up?"} Phrase this fresh each time in your own words – don't reuse the same wording as a template.

Use a line break between the opener, the ticker list, and the closing question. Keep it simple, clear,
casual, engaging. Under 280 characters."""))

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
        # Week-to-date (Monday's open vs. latest close) — same metric the Wednesday post and the
        # zero-LLM weekend caption both use, so this ticker's % doesn't read differently across
        # the day's several "this week" posts depending on which one happened to generate it.
        perf = get_week_to_date_change(tickers)
        if perf:
            perf_lines = "\n".join(
                f"${t}  {'+' if v >= 0 else ''}{v}% WTD" for t, v in sorted(perf.items())
            )
            posts.append(("saturday", f"""Write a Saturday weekly performance post for a financial Twitter account.

Week-to-date performance, Monday's open through the latest close (use exactly these numbers,
tickers already sorted alphabetically):
{perf_lines}

Format:
- Open with "Week in numbers:" or similar
- List each ticker and its week-to-date performance on a separate line exactly as given —
  explicitly label it "week-to-date" or "WTD" somewhere in the post so it's unambiguous this
  covers the whole week, not just today or Friday alone
- End with an engaging question like "Which one surprised you most? 👇"

Keep it casual, direct, under 280 characters."""))

    for key, prompt in posts:
        log_kwargs = dict(mechanism="weekly_engagement", slot_type=key, generation_method="gemini",
                           related_tickers=", ".join(f"${t}" for t in tickers))
        if _gemini_unavailable:
            log.warning("Engagement post [%s] skipped — Gemini unavailable this cycle", key)
            _log_event(state, **log_kwargs, posted="N", skip_reason="gemini_unavailable")
            continue
        try:
            text = _gemini(ENGAGEMENT_SYSTEM, prompt, state).strip('"').strip("'")
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
            log_kwargs["gemini_call_used"] = "Y"
            log_kwargs["gemini_calls_today_after"] = state.get("gemini_calls_today", 0)
            if post_tweet(tweet, state):
                engagement[key] = today_str
                save_state(state)
                _log_event(state, **log_kwargs, pool_used="llm", posted="Y",
                           tweet_text=tweet, tweet_char_count=len(tweet))
            else:
                # Mark done so this doesn't regenerate (and re-spend Gemini) next cycle — the
                # already-written text goes to the backlog for a straight repost retry instead.
                engagement[key] = today_str
                _push_to_backlog(state, tweet, "llm", **log_kwargs)
                save_state(state)
                _log_event(state, **log_kwargs, posted="N", skip_reason="post_failed_backlogged",
                           tweet_text=tweet, tweet_char_count=len(tweet))
        except Exception as e:
            log.error("Engagement post [%s] failed: %s", key, e)
            _log_event(state, **log_kwargs, posted="N", skip_reason=f"exception: {e}")

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


# Content that's already been generated (Gemini spend already happened) but Playwright/X failed
# to actually post it — held here for a straight repost retry, never regeneration, since the
# whole point is to not spend a second Gemini call on text that already exists. Bounded to
# POST_BACKLOG_MAX_AGE_MINUTES because a "pre-market hook" naming specific pre-open prices stops
# being honest a couple hours later, even though the text itself hasn't changed.
POST_BACKLOG_MAX_AGE_MINUTES = int(os.getenv("POST_BACKLOG_MAX_AGE_MINUTES", "90"))


def _push_to_backlog(state: dict, text: str, pool: str, **log_meta):
    backlog = state.setdefault("post_backlog", [])
    backlog.append({"text": text, "pool": pool, "queued_min": _epoch_minutes(), "log_meta": log_meta})


def _flush_post_backlog(state: dict) -> dict:
    """Retries the oldest backlogged post, at most one per cycle — same pacing philosophy as the
    news queue: a recovered outage shouldn't dump several backlogged posts back-to-back."""
    backlog = state.get("post_backlog", [])
    if not backlog:
        return state

    item = backlog[0]
    age = _epoch_minutes() - item["queued_min"]
    if age > POST_BACKLOG_MAX_AGE_MINUTES:
        log.warning("Backlogged post expired (held %d min, no longer honest to post as-is) — "
                    "dropping: %s", age, item["text"][:80])
        _log_event(state, **item["log_meta"], posted="N", skip_reason="backlog_expired",
                   tweet_text=item["text"], tweet_char_count=len(item["text"]))
        backlog.pop(0)
        save_state(state)
        return state

    log.info("Retrying backlogged post (%d min old, no Gemini call needed): %s", age, item["text"][:80])
    if post_tweet(item["text"], state, pool=item["pool"]):
        _log_event(state, **item["log_meta"], pool_used=item["pool"], posted="Y",
                   tweet_text=item["text"], tweet_char_count=len(item["text"]))
        backlog.pop(0)
        save_state(state)
    else:
        log.warning("Backlogged post retry failed again — will retry next cycle.")

    return state


def post_tweet(text: str, state: dict, pool: str = "llm") -> bool:
    counter_key, limit = _POST_POOLS[pool]
    if state.get(counter_key, 0) >= limit:
        log.info("Daily %s post limit (%d) reached – skipping", pool, limit)
        return False

    if DRY_RUN:
        log.info("[DRY RUN] (%d/%d %s)\n%s", state.get(counter_key, 0) + 1, limit, pool, text)
        state[counter_key] = state.get(counter_key, 0) + 1
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

            if "http://" in text or "https://" in text:
                # X fetches the link's Open Graph tags asynchronously to build the rich
                # preview card. A short fixed pause risks clicking post before that finishes,
                # which publishes with a bare link instead of the image/title card. Wait for
                # the card explicitly (best-effort selector — some links genuinely never get
                # a card if the destination site has no usable OG tags, so don't block forever).
                try:
                    page.locator("[data-testid='card.wrapper']").first.wait_for(timeout=8000)
                except Exception:
                    log.warning("Link preview card didn't render in time — posting with plain link.")
                time.sleep(random.uniform(0.5, 1.0))
            else:
                time.sleep(random.uniform(1.5, 3.0))

            post_btn = page.locator("[data-testid='tweetButtonInline']")
            post_btn.wait_for(timeout=10000)
            post_btn.dispatch_event("click")
            time.sleep(random.uniform(2.5, 4.0))

            context.storage_state(path=SESSION_FILE)
            browser.close()

        state[counter_key] = state.get(counter_key, 0) + 1
        log.info("Posted (%d/%d %s): %s", state[counter_key], limit, pool, text[:80])
        return True

    except Exception as e:
        log.error("Tweet post failed: %s", e)
        try:
            log.error("Page URL at failure: %s", page.url)
        except Exception:
            pass
        return False


def post_poll(question: str, options: list[str], state: dict, duration_hours: int = 24) -> bool:
    """Post a native X poll. Zero LLM cost — question/options are template-filled, not generated.
    NOTE: selectors below are X's standard poll-composer pattern as of this writing — this is the
    one Playwright flow in this file that hasn't been tested against a live session, so expect to
    verify/adjust the poll-specific selectors (createPollButton / Choice1.. / durationMinutes etc.)
    against the real compose UI on first run, same as any new UI automation."""
    if state.get("daily_keyword_posts", 0) >= DAILY_KEYWORD_POST_LIMIT:
        log.info("Daily keyword post limit (%d) reached – skipping poll", DAILY_KEYWORD_POST_LIMIT)
        return False
    if len(options) < 2 or len(options) > 4:
        log.error("Poll needs 2-4 options, got %d", len(options))
        return False

    if DRY_RUN:
        log.info("[DRY RUN] POLL (%d/%d keyword)\n%s\n%s", state.get("daily_keyword_posts", 0) + 1,
                  DAILY_KEYWORD_POST_LIMIT, question, options)
        state["daily_keyword_posts"] = state.get("daily_keyword_posts", 0) + 1
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
            for char in question:
                page.keyboard.type(char)
                time.sleep(random.uniform(0.03, 0.11))

            time.sleep(random.uniform(0.5, 1.0))
            page.locator("[data-testid='createPollButton']").first.click()
            time.sleep(random.uniform(1.0, 1.5))

            for i, option_text in enumerate(options):
                if i >= 2:
                    add_choice = page.locator("[data-testid='addChoice']")
                    if add_choice.count() > 0:
                        add_choice.click()
                        time.sleep(random.uniform(0.4, 0.7))
                choice_input = page.locator(f"[data-testid='Choice{i+1}']")
                choice_input.click()
                for char in option_text:
                    page.keyboard.type(char)
                    time.sleep(random.uniform(0.03, 0.08))
                time.sleep(random.uniform(0.3, 0.5))

            time.sleep(random.uniform(1.0, 2.0))

            post_btn = page.locator("[data-testid='tweetButtonInline']")
            post_btn.wait_for(timeout=10000)
            post_btn.dispatch_event("click")
            time.sleep(random.uniform(2.5, 4.0))

            context.storage_state(path=SESSION_FILE)
            browser.close()

        state["daily_keyword_posts"] = state.get("daily_keyword_posts", 0) + 1
        log.info("Posted poll (%d/%d keyword): %s %s", state["daily_keyword_posts"], DAILY_KEYWORD_POST_LIMIT,
                  question, options)
        return True

    except Exception as e:
        log.error("Poll post failed: %s", e)
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
        if _ticker_on_cooldown(state, symbol):
            log.info("PRICE EVENT suppressed — $%s posted within last %d min", symbol, TICKER_POST_COOLDOWN_MINUTES)
            continue
        pool = _consume_price_slot(state, symbol)
        if not pool:
            log.info("PRICE EVENT suppressed — $%s's storyline flexible budget exhausted today", symbol)
            continue

        gemini_used = not _gemini_unavailable
        if _gemini_unavailable:
            log.info("PRICE EVENT for $%s falling back to zero-LLM template — Gemini unavailable this cycle", symbol)
            tweet = generate_zero_llm_price_event(symbol, price_ctx, state)
        else:
            log.info("PRICE EVENT triggered for $%s: %s", symbol, trigger)
            slot  = {"type": "event", "format": "short", "angle": trigger}
            tweet = generate_tweet(symbol, slot, price_ctx, state, event_trigger=trigger)
        if tweet:
            log.info("Price event tweet (%d chars):\n%s", len(tweet), tweet)

        log_kwargs = dict(
            mechanism="price_event", storyline="eu" if symbol in EU_WATCHLIST else "us",
            generation_method="gemini" if gemini_used else "zero_llm_template",
            symbol=symbol, base_symbol=_base_symbol(symbol), exchange=_exchange_for(symbol),
            exchange_open_today="Y", market_phase="open",
            price=price_ctx.get("price"), change_pct=price_ctx.get("change_pct"),
            day_high=price_ctx.get("day_high", ""), day_low=price_ctx.get("day_low", ""),
            day_move_pct=day_pct, gemini_call_used="Y" if gemini_used else "N",
            gemini_calls_today_after=state.get("gemini_calls_today", 0),
        )
        if tweet and post_tweet(tweet, state):
            cooldowns[symbol] = now_minutes()
            _record_ticker_post(state, symbol)
            day_event_fired[symbol] = today()
            _log_event(state, **log_kwargs, pool_used="llm", posted="Y",
                       tweet_text=tweet, tweet_char_count=len(tweet))
        else:
            _refund_slot(state, symbol, pool)
            if tweet:
                _log_event(state, **log_kwargs, posted="N", skip_reason="post_failed",
                           tweet_text=tweet, tweet_char_count=len(tweet))
            else:
                _log_event(state, **log_kwargs, posted="N", skip_reason="generation_failed")
        break  # at most one price-event attempt per cycle, success or not

    save_state(state)
    return state


_DC_CREATOR = "{http://purl.org/dc/elements/1.1/}creator"
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _clean_summary(raw: str | None) -> str:
    """Strip HTML (Google News wraps its <description> in <a>/<font> tags) and collapse
    whitespace (Yahoo/Nasdaq's <description> often has leading/trailing newlines)."""
    if not raw:
        return ""
    import html
    text = html.unescape(_HTML_TAG_RE.sub(" ", raw))
    return re.sub(r"\s+", " ", text).strip()


# Outlets like Motley Fool routinely tack a generic templated clause onto an otherwise
# real, newsworthy headline ("...Expanded to $25 Billion. Here's What Investors Need to
# Know."). Unlike _is_generic_analysis_piece (which rejects headlines that are ENTIRELY a
# templated opinion piece), this strips just the trailing filler clause so the real news
# survives — a fixed, recognizable template, so a regex handles it without needing an LLM.
_CLICKBAIT_TAIL_RE = re.compile(
    r"[.:?!]\s*Here'?s\s+(what|why|how|my\s+take)\b.*$|"
    r"[.:?!]\s*What\s+(Investors|You)\s+(Need\s+to\s+Know|Should\s+Know)\b.*$",
    re.I,
)


def _strip_clickbait_tail(title: str) -> str:
    stripped = _CLICKBAIT_TAIL_RE.sub("", title).strip()
    return stripped if len(stripped) > 15 else title


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
            # Normalize to UTC BEFORE stripping tzinfo — different feeds state pubDate in
            # different zones (Yahoo/Google in UTC, some wires in US Eastern, etc.), and just
            # stripping tzinfo without converting first would leave their "naive" clock times
            # hours apart for the same real moment, silently corrupting any freshness or timing
            # comparison against other sources' timestamps (they're all compared as if UTC
            # elsewhere in this file, e.g. against datetime.utcnow()).
            parsed = parsedate_to_datetime(pub_date) if pub_date else None
            if parsed is not None and parsed.tzinfo is not None:
                pub_dt = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            else:
                pub_dt = parsed
        except Exception:
            pub_dt = None

        # Attribution: Google News <source>, Nasdaq <dc:creator>, else the link's own domain
        # (e.g. a Yahoo item that links straight to fool.com) — never "Yahoo"/"Google" themselves.
        source_el = item.find("source")
        creator = item.findtext(_DC_CREATOR)
        if source_el is not None and source_el.text:
            source = source_el.text.strip()
            # Google News appends " - PublisherName" directly onto the title. Now that
            # we've captured the publisher separately, strip it so the headline text
            # itself doesn't carry an inconsistent, redundant source mention.
            suffix = f" - {source}"
            if title.lower().endswith(suffix.lower()):
                title = title[: -len(suffix)].strip()
        elif creator:
            source = creator.strip()
        elif link:
            domain = urlparse(link).netloc.replace("www.", "")
            source = None if domain in ("finance.yahoo.com", "news.google.com") else domain
        else:
            source = None

        title = _strip_clickbait_tail(title)

        # Google News's <description> is just an HTML restatement of title+source — no real
        # extra content. Yahoo/Nasdaq's is a genuine 1-3 sentence excerpt, worth keeping.
        summary = "" if "news.google.com" in url else _clean_summary(item.findtext("description"))

        results.append({"headline": title, "link": link, "source": source, "summary": summary, "published": pub_dt})
    return results


_GOOGLE_NEWS_SIG_RE = re.compile(r'data-n-a-sg="([^"]+)"')
_GOOGLE_NEWS_TS_RE  = re.compile(r'data-n-a-ts="([^"]+)"')


def _resolve_google_news_url(link: str) -> str:
    """Google News RSS links are a redirect wrapper, not the real article — X's link-preview
    card fetch (and readers who click through) land on a Google interstitial rather than the
    actual publisher. Decodes it to the real destination via Google's internal batchexecute
    endpoint (the same call the News web UI itself makes to resolve the redirect).

    Unofficial/reverse-engineered — Google has changed this encoding before and could again.
    Any failure just falls back to the original link rather than blocking the post."""
    if not link or "news.google.com" not in link:
        return link
    try:
        from urllib.parse import urlparse
        article_id = urlparse(link).path.rsplit("/", 1)[-1]
        headers = {"User-Agent": "Mozilla/5.0"}
        # Bypasses the GDPR consent interstitial that'd otherwise replace the real page
        # (and its embedded signature/timestamp) for EU-geolocated requests.
        resp = requests.get(link, timeout=10, verify=False, headers=headers,
                             cookies={"SOCS": "CAISHAgBEhJnd3NfMjAyNDAxMDEtMF9SQzIaAmVuIAEaBgiA_LyuBg"})
        resp.raise_for_status()
        sig = _GOOGLE_NEWS_SIG_RE.search(resp.text)
        ts  = _GOOGLE_NEWS_TS_RE.search(resp.text)
        if not sig or not ts:
            return link

        inner = json.dumps(["garturlreq", [
            ["en-US", "US", ["FINANCE_TOP_INDICES", "GENESIS_PUBLISHER_SECTION", "WEB_TEST_1_0_0"],
             None, None, 1, 1, "US:en", None, 180, None, None, None, None, None, 0, None, None,
             [1608992183, 723341000]],
            "en-US", "US", 1, [2, 3, 4, 8], 1, 0, "655000234", 0, 0, None, 0],
            article_id, int(ts.group(1)), sig.group(1)])
        payload = {"f.req": json.dumps([[["Fbv4je", inner, None, "generic"]]])}
        resp2 = requests.post("https://news.google.com/_/DotsSplashUi/data/batchexecute",
                               headers={**headers, "content-type": "application/x-www-form-urlencoded;charset=UTF-8"},
                               data=payload, timeout=10, verify=False)
        resp2.raise_for_status()
        # Response body is `)]}'` (XSSI protection) followed by a JSON array; the real URL
        # sits inside a JSON-encoded string one level down, so it needs a second json.loads.
        body = resp2.text.split("\n", 1)[-1]
        outer = json.loads(body)
        inner_result = json.loads(outer[0][2])
        resolved = inner_result[1]
        return resolved if isinstance(resolved, str) and resolved.startswith("http") else link
    except Exception as e:
        log.warning("Google News URL resolve failed, keeping original link: %s", e)
        return link


_SAME_STORY_WORD_OVERLAP_THRESHOLD = 0.5


def _headline_similarity(a: str, b: str) -> float:
    """Rough same-story heuristic — word overlap ratio, ignoring case/punctuation/short filler
    words. Not real NLP, just enough to tell 'two wire distributions of the same story worded
    differently' apart from 'two unrelated headlines that happen to mention the same ticker'."""
    def words(s):
        return {w for w in re.findall(r"[a-z0-9]+", s.lower()) if len(w) > 3}
    wa, wb = words(a), words(b)
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


def _find_verifiable_alternate_link(headline: str, articles: list[dict], exclude_link: str) -> tuple[str, str] | None:
    """When the link we'd otherwise attach is in doubt (still an unresolved Google redirect, or
    fails the destination-page staleness check), look for another article fetched this same
    cycle that's plausibly the same story but from a source we CAN vouch for — rather than either
    posting an imprecise/unverifiable link or dropping a genuinely real story just because ONE
    wire distribution of it happened to be the unverifiable one."""
    for a in articles:
        link = a.get("link")
        if not link or link == exclude_link or "news.google.com" in link:
            continue
        if _headline_similarity(headline, a["headline"]) < _SAME_STORY_WORD_OVERLAP_THRESHOLD:
            continue
        if _is_confirmed_stale(link):
            continue
        return link, a.get("source") or ""
    return None


def _diversify_source(headline: str, source: str, link: str,
                      articles: list[dict], recent_sources: list[str]) -> tuple[str, str]:
    """Keep the feed from reading like a single-source reposter. If `source` was used in a recent
    post, look for the SAME story from a different outlet among the articles already fetched this
    cycle (the per-ticker Google/Yahoo/Nasdaq pull already carries multiple outlets' versions, so
    this needs no extra request) and prefer that outlet — as long as it's one we'd not just posted
    and isn't a low-quality mill. If no diverse alternate exists (story only ran on the one source),
    keep the original — variety is a preference, not a reason to drop or downgrade a real story."""
    if not source or not link:
        return link, source  # nothing attributable to diversify
    recent_lower = {s.lower() for s in recent_sources}
    if source.lower() not in recent_lower:
        return link, source  # this source isn't over-used right now — keep it
    for a in articles:
        alt_src, alt_link = a.get("source"), a.get("link")
        if not alt_src or not alt_link:
            continue
        if alt_src.lower() == source.lower() or alt_src.lower() in recent_lower:
            continue  # same source, or one we also just used — no diversity gained
        if any(b in alt_src.lower() for b in _OPINION_SOURCE_BLOCKLIST):
            continue  # never diversify INTO a known stock-pump mill
        if _headline_similarity(headline, a["headline"]) < _SAME_STORY_WORD_OVERLAP_THRESHOLD:
            continue  # not actually the same story
        log.info("Source diversity: %s -> %s for same story: %s", source, alt_src, headline[:60])
        return alt_link, alt_src
    return link, source


def _record_post_source(state: dict, source: str):
    """Remember the source just posted (rolling window) so _diversify_source can steer the next
    few posts toward other outlets. Shared across news + evergreen for whole-feed diversity."""
    if not source:
        return
    recent = state.setdefault("recent_post_sources", [])
    recent.append(source)
    del recent[:-RECENT_SOURCE_MEMORY]


_JSON_LD_DATE_RE = re.compile(r'"datePublished"\s*:\s*"([^"]+)"')
_OG_DATE_RE      = re.compile(r'<meta[^>]+property="article:published_time"[^>]+content="([^"]+)"')

# msn.com's article pages are a pure client-side-rendered shell — confirmed by directly fetching
# one: 40KB+ of HTML, zero datePublished/article:published_time/JSON-LD, nothing for the checks
# below to find. That's not a one-off gap, it's every MSN article, always — so "missing metadata"
# there can never mean "publisher just doesn't tag dates cleanly", it can only mean "unverifiable".
# Concretely confirmed to matter: a September 2024 Talen Energy buyback filing, re-surfaced via
# MSN with a fresh-looking pubDate, posted as if it were breaking news in July 2026. For domains
# on this list, "can't verify" flips to "reject" instead of the general lenient default below.
_UNVERIFIABLE_STALENESS_DOMAINS = {"msn.com", "www.msn.com"}


def _is_confirmed_stale(url: str) -> bool:
    """Cross-checks a resolved article's own publish-date metadata against age. Google News
    RSS occasionally reports a fresh-looking pubDate for content that's actually a day, weeks,
    or months old — apparently re-surfaced because it's topically relevant to something
    currently trending — which slips right past the RSS-pubDate freshness filter in
    check_news_events. Uses the SAME NEWS_FRESHNESS_HOURS window as that filter, since a looser
    threshold here would just make this a weaker backstop than the filter it's meant to back up.
    Only rejects when a real published date is found AND it's clearly stale; an unparseable or
    missing date defaults to 'assume fresh' so publishers without clean date metadata don't
    get needlessly filtered out — except for domains already proven to systematically hide it
    (see _UNVERIFIABLE_STALENESS_DOMAINS), where that same leniency is what let stale content
    through in the first place."""
    try:
        resp = requests.get(url, timeout=10, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        m = _JSON_LD_DATE_RE.search(resp.text) or _OG_DATE_RE.search(resp.text)
        if not m:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.replace("www.", "")
            if domain in _UNVERIFIABLE_STALENESS_DOMAINS:
                log.warning("No publish-date metadata from %s (known unverifiable domain) — "
                            "treating as stale rather than assuming fresh: %s", domain, url)
                return True
            return False
        published = _parse_datetime_value(m.group(1))
        if published is None:
            log.warning("Article freshness check found an unparseable publish date, assuming fresh: %s", m.group(1))
            return False
        if published.tzinfo is None:
            published = published.replace(tzinfo=datetime.timezone.utc)
        else:
            published = published.astimezone(datetime.timezone.utc)
        age_hours = (datetime.datetime.now(datetime.timezone.utc) - published).total_seconds() / 3600
        return age_hours > NEWS_FRESHNESS_HOURS
    except Exception as e:
        log.warning("Article freshness check failed, assuming fresh: %s", e)
        return False


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
    like a bot firing instantly — it reads as paced, human-like coverage instead.

    Classification routes to Gemini normally, but falls back to zero-LLM keyword
    matching for a ticker whenever its storyline's news budget is already spent, or
    Gemini itself is unavailable this cycle — so news coverage keeps going even when
    the AI-written path can't run, just with a coarser (but still specific) bar and
    a templated tweet instead of AI-written commentary."""
    cooldowns = state.setdefault("event_cooldowns", {})
    news_seen = state.setdefault("news_seen", {})
    queue     = state.setdefault("pending_news_posts", [])
    now_dt    = datetime.datetime.utcnow()
    queued_symbols = {item["symbol"] for item in queue}

    # Recorded the instant a matching headline is SEEN, not when it's posted — an institutional
    # disclosure that shows up but never gets classified as major (budget spent, didn't trip a
    # rule that day) still needs to be remembered, or a later re-issue of the exact same filing
    # would look like a first-ever sighting and get waved through as fresh news.
    holding_seen = state.setdefault("holding_disclosures_seen", {})
    _prune_date_keyed_dict(holding_seen, HOLDING_DISCLOSURE_MEMORY_DAYS)

    # ── Gather: fetch fresh headlines+summaries per ticker, no Gemini calls yet.
    # Always runs regardless of budget/Gemini state — routing happens next, after we
    # know what's actually available to classify. ──
    candidates: dict[str, list[dict]] = {}
    for symbol in symbols:
        if symbol in queued_symbols:
            continue  # already holding a news event for this ticker — don't pile on

        last_event_min = cooldowns.get(f"news_{symbol}", 0)
        if now_minutes() - last_event_min < NEWS_COOLDOWN_MINUTES:
            continue

        articles = get_ticker_context_with_dates(symbol, max_messages=10)

        recent = [
            a for a in articles
            if a["published"] and (now_dt - a["published"]).total_seconds() < NEWS_FRESHNESS_HOURS * 3600
        ]
        new_articles = [a for a in recent if a["headline"] not in news_seen.get(symbol, [])]
        news_seen[symbol] = [a["headline"] for a in articles]

        # Drop recycled institutional-holding disclosures (same filer + same share count as a
        # fingerprint we've seen before, on any prior day) before they ever reach classification.
        deduped = []
        for a in new_articles:
            fp = _holding_disclosure_fingerprint(symbol, a["headline"])
            if fp:
                if fp in holding_seen:
                    log.info("NEWS EVENT ignored for $%s — same filer + share count already seen "
                              "on %s, this looks like a source re-publish of an old filing: %s",
                              symbol, holding_seen[fp], a["headline"])
                    _log_event(state, mechanism="news_event", symbol=symbol, base_symbol=_base_symbol(symbol),
                               headline=a["headline"], headline_source=a.get("source") or "",
                               headline_link=a.get("link") or "",
                               headline_published_utc=_isoformat_or_empty(a.get("published")),
                               holding_disclosure_fingerprint=fp,
                               posted="N", skip_reason="holding_disclosure_recycled")
                    continue
                holding_seen[fp] = today()
            deduped.append(a)
        new_articles = deduped

        if new_articles:
            candidates[symbol] = new_articles

    # ── Route: Gemini when the storyline still has budget, Gemini's reachable this
    # cycle, AND it's within active hours (08:45-22:00 CET) — keyword fallback otherwise,
    # including automatically overnight regardless of budget/availability. A symbol whose
    # exchange is closed today (holiday) always routes to keyword too, regardless of budget —
    # there's no live-reaction angle for a closed market anyway (it's "Last close" framing
    # either way), so there's no reason for it to compete with the actively-trading side for
    # the same shared Gemini pool. This is what actually lets EU "blow through" the budget on a
    # US holiday: it's not that EU gets more calls reserved for it, it's that closed-exchange
    # news is removed from the competition for the shared pool entirely. ──
    use_gemini = _gemini_news_hours_active() and not _gemini_unavailable
    gemini_symbols  = [s for s in candidates
                       if use_gemini and _has_news_budget(state, s) and _ticker_exchange_open_today(s)]
    keyword_symbols = [s for s in candidates if s not in gemini_symbols]

    def _queue_item(symbol, classification, method):
        category = classification["category"]
        meta = next((a for a in candidates[symbol] if a["headline"] == classification["headline"]), {})
        # Single toggle gate for both classifier paths (keyword assigns this category directly;
        # Gemini can too via NEWS_CLASSIFIER_SYSTEM). Enforced here rather than inside each
        # classifier so there's exactly one place the on/off switch lives.
        if category == "large_share_purchases" and not ENABLE_LARGE_SHARE_PURCHASES:
            log.info("NEWS EVENT skipped [%s] for $%s — large_share_purchases muted via toggle: %s",
                     method, symbol, classification["headline"])
            _log_event(state, mechanism="news_event", generation_method=method,
                       symbol=symbol, base_symbol=_base_symbol(symbol),
                       headline=classification["headline"], headline_source=meta.get("source") or "",
                       headline_link=meta.get("link") or "", news_category=category,
                       posted="N", skip_reason="large_share_purchases_disabled")
            return
        if _news_category_posted_recently(state, symbol, category):
            log.info("NEWS EVENT skipped [%s] for $%s [%s] — already posted this category within "
                     "the last %dh (likely same story, different headline): %s",
                     method, symbol, category, NEWS_CATEGORY_DEDUP_MINUTES // 60, classification["headline"])
            _log_event(state, mechanism="news_event", storyline="", generation_method=method,
                       symbol=symbol, base_symbol=_base_symbol(symbol),
                       headline=classification["headline"], headline_source=meta.get("source") or "",
                       headline_link=meta.get("link") or "",
                       headline_published_utc=_isoformat_or_empty(meta.get("published")),
                       news_category=category,
                       posted="N", skip_reason="duplicate_category_24h")
            return
        # Only attach a link if we could identify a real, named, non-aggregator source —
        # never cite/link Yahoo's or Google's own domain as if it were "the source".
        source = meta.get("source")
        link = meta.get("link") if source else None
        log.info("NEWS EVENT queued [%s] for $%s [%s]: %s (source: %s)",
                 method, symbol, category, classification["headline"], source or "unknown")
        queue.append({
            "symbol": symbol,
            "classification": classification,
            "link": link,
            "source": source,
            "method": method,
            "published": meta.get("published"),
            "queued_min": _epoch_minutes(),
            # Snapshot of everything else fetched for this ticker this cycle, so if the primary
            # link turns out to be in doubt at release time, there's something to fall back to
            # besides just dropping the post or using a link we can't vouch for.
            "all_articles": candidates[symbol],
        })

    # ── Classify via Gemini: batch into groups of NEWS_CLASSIFY_BATCH_SIZE, so a busy
    # day costs a handful of calls instead of one call per ticker ──
    for i in range(0, len(gemini_symbols), NEWS_CLASSIFY_BATCH_SIZE):
        if _gemini_unavailable:
            log.warning("Gemini unavailable this cycle — skipping remaining news classification batches (%d tickers left)",
                        len(gemini_symbols) - i)
            break
        batch = gemini_symbols[i:i + NEWS_CLASSIFY_BATCH_SIZE]
        major = classify_news_batch({s: candidates[s] for s in batch}, state)
        if major is None:
            # The call itself failed (e.g. a 429 despite passing the pre-check) — these
            # tickers' headlines are already marked seen from gather, so without a same-cycle
            # fallback they'd be silently lost for the rest of the day. Fall back to keyword
            # classification for just this batch instead of dropping it.
            log.warning("Gemini batch failed for %s — falling back to keyword classification", batch)
            for symbol in batch:
                classification = classify_news_keyword(symbol, candidates[symbol])
                if classification:
                    _queue_item(symbol, classification, "keyword")
            continue
        for symbol, classification in major.items():
            _queue_item(symbol, classification, "gemini")

    # ── Classify via keyword fallback: pure local pattern matching, no API call ──
    for symbol in keyword_symbols:
        classification = classify_news_keyword(symbol, candidates[symbol])
        if classification:
            _queue_item(symbol, classification, "keyword")

    # ── Release: post at most one held event this cycle, respecting the global gap ──
    last_post_min = state.get("last_news_post_min", 0)
    required_gap  = NEWS_POST_MIN_GAP_MINUTES + random.randint(-NEWS_POST_GAP_JITTER_MINUTES, NEWS_POST_GAP_JITTER_MINUTES)
    if queue and _epoch_minutes() - last_post_min >= required_gap:
        # Source diversity: only one item is released per cycle, so which one matters. Try items
        # whose source WASN'T used in the last few posts first, then oldest-first — so a run of
        # different stories that all happen to come from one aggregator doesn't post back-to-back
        # from that same aggregator. Each item keeps its own real link, so there's no mismatch risk
        # (unlike link-swapping, which stays reserved for genuine same-story near-duplicates).
        recent_lower = {s.lower() for s in state.get("recent_post_sources", [])}
        queue.sort(key=lambda it: ((it.get("source") or "").lower() in recent_lower,
                                    it.get("queued_min", 0)))
        remaining = []
        released = False
        for item in queue:
            age = _epoch_minutes() - item["queued_min"]
            symbol, classification = item["symbol"], item["classification"]
            method = item.get("method", "gemini")

            if age > NEWS_QUEUE_MAX_AGE_MINUTES:
                log.warning("NEWS EVENT dropped (stale, held %d min) — $%s: %s",
                            age, symbol, classification["headline"])
                _log_event(state, mechanism="news_event", generation_method=method,
                           symbol=symbol, base_symbol=_base_symbol(symbol),
                           headline=classification["headline"], headline_source=item.get("source") or "",
                           headline_link=item.get("link") or "",
                           headline_published_utc=_isoformat_or_empty(item.get("published")),
                           news_category=classification["category"],
                           posted="N", skip_reason="queue_aged_out")
                continue

            # Gemini's outage only blocks Gemini-sourced items — keyword items don't need it.
            if released or (method == "gemini" and _gemini_unavailable):
                remaining.append(item)
                continue

            if _ticker_on_cooldown(state, symbol):
                remaining.append(item)  # ticker itself busy — try again next cycle
                continue

            # Only Gemini-classified posts draw from the flexible/buffer budget — keyword
            # fallback posts are, by design, only used once that budget is already spent.
            pool = None
            if method == "gemini":
                pool = _consume_news_slot(state, symbol)
                if not pool:
                    remaining.append(item)  # storyline's flexible+buffer budget exhausted for today
                    continue

            price_ctx = get_price_context(symbol)

            # Source diversity FIRST, on the raw link, so an over-used source gets swapped for the
            # same story from a fresher outlet before that link is resolved/staleness-checked below.
            raw_link, source = _diversify_source(
                classification["headline"], item.get("source"), item.get("link"),
                item.get("all_articles", []), state.get("recent_post_sources", []))
            link = _resolve_google_news_url(raw_link)

            # "In doubt" covers two different things: the link never resolved off Google's
            # redirect wrapper at all (imprecise — a bad preview card, not evidence of stale
            # content), or it resolved but failed the destination-page staleness check (evidence
            # the CONTENT itself is old, e.g. an MSN re-publish of a 2024 filing). Either way,
            # prefer a different wire distribution of the same story that we CAN vouch for over
            # posting an imprecise link or dropping a real story over one bad distribution of it.
            unresolved = bool(link) and "news.google.com" in link
            stale      = bool(link) and not unresolved and _is_confirmed_stale(link)

            if unresolved or stale:
                alt = _find_verifiable_alternate_link(classification["headline"],
                                                       item.get("all_articles", []), exclude_link=link)
                if alt:
                    log.info("NEWS EVENT link swapped for $%s (original was %s) — using verified "
                              "alternate source instead: %s",
                              symbol, "an unresolved Google redirect" if unresolved else "confirmed stale",
                              alt[1])
                    link, source = alt
                    stale = False
                elif stale:
                    log.warning("NEWS EVENT dropped (confirmed stale via destination page, "
                                "published >%dh ago, no verifiable alternate source found) — $%s: %s",
                                NEWS_FRESHNESS_HOURS, symbol, classification["headline"])
                    if pool:
                        _refund_slot(state, symbol, pool)
                    _log_event(state, mechanism="news_event", generation_method=method,
                               symbol=symbol, base_symbol=_base_symbol(symbol),
                               headline=classification["headline"], headline_source=source or "",
                               headline_link=link or "", news_category=classification["category"],
                               price=price_ctx.get("price") if price_ctx else "",
                               change_pct=price_ctx.get("change_pct") if price_ctx else "",
                               posted="N", skip_reason="confirmed_stale_source")
                    continue
                # else: still just an unresolved redirect, not confirmed stale, no alternate found
                # — fall through and use it as a last resort, same as before this feature existed.

            if method == "gemini":
                tweet = generate_news_event_tweet(symbol, classification, price_ctx, state,
                                                   link=link, source=source)
            else:
                tweet = generate_keyword_event_tweet(symbol, classification, price_ctx,
                                                      link=link)
            log_kwargs = dict(
                mechanism="news_event", generation_method=method,
                symbol=symbol, base_symbol=_base_symbol(symbol),
                headline=classification["headline"], headline_source=source or "",
                headline_link=link or "", news_category=classification["category"],
                price=price_ctx.get("price") if price_ctx else "",
                change_pct=price_ctx.get("change_pct") if price_ctx else "",
                gemini_call_used="Y" if method == "gemini" else "N",
                gemini_calls_today_after=state.get("gemini_calls_today", 0),
            )
            if tweet:
                log.info("News event tweet [%s] (%d chars):\n%s", method, len(tweet), tweet)
                if post_tweet(tweet, state, pool=("llm" if method == "gemini" else "keyword")):
                    cooldowns[f"news_{symbol}"] = now_minutes()
                    _record_ticker_post(state, symbol)
                    _record_news_category_posted(state, symbol, classification["category"])
                    _record_post_source(state, source)
                    state["last_news_post_min"] = _epoch_minutes()
                    released = True
                    _log_event(state, **log_kwargs, pool_used=("llm" if method == "gemini" else "keyword"),
                               posted="Y", tweet_text=tweet, tweet_char_count=len(tweet))
                    continue
                _log_event(state, **log_kwargs, posted="N", skip_reason="post_failed",
                           tweet_text=tweet, tweet_char_count=len(tweet))
            else:
                _log_event(state, **log_kwargs, posted="N", skip_reason="generation_failed")
            if pool:
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


def _news_category_posted_recently(state: dict, symbol: str, category: str) -> bool:
    """True if a news post about this exact (ticker, category) went out within the last
    NEWS_CATEGORY_DEDUP_MINUTES — catches the same underlying story resurfacing with different
    headline wording from a different outlet, which exact-headline-string tracking and the 2h
    ticker cooldown can both miss if it happens more than 2 hours after the first post.
    Rolling window, not a calendar-day match — a fixed 'today()' comparison would let a story
    posted at 23:58 dodge dedup entirely by reposting at 00:02 the next day."""
    last_min = state.get("news_category_posted", {}).get(symbol, {}).get(category)
    if last_min is None:
        return False
    return now_minutes() - last_min < NEWS_CATEGORY_DEDUP_MINUTES


def _record_news_category_posted(state: dict, symbol: str, category: str):
    state.setdefault("news_category_posted", {}).setdefault(symbol, {})[category] = now_minutes()


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

    # A holiday-induced 'weekend' phase on an actual trading weekday (not a genuine Sat/Sun) means
    # this storyline's whole watchlist has nothing live to report today. Skip the scheduled Gemini
    # call entirely rather than spend one on a reframed "quiet day" post — genuine news, if any,
    # is still covered independently by check_news_events (Gemini or keyword, unaffected by this).
    # GEMINI_DAILY_CALL_LIMIT is one shared pool across both storylines, not split per-storyline,
    # so this reallocates the freed call to whichever storyline IS actively trading automatically —
    # no explicit EU/US split to maintain.
    if phase == "weekend" and not is_weekend():
        log.info("%s slot skipped — exchange closed for a holiday, not a genuine weekend; "
                  "saving the Gemini call for the other storyline", key.upper())
        return state

    overlap = in_overlap_window()

    # Collect eligible tickers (not on 120-min cooldown, and — on a partial-holiday day — not on
    # a closed exchange, so a Xetra holiday reallocates coverage to open Euronext Paris/SIX/Madrid
    # names instead of just posting less. Skipped when phase is already 'weekend': that means
    # EVERY exchange in this storyline is closed today, and the weekend branch below ranks by
    # news/last-close instead of live movement, so excluding everything here would leave nothing.
    eligible = [t for t in watchlist if not _ticker_on_cooldown(state, t)]
    if phase != "weekend":
        eligible = [t for t in eligible if _ticker_exchange_open_today(t)]
    if not eligible:
        log.info("%s slot skipped — all tickers on cooldown or exchange closed", key.upper())
        return state

    # Fetch price for all eligible tickers, rank by absolute daily move
    ticker_data: dict = {}
    for t in eligible:
        ctx = get_price_context(t)
        if ctx:
            ticker_data[t] = ctx

    if not ticker_data:
        return state

    research_spotlight = None
    if phase == "weekend":
        # Stale Fri-close % moves are meaningless — rank by news coverage instead
        news_data = {t: get_ticker_context(t, max_messages=5) for t in ticker_data}
        ranked = sorted(ticker_data, key=lambda t: len(news_data.get(t, [])), reverse=True)
        # If no tickers have news, keep the original ranking
        ranked = [t for t in ranked if news_data.get(t)] or ranked

        # A ticker with one genuine research note but otherwise quiet week can lose to a ticker
        # with five recycled generic articles under pure headline-count ranking — look wider
        # (days, not the usual 24h) and bump the spotlighted ticker to the front if it's not
        # naturally on top, so the post doesn't miss it just because it wasn't already loud.
        research_spotlight = _find_research_spotlight(watchlist, state)
        if research_spotlight and research_spotlight["symbol"] in ticker_data:
            spot_t = research_spotlight["symbol"]
            ranked = [spot_t] + [t for t in ranked if t != spot_t]
            news_data.setdefault(spot_t, [])
            if research_spotlight["headline"] not in news_data[spot_t]:
                news_data[spot_t] = [research_spotlight["headline"]] + news_data[spot_t]
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

    tweet = generate_market_update_tweet(key, context_tickers, ticker_data, news_data, slot, phase, state,
                                          research_spotlight=research_spotlight)

    primary = context_tickers[0] if context_tickers else ""
    primary_ctx = ticker_data.get(primary, {})
    log_kwargs = dict(
        mechanism="scheduled_slot", storyline=key, slot_type=slot["type"], generation_method="gemini",
        symbol=primary, base_symbol=_base_symbol(primary) if primary else "",
        related_tickers=", ".join(f"${_base_symbol(t)}" for t in context_tickers[1:]),
        market_phase=phase, price=primary_ctx.get("price", ""), change_pct=primary_ctx.get("change_pct", ""),
        gemini_call_used="Y", gemini_calls_today_after=state.get("gemini_calls_today", 0),
    )

    if tweet:
        log.info("%s tweet (%d chars):\n%s", key.upper(), len(tweet), tweet)
        # FIXED: Mark slot as posted BEFORE attempting to post, ensure all context_tickers get cooldown
        state[f"{key}_posted"].append(slot["slot"])
        for t in context_tickers:
            _record_ticker_post(state, t)
        save_state(state)
        # Post the tweet (will update daily_posts counter)
        if not post_tweet(tweet, state):
            log.warning("Post failed; slot already marked so it won't regenerate — queuing the "
                        "already-written text to the backlog for a straight repost retry instead.")
            _push_to_backlog(state, tweet, "llm", **log_kwargs)
            _log_event(state, **log_kwargs, posted="N", skip_reason="post_failed_backlogged",
                       tweet_text=tweet, tweet_char_count=len(tweet))
            save_state(state)
        else:
            if research_spotlight:
                state["research_spotlights_used"][research_spotlight["fingerprint"]] = today()
            save_state(state)
            _log_event(state, **log_kwargs, pool_used="llm", posted="Y",
                       tweet_text=tweet, tweet_char_count=len(tweet))
    else:
        # Generation failed — slot was never marked posted, so it naturally retries
        # next cycle (as long as it's still within the slot's fire window).
        log.warning("%s tweet generation failed for slot [%s] — will retry next cycle if still in window.",
                    key.upper(), slot["type"])
        _log_event(state, **log_kwargs, posted="N", skip_reason="generation_failed")

    return state


def check_zero_llm_weekend_content(state: dict) -> dict:
    """Free weekend content — zero Gemini calls, so these run in ADDITION to the storyline
    budget rather than competing with it. One caption post (Saturday) + one poll (Sunday)."""
    if not is_weekend():
        return state

    today_str = today()
    zero_llm  = state.setdefault("zero_llm_posts", {})
    weekday   = datetime.date.today().weekday()
    now       = now_hhmm()
    tickers   = active_tickers_sorted()

    if weekday == 5 and "12:00" <= now <= "13:00" and zero_llm.get("caption") != today_str:
        text = generate_zero_llm_weekend_post(tickers)
        caption_kwargs = dict(mechanism="weekend_content", slot_type="caption",
                               generation_method="zero_llm_template",
                               related_tickers=", ".join(f"${t}" for t in tickers))
        if text:
            log.info("Zero-LLM weekend caption (%d chars):\n%s", len(text), text)
            if post_tweet(text, state, pool="keyword"):
                zero_llm["caption"] = today_str
                save_state(state)
                _log_event(state, **caption_kwargs, pool_used="keyword", posted="Y",
                           tweet_text=text, tweet_char_count=len(text))
            else:
                _log_event(state, **caption_kwargs, posted="N", skip_reason="post_failed",
                           tweet_text=text, tweet_char_count=len(text))
        else:
            _log_event(state, **caption_kwargs, posted="N", skip_reason="generation_failed")

    if weekday == 6 and "14:00" <= now <= "15:00" and zero_llm.get("poll") != today_str:
        result = generate_zero_llm_poll(tickers)
        poll_kwargs = dict(mechanism="poll", slot_type="poll", generation_method="poll_template",
                            related_tickers=", ".join(f"${t}" for t in tickers))
        if result:
            question, options = result
            log.info("Zero-LLM weekend poll: %s %s", question, options)
            poll_text = f"{question} | options: {', '.join(options)}"
            if post_poll(question, options, state):
                zero_llm["poll"] = today_str
                save_state(state)
                _log_event(state, **poll_kwargs, pool_used="keyword", posted="Y",
                           tweet_text=poll_text, tweet_char_count=len(question))
            else:
                _log_event(state, **poll_kwargs, posted="N", skip_reason="post_failed",
                           tweet_text=poll_text, tweet_char_count=len(question))
        else:
            _log_event(state, **poll_kwargs, posted="N", skip_reason="no_volatility_data")

    return state


# ── Evergreen opinion (thematic AI-infra think-pieces, weekend / slow-day filler) ─────────────
# Sourced by TOPIC (not ticker) — a sector essay like AEI's "AI Infrastructure Is a New Asset
# Class" won't show up in any single stock's feed. Query 1 below empirically surfaces credible
# analysis (AEI, McKinsey, Morgan Stanley, Fortune, MIT); a stock-tout "investment thesis" query
# was tested and rejected as too noisy. Quality is enforced by three filters, since a topic search
# inevitably drags in clickbait: the shared generic-analysis/clickbait patterns, a stock-tout
# noise regex, and a source blocklist of known stock-pumping mills.
_EVERGREEN_OPINION_QUERIES = [
    '"AI infrastructure" (opinion OR analysis OR "asset class" OR outlook OR policy)',
    '"data center" (power OR grid OR buildout) AI (policy OR opinion OR analysis)',
    # The "who pays for the buildout" / hidden-enablers angle: grid economics + policy. This is
    # where the stories that move the whole basket (ETN/NEE/POWL/GEV/PWR + the data-center REITs)
    # surface first — interconnection queues, PPAs, demand response, ratepayer/cost-shift fights.
    '("AI infrastructure" OR "data center") (FERC OR "Department of Energy" OR interconnection OR '
    '"power purchase" OR "demand response" OR transmission OR permitting OR moratorium OR '
    'regulator OR ratepayer)',
    # High-signal majors that are paywalled / have no clean RSS — reached via Google News site:
    # search (which indexes their headlines) rather than a direct feed. One OR-group = one fetch.
    '("data center" OR "AI infrastructure" OR "power grid") (site:reuters.com OR site:bloomberg.com '
    'OR site:cnbc.com OR site:politico.com OR site:axios.com)',
]
# Direct RSS feeds from credible AI-infra / grid trade press — fetched alongside the topic searches
# so their coverage reliably surfaces instead of depending on Google News to index it. All confirmed
# live with full pubDate coverage. The source label is forced (these link to their own domain, which
# would otherwise show as a bare hostname). DCD/DCK cover data centers; Power Magazine / Utility Dive
# / Canary Media cover the grid + utility-economics side the basket's power names hinge on.
_EVERGREEN_DIRECT_FEEDS = [
    ("https://www.datacenterdynamics.com/en/rss/", "DatacenterDynamics"),
    ("https://www.datacenterknowledge.com/rss.xml", "Data Center Knowledge"),
    ("https://www.powermag.com/feed/", "POWER Magazine"),
    ("https://www.utilitydive.com/feeds/news/", "Utility Dive"),
    ("https://www.canarymedia.com/feed", "Canary Media"),
]
_AI_INFRA_TOPIC_RE = re.compile(
    r"\b(AI|artificial intelligence|data cent(?:er|re)|infrastructure|compute|hyperscal|grid|"
    r"power|electricity|colocation|nuclear)\b", re.I)
_OPINION_NOISE_RE = re.compile(
    r"\b(top\s+\d+\s+stocks?|best\s+stocks?|stocks?\s+to\s+buy|which\s+of\s+these|buy\s+the\s+dip|"
    r"price\s+target|fair\s+value|\d+%\s+(?:upside|downside)|scorecard|earnings\s+call|"
    r"stock\s+(?:soars?|surges?|plunges?|jumps?|rises?|falls?|is\s+a\s+buy))\b", re.I)
# A headline about a SPECIFIC stock's price/estimate story ("Why Oracle (ORCL)... Estimate Story",
# "Can Caterpillar Stock Keep Climbing?") is a stock piece, not the sector-level "what it means"
# essay this feature is for. A ticker in parentheses / $-notation, or the words "stock"/"shares",
# are reliable tells — genuine theme/policy essays (AEI, McKinsey, Morgan Stanley sector notes)
# don't use them. NOT re.I: the ticker-paren must be uppercase to avoid matching "(the)" etc.
_STOCK_SPECIFIC_RE = re.compile(r"\([A-Z]{2,6}\)|\$[A-Z]{1,6}\b|\b[Ss]tocks?\b|\b[Ss]hares?\b")
_OPINION_SOURCE_BLOCKLIST = {s.lower() for s in [
    "The Motley Fool", "Seeking Alpha", "24/7 Wall St.", "Intellectia AI", "Moomoo", "Zacks",
    "Insider Monkey", "GuruFocus", "Simply Wall St", "Morningstar", "citybiz", "gritdaily.com",
    "foreignpolicyjournal.com", "eciks.org", "MarketBeat", "Benzinga",
]}
# Soft preference (not a hard filter): when multiple pieces qualify, lead with a genuinely credible
# think-tank / consultancy / major-journalism / academic source over a generic financial-news
# aggregator. Substring match against the RSS <source>.
_OPINION_SOURCE_PREFERRED = {s.lower() for s in [
    "AEI", "American Enterprise", "McKinsey", "Morgan Stanley", "Goldman", "Fortune", "WSJ",
    "Wall Street Journal", "MIT", "Brookings", "RAND", "Economist", "Financial Times", "Bloomberg",
    "Reuters", "Bain", "BCG", "Boston Consulting", "Deloitte", "PwC", "Gartner", "IDC", "Harvard",
    "Stanford", "Department of Energy", "CNBC", "Barron", "Axios", "The Atlantic", "Foreign Affairs",
    "Politico",
    # AI-infra / grid trade press + the regulatory bodies that shape the sector
    "DatacenterDynamics", "Data Center Knowledge", "Data Center Frontier", "FERC", "NERC",
    "POWER Magazine", "Utility Dive", "Canary Media",
]}
_NAV_JUNK_RE = re.compile(r"\b(subscribe|donate|newsletter|cookie|sign\s+up|log\s+in|browse|"
                          r"all\s+scholars|menu|advertisement)\b", re.I)
_EVERGREEN_MAX_PER_SOURCE = 3  # diversity cap: no single feed floods the candidate pool

# Neutral framings that read well for BOTH a thematic essay and a concrete sector development,
# since the broadened sourcing (trade press + gov/policy) surfaces news as well as opinion. Avoids
# "interesting take" as the default — that implies opinion and reads wrong on a hard-news item.
EVERGREEN_OPINION_TEMPLATES = [
    "Worth a read from {source}:\n\n\"{headline}\"\n\n{link}",
    "On the radar ({source}):\n\n\"{headline}\"\n\n{link}",
    "{source} on the AI infrastructure buildout:\n\n\"{headline}\"\n\n{link}",
    "Interesting read from {source}:\n\n\"{headline}\"\n\n{link}",
    "From {source}:\n\n\"{headline}\"\n\n{link}",
]

EVERGREEN_OPINION_SYSTEM = """## Role
You are a sharp financial X commentator sharing a thought-provoking piece on AI infrastructure —
the sector this account covers (power, data centers, networking, compute).

## Rules
- Summarize the core point in 1-2 tight sentences a busy trader would find worth their time — the
  central argument if it's analysis/opinion, or what happened and why it matters if it's a
  development. Base it ONLY on the provided article text — never invent claims, numbers, or
  conclusions not in it.
- This is a SECTOR-level piece, not a stock call. Do NOT attach a $ticker, a price, or a buy/sell view.
- Name the source. Frame it as an interesting perspective, not settled fact.
- No hype, no filler, no clickbait. Never use em dash — use en dash (–) only.
- Do NOT include a URL yourself — a link is appended after your text. Keep the text under 255 characters.

## Output
One post only. No surrounding quotes, no commentary."""


def _is_slow_day(state: dict) -> bool:
    """Past the midpoint of the combined EU+US trading day (>=15:30 CET) but not late evening, AND
    fewer than SLOW_DAY_SUBSTANTIVE_POST_THRESHOLD substantive (non-pulse) posts out so far — a
    genuinely quiet news day worth filling with evergreen content."""
    if not ("15:30" <= now_hhmm() <= "22:00"):
        return False
    substantive = state.get("daily_posts", 0) + state.get("daily_keyword_posts", 0)
    return substantive < SLOW_DAY_SUBSTANTIVE_POST_THRESHOLD


def _fetch_article_text(url: str, max_chars: int = 2500) -> str:
    """Best-effort readable body extraction: strip scripts/styles, pull <p> blocks that look like
    real prose (long enough, contain sentences, not nav junk). Returns '' on any failure — the
    caller falls back to the no-LLM template, so a fetch failure just means a simpler post."""
    try:
        import html as _html
        r = requests.get(url, timeout=12, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        body = re.sub(r"(?is)<script.*?</script>|<style.*?</style>", " ", r.text)
        out, total = [], 0
        for p in re.findall(r"(?is)<p[^>]*>(.*?)</p>", body):
            t = re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", " ", p))).strip()
            if len(t) >= 100 and ". " in t and not _NAV_JUNK_RE.search(t):
                out.append(t)
                total += len(t)
                if total > max_chars:
                    break
        return " ".join(out)[:max_chars]
    except Exception as e:
        log.warning("Article text fetch failed for %s: %s", url, e)
        return ""


def _find_evergreen_opinion(state: dict) -> dict | None:
    """Freshest credible AI-infra sector piece — from the topic searches AND the direct trade-press
    feeds — that's not been posted before and clears every quality filter. Returns the article dict
    (+ fingerprint) or None."""
    used = state.setdefault("evergreen_opinion_used", {})
    _prune_date_keyed_dict(used, EVERGREEN_OPINION_MEMORY_DAYS)

    from urllib.parse import quote
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=EVERGREEN_OPINION_LOOKBACK_DAYS)
    seen_titles, candidates, per_source = set(), [], {}

    # (fetch_url, source_override) — Google topic searches carry their own <source>; direct feeds
    # link to their own domain, so their clean label is forced instead.
    to_fetch = [
        (f"https://news.google.com/rss/search?q={quote(q)}+when:{EVERGREEN_OPINION_LOOKBACK_DAYS}d"
         f"&hl=en-US&gl=US&ceid=US:en", None)
        for q in _EVERGREEN_OPINION_QUERIES
    ] + list(_EVERGREEN_DIRECT_FEEDS)

    for url, source_override in to_fetch:
        try:
            items = _fetch_rss_with_dates(url, 25)
        except Exception as e:
            log.warning("Evergreen opinion fetch failed (%s): %s", url, e)
            continue
        for a in items:
            if source_override:
                a["source"] = source_override
            elif a.get("source"):
                # Google News sometimes tags a bare hostname ("Bloomberg.com"); drop the TLD so the
                # posted tweet reads "from Bloomberg", not "from Bloomberg.com".
                a["source"] = re.sub(r"\.(com|org|net|io)$", "", a["source"], flags=re.I).strip()
            h = a["headline"]
            if h in seen_titles:
                continue
            seen_titles.add(h)
            if not a.get("published") or a["published"] < cutoff:
                continue
            if not _AI_INFRA_TOPIC_RE.search(h):
                continue  # tangential result — not actually about AI infra
            if _is_generic_analysis_piece(h) or _OPINION_NOISE_RE.search(h) or _STOCK_SPECIFIC_RE.search(h):
                continue  # stock-tout / valuation clickbait / single-stock piece, not a sector piece
            if any(b in (a.get("source") or "").lower() for b in _OPINION_SOURCE_BLOCKLIST):
                continue  # known stock-pumping mill
            fp = re.sub(r"\s+", " ", h.strip().lower())
            if fp in used:
                continue
            # Cap contributions per source so a single high-volume feed (e.g. DatacenterDynamics
            # with a dozen fresh items) can't monopolize the ranking and bury thematic pieces from
            # lower-volume sources (a WSJ essay, an AEI paper). Keeps the mix diverse.
            src_key = (a.get("source") or "").lower()
            if per_source.get(src_key, 0) >= _EVERGREEN_MAX_PER_SOURCE:
                continue
            per_source[src_key] = per_source.get(src_key, 0) + 1
            candidates.append({**a, "fingerprint": fp})

    # Freshest first, then stably float credible sources above generic aggregators — so a
    # McKinsey/AEI/DCK piece from a few days ago beats a fresher-but-blander wire-aggregator item.
    candidates.sort(key=lambda a: a["published"], reverse=True)
    candidates.sort(key=lambda a: 0 if any(
        p in (a.get("source") or "").lower() for p in _OPINION_SOURCE_PREFERRED) else 1)
    return candidates[0] if candidates else None


def _evergreen_template_tweet(state: dict, item: dict) -> str:
    """No-LLM fallback: 'Interesting take from {source}: "{headline}"' + link, budget-aware."""
    source = item.get("source") or "an industry source"
    headline, link = item["headline"], item["link"]
    text = _draw_template(state, "evergreen", EVERGREEN_OPINION_TEMPLATES).format(
        source=source, headline=headline, link=link)
    if len(text) <= 280:
        return text
    # Too long — trim the headline (the only variable-length free part) to fit.
    fixed = len(text) - len(headline)
    room = 280 - fixed
    if room < 25:
        return f"Worth a read on AI infrastructure from {source}.\n\n{link}"[:280]
    trimmed = headline[:room].rsplit(" ", 1)[0] + "…"
    return text.replace(headline, trimmed)


def generate_evergreen_opinion_tweet(item: dict, article_text: str, state: dict) -> str | None:
    prompt = f"""Source: {item.get('source') or 'unknown'}
Headline: {item['headline']}

Article excerpt:
{article_text}

Write the post: summarize the core argument, name the source, make a trader want to read it.
Do NOT include a URL (it is appended separately)."""
    try:
        text = _gemini(EVERGREEN_OPINION_SYSTEM, prompt, state).strip('"').strip("'")
        budget = 280 - 25  # 23-char shortened link + 2 newlines
        if len(text) > budget:
            trimmed = text[:budget]
            for sep in (". ", ".\n", "? ", "! "):
                idx = trimmed.rfind(sep)
                if idx > 100:
                    text = trimmed[:idx + 1]
                    break
            else:
                text = trimmed.rsplit(" ", 1)[0]
        return f"{text}\n\n{item['link']}"
    except Exception as e:
        log.error("Evergreen opinion generation failed: %s", e)
        return None


def check_evergreen_opinion(state: dict) -> dict:
    """Weekend / slow-day filler: one thematic AI-infra think-piece, summarized by Gemini if budget
    remains, else a clean no-LLM 'interesting take' template. Bounded by a daily cap and a min-gap
    so it stays a garnish, not a firehose."""
    if not ENABLE_EVERGREEN_OPINION:
        return state
    now = now_hhmm()
    eligible = ("10:00" <= now <= "20:00") if is_weekend() else _is_slow_day(state)
    if not eligible:
        return state
    if state.get("evergreen_opinion_posts_today", 0) >= EVERGREEN_OPINION_DAILY_LIMIT:
        return state
    if _epoch_minutes() - state.get("last_evergreen_min", 0) < EVERGREEN_OPINION_MIN_GAP_MINUTES:
        return state

    item = _find_evergreen_opinion(state)
    if not item:
        return state
    item["link"] = _resolve_google_news_url(item.get("link"))

    log_kwargs = dict(mechanism="evergreen_opinion", headline=item["headline"],
                      headline_source=item.get("source") or "", headline_link=item.get("link") or "",
                      headline_published_utc=_isoformat_or_empty(item.get("published")))

    text, used_gemini = None, False
    if not _gemini_unavailable:
        article_text = _fetch_article_text(item["link"])
        if len(article_text) > 200:
            text = generate_evergreen_opinion_tweet(item, article_text, state)
            used_gemini = bool(text)
    if not text:
        text = _evergreen_template_tweet(state, item)
    if not text:
        return state

    pool = "llm" if used_gemini else "keyword"
    log.info("Evergreen opinion (%s, %d chars):\n%s", "gemini" if used_gemini else "template", len(text), text)
    if post_tweet(text, state, pool=pool):
        state["evergreen_opinion_posts_today"] = state.get("evergreen_opinion_posts_today", 0) + 1
        state["last_evergreen_min"] = _epoch_minutes()
        state.setdefault("evergreen_opinion_used", {})[item["fingerprint"]] = today()
        _record_post_source(state, item.get("source"))
        save_state(state)
        _log_event(state, **log_kwargs, generation_method=("gemini" if used_gemini else "template"),
                   pool_used=pool, posted="Y", tweet_text=text, tweet_char_count=len(text))
    else:
        _log_event(state, **log_kwargs, generation_method=("gemini" if used_gemini else "template"),
                   posted="N", skip_reason="post_failed", tweet_text=text, tweet_char_count=len(text))
    return state


def check_zero_llm_pulse(state: dict) -> dict:
    """Guaranteed cadence (a fresh random gap within PULSE_INTERVAL_MIN/MAX_MINUTES each time,
    not a fixed interval), independent of Gemini's budget and of whether any qualifying
    news/price event happened — a heartbeat, not a reaction. Covers the combined EU+US trading
    day (09:00-22:00 CET always has at least one market open, given EU runs 09:00-17:30 and US
    runs 15:30-22:00)."""
    if is_weekend():
        return state
    now = now_hhmm()
    if not ("09:00" <= now <= "22:00"):
        return state

    if "next_pulse_interval" not in state:
        state["next_pulse_interval"] = random.randint(PULSE_INTERVAL_MIN_MINUTES, PULSE_INTERVAL_MAX_MINUTES)

    last_pulse_min = state.get("last_pulse_post_min", 0)
    if _epoch_minutes() - last_pulse_min < state["next_pulse_interval"]:
        return state

    tickers = EU_WATCHLIST + US_WATCHLIST
    if not tickers:
        return state

    text = generate_zero_llm_pulse(tickers, state)
    log_kwargs = dict(mechanism="pulse", generation_method="zero_llm_template")
    if text:
        log.info("Zero-LLM pulse (%d chars):\n%s", len(text), text)
        if post_tweet(text, state, pool="pulse"):
            state["last_pulse_post_min"] = _epoch_minutes()
            state["next_pulse_interval"] = random.randint(PULSE_INTERVAL_MIN_MINUTES, PULSE_INTERVAL_MAX_MINUTES)
            save_state(state)
            _log_event(state, **log_kwargs, pool_used="pulse", posted="Y",
                       tweet_text=text, tweet_char_count=len(text))
        else:
            _log_event(state, **log_kwargs, posted="N", skip_reason="post_failed",
                       tweet_text=text, tweet_char_count=len(text))
    else:
        _log_event(state, **log_kwargs, posted="N", skip_reason="no_valid_mover_data")

    return state


def run_cycle(state: dict) -> dict:
    global _gemini_unavailable
    state = ensure_daily_plans(state)

    # _gemini_unavailable is a fresh per-process flag (correctly reset every cron run so a
    # transient outage doesn't persist past it) but gemini_calls_today persists in state.json
    # across cycles. Without this check, every cycle for the rest of the day would have to
    # rediscover an already-exhausted daily budget reactively — consuming a slot, fetching real
    # news context, THEN failing the Gemini call — instead of skipping Gemini-dependent work
    # up front. This makes that discovery proactive.
    if state.get("gemini_calls_today", 0) >= GEMINI_DAILY_CALL_LIMIT:
        _gemini_unavailable = True
        log.warning("Gemini daily call limit (%d) already reached — skipping all Gemini-dependent "
                    "work this cycle", GEMINI_DAILY_CALL_LIMIT)

    # Retry any already-generated post that failed to actually go out last cycle, before doing
    # anything else this cycle — it's already overdue, and doesn't cost a Gemini call either way.
    state = _flush_post_backlog(state)

    state = check_weekly_engagement(state)
    state = check_zero_llm_weekend_content(state)
    state = check_zero_llm_pulse(state)
    state = process_storyline(state, "eu")
    state = process_storyline(state, "us")

    all_tickers = list(set(EU_WATCHLIST + US_WATCHLIST))
    if all_tickers:
        if not is_weekend():
            state = check_price_events(state, all_tickers)
        state = check_news_events(state, all_tickers)

    # Last, so its "slow day" check reflects everything else this cycle already did — it only fires
    # as filler when the day genuinely stayed quiet (or on a weekend).
    state = check_evergreen_opinion(state)

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
