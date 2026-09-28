"""Crypto news aggregator -> Telegram channel (Russian, link-free).

Every run (GitHub Actions cron):
  * refreshes a pinned "market now" dashboard message;
  * posts price / milestone / volume / whale / risk-off alerts;
  * collects news from Russian and top-tier English crypto media, drops
    off-topic items and clickbait, groups the same story reported by several
    outlets, ranks the stories and posts the best ones in Russian. English
    items are machine-translated; if no translator answers, the item waits
    for the next run instead of going out in English;
  * posts a morning market report, an evening digest and a weekly review.

GitHub's cron fires irregularly (often hours apart), so scheduled posts use
time windows ("first run after 09:00 MSK") rather than an exact hour.

State lives in data/seen.json and is committed back by the workflow.
Set DRY_RUN=1 to print messages instead of sending them.
"""

import calendar
import hashlib
import html
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone

import feedparser
import requests

DRY_RUN = os.environ.get("DRY_RUN") == "1"
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "") if DRY_RUN else os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "") if DRY_RUN else os.environ["TELEGRAM_CHANNEL_ID"]

STATE_PATH = os.environ.get(
    "STATE_PATH", os.path.join(os.path.dirname(__file__), "data", "seen.json")
)
MSK = timezone(timedelta(hours=3))
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

SEEN_MAX_AGE_DAYS = 3
MAX_ENTRY_AGE_HOURS = 12
MAX_NEWS_PER_RUN = 8
MIN_STORY_SCORE = 2
SEND_DELAY_SECONDS = 1.5
EN_DUP_THRESHOLD = 0.5
RU_DUP_THRESHOLD = 0.4
SUMMARY_MAX_CHARS = 320

PRICE_ALERT_THRESHOLD_PCT = 3.0
PRICE_ALERT_MIN_INTERVAL_SECONDS = 30 * 60
MILESTONE_REPEAT_SECONDS = 12 * 60 * 60
VOLUME_SPIKE_PCT = 60.0
VOLUME_ALERT_MIN_INTERVAL_SECONDS = 60 * 60
WHALE_USD_THRESHOLD = 10_000_000
WHALE_SEEN_MAX_AGE_SECONDS = 6 * 60 * 60
USDT_CONTRACT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
USDT_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
# Free public nodes rate-limit GitHub's shared IPs unpredictably; try in turn.
ETH_RPC_URLS = [
    "https://ethereum-rpc.publicnode.com",
    "https://eth.drpc.org",
    "https://rpc.flashbots.net",
]
ETH_MAX_BLOCKS_PER_RUN = 40
RISK_OFF_THRESHOLD_PCT = -1.5
RISK_OFF_RECOVERY_PCT = -0.5

# Scheduled posts: (first MSK hour, last MSK hour) of the window.
MORNING_WINDOW_MSK = (9, 14)
EVENING_WINDOW_START_MSK = 21   # evening digest window runs 21:00-02:59 MSK
WEEKLY_START_MSK = 20           # Monday 20:00 MSK ... Tuesday 13:59 MSK

# Official schedules. Update once a year from:
#   FOMC  https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm
#   CPI   https://www.bls.gov/schedule/news_release/cpi.htm
#   ЦБ РФ https://www.cbr.ru/dkp/cal_mp/
FOMC_DECISION_DATES = [  # second (decision) day of each meeting
    "2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17", "2026-07-29",
    "2026-09-16", "2026-10-28", "2026-12-09",
    "2027-01-27", "2027-03-17", "2027-04-28", "2027-06-09", "2027-07-28",
    "2027-09-15", "2027-10-27", "2027-12-08",
]
US_CPI_DATES = [
    "2026-01-13", "2026-02-13", "2026-03-11", "2026-04-10", "2026-05-12",
    "2026-06-10", "2026-07-14", "2026-08-12", "2026-09-11", "2026-10-14",
    "2026-11-10", "2026-12-10",
]
CBR_RATE_DATES = [
    "2026-02-13", "2026-03-20", "2026-04-24", "2026-06-19", "2026-07-24",
    "2026-09-11", "2026-10-23", "2026-12-18",
]

# (name, url, language, tier) - tier 2 = major outlet / official, 1 = secondary
NEWS_FEEDS = [
    ("ForkLog", "https://forklog.com/feed", "ru", 2),
    ("Bits.media", "https://bits.media/rss2/", "ru", 2),
    ("Incrypted", "https://incrypted.com/feed/", "ru", 2),
    ("BeInCrypto RU", "https://ru.beincrypto.com/feed/", "ru", 1),
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss", "en", 2),
    ("The Block", "https://www.theblock.co/rss.xml", "en", 2),
    ("Cointelegraph", "https://cointelegraph.com/rss", "en", 2),
    ("Decrypt", "https://decrypt.co/feed", "en", 1),
    ("CryptoSlate", "https://cryptoslate.com/feed/", "en", 1),
    ("Federal Reserve", "https://www.federalreserve.gov/feeds/press_all.xml", "en", 2),
    ("SEC", "https://www.sec.gov/news/pressreleases.rss", "en", 2),
    ("CNBC", "https://www.cnbc.com/id/10000664/device/rss/rss.html", "en", 1),
    ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories", "en", 1),
]

BINANCE_CATALOGS = {"New Cryptocurrency Listing", "Delisting"}
# Even inside those catalogs Binance mixes in futures/perpetual-contract and
# tokenized-securities noise; only actual spot listing/delisting moves price.
BINANCE_RELEVANT_RE = re.compile(r"\bwill list\b|\bwill add\b|delist", re.I)


def kw_regex(words):
    """Whole-word keyword matcher: '*' matches any word ending ('ключев* ставк*'),
    're:...' is a raw regex."""
    parts = []
    for w in words:
        if w.startswith("re:"):
            parts.append(w[3:])
            continue
        body = r"\w*".join(re.escape(piece) for piece in w.split("*"))
        parts.append(body if w.endswith("*") else body + r"\b")
    return re.compile(r"\b(?:" + "|".join(parts) + ")", re.I)


RELEVANCE_RE = kw_regex([
    "bitcoin*", "btc", "ethereum", "ether", "eth", "crypto*", "blockchain*",
    "stablecoin*", "tether", "usdt", "usdc", "coinbase", "binance", "kraken",
    "okx", "bybit", "microstrategy", "saylor", "defi", "nft*", "digital asset*",
    "token*", "solana", "xrp", "ripple", "dogecoin", "cardano", "toncoin",
    "altcoin*", "memecoin*", "meme coin*", "halving", "satoshi", "cbdc", "web3",
    "airdrop*", "fomc", "federal funds", "rate cut*", "rate hike*", "cpi",
    "consumer price index",
    "биткоин*", "биткойн*", "крипт*", "блокчейн*", "стейблкоин*", "токен*",
    "эфириум*", "эфир", "эфира", "эфиров", "альткоин*", "мемкоин*", "майнинг*",
    "майнер*", "цифров* рубл*", "цифров* валют*", "цифров* актив*", "цфа",
    "ключев* ставк*", "фрс",
])

JUNK_RE = kw_regex([
    "price prediction*", "price analysis", "price forecast", "could hit",
    "could reach", "presale", "pre-sale", "sponsored", "press release",
    "what happened in crypto today", "hodler’s digest", "hodler's digest",
    "should you buy", "best crypto", "to buy now", "podcast", "newsletter",
    "daybook", "re:top \\d+ (?:crypto|coins|altcoins)", "live updates", "live blog",
    "what is", "how to", "explained", "что такое", "гайд", "руководство",
    r"re:(?:^|:\s)как\b",
    "прогноз цены", "прогноз курса", "реклама", "партнерский материал",
    "партнёрский материал", "на правах рекламы", "дайджест", "подкаст",
    "розыгрыш", "итоги недели", "технический анализ",
])

BREAKING_RE = kw_regex([
    "halts withdrawals", "suspends withdrawals", "pauses withdrawals",
    "freezes withdrawals", "bankrupt*", "insolven*", "all-time high",
    "record high", "crash", "crashes", "plunge*", "collapse*", "rate cut",
    "cuts rates", "cuts interest rates", "rate hike", "raises rates",
    "raises interest rates", "emergency", "depeg*",
    r"re:approv\w*.{0,40}\betf\b", r"re:etf.{0,40}\bapprov\w*",
    "банкрот*", "исторический максимум", "исторического максимума", "обвал*",
    "рухнул*", "обрушил*", "снизил ставку", "снизила ставку", "повысил ставку",
    "повысила ставку", "снизил ключевую", "повысил ключевую",
    "приостановил* вывод*", "заморозил* вывод*", "потерял* привязку",
    r"re:одобрил\w*.{0,40}\betf\b",
])
# Hacks and thefts happen daily; only large ones are "breaking".
SECURITY_RE = kw_regex([
    "hack", "hacked", "hacker*", "exploit", "exploited", "drained", "stolen",
    "взлом*", "хакер*", "эксплойт*", "похитил*", "похищен*", "украл*", "украдено",
])
SECURITY_BREAKING_USD = 50_000_000
AMOUNT_RE = re.compile(
    r"\$\s?(\d+(?:[.,]\d+)?)\s?(трлн|млрд|млн|тыс|trillion|billion|million|bn|b|m|k)\b"
    r"|(\d+(?:[.,]\d+)?)\s?(трлн|млрд|млн|миллиард\w*|миллион\w*|billion|million)\s?(?:долл\w*|\$|usd\w*)",
    re.I,
)
AMOUNT_UNITS = [
    ("трлн", 1e12), ("trillion", 1e12), ("млрд", 1e9), ("миллиард", 1e9),
    ("billion", 1e9), ("bn", 1e9), ("млн", 1e6), ("миллион", 1e6),
    ("million", 1e6), ("тыс", 1e3), ("b", 1e9), ("m", 1e6), ("k", 1e3),
]

BULLISH_RE = kw_regex([
    "all-time high", "record high", "surge*", "rally", "rallies", "soar*",
    "jump*", "approv*", "adoption", "partnership", "integrat*", "bullish",
    "inflow*", "accumulat*", "breakthrough", "outperform*", "buyback*",
    "green light", "skyrocket*", "buys", "bought",
    "рост*", "вырос*", "растет", "растёт", "подорожал*", "исторический максимум",
    "одобрил*", "приток*", "купил*", "докупил*", "скупил*", "нарастил*",
    "партнерств*", "бычий", "бычьи",
])

BEARISH_RE = kw_regex([
    "hack*", "exploit*", "breach*", "stolen", "drained", "halts", "suspends",
    "bankrupt*", "reject*", "lawsuit*", "sues", "sued", "plunge*", "crash*",
    "ban", "bans", "banned", "delist*", "outflow*", "sell-off", "selloff",
    "dump*", "liquidat*", "collapse*", "default", "bearish", "downgrade*",
    "fraud*", "scam*", "fined", "penalt*", "investigat*", "probe", "recession",
    "взлом*", "хакер*", "эксплойт*", "похит*", "украл*", "банкрот*", "обвал*",
    "рухнул*", "упал*", "падени*", "подешевел*", "отток*", "ликвидац*",
    "запрет*", "делистинг*", "иск", "иски", "обвинени*", "штраф*", "мошенни*",
    "расследовани*", "арест*", "медвежий", "медвежьи",
])

KEY_ASSET_RE = kw_regex([
    "bitcoin", "btc", "ethereum", "eth", "etf", "sec", "fed", "federal reserve",
    "fomc", "tether", "usdt", "toncoin", "биткоин*", "биткойн*", "эфириум*",
    "фрс", "toncoin",
])
RUSSIA_RE = kw_regex([
    "russia*", "росси*", "рф", "банк россии", "цб", "минфин*", "госдум*",
    "рубл*",
])

TAG_RULES = [
    (kw_regex(["bitcoin*", "btc", "биткоин*", "биткойн*"]), "#BTC"),
    (kw_regex(["ethereum", "ether", "eth", "эфириум*", "эфир", "эфира", "эфиров"]), "#ETH"),
    (kw_regex(["toncoin", "ton"]), "#TON"),
    (kw_regex(["tether", "usdt"]), "#USDT"),
    (kw_regex(["solana", "солана*", "соланы"]), "#SOL"),
    (kw_regex(["xrp", "ripple"]), "#XRP"),
    (kw_regex(["dogecoin", "doge"]), "#DOGE"),
    (kw_regex(["etf"]), "#ETF"),
    (kw_regex(["sec"]), "#SEC"),
    (kw_regex(["federal reserve", "fomc", "fed", "фрс"]), "#FED"),
    (kw_regex(["binance"]), "#Binance"),
    (kw_regex(["coinbase"]), "#COIN"),
    (kw_regex(["microstrategy", "saylor"]), "#MSTR"),
    (kw_regex(["stablecoin*", "стейблкоин*"]), "#стейблкоины"),
    (RUSSIA_RE, "#Россия"),
]

COMMON_LATIN = {
    "btc", "eth", "usdt", "usdc", "bitcoin", "ethereum", "crypto", "sec", "etf",
    "etfs", "defi", "nft", "ceo", "usd", "dao", "dex", "cex", "api", "fed", "fomc",
    "cpi", "ton", "sol", "xrp", "bnb", "bsc", "the", "and", "web3", "layer", "chain",
}
EN_STOPWORDS = {
    "the", "a", "an", "to", "of", "in", "on", "for", "and", "is", "as", "at",
    "by", "with", "after", "amid", "its", "it", "this", "that", "are", "be",
    "from", "new", "says", "could", "will", "has", "have", "into", "up",
    "down", "over", "out", "what", "why", "how",
}
RU_STOPWORDS = {
    "и", "в", "во", "на", "с", "со", "по", "для", "что", "как", "из", "за",
    "от", "до", "не", "это", "его", "ее", "её", "их", "о", "об", "к", "у", "а",
    "но", "или", "же", "ли", "бы", "при", "после", "над", "под", "между",
    "через", "уже", "еще", "ещё", "также", "может", "могут", "году", "года",
}

# Google renders a few crypto terms differently from Russian crypto media.
TRANSLATION_FIXES = [
    ("иткойн", "иткоин"),
    ("стабильных монет", "стейблкоинов"),
    ("стабильные монеты", "стейблкоины"),
    ("стабильная монета", "стейблкоин"),
    ("стабильной монеты", "стейблкоина"),
    ("мем-монет", "мемкоин"),
]

STABLE_OR_WRAPPED = {
    "usdt", "usdc", "dai", "usde", "fdusd", "pyusd", "usds", "usd1", "usdtb",
    "tusd", "usdd", "susds", "susde", "bsc-usd", "buidl", "usdf", "rlusd",
    "wbtc", "weth", "steth", "wsteth", "weeth", "cbbtc", "reth", "wbeth",
    "lbtc", "solvbtc", "jitosol", "bnsol", "msol", "xaut", "paxg",
}

MONTHS_GEN = [
    "января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа",
    "сентября", "октября", "ноября", "декабря",
]

HTML_TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")
BOILERPLATE_RE = re.compile(
    r"(?:The post .{0,300}? appeared first on .*$)|(?:Read more.*$)|"
    r"(?:Continue reading.*$)|(?:Запись .{0,300}? впервые появилась .*$)|"
    r"(?:Сообщение .{0,300}? появились? сначала на .*$)",
    re.I,
)
CYRILLIC_RE = re.compile(r"[а-яё]", re.I)
LATIN_RE = re.compile(r"[a-z]", re.I)


# ---------------------------------------------------------------- utilities

def now_msk():
    return datetime.now(MSK)


def clean_text(raw):
    if not raw:
        return ""
    text = html.unescape(raw)
    text = HTML_TAG_RE.sub(" ", text)
    return WHITESPACE_RE.sub(" ", text).strip()


def escape_html(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def fmt_num(value, decimals=0):
    s = f"{value:,.{decimals}f}"
    return s.replace(",", "\u00a0").replace(".", ",").replace("-", "−")


def fmt_pct(value, decimals=1):
    if round(value, decimals) == 0:
        value = 0.0
    s = f"{value:+.{decimals}f}" if value else f"{0:.{decimals}f}"
    return s.replace(".", ",").replace("-", "−") + "%"


def fmt_price(value, currency="$"):
    decimals = 0 if value >= 100 else 2 if value >= 1 else 4
    num = fmt_num(value, decimals)
    return f"${num}" if currency == "$" else f"{num}\u00a0{currency}"


def fmt_big_usd(value, signed=False):
    sign = ("+" if value >= 0 else "−") if signed else ("−" if value < 0 else "")
    v = abs(value)
    for div, suffix in ((1e12, "трлн"), (1e9, "млрд"), (1e6, "млн")):
        if v >= div:
            scaled = v / div
            num = fmt_num(scaled, 0 if scaled >= 100 else 1 if scaled >= 10 else 2)
            if "," in num:
                num = num.rstrip("0").rstrip(",")
            return f"{sign}${num}\u00a0{suffix}"
    return f"{sign}${fmt_num(v)}"


def fmt_date_ru(d):
    return f"{d.day}\u00a0{MONTHS_GEN[d.month - 1]}"


def plural_ru(n, one, few, many):
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def http_get_json(url, params=None, retries=1, timeout=15):
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=timeout)
            if resp.status_code == 429 and attempt < retries:
                time.sleep(6)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt >= retries:
                print(f"GET {url.split('?')[0]} failed: {e}", file=sys.stderr)
    return None


def load_state():
    if not os.path.exists(STATE_PATH):
        return {"bootstrapped": False}
    with open(STATE_PATH, "r") as f:
        return json.load(f)


def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=1, ensure_ascii=False)


def migrate_state(state):
    # v1 kept every seen item (with English title tokens) in "items" and
    # breaking headlines in "breaking_today".
    if "items" in state:
        state["seen"] = [{"hash": it["hash"], "ts": it["ts"]} for it in state["items"]]
        state["posted"] = [
            {"en": it.get("tokens", []), "ru": [], "title": "", "score": 0,
             "breaking": False, "ts": it["ts"]}
            for it in state["items"]
        ]
        del state["items"]
        state["last_evening_date"] = (now_msk() - timedelta(hours=3)).date().isoformat()
    for key in ("breaking_today", "last_fng_date", "last_digest_date", "milestone_prices",
                "last_weekly_date", "last_poll_week"):
        state.pop(key, None)
    state.setdefault("seen", [])
    state.setdefault("posted", [])


def prune_state(state):
    cutoff = time.time() - SEEN_MAX_AGE_DAYS * 86400
    state["seen"] = [it for it in state["seen"] if it["ts"] >= cutoff]
    state["posted"] = [it for it in state["posted"] if it["ts"] >= cutoff]
    whale_cutoff = time.time() - WHALE_SEEN_MAX_AGE_SECONDS
    state["whale_seen"] = [w for w in state.get("whale_seen", []) if w["ts"] >= whale_cutoff]


def entry_hash(link, title):
    return hashlib.sha256(f"{link}|{title}".encode("utf-8")).hexdigest()


def en_tokens(text):
    words = re.findall(r"[a-z0-9]+", text.lower())
    return {w for w in words if w not in EN_STOPWORDS and len(w) > 2}


def ru_stems(text):
    """Crude language-agnostic stems for Russian text (and Latin names inside it)."""
    stems = set()
    for w in re.findall(r"[a-zа-яё0-9]+", text.lower().replace("ё", "е")):
        if w in RU_STOPWORDS or w in EN_STOPWORDS:
            continue
        if w.isdigit():
            stems.add(w)
        elif len(w) >= 3:
            stems.add(w[:5] if CYRILLIC_RE.match(w) else w)
    return stems


def big_numbers(text):
    """3+ digit numbers except years - two reports of one event share them."""
    return {n for n in re.findall(r"\d+", text) if len(n) >= 3 and not 1990 <= int(n) <= 2035}


def max_usd_amount(text):
    best = 0.0
    for m in AMOUNT_RE.finditer(text):
        num, unit = (m.group(1), m.group(2)) if m.group(1) else (m.group(3), m.group(4))
        mult = next((v for k, v in AMOUNT_UNITS if unit.lower().startswith(k)), 1)
        best = max(best, float(num.replace(",", ".")) * mult)
    return best


def latin_names(text):
    """Latin words inside Russian text are mostly project/company names."""
    return {w for w in re.findall(r"[a-z][a-z0-9]{2,}", text.lower()) if w not in COMMON_LATIN}


def jaccard(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def cyrillic_share(text):
    cyr = len(CYRILLIC_RE.findall(text))
    lat = len(LATIN_RE.findall(text))
    return cyr / (cyr + lat) if cyr + lat else 1.0


def mostly_cyrillic(text):
    return cyrillic_share(text) >= 0.5


def short_summary(text, title, lang):
    text = BOILERPLATE_RE.sub("", text).strip()
    text = re.sub(r"\s*\[?(?:…|\.\.\.)\]?\s*$", "", text).strip()
    if not text:
        return ""
    title_tokens = en_tokens(title) if lang == "en" else ru_stems(title)
    body_tokens = en_tokens(text) if lang == "en" else ru_stems(text)
    if jaccard(title_tokens, body_tokens) > 0.7:
        return ""  # summary just repeats the headline

    out = ""
    for sentence in re.split(r"(?<=[.!?…])\s+", text):
        if lang == "ru" and not mostly_cyrillic(sentence):
            break  # embedded English tweet/quote
        if not out:
            if len(sentence) > SUMMARY_MAX_CHARS:
                cut = sentence[:SUMMARY_MAX_CHARS].rsplit(" ", 1)[0]
                return cut.rstrip(",;:—-") + "…"
            out = sentence
        elif len(out) + 1 + len(sentence) <= SUMMARY_MAX_CHARS:
            out += " " + sentence
        else:
            break
    return out


# -------------------------------------------------------------- translation

def _translate_google(texts):
    resp = requests.post(
        "https://clients5.google.com/translate_a/t",
        params={"client": "dict-chrome-ex", "sl": "en", "tl": "ru"},
        data=[("q", t) for t in texts],
        headers={"User-Agent": UA},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    out = [item[0] if isinstance(item, list) else item for item in data]
    if len(out) != len(texts):
        raise RuntimeError(f"expected {len(texts)} translations, got {len(out)}")
    return out


def _translate_mymemory(text):
    resp = requests.get(
        "https://api.mymemory.translated.net/get",
        params={"q": text[:500], "langpair": "en|ru"},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    result = (data.get("responseData") or {}).get("translatedText") or ""
    if data.get("responseStatus") != 200 or "MYMEMORY WARNING" in result.upper():
        raise RuntimeError(result or data.get("responseDetails"))
    return html.unescape(result)


def polish_translation(text):
    for wrong, right in TRANSLATION_FIXES:
        text = text.replace(wrong, right).replace(wrong.capitalize(), right.capitalize())
    return text.strip()


def translate_batch(texts):
    """English -> Russian. Returns a list aligned with texts; None where every
    translator failed (callers must never fall back to the English original)."""
    results = [None] * len(texts)
    todo = [i for i, t in enumerate(texts) if t]
    for i, t in enumerate(texts):
        if not t:
            results[i] = ""

    chunk, chunk_len = [], 0
    chunks = []
    for i in todo:
        if chunk and (len(chunk) >= 10 or chunk_len + len(texts[i]) > 4000):
            chunks.append(chunk)
            chunk, chunk_len = [], 0
        chunk.append(i)
        chunk_len += len(texts[i])
    if chunk:
        chunks.append(chunk)

    for chunk in chunks:
        for attempt in range(2):
            try:
                translated = _translate_google([texts[i] for i in chunk])
                for i, tr in zip(chunk, translated):
                    results[i] = tr
                break
            except Exception as e:
                print(f"Google translate failed (attempt {attempt + 1}): {e}", file=sys.stderr)
                time.sleep(3)
        time.sleep(0.5)

    for i in todo:
        if results[i] is None:
            try:
                results[i] = _translate_mymemory(texts[i])
            except Exception as e:
                print(f"MyMemory translate failed: {e}", file=sys.stderr)
            time.sleep(0.5)

    for i in todo:
        tr = results[i]
        if tr is not None and cyrillic_share(tr) < 0.3:
            results[i] = None  # translator echoed English back
        elif tr is not None:
            results[i] = polish_translation(tr)
    return results


# ----------------------------------------------------------------- telegram

LAST_TELEGRAM_ERROR = ""


def telegram_call(method, payload):
    """Returns the parsed Telegram response on success, None on failure
    (the error text is kept in LAST_TELEGRAM_ERROR)."""
    global LAST_TELEGRAM_ERROR
    if DRY_RUN:
        shown = payload.get("text") or payload.get("question") or ""
        print(f"\n--- [{method}] ---\n{shown}")
        return {"ok": True, "result": {"message_id": 0}}

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    for attempt in range(2):
        try:
            resp = requests.post(url, json=payload, timeout=15)
        except Exception as e:
            LAST_TELEGRAM_ERROR = str(e)
            print(f"Telegram {method} failed: {e}", file=sys.stderr)
            return None
        try:
            data = resp.json()
        except ValueError:
            data = {}
        if resp.status_code == 429 and attempt == 0:
            time.sleep(data.get("parameters", {}).get("retry_after", 5) + 1)
            continue
        if resp.ok and data.get("ok"):
            return data
        desc = data.get("description", resp.text[:200])
        if "message is not modified" in desc:
            return {"ok": True, "result": {}}
        LAST_TELEGRAM_ERROR = desc
        print(f"Telegram {method} failed: {resp.status_code} {desc}", file=sys.stderr)
        return None
    return None


def send_text(text):
    return telegram_call("sendMessage", {
        "chat_id": CHAT_ID, "text": text, "parse_mode": "HTML",
        "disable_web_page_preview": True,
    })


# ------------------------------------------------------------- market data

PRICE_COINS = {"btc": "bitcoin", "eth": "ethereum", "ton": "the-open-network"}
PRICE_ICONS = (("btc", "Ⓑ"), ("eth", "Ⓔ"), ("ton", "Ⓝ"))


def trend_emoji(change_pct):
    if change_pct is None:
        return ""
    if change_pct >= 1:
        return "📈"
    if change_pct <= -1:
        return "📉"
    return "➡️"


def get_prices():
    data = http_get_json(
        "https://api.coingecko.com/api/v3/simple/price",
        params={
            "ids": ",".join(PRICE_COINS.values()),
            "vs_currencies": "usd,rub", "include_24hr_change": "true",
        },
    )
    try:
        return {
            sym: {
                "usd": data[coin_id]["usd"],
                "rub": data[coin_id]["rub"],
                "change_24h": data[coin_id]["usd_24h_change"],
            }
            for sym, coin_id in PRICE_COINS.items()
        }
    except Exception as e:
        print(f"Price data incomplete: {e}", file=sys.stderr)
        return None


def get_market_data():
    return http_get_json(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": "usd", "order": "market_cap_desc", "per_page": 100,
            "page": 1, "price_change_percentage": "24h,7d",
        },
    )


def get_global():
    data = http_get_json("https://api.coingecko.com/api/v3/global")
    try:
        d = data["data"]
        return {
            "mcap": d["total_market_cap"]["usd"],
            "mcap_change": d["market_cap_change_percentage_24h_usd"],
            "volume": d["total_volume"]["usd"],
            "btc_dom": d["market_cap_percentage"]["btc"],
            "eth_dom": d["market_cap_percentage"]["eth"],
        }
    except Exception:
        return None


FNG_LABELS = {
    "Extreme Fear": ("Крайний страх", "🥶"),
    "Fear": ("Страх", "😟"),
    "Neutral": ("Нейтрально", "😐"),
    "Greed": ("Жадность", "😏"),
    "Extreme Greed": ("Крайняя жадность", "🤑"),
}


def get_fear_greed():
    data = http_get_json("https://api.alternative.me/fng/", params={"limit": 8})
    try:
        items = data["data"]
        label, emoji = FNG_LABELS.get(items[0]["value_classification"], (items[0]["value_classification"], ""))
        return {
            "value": int(items[0]["value"]), "label": label, "emoji": emoji,
            "yesterday": int(items[1]["value"]) if len(items) > 1 else None,
            "week_ago": int(items[7]["value"]) if len(items) > 7 else None,
        }
    except Exception:
        return None


def get_btc_network():
    fees = http_get_json("https://mempool.space/api/v1/fees/recommended")
    hashrate = http_get_json("https://mempool.space/api/v1/mining/hashrate/1w")
    adj = http_get_json("https://mempool.space/api/v1/difficulty-adjustment")
    out = {}
    if fees:
        out["fee"] = fees.get("halfHourFee")
    if hashrate and hashrate.get("currentHashrate"):
        out["hashrate_eh"] = hashrate["currentHashrate"] / 1e18
    if adj:
        out["retarget_days"] = adj.get("remainingTime", 0) / 86_400_000
        out["retarget_pct"] = adj.get("difficultyChange")
    return out or None


def get_cbr_usd():
    data = http_get_json("https://www.cbr-xml-daily.ru/daily_json.js")
    try:
        usd = data["Valute"]["USD"]
        return {"value": usd["Value"], "prev": usd["Previous"]}
    except Exception:
        return None


def get_stablecoins():
    data = http_get_json("https://stablecoins.llama.fi/stablecoincharts/all", timeout=30)
    try:
        now = sum(data[-1]["totalCirculatingUSD"].values())
        week = sum(data[-8]["totalCirculatingUSD"].values())
        return {"total": now, "week_change": now - week}
    except Exception:
        return None


def get_defi_tvl():
    data = http_get_json("https://api.llama.fi/v2/historicalChainTvl", timeout=30)
    try:
        now, week = data[-1]["tvl"], data[-8]["tvl"]
        return {"tvl": now, "week_pct": (now - week) / week * 100}
    except Exception:
        return None


def get_okx_derivatives(inst_id):
    funding = http_get_json("https://www.okx.com/api/v5/public/funding-rate", params={"instId": inst_id})
    oi = http_get_json(
        "https://www.okx.com/api/v5/public/open-interest",
        params={"instType": "SWAP", "instId": inst_id},
    )
    try:
        f = funding["data"][0]
        hours = (int(f["nextFundingTime"]) - int(f["fundingTime"])) / 3_600_000
        return {
            "funding_pct": float(f["fundingRate"]) * 100,
            "interval_h": round(hours) or 8,
            "oi_usd": float(oi["data"][0]["oiUsd"]) if oi else None,
        }
    except Exception:
        return None


def eth_rpc(method, params):
    last_error = None
    for url in list(ETH_RPC_URLS):
        try:
            resp = requests.post(
                url, json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1}, timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise RuntimeError(data["error"])
            if url != ETH_RPC_URLS[0]:  # stick with the node that answered
                ETH_RPC_URLS.remove(url)
                ETH_RPC_URLS.insert(0, url)
            return data["result"]
        except Exception as e:
            last_error = e
    raise RuntimeError(f"all ETH RPC nodes failed, last: {last_error}")


def get_eth_gas_gwei():
    try:
        return int(eth_rpc("eth_gasPrice", []), 16) / 1e9
    except Exception as e:
        print(f"ETH gas fetch failed: {e}", file=sys.stderr)
        return None


def get_yahoo_change(symbol):
    data = http_get_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        params={"interval": "1d", "range": "1d"},
    )
    try:
        meta = data["chart"]["result"][0]["meta"]
        price = meta["regularMarketPrice"]
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        return {"price": price, "pct": (price - prev) / prev * 100}
    except Exception:
        return None


def get_index_data():
    indices = {}
    for name, symbol in (("S&P 500", "^GSPC"), ("Nasdaq", "^IXIC")):
        q = get_yahoo_change(symbol)
        if q:
            indices[name] = q["pct"]
    return indices


# ------------------------------------------------------------- info blocks

def format_price_line(prices):
    if not prices:
        return ""
    lines = []
    for sym, icon in PRICE_ICONS:
        c = prices[sym]
        lines.append(
            f"{icon} {sym.upper()}: {fmt_price(c['usd'])} / {fmt_price(c['rub'], '₽')} "
            f"({fmt_pct(c['change_24h'])} 24ч) {trend_emoji(c['change_24h'])}"
        )
    return "\n".join(lines)


def funding_hint(pct):
    if pct >= 0.03:
        return "лонги перегреты — риск резкого отката"
    if pct >= 0.01:
        return "покупатели с плечом преобладают"
    if pct > -0.01:
        return "баланс, перекоса нет"
    return "преобладают шорты — возможен резкий вынос вверх"


def fng_hint(value):
    if value >= 75:
        return "рынок в эйфории, выше риск коррекции"
    if value <= 25:
        return "паника — исторически время для аккуратных покупок"
    return ""


def next_event(dates, today):
    for d in dates:
        day = date.fromisoformat(d)
        if day >= today:
            return day
    return None


def when_ru(day, today):
    delta = (day - today).days
    if delta == 0:
        return "сегодня"
    if delta == 1:
        return "завтра"
    return f"через {delta} {plural_ru(delta, 'день', 'дня', 'дней')}"


def calendar_lines(today):
    lines = []
    for name, dates in (
        ("Решение ФРС по ставке", FOMC_DECISION_DATES),
        ("Инфляция в США (CPI)", US_CPI_DATES),
        ("Ключевая ставка ЦБ РФ", CBR_RATE_DATES),
    ):
        day = next_event(dates, today)
        if day and (day - today).days <= 45:
            lines.append(f"{name}: {fmt_date_ru(day)} ({when_ru(day, today)})")
    return lines


def format_ticker_text(prices, global_data, fng, btc_net, cbr):
    lines = ["📌 <b>Рынок сейчас</b>", "", format_price_line(prices), ""]
    if global_data:
        lines.append(
            f"💰 Капитализация рынка: {fmt_big_usd(global_data['mcap'])} "
            f"({fmt_pct(global_data['mcap_change'])} 24ч)"
        )
        lines.append(f"👑 Доля биткоина: {fmt_num(global_data['btc_dom'], 1)}%")
    if fng:
        lines.append(f"{fng['emoji']} Страх и жадность: {fng['value']}/100 — {fng['label']}")
    if btc_net and btc_net.get("fee") is not None:
        lines.append(f"⛽️ Комиссия в сети BTC: {btc_net['fee']} сат/vB")
    if cbr:
        lines.append(f"💵 Курс ЦБ: {fmt_num(cbr['value'], 2)} ₽ за $1")
    lines.append("")
    lines.append(f"Обновлено: {now_msk().strftime('%d.%m %H:%M')} МСК")
    return "\n".join(lines)


def update_pinned_ticker(state, prices, global_data, fng, btc_net, cbr):
    if not prices:
        return  # keep the last good numbers instead of an error placeholder
    text = format_ticker_text(prices, global_data, fng, btc_net, cbr)
    msg_id = state.get("pinned_ticker_message_id")
    if msg_id:
        if telegram_call("editMessageText", {
            "chat_id": CHAT_ID, "message_id": msg_id, "text": text, "parse_mode": "HTML",
        }):
            return
        if not re.search(r"not found|can't be edited", LAST_TELEGRAM_ERROR):
            return  # transient error: don't spawn a second pinned message
    # message deleted or no longer editable -> send and pin a new one
    resp = send_text(text)
    if resp:
        new_id = resp["result"]["message_id"]
        state["pinned_ticker_message_id"] = new_id
        telegram_call("pinChatMessage", {
            "chat_id": CHAT_ID, "message_id": new_id, "disable_notification": True,
        })


def build_morning_report(market_data, global_data, fng, btc_net, cbr):
    today = now_msk().date()
    lines = [f"☀️ <b>Сводка рынка · {fmt_date_ru(today)}</b>", ""]
    sources = []

    by_id = {c["id"]: c for c in (market_data or [])}
    price_lines = []
    for sym, icon in PRICE_ICONS:
        c = by_id.get(PRICE_COINS[sym])
        if not c:
            continue
        d24 = c.get("price_change_percentage_24h_in_currency")
        d7 = c.get("price_change_percentage_7d_in_currency")
        parts = [f"{icon} {sym.upper()} {fmt_price(c['current_price'])}"]
        if d24 is not None:
            parts.append(f"24ч {fmt_pct(d24)}")
        if d7 is not None:
            parts.append(f"7д {fmt_pct(d7)}")
        price_lines.append(" · ".join(parts))
    if price_lines:
        lines += ["<b>Цены</b>", *price_lines, ""]
        sources.append("CoinGecko")

    if global_data:
        lines += [
            "<b>Рынок в целом</b>",
            f"Капитализация: {fmt_big_usd(global_data['mcap'])} ({fmt_pct(global_data['mcap_change'])} за сутки)",
            f"Объём торгов за сутки: {fmt_big_usd(global_data['volume'])}",
            f"Доля BTC: {fmt_num(global_data['btc_dom'], 1)}% · ETH: {fmt_num(global_data['eth_dom'], 1)}%"
            " (рост доли BTC = деньги уходят из альткоинов в биткоин)",
            "",
        ]
        if "CoinGecko" not in sources:
            sources.append("CoinGecko")

    if fng:
        cmp = []
        if fng["yesterday"] is not None:
            cmp.append(f"вчера {fng['yesterday']}")
        if fng["week_ago"] is not None:
            cmp.append(f"неделю назад {fng['week_ago']}")
        line = f"{fng['emoji']} Страх и жадность: <b>{fng['value']}/100</b> — {fng['label']}"
        if cmp:
            line += f" ({', '.join(cmp)})"
        lines += ["<b>Настроение</b>", line]
        hint = fng_hint(fng["value"])
        if hint:
            lines.append(f"↳ {hint}")
        lines.append("")
        sources.append("alternative.me")

    stables, tvl = get_stablecoins(), get_defi_tvl()
    if stables or tvl:
        lines.append("<b>Деньги в крипте</b>")
        if stables:
            direction = "приток новых денег" if stables["week_change"] >= 0 else "деньги выходят с рынка"
            lines.append(
                f"Стейблкоины: {fmt_big_usd(stables['total'])} "
                f"({fmt_big_usd(stables['week_change'], signed=True)} за неделю — {direction})"
            )
        if tvl:
            lines.append(
                f"Заблокировано в DeFi: {fmt_big_usd(tvl['tvl'])} ({fmt_pct(tvl['week_pct'])} за неделю)"
            )
        lines.append("")
        sources.append("DefiLlama")

    deriv = get_okx_derivatives("BTC-USDT-SWAP")
    if deriv:
        lines += [
            "<b>Фьючерсы на BTC (OKX)</b>",
            f"Ставка финансирования: {fmt_pct(deriv['funding_pct'], 4)} за {deriv['interval_h']} ч"
            f" — {funding_hint(deriv['funding_pct'])}",
        ]
        if deriv["oi_usd"]:
            lines.append(f"Открытые позиции: {fmt_big_usd(deriv['oi_usd'])}")
        lines.append("")
        sources.append("OKX")

    net_lines = []
    if btc_net:
        if btc_net.get("fee") is not None:
            btc_usd = by_id.get("bitcoin", {}).get("current_price")
            fee_usd = f" (~${fmt_num(btc_net['fee'] * 140 * btc_usd / 1e8, 2)} за обычный перевод)" if btc_usd else ""
            net_lines.append(f"Комиссия BTC: {btc_net['fee']} сат/vB{fee_usd}")
        if btc_net.get("hashrate_eh"):
            net_lines.append(f"Хешрейт BTC: {fmt_num(btc_net['hashrate_eh'])} EH/s (мощность майнеров)")
        if btc_net.get("retarget_pct") is not None and btc_net.get("retarget_days") is not None:
            net_lines.append(
                f"Пересчёт сложности через ~{fmt_num(btc_net['retarget_days'], 1)} дн.: "
                f"{fmt_pct(btc_net['retarget_pct'])}"
            )
        sources.append("mempool.space")
    gas = get_eth_gas_gwei()
    if gas is not None:
        net_lines.append(f"Газ в Ethereum: {fmt_num(gas, 2)} gwei")
    if net_lines:
        lines += ["<b>Блокчейны</b>", *net_lines, ""]

    indices = [
        f"{name}: {fmt_pct(q['pct'])}"
        for name, q in (("S&P 500", get_yahoo_change("^GSPC")), ("Nasdaq", get_yahoo_change("^IXIC")))
        if q
    ]
    dxy = get_yahoo_change("DX-Y.NYB")
    if indices or dxy or cbr:
        lines.append("<b>Фондовый рынок и валюты</b>")
        if indices:
            lines.append(" · ".join(indices) + " за последнюю сессию")
        if dxy:
            lines.append(
                f"Индекс доллара DXY: {fmt_num(dxy['price'], 1)} ({fmt_pct(dxy['pct'])})"
                " — сильный доллар обычно давит на крипту"
            )
        if indices or dxy:
            sources.append("Yahoo Finance")
        if cbr:
            diff = cbr["value"] - cbr["prev"]
            sign = "+" if diff >= 0 else "−"
            lines.append(f"Курс ЦБ РФ: {fmt_num(cbr['value'], 2)} ₽ за $1 ({sign}{fmt_num(abs(diff), 2)})")
            sources.append("ЦБ РФ")
        lines.append("")

    events = calendar_lines(today)
    if events:
        lines += ["<b>Календарь</b>", *events, ""]

    lines.append(f"<i>Данные: {', '.join(dict.fromkeys(sources))}</i>")
    return "\n".join(lines)


def in_window(hour, start, end):
    return start <= hour <= end


def maybe_send_morning_report(state, market_data, global_data, fng, btc_net, cbr):
    now = now_msk()
    today = now.date().isoformat()
    if state.get("last_morning_date") == today or not in_window(now.hour, *MORNING_WINDOW_MSK):
        return
    if not market_data and not global_data:
        return  # try again on the next run inside the window
    if send_text(build_morning_report(market_data, global_data, fng, btc_net, cbr)):
        state["last_morning_date"] = today


def maybe_send_evening_digest(state, prices):
    now = now_msk()
    shifted = now - timedelta(hours=3)  # 21:00-02:59 MSK belongs to one "evening"
    day_key = shifted.date().isoformat()
    if shifted.hour < EVENING_WINDOW_START_MSK - 3 or state.get("last_evening_date") == day_key:
        return

    cutoff = time.time() - 24 * 3600
    stories = [p for p in state["posted"] if p["ts"] >= cutoff and p.get("title")]
    state["last_evening_date"] = day_key
    if not stories:
        return
    stories.sort(key=lambda p: (p.get("score", 0), p["ts"]), reverse=True)

    lines = [f"🗞 <b>Главное за день · {fmt_date_ru(shifted.date())}</b>", ""]
    for p in stories[:8]:
        mark = "🚨" if p.get("breaking") else "•"
        lines.append(f"{mark} {escape_html(p['title'])}")
    if prices:
        lines += ["", format_price_line(prices)]
    send_text("\n".join(lines))


def maybe_send_weekly(state, market_data, prices):
    now = now_msk()
    monday_evening = now.weekday() == 0 and now.hour >= WEEKLY_START_MSK
    tuesday_morning = now.weekday() == 1 and now.hour < 14
    if not (monday_evening or tuesday_morning):
        return
    week_key = now.strftime("%G-W%V")
    if state.get("last_weekly_week") == week_key:
        return
    state["last_weekly_week"] = week_key

    if market_data:
        coins = [
            c for c in market_data[:60]
            if c["symbol"].lower() not in STABLE_OR_WRAPPED
            and c.get("price_change_percentage_7d_in_currency") is not None
        ]
        coins.sort(key=lambda c: c["price_change_percentage_7d_in_currency"], reverse=True)
        lines = ["📅 <b>Итоги недели</b>", ""]
        if prices:
            lines += [format_price_line(prices), ""]
        lines.append("🟢 Лидеры роста за 7 дней (топ-60 монет):")
        for c in coins[:3]:
            lines.append(f"  {c['symbol'].upper()}: {fmt_pct(c['price_change_percentage_7d_in_currency'])}")
        lines.append("")
        lines.append("🔴 Лидеры падения:")
        for c in reversed(coins[-3:]):
            lines.append(f"  {c['symbol'].upper()}: {fmt_pct(c['price_change_percentage_7d_in_currency'])}")
        send_text("\n".join(lines))

    telegram_call("sendPoll", {
        "chat_id": CHAT_ID,
        "question": "Куда пойдёт BTC на этой неделе?",
        "options": ["🚀 Вверх", "🔻 Вниз", "➡️ Без изменений"],
        "is_anonymous": True,
    })


# ------------------------------------------------------------------ alerts

def check_price_alert(state, prices):
    if not prices:
        return
    baseline = state.get("price_alert_baseline")
    now = time.time()
    if not baseline:
        state["price_alert_baseline"] = {"btc": prices["btc"]["usd"], "eth": prices["eth"]["usd"], "ts": now}
        return
    if now - baseline["ts"] < PRICE_ALERT_MIN_INTERVAL_SECONDS:
        return

    minutes = int((now - baseline["ts"]) / 60)
    period = f"~{minutes} мин" if minutes < 90 else f"~{fmt_num(minutes / 60, 1)} ч"
    alerts = []
    for sym, key in (("BTC", "btc"), ("ETH", "eth")):
        old, new = baseline[key], prices[key]["usd"]
        if old <= 0:
            continue
        pct = (new - old) / old * 100
        if abs(pct) >= PRICE_ALERT_THRESHOLD_PCT:
            arrow = "🚀" if pct > 0 else "🔻"
            alerts.append(f"{arrow} {sym}: {fmt_pct(pct)} за {period} ({fmt_price(old)} → {fmt_price(new)})")
    if alerts:
        send_text("⚡ <b>Резкое движение цены</b>\n\n" + "\n".join(alerts))

    state["price_alert_baseline"] = {"btc": prices["btc"]["usd"], "eth": prices["eth"]["usd"], "ts": now}


MILESTONE_STEP = {"btc": 10_000, "eth": 100, "ton": 1}
MILESTONE_NAME = {"btc": "Bitcoin", "eth": "Ethereum", "ton": "TON"}


def check_price_milestones(state, prices):
    if not prices:
        return
    last = state.setdefault("milestone_last", {})
    alerted = state.setdefault("milestone_alerted", {})
    now = time.time()

    for sym, step in MILESTONE_STEP.items():
        price = prices[sym]["usd"]
        prev = last.get(sym)
        last[sym] = price
        if prev is None or int(prev // step) == int(price // step):
            continue
        up = price > prev
        boundary = max(int(prev // step), int(price // step)) * step
        key = f"{sym}:{boundary}"
        # Price hovering around a round number would otherwise alert every run.
        if now - alerted.get(key, 0) < MILESTONE_REPEAT_SECONDS:
            continue
        alerted[key] = now
        arrow, verb = ("🚀", "пробивает") if up else ("⚠️", "падает ниже")
        send_text(f"{arrow} <b>{MILESTONE_NAME[sym]} {verb} {fmt_price(boundary)}</b>")

    for key in [k for k, ts in alerted.items() if now - ts > 2 * MILESTONE_REPEAT_SECONDS]:
        del alerted[key]


def check_volume_spike(state, market_data):
    if not market_data:
        return
    by_id = {c["id"]: c for c in market_data}
    btc, eth = by_id.get("bitcoin"), by_id.get("ethereum")
    if not btc or not eth:
        return

    now = time.time()
    baseline = state.get("volume_baseline")
    if not baseline:
        state["volume_baseline"] = {"btc": btc["total_volume"], "eth": eth["total_volume"], "ts": now}
        return
    if now - baseline["ts"] < VOLUME_ALERT_MIN_INTERVAL_SECONDS:
        return

    hours = fmt_num((now - baseline["ts"]) / 3600, 1)
    alerts = []
    for sym, key, cur in (("BTC", "btc", btc["total_volume"]), ("ETH", "eth", eth["total_volume"])):
        old = baseline.get(key, 0)
        if old > 0:
            pct = (cur - old) / old * 100
            if pct >= VOLUME_SPIKE_PCT:
                alerts.append(f"📊 {sym}: суточный объём торгов {fmt_pct(pct, 0)} за ~{hours} ч")
    if alerts:
        send_text("⚠️ <b>Аномальный объём торгов</b>\n\n" + "\n".join(alerts))

    state["volume_baseline"] = {"btc": btc["total_volume"], "eth": eth["total_volume"], "ts": now}


def check_risk_off(state, indices):
    if not indices:
        return
    worst = min(indices.values())
    active = state.get("risk_off_active", False)
    if worst <= RISK_OFF_THRESHOLD_PCT and not active:
        lines = ["⚠️ <b>Распродажа на фондовом рынке</b>", "", "Падение индексов США часто тянет крипту вниз:"]
        lines += [f"{name}: {fmt_pct(pct)}" for name, pct in indices.items()]
        send_text("\n".join(lines))
        state["risk_off_active"] = True
    elif worst > RISK_OFF_RECOVERY_PCT and active:
        state["risk_off_active"] = False


def _whale_seen(state):
    return {w["hash"] for w in state.get("whale_seen", [])}


def _whale_mark(state, h):
    state.setdefault("whale_seen", []).append({"hash": h, "ts": time.time()})


def collect_btc_whales(state, prices):
    btc_usd = prices["btc"]["usd"] if prices else None
    if not btc_usd:
        return []
    data = http_get_json("https://blockchain.info/unconfirmed-transactions", params={"format": "json"})
    if not data:
        return []
    seen, lines = _whale_seen(state), []
    for tx in data.get("txs", []):
        h = tx.get("hash")
        if not h or h in seen:
            continue
        # Largest single output, not the sum: the sum counts the change
        # returned to the sender and inflates the amount.
        biggest = max((o.get("value", 0) for o in tx.get("out", [])), default=0) / 1e8
        if biggest * btc_usd >= WHALE_USD_THRESHOLD:
            _whale_mark(state, h)
            lines.append(f"Ⓑ {fmt_num(biggest, 1)} BTC (~{fmt_big_usd(biggest * btc_usd)})")
    return lines


def collect_eth_usdt_whales(state, prices):
    try:
        latest = int(eth_rpc("eth_blockNumber", []), 16)
    except Exception as e:
        print(f"ETH RPC fetch failed: {e}", file=sys.stderr)
        return []
    last_block = state.get("last_eth_block")
    state["last_eth_block"] = latest
    if last_block is None:
        return []
    from_block = max(last_block + 1, latest - ETH_MAX_BLOCKS_PER_RUN + 1)
    if from_block > latest:
        return []

    seen, lines = _whale_seen(state), []
    eth_usd = prices["eth"]["usd"] if prices else None
    if eth_usd:
        failures = 0
        for block_num in range(from_block, latest + 1):
            try:
                block = eth_rpc("eth_getBlockByNumber", [hex(block_num), True])
                failures = 0
            except Exception as e:
                failures += 1
                if failures >= 3:
                    print(f"ETH block scan stopped at {block_num}: {e}", file=sys.stderr)
                    break
                continue
            for tx in (block or {}).get("transactions", []):
                h = tx.get("hash")
                if not h or h in seen or not tx.get("to"):
                    continue
                value_eth = int(tx["value"], 16) / 1e18
                if value_eth * eth_usd >= WHALE_USD_THRESHOLD:
                    _whale_mark(state, h)
                    lines.append(f"Ⓔ {fmt_num(value_eth)} ETH (~{fmt_big_usd(value_eth * eth_usd)})")

    try:
        logs = eth_rpc("eth_getLogs", [{
            "fromBlock": hex(from_block), "toBlock": hex(latest),
            "address": USDT_CONTRACT, "topics": [USDT_TRANSFER_TOPIC],
        }])
        for log in logs:
            key = f"{log.get('transactionHash')}:{log.get('logIndex', '0')}"
            if key in seen:
                continue
            amount = int(log["data"], 16) / 1e6
            if amount >= WHALE_USD_THRESHOLD:
                _whale_mark(state, key)
                lines.append(f"💵 {fmt_big_usd(amount).lstrip('$')} USDT")
    except Exception as e:
        print(f"USDT logs fetch failed: {e}", file=sys.stderr)
    return lines


def send_whale_alerts(state, prices):
    lines = collect_btc_whales(state, prices) + collect_eth_usdt_whales(state, prices)
    if lines:
        send_text(
            f"🐳 <b>Крупные переводы в блокчейне (от {fmt_big_usd(WHALE_USD_THRESHOLD)})</b>\n\n"
            + "\n".join(lines[:6])
        )


# ------------------------------------------------------------------- news

def parse_published(entry):
    for key in ("published_parsed", "updated_parsed"):
        t = entry.get(key)
        if t:
            return calendar.timegm(t)
    return None


def fetch_feed_entries():
    collected = []
    for source, url, lang, tier in NEWS_FEEDS:
        try:
            resp = requests.get(url, headers={"User-Agent": UA}, timeout=15)
            resp.raise_for_status()
            parsed = feedparser.parse(resp.content)
        except Exception as e:
            print(f"Feed {source} failed: {e}", file=sys.stderr)
            continue
        for entry in parsed.entries[:30]:
            title = clean_text(entry.get("title", ""))
            link = entry.get("link", "").strip()
            if not title or not link:
                continue
            collected.append({
                "source": source, "lang": lang, "tier": tier, "official": False,
                "title": title, "link": link,
                "summary": clean_text(entry.get("summary", "")),
                "published": parse_published(entry),
            })
    return collected


def fetch_binance_entries():
    data = http_get_json(
        "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
        params={"type": 1, "pageNo": 1, "pageSize": 15},
    )
    collected = []
    for catalog in ((data or {}).get("data") or {}).get("catalogs", []):
        if catalog.get("catalogName") not in BINANCE_CATALOGS:
            continue
        for article in catalog.get("articles", [])[:15]:
            title = clean_text(article.get("title", ""))
            code = article.get("code")
            if not title or not code or not BINANCE_RELEVANT_RE.search(title):
                continue
            release = article.get("releaseDate")
            collected.append({
                "source": "Binance", "lang": "en", "tier": 2, "official": True,
                "title": title,
                "link": f"https://www.binance.com/en/support/announcement/{code}",
                "summary": "",
                "published": release / 1000 if release else None,
            })
    return collected


def select_candidates(entries, seen_hashes):
    cutoff = time.time() - MAX_ENTRY_AGE_HOURS * 3600
    out = []
    for e in entries:
        e["hash"] = entry_hash(e["link"], e["title"])
        if e["hash"] in seen_hashes:
            continue
        if e["published"] and e["published"] < cutoff:
            continue
        text = f"{e['title']} {e['summary']}"
        if JUNK_RE.search(e["title"]):
            continue
        if not e["official"] and not RELEVANCE_RE.search(text):
            continue
        e["summary"] = short_summary(e["summary"], e["title"], e["lang"])
        e["en"] = en_tokens(e["title"]) if e["lang"] == "en" else set()
        e["ru"] = ru_stems(e["title"]) if e["lang"] == "ru" else set()
        e["nums"] = big_numbers(f"{e['title']} {e['summary']}")
        e["names"] = latin_names(e["title"]) if e["lang"] == "ru" else set()
        out.append(e)
    return out


def same_story(a, b):
    """a, b: dicts with 'en' tokens, 'ru' stems and 'nums' (sets)."""
    for key, threshold in (("en", EN_DUP_THRESHOLD), ("ru", RU_DUP_THRESHOLD)):
        if a[key] and b[key]:
            sim = jaccard(a[key], b[key])
            if sim >= threshold:
                return True
            # Differently worded headlines about one event still quote the
            # same figures ("1665 BTC", "$142,7 млн").
            if sim > 0 and len(a["nums"] & b["nums"]) >= 2:
                return True
            # Same named project/company and overlapping wording ("Bitget ... взлом").
            if key == "ru" and sim >= 0.2 and a.get("names", set()) & b.get("names", set()):
                return True
    return False


def matches_posted(item, posted):
    for p in posted:
        other = {
            "en": set(p["en"]), "ru": set(p["ru"]),
            "nums": set(p.get("nums", [])), "names": set(p.get("names", [])),
        }
        if same_story(item, other):
            return True
    return False


def cluster(items):
    stories = []
    for item in items:
        for story in stories:
            if any(same_story(item, other) for other in story["items"]):
                story["items"].append(item)
                break
        else:
            stories.append({"items": [item]})
    return stories


def story_titles(story):
    return " ".join(it["title"] for it in story["items"]) + " " + (story.get("title_ru") or "")


def score_story(story):
    sources = {it["source"] for it in story["items"]}
    titles = story_titles(story)
    security = SECURITY_RE.search(titles)
    amount = max_usd_amount(titles + " " + " ".join(it["summary"] for it in story["items"]))
    story["breaking"] = bool(
        BREAKING_RE.search(titles) or (security and amount >= SECURITY_BREAKING_USD)
    )
    score = max(it["tier"] for it in story["items"]) + 2 * (len(sources) - 1)
    if story["breaking"]:
        score += 3
    if KEY_ASSET_RE.search(titles):
        score += 1
    if RUSSIA_RE.search(titles):
        score += 1
    story["score"] = score
    story["sources"] = len(sources)
    return score


def pick_representative(story):
    ru = [it for it in story["items"] if it["lang"] == "ru"]
    pool = ru or story["items"]
    return max(pool, key=lambda it: (it["tier"], len(it["summary"])))


def translate_stories(stories):
    """Fill title_ru/summary_ru for English-only stories. Stories whose title
    could not be translated get title_ru=None and are retried next run."""
    jobs = []
    for s in stories:
        rep = s["rep"]
        if rep["lang"] == "ru":
            s["title_ru"], s["summary_ru"] = rep["title"], rep["summary"]
        else:
            jobs.append(s)
    texts = []
    for s in jobs:
        texts += [s["rep"]["title"], s["rep"]["summary"]]
    results = translate_batch(texts) if texts else []
    for i, s in enumerate(jobs):
        s["title_ru"] = results[2 * i]
        s["summary_ru"] = results[2 * i + 1] or ""


def format_news(story, prices):
    title, summary = story["title_ru"], story["summary_ru"]
    text_all = f"{story_titles(story)} {summary} " + " ".join(it["summary"] for it in story["items"])
    bullish, bearish = BULLISH_RE.search(text_all), BEARISH_RE.search(text_all)
    sentiment = "📈" if bullish and not bearish else "📉" if bearish and not bullish else ""

    headlines = story_titles(story)
    tags = [tag for rx, tag in TAG_RULES if rx.search(headlines)][:5]
    footer = [sentiment] if sentiment else []
    footer += tags
    if story["sources"] >= 2:
        n = story["sources"]
        footer.append(f"✅ {n} {plural_ru(n, 'источник', 'источника', 'источников')}")

    parts = []
    if story["breaking"]:
        parts.append("🚨 <b>СРОЧНО</b>")
    parts.append(f"<b>{escape_html(title)}</b>")
    if summary:
        parts.append(f"\n{escape_html(summary)}")
    if prices:
        parts.append(f"\n{format_price_line(prices)}")
    if footer:
        parts.append(("\n" if not prices else "") + " ".join(footer))
    return "\n".join(parts)


def process_news(state, prices, bootstrap):
    entries = fetch_feed_entries() + fetch_binance_entries()
    seen_hashes = {it["hash"] for it in state["seen"]}
    now = time.time()

    def mark_seen(items):
        for it in items:
            if it["hash"] not in seen_hashes:
                seen_hashes.add(it["hash"])
                state["seen"].append({"hash": it["hash"], "ts": now})

    candidates = select_candidates(entries, seen_hashes)
    if bootstrap:
        mark_seen(candidates)
        print(f"Bootstrap: recorded {len(candidates)} items, sent 0.")
        return 0

    fresh = []
    for c in candidates:
        if matches_posted(c, state["posted"]):
            mark_seen([c])  # story already covered by an earlier post
        else:
            fresh.append(c)

    stories = cluster(fresh)
    for s in stories:
        score_story(s)
        s["rep"] = pick_representative(s)
    stories.sort(key=lambda s: s["score"], reverse=True)

    shortlist = [s for s in stories if s["score"] >= MIN_STORY_SCORE][: MAX_NEWS_PER_RUN * 2]
    if DRY_RUN:
        for s in stories[:30]:
            print(f"  [{s['score']}|{s['sources']}] {s['rep']['source']}: {s['rep']['title'][:90]}",
                  file=sys.stderr)
    shortlisted = {id(s) for s in shortlist}
    for s in stories:
        if id(s) not in shortlisted:
            mark_seen(s["items"])  # low-value or outranked: don't resurface it later

    translate_stories(shortlist)

    # Merge English stories into Russian ones about the same event, now that
    # both sides have Russian text to compare.
    merged = []
    for s in shortlist:
        if s["title_ru"] is None:
            merged.append(s)
            continue
        s["ru_key"] = ru_stems(s["title_ru"])
        s["key"] = {
            "en": set(), "ru": s["ru_key"],
            "nums": big_numbers(f"{s['title_ru']} {s['summary_ru']}")
            | set().union(*(it["nums"] for it in s["items"])),
            "names": latin_names(s["title_ru"]),
        }
        twin = next((m for m in merged if m.get("key") and same_story(s["key"], m["key"])), None)
        if twin:
            twin["items"] += s["items"]
            twin["key"]["nums"] |= s["key"]["nums"]
            twin["key"]["names"] |= s["key"]["names"]
            score_story(twin)
        elif matches_posted(s["key"], state["posted"]):
            mark_seen(s["items"])
        else:
            merged.append(s)
    merged.sort(key=lambda s: s["score"], reverse=True)

    sent = 0
    for s in merged:
        if s["title_ru"] is None:
            print(f"Postponed (no translation): {s['rep']['title'][:80]}", file=sys.stderr)
            continue
        if sent >= MAX_NEWS_PER_RUN:
            mark_seen(s["items"])
            continue
        if send_text(format_news(s, prices)):
            sent += 1
            mark_seen(s["items"])
            state["posted"].append({
                "en": sorted(set().union(*(it["en"] for it in s["items"]))),
                "ru": sorted(s["ru_key"] | set().union(*(it["ru"] for it in s["items"]))),
                "nums": sorted(s["key"]["nums"]),
                "names": sorted(s["key"]["names"]),
                "title": s["title_ru"], "score": s["score"],
                "breaking": s["breaking"], "ts": time.time(),
            })
            time.sleep(0 if DRY_RUN else SEND_DELAY_SECONDS)
    print(f"News: {len(entries)} fetched, {len(candidates)} candidates, "
          f"{len(stories)} stories, {sent} sent.")
    return sent


# ------------------------------------------------------------------- main

def main():
    state = load_state()
    migrate_state(state)
    prune_state(state)
    bootstrap = not state.get("bootstrapped", False)

    prices = get_prices()
    market_data = get_market_data()
    global_data = get_global()
    fng = get_fear_greed()
    btc_net = get_btc_network()
    cbr = get_cbr_usd()

    update_pinned_ticker(state, prices, global_data, fng, btc_net, cbr)
    if not bootstrap:
        check_price_alert(state, prices)
        check_price_milestones(state, prices)
        check_volume_spike(state, market_data)
        send_whale_alerts(state, prices)
        check_risk_off(state, get_index_data())

    process_news(state, prices, bootstrap)

    if not bootstrap:
        maybe_send_morning_report(state, market_data, global_data, fng, btc_net, cbr)
        maybe_send_evening_digest(state, prices)
        maybe_send_weekly(state, market_data, prices)

    state["bootstrapped"] = True
    save_state(state)


if __name__ == "__main__":
    main()
