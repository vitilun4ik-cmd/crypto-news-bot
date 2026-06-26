"""Crypto + market news aggregator -> Telegram channel (Russian, link-free).

Polls RSS feeds (+ optional CryptoPanic), filters for crypto relevance,
deduplicates (exact + near-duplicate across sources), translates to Russian,
tags breaking/high-impact items, attaches a live BTC/ETH price line, and
posts full-text items to a Telegram channel. Also maintains a pinned
BTC/ETH price ticker, price-spike alerts, a daily digest, and a daily
Fear & Greed Index post.

State is persisted to data/seen.json, committed back by the GitHub Actions
workflow after each run.
"""

import html
import json
import hashlib
import os
import re
import sys
import time
from datetime import datetime, timezone

import feedparser
import requests
from deep_translator import GoogleTranslator

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHANNEL_ID"]
CRYPTOPANIC_TOKEN = os.environ.get("CRYPTOPANIC_TOKEN", "").strip()

STATE_PATH = os.path.join(os.path.dirname(__file__), "data", "seen.json")
MAX_AGE_DAYS = 4
SIMILARITY_THRESHOLD = 0.55
MAX_MESSAGES_PER_RUN = 25
SEND_DELAY_SECONDS = 1.2
PRICE_ALERT_THRESHOLD_PCT = 3.0
PRICE_ALERT_MIN_INTERVAL_SECONDS = 30 * 60
DIGEST_UTC_HOUR = 14   # ~21:00 local (UTC+7)
FNG_UTC_HOUR = 2        # ~09:00 local (UTC+7)
WEEKLY_UTC_HOUR = 14   # ~21:00 local (UTC+7), Monday
VOLUME_SPIKE_PCT = 60.0
VOLUME_ALERT_MIN_INTERVAL_SECONDS = 60 * 60
WHALE_USD_THRESHOLD = 500_000
USDT_CONTRACT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
USDT_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ETH_RPC_URL = "https://eth.drpc.org"
ETH_MAX_BLOCKS_PER_RUN = 40
TON_API_URL = "https://toncenter.com/api/v2"
TON_MAX_TX_DETAILS_PER_RUN = 10
WHALE_SEEN_MAX_AGE_SECONDS = 3 * 60 * 60
RISK_OFF_THRESHOLD_PCT = -1.5
RISK_OFF_RECOVERY_PCT = -0.5

CRYPTO_FEEDS = {
    "CoinDesk": "https://www.coindesk.com/arc/outboundfeeds/rss",
    "Cointelegraph": "https://cointelegraph.com/rss",
    "Decrypt": "https://decrypt.co/feed",
    "CryptoSlate": "https://cryptoslate.com/feed/",
    "NewsBTC": "https://www.newsbtc.com/feed/",
    "BeInCrypto": "https://beincrypto.com/feed/",
    "CryptoPotato": "https://cryptopotato.com/feed/",
    "Bitcoinist": "https://bitcoinist.com/feed/",
    "CoinJournal": "https://coinjournal.net/feed/",
    "DailyHodl": "https://dailyhodl.com/feed/",
    "U.Today": "https://u.today/rss.php",
    "Reddit r/CryptoCurrency": "https://www.reddit.com/r/CryptoCurrency/hot/.rss",
}

RECURRING_THREAD_RE = re.compile(
    r"daily crypto discussion|weekly crypto discussion|monthly crypto discussion",
    re.I,
)

BINANCE_CATALOGS = {
    "New Cryptocurrency Listing",
    "Delisting",
}
# Even inside those catalogs Binance mixes in futures/perpetual-contract and
# tokenized-securities noise; only actual spot listing/delisting moves price.
BINANCE_RELEVANT_RE = re.compile(r"\bwill list\b|\bwill add\b|delist", re.I)

MACRO_FEEDS = {
    "CNBC Finance": "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "Yahoo Finance": "https://finance.yahoo.com/news/rssindex",
    "Yahoo Finance Tickers": (
        "https://feeds.finance.yahoo.com/rss/2.0/headline?"
        "s=COIN,MSTR,TSLA,NVDA,IBIT&region=US&lang=en-US"
    ),
    "Federal Reserve": "https://www.federalreserve.gov/feeds/press_all.xml",
    "SEC": "https://www.sec.gov/news/pressreleases.rss",
    "MarketWatch": "https://feeds.content.dowjones.io/public/rss/mw_topstories",
}

ALL_FEEDS = {**CRYPTO_FEEDS, **MACRO_FEEDS}

CRYPTO_RELEVANCE_KEYWORDS = [
    "bitcoin", "crypto", "ethereum", "blockchain", "btc", "eth", "stablecoin",
    "coinbase", "binance", "etf", "interest rate", "rate cut", "rate hike",
    "inflation", "cpi", "treasury yield", "regulation", "microstrategy",
    "strategy inc", "tether", "defi", "nft", "miner", "mining", "halving",
    "tariff", "recession", "fomc", "powell", "sec charges", "sec sues",
    "digital asset", "solana", "ripple", "xrp", "dogecoin",
]

BREAKING_KEYWORDS = [
    "hack", "exploit", "breach", "stolen", "halt", "suspend", "bankrupt",
    "approved", "rejected", "lawsuit", "sues", "charges", "all-time high",
    "record high", "plunge", "crash", "surge", "ban", "banned", "emergency",
    "collapse", "liquidat", "rate cut", "rate hike", "default",
]

TICKER_TAGS = [
    ("bitcoin", "#BTC"), ("btc", "#BTC"), ("ethereum", "#ETH"), ("eth", "#ETH"),
    ("coinbase", "#COIN"), ("microstrategy", "#MSTR"), ("tesla", "#TSLA"),
    ("nvidia", "#NVDA"), ("solana", "#SOL"), ("ripple", "#XRP"), ("xrp", "#XRP"),
    ("dogecoin", "#DOGE"), ("cardano", "#ADA"), ("polkadot", "#DOT"),
    ("litecoin", "#LTC"), ("chainlink", "#LINK"), ("avalanche", "#AVAX"),
    ("polygon", "#MATIC"), ("shiba", "#SHIB"), ("ton ", "#TON"),
    ("sec ", "#SEC"), ("federal reserve", "#FED"), ("fomc", "#FED"),
    ("etf", "#ETF"), ("stablecoin", "#Stablecoin"), ("tether", "#USDT"),
]

STOPWORDS = {
    "the", "a", "an", "to", "of", "in", "on", "for", "and", "is", "as", "at",
    "by", "with", "after", "amid", "its", "it", "this", "that", "are", "be",
    "from", "new", "says", "could", "will", "has", "have", "into", "up",
    "down", "over", "out", "what", "why", "how",
}

HTML_TAG_RE = re.compile(r"<[^>]+>")
WHITESPACE_RE = re.compile(r"\s+")


# ---------------------------------------------------------------- utilities

def clean_text(raw):
    if not raw:
        return ""
    text = html.unescape(raw)
    text = HTML_TAG_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text


def translate_ru(text, max_len=600):
    text = text[:max_len]
    if not text:
        return ""
    try:
        return GoogleTranslator(source="auto", target="ru").translate(text)
    except Exception as e:
        print(f"Translate failed, using original text: {e}", file=sys.stderr)
        return text


def escape_html(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def load_state():
    if not os.path.exists(STATE_PATH):
        return {"bootstrapped": False, "items": []}
    with open(STATE_PATH, "r") as f:
        return json.load(f)


def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def prune_state(state):
    cutoff = time.time() - MAX_AGE_DAYS * 86400
    state["items"] = [it for it in state["items"] if it["ts"] >= cutoff]
    state["breaking_today"] = [
        it for it in state.get("breaking_today", []) if it["ts"] >= cutoff
    ]
    prune_whale_seen(state)


def entry_hash(link, title):
    return hashlib.sha256(f"{link}|{title}".encode("utf-8")).hexdigest()


def normalize_title(title):
    words = re.findall(r"[a-z0-9]+", title.lower())
    return {w for w in words if w not in STOPWORDS and len(w) > 2}


def jaccard(a, b):
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def is_near_duplicate(tokens, state_items):
    for it in state_items:
        if jaccard(tokens, set(it["tokens"])) >= SIMILARITY_THRESHOLD:
            return True
    return False


def is_crypto_relevant(category, text_lower):
    if category == "crypto":
        return True
    # "macro" and "reddit" (community noise/memes) both need a keyword match
    return any(kw in text_lower for kw in CRYPTO_RELEVANCE_KEYWORDS)


def is_breaking(text_lower):
    return any(kw in text_lower for kw in BREAKING_KEYWORDS)


def tags_for(text_lower):
    tags = []
    for kw, tag in TICKER_TAGS:
        if kw in text_lower and tag not in tags:
            tags.append(tag)
    return tags[:5]


# ------------------------------------------------------------- price helpers

PRICE_COINS = {"btc": "bitcoin", "eth": "ethereum", "ton": "the-open-network"}


def get_prices():
    ids = ",".join(PRICE_COINS.values())
    url = (
        f"https://api.coingecko.com/api/v3/simple/price?ids={ids}"
        "&vs_currencies=usd,rub&include_24hr_change=true"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return {
            sym: {
                "usd": data[coin_id]["usd"],
                "rub": data[coin_id]["rub"],
                "change_24h": data[coin_id]["usd_24h_change"],
            }
            for sym, coin_id in PRICE_COINS.items()
        }
    except Exception as e:
        print(f"Failed to fetch prices: {e}", file=sys.stderr)
        return None


def trend_emoji(change_pct):
    if change_pct is None:
        return ""
    if change_pct >= 1:
        return "📈"
    if change_pct <= -1:
        return "📉"
    return "➡️"


def format_price_line(prices):
    if not prices:
        return ""
    lines = []
    for symbol, icon in (("btc", "Ⓑ"), ("eth", "Ⓔ"), ("ton", "Ⓝ")):
        c = prices[symbol]
        rub_fmt = f"{c['rub']:,.2f}" if c['rub'] < 100 else f"{c['rub']:,.0f}"
        usd_fmt = f"{c['usd']:,.2f}" if c['usd'] < 100 else f"{c['usd']:,.0f}"
        lines.append(
            f"{icon} {symbol.upper()}: ${usd_fmt} / {rub_fmt}₽ "
            f"({c['change_24h']:+.1f}% 24ч) {trend_emoji(c['change_24h'])}"
        )
    return "\n".join(lines)


def format_ticker_text(prices):
    if not prices:
        return "Курс временно недоступен"
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    return (
        "📌 <b>Курс BTC / ETH / TON</b>\n\n"
        f"{format_price_line(prices)}\n\n"
        f"Обновлено: {now}"
    )


def update_pinned_ticker(state, prices):
    text = format_ticker_text(prices)
    msg_id = state.get("pinned_ticker_message_id")

    if msg_id:
        ok = telegram_call("editMessageText", {
            "chat_id": CHAT_ID, "message_id": msg_id, "text": text, "parse_mode": "HTML",
        })
        if ok:
            return
        # message_id no longer editable (deleted/unpinned) -> fall through and resend

    resp = telegram_call("sendMessage", {
        "chat_id": CHAT_ID, "text": text, "parse_mode": "HTML",
    }, return_response=True)
    if resp and resp.get("ok"):
        new_id = resp["result"]["message_id"]
        state["pinned_ticker_message_id"] = new_id
        telegram_call("pinChatMessage", {
            "chat_id": CHAT_ID, "message_id": new_id, "disable_notification": True,
        })


def check_price_alert(state, prices):
    if not prices:
        return
    baseline = state.get("price_alert_baseline")
    now = time.time()
    if not baseline:
        state["price_alert_baseline"] = {
            "btc": prices["btc"]["usd"], "eth": prices["eth"]["usd"], "ts": now,
        }
        return

    if now - baseline["ts"] < PRICE_ALERT_MIN_INTERVAL_SECONDS:
        return

    alerts = []
    for sym, key in (("BTC", "btc"), ("ETH", "eth")):
        old = baseline[key]
        new = prices[key]["usd"]
        if old <= 0:
            continue
        pct = (new - old) / old * 100
        if abs(pct) >= PRICE_ALERT_THRESHOLD_PCT:
            arrow = "🚀" if pct > 0 else "🔻"
            alerts.append(f"{arrow} {sym}: {pct:+.1f}% за последние ~{int((now - baseline['ts']) / 60)} мин (${old:,.0f} → ${new:,.0f})")

    if alerts:
        text = "⚡ <b>Резкое движение цены</b>\n\n" + "\n".join(alerts)
        telegram_call("sendMessage", {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})

    state["price_alert_baseline"] = {
        "btc": prices["btc"]["usd"], "eth": prices["eth"]["usd"], "ts": now,
    }


MILESTONE_STEP = {"btc": 10_000, "eth": 100, "ton": 1}
MILESTONE_NAME = {"btc": "Bitcoin", "eth": "Ethereum", "ton": "TON"}


def check_price_milestones(state, prices):
    if not prices:
        return
    milestones = state.setdefault("milestone_prices", {})
    alerts = []

    for sym, step in MILESTONE_STEP.items():
        price = prices[sym]["usd"]
        prev = milestones.get(sym)
        milestones[sym] = price
        if prev is None:
            continue

        prev_level = int(prev // step)
        new_level = int(price // step)
        if new_level == prev_level:
            continue

        up = new_level > prev_level
        boundary = max(prev_level, new_level) * step
        arrow = "🚀" if up else "⚠️"
        verb = "пробивает" if up else "падает ниже"
        alerts.append(f"{arrow} <b>{MILESTONE_NAME[sym]} {verb} ${boundary:,.0f}</b>")

    for text in alerts:
        telegram_call("sendMessage", {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})


def maybe_send_fear_greed(state):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_hour = datetime.now(timezone.utc).hour
    if state.get("last_fng_date") == today or now_hour != FNG_UTC_HOUR:
        return
    try:
        resp = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        resp.raise_for_status()
        item = resp.json()["data"][0]
        value = int(item["value"])
        label_ru = {
            "Extreme Fear": "Крайний страх 🥶",
            "Fear": "Страх 😟",
            "Neutral": "Нейтрально 😐",
            "Greed": "Жадность 😏",
            "Extreme Greed": "Крайняя жадность 🤑",
        }.get(item["value_classification"], item["value_classification"])
        filled = round(value / 10)
        bar = "🟥" * min(filled, 4) + "🟧" * (1 if 4 < filled <= 6 else 0) + "🟩" * max(0, filled - 6)
        bar = bar[:10] if bar else "—"
        text = (
            "📊 <b>Индекс страха и жадности</b>\n\n"
            f"{value}/100 — {label_ru}\n"
            f"{bar}"
        )
        telegram_call("sendMessage", {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})
        state["last_fng_date"] = today
    except Exception as e:
        print(f"Fear&Greed fetch failed: {e}", file=sys.stderr)


def maybe_send_daily_digest(state, prices):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_hour = datetime.now(timezone.utc).hour
    if state.get("last_digest_date") == today or now_hour != DIGEST_UTC_HOUR:
        return

    items = state.get("breaking_today", [])
    if not items:
        state["last_digest_date"] = today
        return

    lines = ["🗞 <b>Главное за день</b>\n"]
    for it in items[:10]:
        lines.append(f"🚨 {it['title']}")
    if prices:
        lines.append("")
        lines.append(format_price_line(prices))

    telegram_call("sendMessage", {"chat_id": CHAT_ID, "text": "\n".join(lines), "parse_mode": "HTML"})
    state["last_digest_date"] = today
    state["breaking_today"] = []


# -------------------------------------------------------------- market data

def get_market_data():
    url = (
        "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
        "&order=market_cap_desc&per_page=20&page=1&price_change_percentage=7d"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Failed to fetch market data: {e}", file=sys.stderr)
        return None


def maybe_send_weekly_review(state, market_data, prices):
    now = datetime.now(timezone.utc)
    if now.weekday() != 0 or now.hour != WEEKLY_UTC_HOUR:
        return
    today = now.strftime("%Y-%m-%d")
    if state.get("last_weekly_date") == today:
        return
    if not market_data:
        state["last_weekly_date"] = today
        return

    ranked = sorted(
        market_data,
        key=lambda c: c.get("price_change_percentage_7d_in_currency") or 0,
        reverse=True,
    )
    gainers = ranked[:3]
    losers = ranked[-3:]

    lines = ["📅 <b>Итоги недели</b>\n"]
    if prices:
        lines.append(format_price_line(prices))
        lines.append("")
    lines.append("🟢 Лидеры роста (7д):")
    for c in gainers:
        pct = c.get("price_change_percentage_7d_in_currency") or 0
        lines.append(f"  {c['symbol'].upper()}: {pct:+.1f}%")
    lines.append("\n🔴 Лидеры падения (7д):")
    for c in reversed(losers):
        pct = c.get("price_change_percentage_7d_in_currency") or 0
        lines.append(f"  {c['symbol'].upper()}: {pct:+.1f}%")

    telegram_call("sendMessage", {"chat_id": CHAT_ID, "text": "\n".join(lines), "parse_mode": "HTML"})
    state["last_weekly_date"] = today


def maybe_send_weekly_poll(state):
    now = datetime.now(timezone.utc)
    if now.weekday() != 0 or now.hour != WEEKLY_UTC_HOUR:
        return
    week_key = now.strftime("%Y-W%U")
    if state.get("last_poll_week") == week_key:
        return
    telegram_call("sendPoll", {
        "chat_id": CHAT_ID,
        "question": "Куда пойдёт BTC на этой неделе?",
        "options": ["🚀 Вверх", "🔻 Вниз", "➡️ Без изменений"],
        "is_anonymous": True,
    })
    state["last_poll_week"] = week_key


def check_volume_spike(state, market_data):
    if not market_data:
        return
    btc = next((c for c in market_data if c["id"] == "bitcoin"), None)
    eth = next((c for c in market_data if c["id"] == "ethereum"), None)
    if not btc or not eth:
        return

    now = time.time()
    baseline = state.get("volume_baseline")
    if not baseline:
        state["volume_baseline"] = {
            "btc": btc["total_volume"], "eth": eth["total_volume"], "ts": now,
        }
        return
    if now - baseline["ts"] < VOLUME_ALERT_MIN_INTERVAL_SECONDS:
        return

    alerts = []
    for sym, key, cur in (("BTC", "btc", btc["total_volume"]), ("ETH", "eth", eth["total_volume"])):
        old = baseline.get(key, 0)
        if old <= 0:
            continue
        pct = (cur - old) / old * 100
        if pct >= VOLUME_SPIKE_PCT:
            alerts.append(f"📊 {sym}: объём торгов вырос на {pct:+.0f}% за последний час")

    if alerts:
        text = "⚠️ <b>Аномальный объём торгов</b>\n\n" + "\n".join(alerts)
        telegram_call("sendMessage", {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})

    state["volume_baseline"] = {"btc": btc["total_volume"], "eth": eth["total_volume"], "ts": now}


# -------------------------------------------------------------- whale alerts

def _whale_mark_seen(state, h):
    state.setdefault("whale_seen", [])
    state["whale_seen"].append({"hash": h, "ts": time.time()})


def _whale_seen_hashes(state):
    return {w["hash"] for w in state.get("whale_seen", [])}


def _send_whale_alert(title, lines):
    if not lines:
        return
    text = f"🐳 <b>{title}</b>\n\n" + "\n".join(lines[:5])
    telegram_call("sendMessage", {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"})


def fetch_btc_whale_alerts(state, prices):
    try:
        resp = requests.get("https://blockchain.info/unconfirmed-transactions?format=json", timeout=10)
        resp.raise_for_status()
        txs = resp.json().get("txs", [])
    except Exception as e:
        print(f"BTC whale fetch failed: {e}", file=sys.stderr)
        return

    seen_hashes = _whale_seen_hashes(state)
    btc_usd = prices["btc"]["usd"] if prices else None
    threshold_btc = (WHALE_USD_THRESHOLD / btc_usd) if btc_usd else 10
    alerts = []

    for tx in txs:
        h = tx.get("hash")
        if not h or h in seen_hashes:
            continue
        seen_hashes.add(h)
        _whale_mark_seen(state, h)

        total_sat = sum(o.get("value", 0) for o in tx.get("out", []))
        btc_amount = total_sat / 1e8
        if btc_amount >= threshold_btc:
            usd_val = f" (~${btc_amount * btc_usd:,.0f})" if btc_usd else ""
            alerts.append(f"Ⓑ {btc_amount:,.1f} BTC{usd_val}")

    _send_whale_alert("Крупные транзакции BTC", alerts)


def eth_rpc(method, params):
    resp = requests.post(
        ETH_RPC_URL, json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1}, timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(data["error"])
    return data["result"]


def fetch_eth_and_usdt_whale_alerts(state, prices):
    try:
        latest_hex = eth_rpc("eth_blockNumber", [])
        latest = int(latest_hex, 16)
    except Exception as e:
        print(f"ETH RPC fetch failed: {e}", file=sys.stderr)
        return

    last_block = state.get("last_eth_block")
    if last_block is None:
        state["last_eth_block"] = latest
        return

    from_block = max(last_block + 1, latest - ETH_MAX_BLOCKS_PER_RUN + 1)
    if from_block > latest:
        return

    seen_hashes = _whale_seen_hashes(state)
    eth_usd = prices["eth"]["usd"] if prices else None
    threshold_eth = (WHALE_USD_THRESHOLD / eth_usd) if eth_usd else 200

    eth_alerts = []
    for block_num in range(from_block, latest + 1):
        try:
            block = eth_rpc("eth_getBlockByNumber", [hex(block_num), True])
        except Exception as e:
            print(f"ETH block {block_num} fetch failed: {e}", file=sys.stderr)
            continue
        if not block:
            continue
        for tx in block.get("transactions", []):
            h = tx.get("hash")
            if not h or h in seen_hashes or not tx.get("to"):
                continue
            value_eth = int(tx["value"], 16) / 1e18
            if value_eth >= threshold_eth:
                seen_hashes.add(h)
                _whale_mark_seen(state, h)
                usd_val = f" (~${value_eth * eth_usd:,.0f})" if eth_usd else ""
                eth_alerts.append(f"Ⓔ {value_eth:,.0f} ETH{usd_val}")

    _send_whale_alert("Крупные транзакции ETH", eth_alerts)

    usdt_alerts = []
    try:
        logs = eth_rpc("eth_getLogs", [{
            "fromBlock": hex(from_block), "toBlock": hex(latest),
            "address": USDT_CONTRACT, "topics": [USDT_TRANSFER_TOPIC],
        }])
        for log in logs:
            h = log.get("transactionHash")
            log_index = log.get("logIndex", "0")
            key = f"{h}:{log_index}"
            if not h or key in seen_hashes:
                continue
            amount = int(log["data"], 16) / 1e6
            if amount >= WHALE_USD_THRESHOLD:
                seen_hashes.add(key)
                _whale_mark_seen(state, key)
                usdt_alerts.append(f"💵 {amount:,.0f} USDT (~${amount:,.0f})")
    except Exception as e:
        print(f"USDT logs fetch failed: {e}", file=sys.stderr)

    _send_whale_alert("Крупные транзакции USDT", usdt_alerts)

    state["last_eth_block"] = latest


def ton_rpc(method, params):
    resp = requests.get(f"{TON_API_URL}/{method}", params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(data.get("error"))
    return data["result"]


def fetch_ton_whale_alerts(state, prices):
    """Best-effort: TON has no free API for network-wide large-transfer
    scanning, so this samples a handful of transactions from the latest
    masterchain block only. It will miss most whale activity, but it's
    the best coverage achievable without a paid API."""
    try:
        info = ton_rpc("getMasterchainInfo", {})
        seqno = info["last"]["seqno"]
        shards = ton_rpc("shards", {"seqno": seqno})["shards"]
    except Exception as e:
        print(f"TON shards fetch failed: {e}", file=sys.stderr)
        return

    seen_hashes = _whale_seen_hashes(state)
    ton_usd = prices["ton"]["usd"] if prices else None
    threshold_ton = (WHALE_USD_THRESHOLD / ton_usd) if ton_usd else 300_000

    short_txs = []
    for shard in shards[:2]:
        try:
            res = ton_rpc("getBlockTransactions", {
                "workchain": shard["workchain"], "shard": shard["shard"],
                "seqno": shard["seqno"], "count": 20,
            })
            short_txs.extend(res.get("transactions", []))
        except Exception as e:
            print(f"TON block transactions fetch failed: {e}", file=sys.stderr)

    alerts = []
    for stx in short_txs[:TON_MAX_TX_DETAILS_PER_RUN]:
        h = stx.get("hash")
        if not h or h in seen_hashes:
            continue
        seen_hashes.add(h)
        _whale_mark_seen(state, h)

        time.sleep(0.4)  # be polite to the free, unauthenticated rate limit
        try:
            detail = ton_rpc("getTransactions", {
                "address": stx["account"], "lt": stx["lt"], "hash": h, "limit": 1,
            })
        except Exception as e:
            print(f"TON tx detail fetch failed: {e}", file=sys.stderr)
            continue
        if not detail:
            continue
        tx = detail[0]
        values_nanoton = [int(tx.get("in_msg", {}).get("value", 0) or 0)]
        values_nanoton += [int(m.get("value", 0) or 0) for m in tx.get("out_msgs", [])]
        max_ton = max(values_nanoton, default=0) / 1e9
        if max_ton >= threshold_ton:
            usd_val = f" (~${max_ton * ton_usd:,.0f})" if ton_usd else ""
            alerts.append(f"Ⓝ {max_ton:,.0f} TON{usd_val}")

    _send_whale_alert("Крупные транзакции TON", alerts)


def prune_whale_seen(state):
    cutoff = time.time() - WHALE_SEEN_MAX_AGE_SECONDS
    state["whale_seen"] = [w for w in state.get("whale_seen", []) if w["ts"] >= cutoff]


# --------------------------------------------------------------- risk-off

def get_index_data():
    indices = {}
    for name, yid in (("S&P500", "%5EGSPC"), ("Nasdaq", "%5EIXIC")):
        try:
            resp = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{yid}?interval=1d&range=1d",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            resp.raise_for_status()
            meta = resp.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev = meta.get("chartPreviousClose") or meta.get("previousClose")
            if price and prev:
                indices[name] = (price - prev) / prev * 100
        except Exception as e:
            print(f"Index fetch failed for {name}: {e}", file=sys.stderr)
    return indices


def check_risk_off(state, indices):
    if not indices:
        return
    worst = min(indices.values())
    active = state.get("risk_off_active", False)

    if worst <= RISK_OFF_THRESHOLD_PCT and not active:
        lines = ["⚠️ <b>Risk-off на рынках акций</b>", "", "Падение индексов может потащить крипту вниз:"]
        for name, pct in indices.items():
            lines.append(f"{name}: {pct:+.1f}%")
        telegram_call("sendMessage", {"chat_id": CHAT_ID, "text": "\n".join(lines), "parse_mode": "HTML"})
        state["risk_off_active"] = True
    elif worst > RISK_OFF_RECOVERY_PCT and active:
        state["risk_off_active"] = False



# ----------------------------------------------------------------- telegram

def telegram_call(method, payload, return_response=False):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=15)
    except Exception as e:
        print(f"Telegram call {method} failed: {e}", file=sys.stderr)
        return None if return_response else False

    if resp.status_code == 429:
        retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
        time.sleep(retry_after + 1)
        resp = requests.post(url, json=payload, timeout=15)

    data = resp.json() if resp.content else {}
    if not resp.ok or not data.get("ok"):
        print(f"Telegram {method} failed: {resp.status_code} {resp.text}", file=sys.stderr)
        return None if return_response else False
    return data if return_response else True


def format_message(title_ru, summary_ru, tags, breaking, prices):
    prefix = "🚨 <b>СРОЧНО</b>\n" if breaking else ""
    tag_line = f"\n{' '.join(tags)}" if tags else ""
    summary_block = f"\n{escape_html(summary_ru)}\n" if summary_ru else "\n"
    price_block = f"\n{format_price_line(prices)}" if prices else ""
    return (
        f"{prefix}<b>{escape_html(title_ru)}</b>\n"
        f"{summary_block}"
        f"{price_block}"
        f"{tag_line}"
    ).rstrip()


# -------------------------------------------------------------------- fetch

def fetch_rss_entries():
    collected = []
    for source, url in ALL_FEEDS.items():
        is_reddit = source == "Reddit r/CryptoCurrency"
        if is_reddit:
            category = "reddit"
        elif source in CRYPTO_FEEDS:
            category = "crypto"
        else:
            category = "macro"

        try:
            parsed = feedparser.parse(url)
        except Exception as e:
            print(f"Failed to fetch {source}: {e}", file=sys.stderr)
            continue
        for entry in parsed.entries[:20]:
            title = clean_text(entry.get("title", ""))
            link = entry.get("link", "").strip()
            if not title or not link:
                continue
            if is_reddit and RECURRING_THREAD_RE.search(title):
                continue
            # Reddit's RSS summary is raw post markup (image tables, mod
            # disclaimers) - not useful as a news body, so skip it.
            summary = "" if is_reddit else clean_text(entry.get("summary", ""))
            text_lower = f"{title} {summary}".lower()
            collected.append({
                "source": source, "category": category,
                "title": title, "link": link,
                "summary": summary, "text_lower": text_lower,
            })
    return collected


def fetch_binance_entries():
    url = (
        "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
        "?type=1&pageNo=1&pageSize=15"
    )
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
        catalogs = resp.json().get("data", {}).get("catalogs", [])
    except Exception as e:
        print(f"Binance fetch failed: {e}", file=sys.stderr)
        return []

    collected = []
    for catalog in catalogs:
        if catalog.get("catalogName") not in BINANCE_CATALOGS:
            continue
        for article in catalog.get("articles", [])[:15]:
            title = clean_text(article.get("title", ""))
            code = article.get("code")
            if not title or not code:
                continue
            if not BINANCE_RELEVANT_RE.search(title):
                continue
            link = f"https://www.binance.com/en/support/announcement/{code}"
            collected.append({
                "source": "Binance", "category": "crypto",
                "title": title, "link": link,
                "summary": "", "text_lower": title.lower(),
            })
    return collected


def fetch_cryptopanic_entries():
    if not CRYPTOPANIC_TOKEN:
        return []
    url = (
        f"https://cryptopanic.com/api/v1/posts/?auth_token={CRYPTOPANIC_TOKEN}"
        "&public=true&kind=news&filter=hot"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        posts = resp.json().get("results", [])
    except Exception as e:
        print(f"CryptoPanic fetch failed: {e}", file=sys.stderr)
        return []

    collected = []
    for post in posts[:30]:
        title = clean_text(post.get("title", ""))
        link = post.get("url", "").strip() or f"cryptopanic:{post.get('id')}"
        if not title:
            continue
        source_title = (post.get("source") or {}).get("title", "CryptoPanic")
        collected.append({
            "source": f"CryptoPanic/{source_title}", "category": "crypto",
            "title": title, "link": link,
            "summary": "", "text_lower": title.lower(),
        })
    return collected


# ---------------------------------------------------------------------- main

def main():
    state = load_state()
    state.setdefault("items", [])
    state.setdefault("breaking_today", [])
    prune_state(state)

    prices = get_prices()
    update_pinned_ticker(state, prices)
    check_price_alert(state, prices)
    check_price_milestones(state, prices)

    market_data = get_market_data()
    check_volume_spike(state, market_data)
    fetch_btc_whale_alerts(state, prices)
    fetch_eth_and_usdt_whale_alerts(state, prices)
    fetch_ton_whale_alerts(state, prices)

    indices = get_index_data()
    check_risk_off(state, indices)

    entries = fetch_rss_entries() + fetch_cryptopanic_entries() + fetch_binance_entries()
    bootstrap = not state.get("bootstrapped", False)

    to_send = []
    seen_hashes = {it["hash"] for it in state["items"]}

    for entry in entries:
        h = entry_hash(entry["link"], entry["title"])
        if h in seen_hashes:
            continue
        if not is_crypto_relevant(entry["category"], entry["text_lower"]):
            continue

        tokens = normalize_title(entry["title"])
        if is_near_duplicate(tokens, state["items"]):
            seen_hashes.add(h)
            state["items"].append({"hash": h, "tokens": list(tokens), "ts": time.time()})
            continue

        state["items"].append({"hash": h, "tokens": list(tokens), "ts": time.time()})
        seen_hashes.add(h)

        if bootstrap:
            continue

        to_send.append(entry)

    if bootstrap:
        state["bootstrapped"] = True
        save_state(state)
        print(f"Bootstrap complete: recorded {len(entries)} existing items, sent 0.")
        return

    sent = 0
    for entry in to_send[:MAX_MESSAGES_PER_RUN]:
        text_lower = entry["text_lower"]
        breaking = is_breaking(text_lower)
        tags = tags_for(text_lower)

        title_ru = translate_ru(entry["title"])
        summary_ru = translate_ru(entry["summary"]) if entry["summary"] else ""

        message = format_message(title_ru, summary_ru, tags, breaking, prices)
        if telegram_call("sendMessage", {
            "chat_id": CHAT_ID, "text": message, "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }):
            sent += 1
            if breaking:
                state["breaking_today"].append({"title": title_ru, "ts": time.time()})
            time.sleep(SEND_DELAY_SECONDS)

    maybe_send_weekly_review(state, market_data, prices)
    maybe_send_weekly_poll(state)
    maybe_send_daily_digest(state, prices)
    maybe_send_fear_greed(state)

    save_state(state)
    print(f"Run complete: {len(entries)} fetched, {sent} sent.")


if __name__ == "__main__":
    main()
