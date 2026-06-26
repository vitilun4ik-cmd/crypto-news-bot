"""Crypto + market news aggregator -> Telegram channel.

Polls a set of RSS feeds, filters for crypto relevance, deduplicates
(exact + near-duplicate across sources), tags breaking/high-impact items,
and posts new items to a Telegram channel via the Bot API.

State (which items were already posted) is persisted to data/seen.json,
which the GitHub Actions workflow commits back to the repo after each run.
"""

import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import feedparser
import requests

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHANNEL_ID"]

STATE_PATH = os.path.join(os.path.dirname(__file__), "data", "seen.json")
MAX_AGE_DAYS = 4
SIMILARITY_THRESHOLD = 0.55
MAX_MESSAGES_PER_RUN = 25
SEND_DELAY_SECONDS = 1.2

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
}

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

# Macro/stock items only get posted if their title/summary mentions one of these.
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
    ("dogecoin", "#DOGE"), ("sec ", "#SEC"), ("federal reserve", "#FED"),
    ("fomc", "#FED"), ("etf", "#ETF"), ("stablecoin", "#Stablecoin"),
    ("tether", "#USDT"),
]

STOPWORDS = {
    "the", "a", "an", "to", "of", "in", "on", "for", "and", "is", "as", "at",
    "by", "with", "after", "amid", "its", "it", "this", "that", "are", "be",
    "from", "new", "says", "could", "will", "has", "have", "into", "up",
    "down", "over", "out", "what", "why", "how",
}


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
    return any(kw in text_lower for kw in CRYPTO_RELEVANCE_KEYWORDS)


def is_breaking(text_lower):
    return any(kw in text_lower for kw in BREAKING_KEYWORDS)


def tags_for(text_lower):
    tags = []
    for kw, tag in TICKER_TAGS:
        if kw in text_lower and tag not in tags:
            tags.append(tag)
    return tags[:5]


def escape_html(text):
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_message(source, title, link, tags, breaking):
    prefix = "\U0001F6A8 <b>BREAKING</b>\n" if breaking else ""
    tag_line = f"\n{' '.join(tags)}" if tags else ""
    return (
        f"{prefix}<b>{escape_html(title)}</b>\n"
        f"\U0001F4F0 {escape_html(source)}{tag_line}\n"
        f"{link}"
    )


def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = requests.post(
        url,
        json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        },
        timeout=15,
    )
    if resp.status_code == 429:
        retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
        time.sleep(retry_after + 1)
        resp = requests.post(
            url,
            json={
                "chat_id": CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            },
            timeout=15,
        )
    if not resp.ok:
        print(f"Telegram send failed: {resp.status_code} {resp.text}", file=sys.stderr)
    return resp.ok


def fetch_entries():
    collected = []
    for source, url in ALL_FEEDS.items():
        category = "crypto" if source in CRYPTO_FEEDS else "macro"
        try:
            parsed = feedparser.parse(url)
        except Exception as e:
            print(f"Failed to fetch {source}: {e}", file=sys.stderr)
            continue
        for entry in parsed.entries[:20]:
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            if not title or not link:
                continue
            summary = entry.get("summary", "") or ""
            text_lower = f"{title} {summary}".lower()
            collected.append({
                "source": source,
                "category": category,
                "title": title,
                "link": link,
                "text_lower": text_lower,
            })
    return collected


def main():
    state = load_state()
    state.setdefault("items", [])
    prune_state(state)

    entries = fetch_entries()
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

        record = {"hash": h, "tokens": list(tokens), "ts": time.time()}
        state["items"].append(record)
        seen_hashes.add(h)

        if bootstrap:
            continue  # first run: build state silently, don't flood the channel

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
        message = format_message(entry["source"], entry["title"], entry["link"], tags, breaking)
        if send_telegram(message):
            sent += 1
            time.sleep(SEND_DELAY_SECONDS)

    save_state(state)
    print(f"Run complete: {len(entries)} fetched, {sent} sent.")


if __name__ == "__main__":
    main()
