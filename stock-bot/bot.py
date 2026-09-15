"""
Stock Bot for Telegram Groups - 纯文字卡片版（无K线图）
监听 #TSLA #AAPL 等股票代码，返回行情卡片
数据源: Finnhub (报价+基本面+新闻) + 东方财富 (中文新闻)
"""

from __future__ import annotations
import os, re, logging, datetime, json, subprocess, time, sqlite3, threading

# 使用系统代理（Clash），不强制直连
import asyncio
from concurrent.futures import ThreadPoolExecutor
import requests
import finnhub

EXECUTOR = ThreadPoolExecutor(max_workers=10)
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters, ContextTypes
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest
from telegram.error import RetryAfter

# ─── TTL 缓存（防止高并发重复打 API）────────────────────────────────
_cache_lock    = threading.Lock()
_stock_cache: dict[str, tuple[float, dict]] = {}   # ticker → (ts, data)
_news_cache:  dict[str, tuple[float, list]] = {}   # ticker → (ts, news)
STOCK_CACHE_TTL = 180   # 3 分钟：行情数据
NEWS_CACHE_TTL  = 300   # 5 分钟：新闻

# asyncio 层：同 ticker 并发去重（只发一次真实 API 请求）
_ticker_fetch_events: dict[str, asyncio.Event] = {}
_ticker_fetch_lock = asyncio.Lock()   # 保护 _ticker_fetch_events 字典本身
# ─────────────────────────────────────────────────────────────────────

# ─── 配置区 ──────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN")
FINNHUB_API_KEY    = os.getenv("FINNHUB_API_KEY",    "YOUR_FINNHUB_KEY")
# 代理：固定用 http://127.0.0.1:12000，不读环境变量（plist 注入的 socks5://7890 不可用）
# 迁移到服务器后改为空字符串: _PROXY = ""
_PROXY = "http://127.0.0.1:12000"
BINANCE_INVITE_URL = os.getenv("BINANCE_INVITE_URL", "https://www.binance.com")
# 底部按钮链接（待填写）
URL_OPEN_ACCOUNT  = os.getenv("URL_OPEN_ACCOUNT",  "https://www.binance.com")  # 美股开户
URL_BUY_TUTORIAL  = os.getenv("URL_BUY_TUTORIAL",  "https://www.binance.com")  # 购买教程
URL_GET_BONUS     = os.getenv("URL_GET_BONUS",     "https://www.binance.com")  # 领取福利
STATS_DB          = os.getenv("STATS_DB", "stats.db")                          # 统计数据库
ADMIN_USER_ID     = int(os.getenv("ADMIN_USER_ID", "0"))                          # 管理员 TG ID
# ─────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─── 统计数据库 ──────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(STATS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS queries (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id    TEXT,
            chat_title TEXT,
            ticker     TEXT,
            ts         INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS push_sent_msgs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            push_type  TEXT,
            chat_id    TEXT,
            message_id INTEGER,
            sent_at    INTEGER
        )
    """)
    conn.commit()
    conn.close()
    logger.info(f"Stats DB ready: {STATS_DB}")


def _get_daily_pushed_date() -> str:
    conn = sqlite3.connect(STATS_DB)
    row = conn.execute("SELECT value FROM config WHERE key='daily_pushed_date'").fetchone()
    conn.close()
    return row[0] if row else ""


def _set_daily_pushed_date(date_str: str):
    conn = sqlite3.connect(STATS_DB)
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('daily_pushed_date', ?)", (date_str,))
    conn.commit()
    conn.close()


def _save_push_msg(push_type: str, chat_id: int, message_id: int):
    conn = sqlite3.connect(STATS_DB)
    conn.execute(
        "INSERT INTO push_sent_msgs (push_type, chat_id, message_id, sent_at) VALUES (?,?,?,?)",
        (push_type, str(chat_id), message_id, int(time.time()))
    )
    conn.commit()
    conn.close()


def _fetch_blockbeats_all() -> list:
    """从律动财经拉取24h新闻，返回 [{title, link}, ...]"""
    try:
        BLOCKBEATS_KEY = os.getenv("BLOCKBEATS_KEY", "YOUR_BLOCKBEATS_KEY")
        _proxy = _PROXY
        _proxy_args = ["--proxy", _proxy] if _proxy else []
        r = subprocess.run(
            ["curl", "-s", "--max-time", "15",
             "-H", f"api-key: {BLOCKBEATS_KEY}"] + _proxy_args +
            ["https://api-pro.theblockbeats.info/v1/newsflash/24h?lang=cn"],
            capture_output=True, text=True, timeout=18
        )
        d = json.loads(r.stdout)
        items = d.get("data", {}).get("data", []) or []
        results, seen = [], set()
        for item in items[:200]:
            title = (item.get("title") or item.get("content") or "").strip()
            if title and title not in seen:
                seen.add(title)
                results.append({"title": title, "link": item.get("link", "")})
        return results
    except Exception as e:
        logger.warning(f"[daily_push] blockbeats error: {e}")
        return []


_MACRO_KW  = ["美联储","CPI","通胀","利率","就业","GDP","降息","加息","财政","特朗普","关税","美债","国债","经济","衰退","鲍威尔","议息","非农","零售","贸易战","制裁","日元","欧央行","黄金","原油","石油","宏观"]
_STOCK_KW  = ["股","纳斯达克","标普","道琼斯","英伟达","苹果","特斯拉","微软","谷歌","亚马逊","Meta","NYSE","IPO","收购","财报","营收","净利","NVIDIA","Apple","Tesla","美股","上市","华尔街","AI芯片"]
_CRYPTO_KW = ["BTC","ETH","比特币","以太坊","代币","链上","矿工","DEX","DeFi","NFT","USDT","加密","巨鲸","币安","交易所","USDC","稳定币","山寨","Meme","空投","合约","Web3","区块链","Coinbase","Solana","SOL"]


def _classify_news(title: str) -> str:
    t = title.upper()
    is_macro  = any(k.upper() in t for k in _MACRO_KW)
    is_stock  = any(k.upper() in t for k in _STOCK_KW)
    is_crypto = any(k.upper() in t for k in _CRYPTO_KW)
    if is_macro and not is_crypto: return "宏观"
    if is_stock and not is_crypto: return "美股"
    if is_crypto:                  return "币圈"
    if is_stock:                   return "美股"
    return "其他"


async def _get_push_targets_self(app) -> list:
    """自用版推送目标：所有出现过查询记录的群（不限人数）"""
    conn = sqlite3.connect(STATS_DB)
    rows = conn.execute(
        "SELECT DISTINCT chat_id FROM queries WHERE CAST(chat_id AS INTEGER) < 0"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


async def _daily_news_push_job(app):
    """每日10:00推送10条精选新闻（宏观3+美股3+币圈4）"""
    chats = await _get_push_targets_self(app)
    if not chats:
        logger.info("[daily_push] no target chats, skip")
        return
    all_items = await asyncio.get_event_loop().run_in_executor(EXECUTOR, _fetch_blockbeats_all)
    buckets = {"宏观": [], "美股": [], "币圈": []}
    for item in all_items:
        cat = _classify_news(item["title"])
        if cat in buckets:
            buckets[cat].append(item)
    targets = {"宏观": 3, "美股": 3, "币圈": 4}
    selected = {cat: buckets[cat][:n] for cat, n in targets.items()}
    total = sum(len(v) for v in selected.values())
    # 不足10条时从剩余新闻补齐
    if total < 10:
        used = set(i["title"] for its in selected.values() for i in its)
        for item in all_items:
            if total >= 10: break
            if item["title"] not in used:
                selected["币圈"].append(item)
                used.add(item["title"]); total += 1
    if not any(selected.values()):
        logger.warning("[daily_push] no news fetched")
        return
    today = datetime.datetime.now().strftime("%m月%d日")
    cat_emoji = {"宏观": "🌐", "美股": "📈", "币圈": "🪙"}
    cat_label = {"宏观": "宏观", "美股": "美股", "币圈": "加密货币"}
    lines = [f"📰 <b>{today} 热点资讯日报</b>\n"]
    idx = 1
    for cat in ["宏观", "美股", "币圈"]:
        its = selected[cat]
        if not its: continue
        lines.append(f"\n{cat_emoji[cat]} <b>{cat_label[cat]}</b>")
        for i in its:
            lk = i.get("link", "")
            lines.append(f'{idx}. <a href="{lk}">{i["title"]}</a>' if lk else f'{idx}. {i["title"]}')
            idx += 1
    lines.append("\n📊 数据来源：律动财经")
    msg = "\n".join(lines)
    from telegram.error import RetryAfter as _RetryAfter
    n = len(chats)
    interval = min(60.0 / max(n, 1), 3.0)
    for chat_id in chats:
        for attempt in range(4):
            try:
                sent = await app.bot.send_message(
                    chat_id=int(chat_id), text=msg,
                    parse_mode="HTML", disable_web_page_preview=True
                )
                if sent:
                    _save_push_msg("daily", int(chat_id), sent.message_id)
                break
            except _RetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except Exception as e:
                logger.warning(f"[daily_push] chat={chat_id}: {e}")
                break
        await asyncio.sleep(interval)
    logger.info(f"[daily_push] done, pushed to {len(chats)} chats")


async def _scheduler(app):
    while True:
        try:
            now = datetime.datetime.now()
            today_str = now.strftime("%Y-%m-%d")
            if now.hour == 10 and now.minute == 0:
                if _get_daily_pushed_date() != today_str:
                    _set_daily_pushed_date(today_str)
                    logger.info("[scheduler] 10:00 daily news push")
                    await _daily_news_push_job(app)
        except Exception as e:
            logger.warning(f"[scheduler] error: {e}")
        await asyncio.sleep(60)


async def _post_init(app):
    asyncio.create_task(_scheduler(app))


def record_query(chat_id: str, chat_title: str, ticker: str):
    try:
        conn = sqlite3.connect(STATS_DB)
        conn.execute(
            "INSERT INTO queries (chat_id, chat_title, ticker, ts) VALUES (?,?,?,?)",
            (str(chat_id), chat_title or "私聊", ticker.upper(), int(time.time()))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"record_query error: {e}")

# ── 加密货币白名单（市值 Top100 常见）──
# key = 大写 symbol，value = 中文名
CRYPTO_NAMES: dict[str, str] = {
    "BTC": "比特币", "ETH": "以太坊", "BNB": "币安币", "SOL": "Solana",
    "XRP": "瑞波币", "USDC": "USDC", "ADA": "艾达币", "AVAX": "雪崩",
    "DOGE": "狗狗币", "TRX": "波场", "DOT": "波卡", "LINK": "Chainlink",
    "TON": "Toncoin", "MATIC": "Polygon", "POL": "Polygon", "SHIB": "柴犬币",
    "LTC": "莱特币", "BCH": "比特币现金", "UNI": "Uniswap", "ATOM": "Cosmos",
    "XLM": "恒星币", "OKB": "OKB", "ETC": "以太坊经典", "HBAR": "Hedera",
    "APT": "Aptos", "ARB": "Arbitrum", "OP": "Optimism", "SUI": "Sui",
    "FIL": "Filecoin", "VET": "唯链", "GRT": "The Graph", "ALGO": "Algorand",
    "AAVE": "Aave", "MKR": "Maker", "SAND": "The Sandbox", "MANA": "Decentraland",
    "AXS": "Axie", "FTM": "Fantom", "NEAR": "NEAR", "FLR": "Flare",
    "INJ": "Injective", "IMX": "Immutable X", "STX": "Stacks", "RUNE": "THORChain",
    "EOS": "EOS", "CAKE": "PancakeSwap", "FLOW": "Flow", "EGLD": "MultiversX",
    "CHZ": "Chiliz", "MINA": "Mina", "ZEC": "Zcash", "DASH": "Dash",
    "BAT": "Basic Attention Token", "1INCH": "1inch", "CRV": "Curve",
    "LDO": "Lido", "SNX": "Synthetix", "ENJ": "Enjin", "ZIL": "Zilliqa",
    "KAVA": "Kava", "ROSE": "Oasis", "OCEAN": "Ocean Protocol",
    "PEPE": "PEPE", "WIF": "dogwifhat", "BONK": "Bonk", "FLOKI": "Floki",
    "JUP": "Jupiter", "PYTH": "Pyth", "W": "Wormhole", "STRK": "Starknet",
    "NOT": "Notcoin", "DOGS": "DOGS", "CATI": "Catizen", "HMSTR": "Hamster",
}
# symbol 集合，用于快速判断
_CRYPTO_SYMBOLS: set[str] = set(CRYPTO_NAMES.keys())

# 中文加密别名 → symbol
CRYPTO_CN_MAP: dict[str, str] = {
    v: k for k, v in CRYPTO_NAMES.items() if v not in ("USDC", "OKB")
}
# 补充常见口语别名
CRYPTO_CN_MAP.update({
    "比特": "BTC", "以太": "ETH", "以太坊": "ETH", "币安链": "BNB",
    "狗狗": "DOGE", "柴犬": "SHIB", "莱特": "LTC", "波卡": "DOT",
    "波场": "TRX", "瑞波": "XRP", "艾达": "ADA",
})

TICKER_NAMES = {
    "TSLA": "特斯拉", "AAPL": "苹果", "NVDA": "英伟达", "MSFT": "微软",
    "GOOGL": "谷歌", "AMZN": "亚马逊", "META": "Meta", "NFLX": "奈飞",
    "AMD": "AMD", "BABA": "阿里巴巴", "PDD": "拼多多", "JD": "京东",
    "BIDU": "百度", "NIO": "蔚来", "XPEV": "小鹏", "LI": "理想",
    "MSTR": "MicroStrategy", "COIN": "Coinbase", "HOOD": "Robinhood",
    "SPY": "标普500 ETF", "QQQ": "纳指100 ETF",
}


# ───────────────────────────────────────────────────────
# 加密货币模块
# ───────────────────────────────────────────────────────

_COINGECKO_ID_MAP: dict[str, str] = {
    "BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin", "SOL": "solana",
    "XRP": "ripple", "USDC": "usd-coin", "ADA": "cardano", "AVAX": "avalanche-2",
    "DOGE": "dogecoin", "TRX": "tron", "DOT": "polkadot", "LINK": "chainlink",
    "TON": "the-open-network", "MATIC": "matic-network", "POL": "matic-network",
    "SHIB": "shiba-inu", "LTC": "litecoin", "BCH": "bitcoin-cash",
    "UNI": "uniswap", "ATOM": "cosmos", "XLM": "stellar", "ETC": "ethereum-classic",
    "HBAR": "hedera-hashgraph", "APT": "aptos", "ARB": "arbitrum",
    "OP": "optimism", "SUI": "sui", "FIL": "filecoin", "VET": "vechain",
    "GRT": "the-graph", "ALGO": "algorand", "AAVE": "aave", "MKR": "maker",
    "NEAR": "near", "INJ": "injective-protocol", "STX": "blockstack",
    "RUNE": "thorchain", "EOS": "eos", "CAKE": "pancakeswap-token",
    "FLOW": "flow", "LDO": "lido-dao", "CRV": "curve-dao-token",
    "PEPE": "pepe", "WIF": "dogwifcoin", "BONK": "bonk",
    "JUP": "jupiter-exchange-solana", "NOT": "notcoin",
}


def _curl_get_json(url: str) -> dict | list:
    """curl 请求返回 JSON，自动带代理"""
    _proxy = _PROXY
    _proxy_args = ["--proxy", _proxy] if _proxy else []
    cmd = ["curl", "-s", "--max-time", "10",
           "-H", "User-Agent: Mozilla/5.0",
           "-H", "Accept: application/json"
           ] + _proxy_args + [url]
    raw = subprocess.run(cmd, capture_output=True, text=True, timeout=12).stdout
    return json.loads(raw)


def get_crypto_data(symbol: str) -> dict | None:
    """加密货币行情：币安 API 实时报价 + CoinGecko 市值/排名"""
    symbol = symbol.upper()
    pair   = symbol + "USDT"
    try:
        # 币安 24h 行情
        ticker_url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={pair}"
        d = _curl_get_json(ticker_url)
        if "code" in d:          # 不支持 USDT 对，试 BUSD
            raise ValueError(f"binance ticker error: {d}")
        price      = float(d.get("lastPrice", 0))
        if not price:
            raise ValueError("price=0")
        prev_close = float(d.get("prevClosePrice", 0))
        change     = price - prev_close
        change_pct = float(d.get("priceChangePercent", 0))
        high_day   = float(d.get("highPrice", 0))
        low_day    = float(d.get("lowPrice", 0))
        amplitude  = ((high_day - low_day) / prev_close * 100) if prev_close else 0
        volume_24h = float(d.get("quoteVolume", 0))  # USDT 成交额
    except Exception as e:
        logger.error(f"[crypto] binance ticker error {symbol}: {e}")
        return None

    # CoinGecko 市值 + 排名
    market_cap = 0
    rank       = None
    cg_id      = _COINGECKO_ID_MAP.get(symbol)
    if cg_id:
        try:
            cg_url = f"https://api.coingecko.com/api/v3/coins/{cg_id}?localization=false&tickers=false&community_data=false&developer_data=false"
            cg = _curl_get_json(cg_url)
            mkt = cg.get("market_data", {})
            market_cap = mkt.get("market_cap", {}).get("usd", 0)
            rank       = cg.get("market_cap_rank")
        except Exception as e:
            logger.warning(f"[crypto] coingecko error {symbol}: {e}")

    name = CRYPTO_NAMES.get(symbol, symbol)
    return {
        "type":       "crypto",
        "ticker":     symbol,
        "name":       name,
        "price":      price,
        "change":     change,
        "change_pct": change_pct,
        "high_day":   high_day,
        "low_day":    low_day,
        "amplitude":  amplitude,
        "volume_24h": volume_24h,
        "market_cap": market_cap,
        "rank":       rank,
        "currency":   "USD",
    }


# 加密币种新闻匹配关键词表
# 加密币新闻关键词（正则匹配，避免误匹配）
_CRYPTO_NEWS_KEYWORDS: dict[str, list[str]] = {
    "BTC":  [r"\bbitcoin\b", r"\bbtc\b"],
    "ETH":  [r"\bethereum\b", r"\bether\b", r"\beth\s", r"\beth,", r"\beth\.\s"],
    "SOL":  [r"\bsolana\b", r"\bsol\s", r"\bsol,", r"\bsol\."],
    "BNB":  [r"\bbnb\b", r"binance\s+chain", r"\bbsc\b"],
    "XRP":  [r"\bripple\b", r"\bxrp\b"],
    "DOGE": [r"\bdogecoin\b", r"\bdoge\b"],
    "ADA":  [r"\bcardano\b", r"\bada\b"],
    "AVAX": [r"\bavalanche\b", r"\bavax\b"],
    "DOT":  [r"\bpolkadot\b", r"\bdot\s"],
    "TRX":  [r"\btron\b", r"\btrx\b"],
    "LINK": [r"\bchainlink\b"],
    "TON":  [r"\btoncoin\b", r"\bton\s"],
    "LTC":  [r"\blitecoin\b", r"\bltc\b"],
    "UNI":  [r"\buniswap\b"],
    "SHIB": [r"\bshiba\b", r"\bshib\b"],
    "PEPE": [r"\bpepe\b"],
    "NEAR": [r"\bnear\s+protocol\b", r"\bnear\b"],
    "APT":  [r"\baptos\b"],
    "ARB":  [r"\barbitrum\b"],
    "OP":   [r"\boptimism\b"],
    "SUI":  [r"\bsui\b"],
    "AAVE": [r"\baave\b"],
    "INJ":  [r"\binjective\b"],
    "STX":  [r"\bstacks\b", r"\bstx\b"],
    "WIF":  [r"\bdogwifhat\b", r"\bwif\b"],
    "FET":  [r"\bfetch\.ai\b", r"\bfet\b"],
    "JUP":  [r"\bjupiter\b"],
    "HYPE": [r"\bhyperliquid\b"],
}

# 并发拉取 RSS 缓存（每 10 分钟更新一次）
_rss_cache: dict = {"ts": 0, "items": []}
_RSS_SOURCES = [
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    "https://www.theblock.co/rss.xml",
    "https://cryptopotato.com/feed/",
    "https://www.newsbtc.com/feed/",
    "https://ambcrypto.com/feed/",
]

# BlockBeats 中文快讯缓存
_bb_cache: dict = {"ts": 0, "items": []}


def _fetch_all_crypto_rss() -> list[tuple[str, str, float]]:
    """\u5e76发拉取所有 RSS 源，返回 (title, link, age_h) 列表，缓存 10 分钟"""
    from concurrent.futures import ThreadPoolExecutor
    from email.utils import parsedate_to_datetime
    global _rss_cache
    now_ts = int(time.time())
    if now_ts - _rss_cache["ts"] < 600:  # 10分钟内用缓存
        return _rss_cache["items"]

    _proxy = _PROXY
    _proxy_args = ["--proxy", _proxy] if _proxy else []

    def fetch_one(url):
        try:
            out = subprocess.run(
                ["curl", "-s", "--max-time", "8", "-H", "User-Agent: Mozilla/5.0"]
                + _proxy_args + [url],
                capture_output=True, text=True, timeout=10
            ).stdout
            items = re.findall(r"<item>(.*?)</item>", out, re.DOTALL)
            result = []
            for item in items:
                title_m = re.search(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", item, re.DOTALL)
                link_m  = re.search(r"<link>(.*?)</link>", item)
                pub_m   = re.search(r"<pubDate>(.*?)</pubDate>", item)
                if not title_m: continue
                title = re.sub(r"&[a-z]+;|&#\d+;", " ", title_m.group(1)).strip()
                link  = link_m.group(1).strip() if link_m else ""
                age_h = 999.0
                if pub_m:
                    try:
                        age_h = (now_ts - parsedate_to_datetime(pub_m.group(1).strip()).timestamp()) / 3600
                    except Exception:
                        pass
                if age_h <= 72:
                    result.append((title, link, age_h))
            return result
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=6) as ex:
        batches = list(ex.map(fetch_one, _RSS_SOURCES))
    all_items = [item for batch in batches for item in batch]
    _rss_cache = {"ts": now_ts, "items": all_items}
    logger.info(f"[rss_cache] 更新 {len(all_items)} 条")
    return all_items


def _fetch_blockbeats_news() -> list[tuple[str, str]]:
    """拉取律动 BlockBeats 24h 中文快讯，缓存 10 分钟，返回 (title, link) 列表"""
    global _bb_cache
    now_ts = int(time.time())
    if now_ts - _bb_cache["ts"] < 600:
        return _bb_cache["items"]
    try:
        _proxy = _PROXY
        _proxy_args = ["--proxy", _proxy] if _proxy else []
        _key = os.getenv("BLOCKBEATS_KEY", "")
        if not _key:
            return []
        out = subprocess.run(
            ["curl", "-s", "--max-time", "10",
             "-H", f"api-key: {_key}",
             "-H", "User-Agent: Mozilla/5.0"]
            + _proxy_args
            + ["https://api-pro.theblockbeats.info/v1/newsflash/24h?lang=cn"],
            capture_output=True, text=True, timeout=12
        ).stdout
        d = json.loads(out)
        raw = (d.get("data") or {}).get("data") or []
        items = [(i.get("title", ""), i.get("link", "") or i.get("url", "")) for i in raw if i.get("title")]
        _bb_cache = {"ts": now_ts, "items": items}
        logger.info(f"[blockbeats] 缓存更新 {len(items)} 条")
        return items
    except Exception as e:
        logger.warning(f"[blockbeats] 拉取失败: {e}")
        return _bb_cache["items"]  # 降级用旧缓存


# 各币种中文关键词（用于 BlockBeats 快讯过滤）
_CRYPTO_CN_KEYWORDS: dict[str, list[str]] = {
    "BTC":  ["比特币", "BTC"],
    "ETH":  ["以太坊", "以太", "ETH"],
    "SOL":  ["Solana", "SOL", "索拉纳"],
    "BNB":  ["BNB", "币安链", "BSC"],
    "XRP":  ["XRP", "瑞波", "Ripple"],
    "DOGE": ["狗狗币", "DOGE", "Dogecoin"],
    "ADA":  ["卡尔达诺", "ADA", "Cardano"],
    "AVAX": ["雪崩", "AVAX", "Avalanche"],
    "TRX":  ["波场", "TRX", "TRON"],
    "DOT":  ["波卡", "DOT", "Polkadot"],
    "LINK": ["Chainlink", "LINK"],
    "LTC":  ["莱特币", "LTC", "Litecoin"],
    "TON":  ["TON", "Toncoin"],
    "UNI":  ["Uniswap", "UNI"],
    "SHIB": ["柴犬", "SHIB", "Shiba"],
    "PEPE": ["PEPE"],
    "SUI":  ["Sui", "SUI"],
    "APT":  ["Aptos", "APT"],
    "ARB":  ["Arbitrum", "ARB"],
    "OP":   ["Optimism", "OP"],
    "NEAR": ["NEAR", "Near Protocol"],
    "AAVE": ["Aave", "AAVE"],
    "INJ":  ["Injective", "INJ"],
    "STX":  ["Stacks", "STX"],
    "WIF":  ["WIF", "dogwifhat"],
    "JUP":  ["Jupiter", "JUP"],
    "HYPE": ["Hyperliquid", "HYPE"],
}


def _get_blockbeats_news(symbol: str) -> list[tuple[str, str, bool]]:
    """从 BlockBeats 24h 快讯中按币种关键词过滤，返回最多 3 条"""
    kws_cn  = _CRYPTO_CN_KEYWORDS.get(symbol, [symbol])
    kws_en  = _CRYPTO_NEWS_KEYWORDS.get(symbol, [])
    cn_name = CRYPTO_NAMES.get(symbol, "")
    if cn_name and cn_name not in kws_cn:
        kws_cn = kws_cn + [cn_name]

    all_items = _fetch_blockbeats_news()
    results = []
    for title, link in all_items:
        # 中英文关键词均匹配
        t_low = title.lower()
        if any(kw in title for kw in kws_cn) or any(re.search(kw, t_low) for kw in kws_en):
            results.append((f"[律动] {title[:75]}", link, False))
        if len(results) >= 3:
            break
    return results


def _get_crypto_rss_news(symbol: str) -> list[tuple[str, str, bool]]:
    """从多源英文 RSS 过滤加密币新闻（作为补充）"""
    kws = _CRYPTO_NEWS_KEYWORDS.get(symbol, [r"\b" + re.escape(symbol.lower()) + r"\b"])
    cn  = CRYPTO_NAMES.get(symbol, "").lower()
    if cn:
        kws = list(kws) + [re.escape(cn)]

    all_items = _fetch_all_crypto_rss()
    results = []
    for title, link, age_h in sorted(all_items, key=lambda x: x[2]):
        t_low = title.lower()
        if not any(re.search(kw, t_low) for kw in kws):
            continue
        zh = translate_to_zh(title)
        display = zh if zh else title[:80]
        results.append((display, link, bool(zh)))
        if len(results) >= 3:
            break
    return results


def get_crypto_news(symbol: str) -> list[tuple[str, str, bool]]:
    """加密货币新闻：币安（1条）+ 律动BlockBeats中文（优先）+ 英文RSS（补充）"""
    # 1. 币安新闻（tradingPairs 标签匹配）
    binance_news = []
    kws = _CRYPTO_NEWS_KEYWORDS.get(symbol, [r"\b" + re.escape(symbol.lower()) + r"\b"])
    cn  = CRYPTO_NAMES.get(symbol, "").lower()
    if cn:
        kws = list(kws) + [re.escape(cn)]
    raw = get_news_binance(symbol)
    for item in raw:
        if any(re.search(kw, item[0].lower()) for kw in kws):
            binance_news.append(item)
        if len(binance_news) >= 1:
            break

    # 2. 律动 BlockBeats 中文快讯（主力来源）
    bb_news = _get_blockbeats_news(symbol)

    # 3. 英文 RSS（补充到3条）
    rss_news = _get_crypto_rss_news(symbol)

    # 组合：币安1条 + 律动优先补充 + RSS兜底，共3条
    final = binance_news[:1]
    seen  = {item[0][:30] for item in final}
    for item in bb_news + rss_news:
        if item[0][:30] not in seen:
            final.append(item)
            seen.add(item[0][:30])
        if len(final) >= 3:
            break
    return final


def build_crypto_message(data: dict, news: list[tuple[str, str]]) -> str:
    """Crypto 行情卡片格式"""
    arrow  = "🔴" if data["change"] < 0 else "🟢"
    sign   = "+" if data["change"] >= 0 else ""
    mc_str = fmt(data["market_cap"]) if data["market_cap"] else "N/A"
    rank_str = f"  #{data['rank']}" if data.get("rank") else ""
    vol_str  = fmt(data["volume_24h"]) if data["volume_24h"] else "N/A"

    # 价格格式：< 0.01 用科学计数法显示
    p = data["price"]
    if p >= 1:
        price_str = f"{p:,.2f}"
        chg_str   = f"{sign}{data['change']:,.2f}"
    elif p >= 0.0001:
        price_str = f"{p:.6f}"
        chg_str   = f"{sign}{data['change']:.6f}"
    else:
        price_str = f"{p:.2e}"
        chg_str   = f"{sign}{data['change']:.2e}"

    news_lines = ""
    for i, item in enumerate(news[:3], 1):
        title, link = item[0], item[1]
        is_translated = item[2] if len(item) > 2 else False
        if link and is_translated:
            news_lines += f'  {i}. {he(title)} <a href="{link}">[英文原文]</a>\n'
        elif link:
            news_lines += f'  {i}. <a href="{link}">{he(title)}</a>\n'
        else:
            news_lines += f"  {i}. {he(title)}\n"
    if not news_lines:
        news_lines = "  暂无相关新闻\n"

    return (
        f"📊 <b>{data['ticker']}</b> · {he(data['name'])}{rank_str}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{arrow} <b>${price_str}</b>   "
        f"<code>{chg_str} ({sign}{data['change_pct']:.2f}%)</code>\n\n"
        f"💰 市值: <code>{he(mc_str)}</code>\n"
        f"📦 24H区间: <code>${data['low_day']:,.4g} — ${data['high_day']:,.4g}</code>  振幅 <code>{data['amplitude']:.1f}%</code>\n"
        f"📉 24H成交额: <code>{vol_str}</code>\n\n"
        f"📰 最新动态\n"
        f"{news_lines}"
    )


# ───────────────────────────────────────────────────────
def _finnhub_curl(path: str) -> dict:
    """用 curl 直接调 Finnhub REST API，绕过 Python SSL 问题"""
    _proxy = _PROXY
    _proxy_args = ["--proxy", _proxy] if _proxy else []
    url = f"https://api.finnhub.io/api/v1/{path}&token={FINNHUB_API_KEY}"
    cmd = ["curl", "-s", "--max-time", "8", "-H", "User-Agent: Mozilla/5.0"] + _proxy_args + [url]
    raw = subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout
    return json.loads(raw)


def is_hk_ticker(ticker: str) -> bool:
    """判断是否是港股（格式：4位数字.HK）"""
    return bool(re.match(r'^\d{4}\.HK$', ticker.upper()))


def get_stock_data_hk_yahoo(ticker: str) -> dict | None:
    """港股备用数据源：Yahoo Finance（走代理，gtimg 失败时使用）"""
    try:
        _proxy = _PROXY
        _proxy_args = ["--proxy", _proxy] if _proxy else []
        cmd = ["curl", "-s", "--max-time", "10",
               "-H", "User-Agent: Mozilla/5.0"] + _proxy_args + [
               f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"]
        raw = subprocess.run(cmd, capture_output=True, text=True, timeout=12).stdout
        d = json.loads(raw)
        result = d["chart"]["result"][0]
        meta = result["meta"]
        price = meta.get("regularMarketPrice", 0)
        if not price:
            return None
        prev       = meta.get("previousClose") or meta.get("chartPreviousClose") or price
        high_day   = meta.get("regularMarketDayHigh", price)
        low_day    = meta.get("regularMarketDayLow", price)
        week52_high = meta.get("fiftyTwoWeekHigh", 0)
        week52_low  = meta.get("fiftyTwoWeekLow", 0)
        market_cap  = meta.get("marketCap", 0)
        change      = price - prev
        change_pct  = (change / prev * 100) if prev else 0
        amplitude   = ((high_day - low_day) / prev * 100) if prev else 0

        # 名称
        name = ticker
        for cn, sym2 in CN_TO_TICKER.items():
            if sym2 == ticker.upper():
                name = cn
                break

        if week52_high and week52_low and week52_high != week52_low:
            position = (price - week52_low) / (week52_high - week52_low) * 100
            if position >= 80:   trend = "📈 接近52周高点"
            elif position <= 20: trend = "📉 接近52周低点"
            elif position >= 50: trend = "↗️ 处于年内中高位"
            else:                trend = "↘️ 处于年内中低位"
            pos_str = f"{position:.0f}%"
        else:
            trend   = "—"
            pos_str = "N/A"

        tech = get_technical_signals(ticker, None, price)

        return {
            "ticker":      ticker,
            "name":        name,
            "price":       price,
            "change":      change,
            "change_pct":  change_pct,
            "high_day":    high_day,
            "low_day":     low_day,
            "amplitude":   amplitude,
            "market_cap":  market_cap,
            "week52_high": week52_high,
            "week52_low":  week52_low,
            "pe_ratio":    None,
            "trend":       trend,
            "pos_str":     pos_str,
            "currency":    "HKD",
            **tech,
        }
    except Exception as e:
        logger.error(f"get_stock_data_hk_yahoo error: {e}")
        return None


def get_stock_data_hk(ticker: str) -> dict | None:
    """港股数据：腾讯财经 API（国内直连，有市值/PE/52周数据）
    ticker 格式: 0700.HK → 腾讯财经 sym: hk00700
    """
    try:
        code = ticker.split(".")[0].zfill(5)
        sym  = "hk" + code
        cmd  = ["curl", "-s", "--max-time", "5",
                "-H", "User-Agent: Mozilla/5.0",
                f"https://qt.gtimg.cn/q={sym}"]
        raw = subprocess.run(cmd, capture_output=True, timeout=7).stdout.decode("gbk", errors="replace")
        import re as _re
        m = _re.search(r'"([^"]+)"', raw)
        if not m:
            logger.warning(f"[HK] gtimg no data for {ticker}, trying Yahoo fallback")
            return get_stock_data_hk_yahoo(ticker)
        parts = m.group(1).split("~")
        if len(parts) < 50:
            logger.warning(f"[HK] gtimg too few fields ({len(parts)}) for {ticker}, trying Yahoo fallback")
            return get_stock_data_hk_yahoo(ticker)

        name_cn     = parts[1]
        price       = float(parts[3])
        prev        = float(parts[4])
        high_day    = float(parts[33])
        low_day     = float(parts[34])
        mktcap_raw  = float(parts[37]) if parts[37] else 0   # 亿HKD
        pe_raw      = float(parts[39]) if parts[39] else None
        week52_high = float(parts[48]) if parts[48] else 0
        week52_low  = float(parts[49]) if parts[49] else 0

        market_cap  = mktcap_raw * 1e8  # 亿 → 元

        change     = price - prev
        change_pct = (change / prev * 100) if prev else 0
        amplitude  = ((high_day - low_day) / prev * 100) if prev else 0

        name = name_cn
        for cn, sym2 in CN_TO_TICKER.items():
            if sym2 == ticker:
                name = cn
                break

        if week52_high and week52_low and week52_high != week52_low:
            position = (price - week52_low) / (week52_high - week52_low) * 100
            if position >= 80:   trend = "📈 接近52周高点"
            elif position <= 20: trend = "📉 接近52周低点"
            elif position >= 50: trend = "↗️ 处于年内中高位"
            else:                trend = "↘️ 处于年内中低位"
            pos_str = f"{position:.0f}%"
        else:
            trend   = "—"
            pos_str = "N/A"

        tech = get_technical_signals(ticker, None, price)

        return {
            "ticker":      ticker,
            "name":        name,
            "price":       price,
            "change":      change,
            "change_pct":  change_pct,
            "high_day":    high_day,
            "low_day":     low_day,
            "amplitude":   amplitude,
            "market_cap":  market_cap,
            "week52_high": week52_high,
            "week52_low":  week52_low,
            "pe_ratio":    pe_raw,
            "trend":       trend,
            "pos_str":     pos_str,
            "currency":    "HKD",
            **tech,
        }
    except Exception as e:
        logger.error(f"get_stock_data_hk error: {e}", exc_info=True)
        return None

def get_stock_data(ticker: str) -> dict | None:
    if is_hk_ticker(ticker):
        return get_stock_data_hk(ticker)
    try:
        client = finnhub.Client(api_key=FINNHUB_API_KEY)

        # ── 实时报价（Python client 失败自动 curl fallback）──
        try:
            quote = client.quote(ticker)
        except Exception as e:
            logger.warning(f"quote python client failed ({e}), trying curl fallback")
            quote = _finnhub_curl(f"quote?symbol={ticker}")
        price = quote.get("c", 0)
        if not price:
            logger.error(f"quote empty for {ticker}: {quote}")
            return None

        prev       = quote.get("pc", 0)
        change     = price - prev
        change_pct = (change / prev * 100) if prev else 0
        high_day   = quote.get("h", 0)
        low_day    = quote.get("l", 0)
        amplitude  = ((high_day - low_day) / prev * 100) if prev else 0

        # ── 公司基本面（失败用默认值）──
        market_cap  = 0
        name        = TICKER_NAMES.get(ticker, ticker)
        try:
            try:
                profile = client.company_profile2(symbol=ticker)
            except Exception:
                profile = _finnhub_curl(f"stock/profile2?symbol={ticker}")
            market_cap = profile.get("marketCapitalization", 0) * 1e6
            name       = TICKER_NAMES.get(ticker, profile.get("name", ticker))
        except Exception as e:
            logger.warning(f"profile error (ignored): {e}")

        # ── 财务指标（失败用默认值）──
        week52_high = 0
        week52_low  = 0
        pe_ratio    = None
        try:
            try:
                metrics = client.company_basic_financials(ticker, "all")
            except Exception:
                metrics = _finnhub_curl(f"stock/metric?symbol={ticker}&metric=all")
            metric      = metrics.get("metric", {})
            week52_high = metric.get("52WeekHigh", 0)
            week52_low  = metric.get("52WeekLow", 0)
            pe_ratio    = metric.get("peExclExtraTTM") or metric.get("peTTM")
        except Exception as e:
            logger.warning(f"basic_financials error (ignored): {e}")

        # ── 52周位置 ──
        if week52_high and week52_low and week52_high != week52_low:
            position = (price - week52_low) / (week52_high - week52_low) * 100
            if position >= 80:   trend = "📈 接近52周高点"
            elif position <= 20: trend = "📉 接近52周低点"
            elif position >= 50: trend = "↗️ 处于年内中高位"
            else:                trend = "↘️ 处于年内中低位"
            pos_str = f"{position:.0f}%"
        else:
            trend   = "—"
            pos_str = "N/A"

        # ── 技术指标（RSI / MA / MACD / 量比）──
        tech = get_technical_signals(ticker, client, price)

        return {
            "ticker":      ticker,
            "name":        name,
            "price":       price,
            "change":      change,
            "change_pct":  change_pct,
            "high_day":    high_day,
            "low_day":     low_day,
            "amplitude":   amplitude,
            "market_cap":  market_cap,
            "week52_high": week52_high,
            "week52_low":  week52_low,
            "pe_ratio":    pe_ratio,
            "trend":       trend,
            "pos_str":     pos_str,
            **tech,
        }
    except Exception as e:
        logger.error(f"get_stock_data error: {e}")
        return None


# ─── 技术指标计算辅助函数 ──────────────────────────────────────

def _sma(values: list, n: int):
    if len(values) < n:
        return None
    return sum(values[-n:]) / n


def _ema(values: list, n: int) -> list:
    """返回 EMA 序列（长度 = len(values) - n + 1）"""
    if len(values) < n:
        return []
    k = 2.0 / (n + 1)
    result = [sum(values[:n]) / n]
    for v in values[n:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


def _rsi(closes: list, period: int = 14):
    if len(closes) < period + 2:
        return None
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    avg_g  = sum(max(d, 0) for d in deltas[-period:]) / period
    avg_l  = sum(max(-d, 0) for d in deltas[-period:]) / period
    if avg_l == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_g / avg_l), 1)


def get_technical_signals(ticker: str, client, price: float) -> dict:
    """用 Yahoo Finance curl 拿日线，本地算 RSI/MA/MACD/量比"""
    out = {"rsi_line": "", "macd_line": "", "ma_line": "", "vol_line": ""}
    try:
        _proxy = _PROXY
        _proxy_args = ["--proxy", _proxy] if _proxy else []
        cmd = ["curl", "-s", "--max-time", "10",
               "-H", "User-Agent: Mozilla/5.0"] + _proxy_args + [
               f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=3mo"]
        raw = subprocess.run(cmd, capture_output=True, text=True, timeout=12).stdout
        logger.info(f"[tech] {ticker} yahoo raw len={len(raw)}")
        d = json.loads(raw)
        q = d["chart"]["result"][0]["indicators"]["quote"][0]
        closes  = [x for x in q.get("close", [])  if x is not None]
        volumes = [x for x in q.get("volume", []) if x is not None]
        logger.info(f"[tech] {ticker} closes={len(closes)} volumes={len(volumes)}")
        if len(closes) < 20:
            logger.warning(f"[tech] {ticker} not enough closes ({len(closes)})")
            return out

        # ── RSI(14) ──
        rsi = _rsi(closes, 14)
        if rsi is not None:
            if rsi >= 70:
                out["rsi_line"] = f"RSI(14): <code>{rsi:.0f}</code> ⚠️ 超买区间"
            elif rsi <= 30:
                out["rsi_line"] = f"RSI(14): <code>{rsi:.0f}</code> 💡 超卖区间"
            elif rsi >= 60:
                out["rsi_line"] = f"RSI(14): <code>{rsi:.0f}</code> 偏强"
            elif rsi <= 40:
                out["rsi_line"] = f"RSI(14): <code>{rsi:.0f}</code> 偏弱"
            else:
                out["rsi_line"] = f"RSI(14): <code>{rsi:.0f}</code> 中性"

        # ── MA20 / MA50 ──
        ma20 = _sma(closes, 20)
        ma50 = _sma(closes, 50) if len(closes) >= 50 else None
        if ma20:
            above20 = price > ma20
            if ma50:
                above50 = price > ma50
                golden  = ma20 > ma50
                if above20 and above50 and golden:
                    out["ma_line"] = "均线: 站上MA20/50 多头排列 ✅"
                elif above20 and above50:
                    out["ma_line"] = "均线: 站上MA20/50"
                elif above20 and not above50:
                    out["ma_line"] = "均线: 站上MA20 未突破MA50"
                elif not above20 and above50:
                    out["ma_line"] = "均线: 跌破MA20 守住MA50"
                else:
                    out["ma_line"] = "均线: 跌破MA20/50 空头排列 ⚠️"
            else:
                out["ma_line"] = f"均线: {'站上' if above20 else '跌破'} MA20"

        # ── MACD(12,26,9) ──
        if len(closes) >= 35:
            ema12 = _ema(closes, 12)
            ema26 = _ema(closes, 26)
            off   = len(ema12) - len(ema26)
            macd  = [ema12[i + off] - ema26[i] for i in range(len(ema26))]
            if len(macd) >= 10:
                sig9 = _ema(macd, 9)
                if len(sig9) >= 2:
                    off2 = len(macd) - len(sig9)
                    hist = [macd[i + off2] - sig9[i] for i in range(len(sig9))]
                    m_c, m_p = macd[-1], macd[-2]
                    s_c, s_p = sig9[-1], sig9[-2]
                    h_c = hist[-1]
                    h_p = hist[-2] if len(hist) >= 2 else 0
                    if m_p < s_p and m_c > s_c:
                        out["macd_line"] = "MACD: 金叉信号 ↗️"
                    elif m_p > s_p and m_c < s_c:
                        out["macd_line"] = "MACD: 死叉信号 ↘️"
                    elif m_c > s_c:
                        out["macd_line"] = f"MACD: 多头{'增强 📈' if h_c > h_p else '走弱'}"
                    else:
                        out["macd_line"] = f"MACD: 空头{'增强 📉' if h_c < h_p else '走弱'}"

        # ── 成交量比（今日 vs 10日均量）──
        if len(volumes) >= 11:
            today_v = volumes[-1]
            avg_v   = sum(volumes[-11:-1]) / 10
            if avg_v > 0:
                ratio = today_v / avg_v
                if ratio >= 2.0:
                    out["vol_line"] = f"成交量: <code>{ratio:.1f}x</code> 均量 🔥 异动放量"
                elif ratio >= 1.3:
                    out["vol_line"] = f"成交量: <code>{ratio:.1f}x</code> 均量 温和放量"
                elif ratio <= 0.5:
                    out["vol_line"] = f"成交量: <code>{ratio:.1f}x</code> 均量 明显缩量"
                else:
                    out["vol_line"] = f"成交量: <code>{ratio:.1f}x</code> 均量"

    except Exception as e:
        logger.warning(f"technical_signals error for {ticker}: {e}", exc_info=True)
    return out


CN_MAP = {
    # 美股
    "TSLA": "特斯拉", "AAPL": "苹果", "NVDA": "英伟达", "MSFT": "微软",
    "GOOGL": "谷歌", "AMZN": "亚马逊", "META": "Meta", "NFLX": "奈飞",
    "AMD": "AMD", "BABA": "阿里巴巴", "PDD": "拼多多", "JD": "京东",
    "BIDU": "百度", "NIO": "蔚来", "XPEV": "小鹏", "LI": "理想汽车",
    # 港股
    "0700.HK": "腾讯", "9988.HK": "阿里巴巴", "1810.HK": "小米",
    "3690.HK": "美团", "9618.HK": "京东", "9888.HK": "百度",
    "9999.HK": "网易", "1024.HK": "快手", "1211.HK": "比亚迪",
    "2015.HK": "理想汽车", "9868.HK": "小鹏汽车", "9863.HK": "蔚来",
    "0939.HK": "建设银行", "1398.HK": "工商银行", "2318.HK": "中国平安",
    "0941.HK": "中国移动", "0883.HK": "中海油", "0386.HK": "中国石化",
    "0857.HK": "中国石油", "1177.HK": "中国生物制药", "2269.HK": "药明生物",
    "6862.HK": "海底捞", "2020.HK": "安踏体育", "0293.HK": "国泰航空",
    "1299.HK": "友邦保险", "0005.HK": "汇丰银行", "0388.HK": "港交所",
}

# 中文名 → ticker 反向映射（支持多个别名）
# 中文名 → ticker 直接映射
CN_TO_TICKER: dict[str, str] = {
    # 科技巨头
    "特斯拉": "TSLA",
    "苹果": "AAPL",
    "英伟达": "NVDA", "英伟": "NVDA",
    "微软": "MSFT",
    "谷歌": "GOOGL", "谷歌A": "GOOGL", "谷歌C": "GOOG",
    "亚马逊": "AMZN",
    "Meta": "META", "meta": "META", "脸书": "META",
    "奈飞": "NFLX", "网飞": "NFLX",
    "AMD": "AMD", "超微": "AMD",
    "英特尔": "INTC",
    "高通": "QCOM",
    "博通": "AVGO",
    "德州仪器": "TXN",
    "应用材料": "AMAT",
    "科磊": "KLAC",
    "泛林集团": "LRCX", "泛林": "LRCX",
    "台积电": "TSM", "台湾积电": "TSM",
    "美光": "MU", "美光科技": "MU",
    "安谋": "ARM", "ARM": "ARM",
    "闪迪": "SNDK",
    "西部数据": "WDC",
    "希捷": "STX",
    "赛默飞": "TMO",
    "甲骨文": "ORCL",
    "赛富时": "CRM", "Salesforce": "CRM",
    "奥多比": "ADBE", "Adobe": "ADBE",
    "思科": "CSCO",
    "IBM": "IBM",
    "惠普": "HPQ",
    "戴尔": "DELL",
    "云计算": "CRM",
    # 金融
    "摩根大通": "JPM", "摩根": "JPM",
    "美国银行": "BAC",
    "花旗": "C",
    "富国银行": "WFC",
    "高盛": "GS",
    "摩根士丹利": "MS",
    "伯克希尔": "BRK.B", "巴菲特": "BRK.B",
    "Visa": "V", "维萨": "V",
    "万事达": "MA",
    "贝莱德": "BLK",
    # 消费/零售
    "沃尔玛": "WMT",
    "好市多": "COST", "Costco": "COST",
    "耐克": "NKE",
    "麦当劳": "MCD",
    "星巴克": "SBUX",
    "可口可乐": "KO",
    "百事": "PEP",
    "迪士尼": "DIS",
    # 中概股
    "阿里巴巴": "BABA", "阿里": "BABA",
    "拼多多": "PDD",
    "京东": "JD",
    "百度": "BIDU",
    "蔚来": "NIO",
    "小鹏": "XPEV",
    "理想": "LI", "理想汽车": "LI",
    "腾讯": "TCEHY",
    "网易": "NTES",
    "爱奇艺": "IQ",
    "哔哩哔哩": "BILI", "B站": "BILI",
    "虎牙": "HUYA",
    "斗鱼": "DOYU",
    "携程": "TCOM",
    "新东方": "EDU",
    "好未来": "TAL",
    "滴滴": "DIDI",
    "满帮": "YMM",
    "贝壳": "BEKE",
    "小鹏汽车": "XPEV",
    "零跑": "LEAP",
    # 加密/金融科技
    "MicroStrategy": "MSTR",
    "Coinbase": "COIN", "coinbase": "COIN",
    "Robinhood": "HOOD", "robinhood": "HOOD",
    "PayPal": "PYPL", "贝宝": "PYPL",
    "Square": "SQ", "Block": "SQ",
    # ETF
    "标普": "SPY", "标普500": "SPY",
    "纳指": "QQQ", "纳斯达克": "QQQ",
    "道指": "DIA",
    # 其他热门
    "辉瑞": "PFE",
    "强生": "JNJ",
    "礼来": "LLY",
    "诺和诺德": "NVO",
    "优步": "UBER",
    "Lyft": "LYFT",
    "Airbnb": "ABNB", "爱彼迎": "ABNB",
    "Spotify": "SPOT",
    "推特": "X", "X": "X",
    "Snap": "SNAP", "色拉布": "SNAP",
    "Pinterest": "PINS",
    "Reddit": "RDDT",
    "OpenAI": "OPENAI",
    "Palantir": "PLTR", "大数据": "PLTR",
    "SpaceX": "SPCX",
    "波音": "BA",
    "洛克希德": "LMT",
    "埃克森": "XOM",
    "雪佛龙": "CVX",
    # 港股（格式：数字代码.HK）
    "腾讯": "0700.HK", "腾讯港股": "0700.HK",
    "阿里巴巴港股": "9988.HK", "阿里港股": "9988.HK",
    "小米": "1810.HK", "小米集团": "1810.HK",
    "美团": "3690.HK", "美团点评": "3690.HK",
    "京东港股": "9618.HK", "京东集团": "9618.HK",
    "百度港股": "9888.HK",
    "网易港股": "9999.HK",
    "快手": "1024.HK",
    "联想集团": "0992.HK", "联想": "0992.HK",
    "金蝶国际": "0268.HK",
    "比亚迪电子": "0285.HK",
    "京东健康": "6618.HK",
    "携程港股": "9961.HK", "携程集团": "9961.HK",
    "阿里健康": "0241.HK",
    "同程旅行": "0780.HK",
    "汽车之家港股": "2518.HK",
    "ASM太平洋": "0522.HK",
    "金山软件": "3888.HK",
    "华虹半导体": "1347.HK",
    "中芯国际": "0981.HK",
    "比亚迪": "1211.HK", "比亚迪港股": "1211.HK",
    "理想汽车港股": "2015.HK", "理想港股": "2015.HK",
    "小鹏汽车港股": "9868.HK", "小鹏港股": "9868.HK",
    "蔚来港股": "9863.HK",
    "极氪汽车": "2594.HK",
    "华润电力": "0836.HK",
    "龙源电力": "0916.HK",
    "中国电力": "2380.HK",
    "金风科技": "2208.HK",
    "汇丰银行": "0005.HK", "汇丰": "0005.HK",
    "恒生银行": "0011.HK", "恒生": "0011.HK",
    "港交所": "0388.HK", "香港交易所": "0388.HK",
    "建行": "0939.HK", "中国建设银行": "0939.HK",
    "工行": "1398.HK", "工商银行": "1398.HK",
    "农行": "1288.HK", "农业银行": "1288.HK",
    "中国银行港股": "3988.HK", "中行港股": "3988.HK",
    "中国平安": "2318.HK", "平安": "2318.HK",
    "中国人寿": "2628.HK", "国寿": "2628.HK",
    "新华保险": "1336.HK",
    "中国太保": "0966.HK",
    "友邦保险": "1299.HK", "友邦": "1299.HK",
    "中国银河证券": "6881.HK",
    "海通证券港股": "6837.HK",
    "华润置地": "1109.HK",
    "中国海外发展": "0688.HK", "中海发展": "0688.HK",
    "新世界发展": "0017.HK",
    "恒基地产": "0012.HK", "恒基": "0012.HK",
    "新鸿基地产": "0016.HK", "新地": "0016.HK",
    "长和": "0001.HK", "长实集团": "0001.HK",
    "碧桂园": "2007.HK",
    "恒大": "3333.HK",
    "中国移动": "0941.HK", "移动港股": "0941.HK",
    "中国联通": "0762.HK", "联通港股": "0762.HK",
    "中国电信": "0728.HK", "电信港股": "0728.HK",
    "中电控股": "0002.HK", "中电": "0002.HK",
    "香港中华煤气": "0003.HK", "煤气": "0003.HK",
    "电能实业": "0006.HK",
    "中海油": "0883.HK", "中国海洋石油": "0883.HK",
    "中国石化": "0386.HK", "石化港股": "0386.HK",
    "中国石油": "0857.HK", "中石油港股": "0857.HK",
    "中国神华": "1088.HK", "神华能源": "1088.HK",
    "协鑫科技": "3800.HK",
    "中国生物制药": "1177.HK", "中生制药": "1177.HK",
    "石药集团": "1093.HK", "石药": "1093.HK",
    "药明生物": "2269.HK",
    "百济神州港股": "6160.HK",
    "康哲药业": "0867.HK",
    "上海医药港股": "2196.HK",
    "金斯瑞生物": "1548.HK", "金斯瑞": "1548.HK",
    "康方生物": "6998.HK",
    "舜宇光学": "2382.HK",
    "银河娱乐": "0027.HK", "澳门银河": "0027.HK",
    "金沙中国": "1928.HK", "澳门金沙": "1928.HK",
    "SJM控股": "0880.HK", "葡京": "0880.HK",
    "永利澳门": "1128.HK",
    "海底捞": "6862.HK",
    "新东方港股": "9901.HK",
    "华润啤酒": "0291.HK", "华润雪花": "0291.HK",
    "安踏体育": "2020.HK", "安踏": "2020.HK",
    "周大福": "1929.HK",
    "高鑫零售": "6808.HK", "大润发": "6808.HK",
    "国泰航空": "0293.HK", "国泰": "0293.HK",
    "中国南方航空港股": "1055.HK", "南航港股": "1055.HK",
    "中国东方航空港股": "0670.HK", "东航港股": "0670.HK",
    "中国国际航空港股": "0753.HK", "国航港股": "0753.HK",
    "中远海运集装箱": "2866.HK",
    "中远海运港口": "1199.HK",
    "信和置业": "0083.HK",
    "恒生ETF": "2800.HK", "盈富基金": "2800.HK",
    "安硕A50": "2823.HK", "A50港股": "2823.HK",
    "华夏黄金ETF": "3033.HK", "黄金ETF": "3033.HK",
    "华夏恒生科技ETF": "3032.HK", "恒科ETF": "3032.HK",
    "华夏纳指100ETF": "3067.HK",

}

# 中文名 → 英文搜索关键词（用于 Finnhub 兜底搜索）
CN_TO_SEARCH: dict[str, str] = {
    "闪迪": "SanDisk",
    "西部数据": "Western Digital",
    "希捷": "Seagate",
    "英特尔": "Intel",
    "高通": "Qualcomm",
    "博通": "Broadcom",
    "德州仪器": "Texas Instruments",
    "应用材料": "Applied Materials",
    "泛林": "Lam Research",
    "科磊": "KLA",
    "甲骨文": "Oracle",
    "思科": "Cisco",
    "戴尔": "Dell",
    "摩根大通": "JPMorgan",
    "美国银行": "Bank of America",
    "花旗": "Citigroup",
    "富国银行": "Wells Fargo",
    "高盛": "Goldman Sachs",
    "摩根士丹利": "Morgan Stanley",
    "贝莱德": "BlackRock",
    "沃尔玛": "Walmart",
    "好市多": "Costco",
    "耐克": "Nike",
    "麦当劳": "McDonald",
    "星巴克": "Starbucks",
    "可口可乐": "Coca-Cola",
    "百事": "PepsiCo",
    "迪士尼": "Disney",
    "腾讯": "Tencent",
    "网易": "NetEase",
    "哔哩哔哩": "Bilibili",
    "携程": "Trip.com",
    "新东方": "New Oriental",
    "优步": "Uber",
    "爱彼迎": "Airbnb",
    "波音": "Boeing",
    "洛克希德": "Lockheed Martin",
    "埃克森": "ExxonMobil",
    "雪佛龙": "Chevron",
    "辉瑞": "Pfizer",
    "强生": "Johnson Johnson",
    "礼来": "Eli Lilly",
    "诺和诺德": "Novo Nordisk",
    # 港股英文名（用于 Yahoo 新闻搜索）
    "腾讯": "Tencent",
    "腾讯港股": "Tencent",
    "阿里巴巴港股": "Alibaba",
    "阿里港股": "Alibaba",
    "小米": "Xiaomi",
    "小米集团": "Xiaomi",
    "美团": "Meituan",
    "美团点评": "Meituan",
    "京东港股": "Jd.Com",
    "京东集团": "Jd.Com",
    "百度港股": "Baidu",
    "网易港股": "Netease",
    "快手": "Kuaishou",
    "联想集团": "Lenovo",
    "联想": "Lenovo",
    "金蝶国际": "Kingdee",
    "比亚迪电子": "Byd Electronic",
    "京东健康": "Jd Health",
    "携程港股": "Trip.Com",
    "携程集团": "Trip.Com",
    "阿里健康": "Ali Health",
    "同程旅行": "Tongcheng Travel",
    "汽车之家港股": "Autohome",
    "ASM太平洋": "Asm Pacific",
    "金山软件": "Kingsoft",
    "华虹半导体": "Hua Hong Semiconductor",
    "中芯国际": "Smic",
    "比亚迪": "Byd",
    "比亚迪港股": "Byd",
    "理想汽车港股": "Li Auto",
    "理想港股": "Li Auto",
    "小鹏汽车港股": "Xpeng",
    "小鹏港股": "Xpeng",
    "蔚来港股": "Nio Inc",
    "极氪汽车": "Zeekr",
    "华润电力": "China Resources Power",
    "龙源电力": "Longyuan Power",
    "中国电力": "China Power International",
    "金风科技": "Goldwind",
    "汇丰银行": "Hsbc",
    "汇丰": "Hsbc",
    "恒生银行": "Hang Seng Bank",
    "恒生": "Hang Seng Bank",
    "港交所": "Hkex",
    "香港交易所": "Hkex",
    "建行": "China Construction Bank",
    "中国建设银行": "China Construction Bank",
    "工行": "Icbc",
    "工商银行": "Icbc",
    "农行": "Agricultural Bank",
    "农业银行": "Agricultural Bank",
    "中国银行港股": "Bank Of China",
    "中行港股": "Bank Of China",
    "中国平安": "Ping An",
    "平安": "Ping An",
    "中国人寿": "China Life",
    "国寿": "China Life",
    "新华保险": "New China Life",
    "中国太保": "China Taiping",
    "友邦保险": "Aia",
    "友邦": "Aia",
    "中国银河证券": "China Galaxy",
    "海通证券港股": "Haitong Securities",
    "华润置地": "China Resources Land",
    "中国海外发展": "Coli",
    "中海发展": "Coli",
    "新世界发展": "New World Development",
    "恒基地产": "Henderson Land",
    "恒基": "Henderson Land",
    "新鸿基地产": "Sun Hung Kai",
    "新地": "Sun Hung Kai",
    "长和": "Ck Hutchison",
    "长实集团": "Ck Hutchison",
    "碧桂园": "Country Garden",
    "恒大": "Evergrande",
    "中国移动": "China Mobile",
    "移动港股": "China Mobile",
    "中国联通": "China Unicom",
    "联通港股": "China Unicom",
    "中国电信": "China Telecom",
    "电信港股": "China Telecom",
    "中电控股": "Clp",
    "中电": "Clp",
    "香港中华煤气": "Towngas",
    "煤气": "Towngas",
    "电能实业": "Power Assets",
    "中海油": "Cnooc",
    "中国海洋石油": "Cnooc",
    "中国石化": "Sinopec",
    "石化港股": "Sinopec",
    "中国石油": "Petrochina",
    "中石油港股": "Petrochina",
    "中国神华": "China Shenhua",
    "神华能源": "China Shenhua",
    "协鑫科技": "Gcl Technology",
    "中国生物制药": "Sino Biopharmaceutical",
    "中生制药": "Sino Biopharmaceutical",
    "石药集团": "Cspc Pharmaceutical",
    "石药": "Cspc Pharmaceutical",
    "药明生物": "Wuxi Biologics",
    "百济神州港股": "Beigene",
    "康哲药业": "China Medical System",
    "上海医药港股": "Shanghai Pharmaceuticals",
    "金斯瑞生物": "Genscript Biotech",
    "金斯瑞": "Genscript Biotech",
    "康方生物": "Akeso",
    "舜宇光学": "Sunny Optical",
    "银河娱乐": "Galaxy Entertainment",
    "澳门银河": "Galaxy Entertainment",
    "金沙中国": "Sands China",
    "澳门金沙": "Sands China",
    "SJM控股": "Sjm Holdings",
    "葡京": "Sjm Holdings",
    "永利澳门": "Wynn Macau",
    "海底捞": "Haidilao",
    "新东方港股": "New Oriental",
    "华润啤酒": "China Resources Beer",
    "华润雪花": "China Resources Beer",
    "安踏体育": "Anta Sports",
    "安踏": "Anta Sports",
    "周大福": "Chow Tai Fook",
    "高鑫零售": "Sun Art Retail",
    "大润发": "Sun Art Retail",
    "国泰航空": "Cathay Pacific",
    "国泰": "Cathay Pacific",
    "中国南方航空港股": "China Southern Airlines",
    "南航港股": "China Southern Airlines",
    "中国东方航空港股": "China Eastern Airlines",
    "东航港股": "China Eastern Airlines",
    "中国国际航空港股": "Air China",
    "国航港股": "Air China",
    "中远海运集装箱": "Cosco Shipping",
    "中远海运港口": "Cosco Ports",
    "信和置业": "Sino Land",
    "恒生ETF": "Tracker Fund",
    "盈富基金": "Tracker Fund",
    "安硕A50": "Ishares A50",
    "A50港股": "Ishares A50",
    "华夏黄金ETF": "Gold Etf",
    "黄金ETF": "Gold Etf",
    "华夏恒生科技ETF": "Hs Tech Etf",
    "恒科ETF": "Hs Tech Etf",
    "华夏纳指100ETF": "Nasdaq Etf Hk",
}


def curl_get(url: str, referer: str = "", proxy: bool = False) -> str:
    """用系统 curl 绕过 Python SSL 问题，返回响应文本
    proxy=False 表示直连（华尔街见闻等国内接口）
    proxy=True  表示走系统代理（Yahoo Finance 等需要代理的接口）
    """
    cmd = ["curl", "-s", "--max-time", "8", "-H", "User-Agent: Mozilla/5.0"]
    if referer:
        cmd += ["-H", f"Referer: {referer}"]
    if proxy:
        _p = _PROXY
        if _p:
            cmd += ["--proxy", _p]
    else:
        cmd += ["--noproxy", "*"]
    cmd.append(url)
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    return result.stdout


# 翻译缓存，避免同标题重复请求触发限速
_translate_cache: dict[str, str] = {}


def translate_to_zh(text: str) -> str | None:
    """多接口翻译（有道→MyMemory兜底），成功返回中文，失败返回 None"""
    if not text:
        return None
    if text in _translate_cache:
        return _translate_cache[text]
    result = _try_youdao(text) or _try_mymemory(text)
    if result:
        _translate_cache[text] = result
    return result


def _try_youdao(text: str) -> str | None:
    try:
        import urllib.parse
        encoded = urllib.parse.quote(text)
        cmd = ["curl", "-s", "--max-time", "6", "--noproxy", "*",
               "-X", "POST",
               "-H", "User-Agent: Mozilla/5.0",
               "-d", f"q={encoded}&from=en&to=zh-CHS",
               "https://aidemo.youdao.com/trans"]
        raw = subprocess.run(cmd, capture_output=True, text=True, timeout=8).stdout
        d = json.loads(raw)
        result = d.get("translation", "")
        if isinstance(result, list):
            result = result[0] if result else ""
        if result and result.strip():
            return result.strip()[:80]
    except Exception as e:
        logger.warning(f"youdao translate error: {e}")
    return None


def _try_mymemory(text: str) -> str | None:
    """MyMemory 免费翻译接口"""
    try:
        import urllib.parse
        encoded = urllib.parse.quote(text[:500])
        cmd = ["curl", "-s", "--max-time", "8", "--noproxy", "*",
               "-H", "User-Agent: Mozilla/5.0",
               f"https://api.mymemory.translated.net/get?q={encoded}&langpair=en|zh"]
        raw = subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout
        d = json.loads(raw)
        translated = d.get("responseData", {}).get("translatedText", "")
        if translated and translated.strip() and translated.upper() != text.upper():
            return translated.strip()[:80]
    except Exception as e:
        logger.warning(f"mymemory translate error: {e}")
    return None


def get_news_yahoo(ticker: str, filter_ticker: str | None = None) -> list[tuple[str, str, bool]]:
    """Yahoo Finance 新闻 - 实时专业媒体，英文标题自动翻译中文"""
    _filter = filter_ticker or ticker
    import html as html_mod, email.utils
    results = []
    try:
        # query1/query2 轮试，其中一个限速时用另一个
        for host in ["query1", "query2"]:
            _proxy = _PROXY
            _proxy_args = ["--proxy", _proxy] if _proxy else []
            cmd = ["curl", "-s", "--max-time", "10",
                   "-H", "User-Agent: Mozilla/5.0"] + _proxy_args + [
                   f"https://{host}.finance.yahoo.com/v1/finance/search?q={ticker}&newsCount=5&quotesCount=0&enableFuzzyQuery=false"]
            raw = subprocess.run(cmd, capture_output=True, text=True, timeout=12).stdout
            if raw.strip().startswith("{"):
                break
        d = json.loads(raw)
        now_ts = int(time.time())
        candidates = []
        for item in d.get("news", [])[:5]:
            title  = item.get("title", "").strip()
            link   = item.get("link", "")
            pub_ts = item.get("providerPublishTime", 0)
            if not title or not link:
                continue
            age_h = (now_ts - pub_ts) / 3600 if pub_ts else 999
            if age_h < 1:
                time_tag = f"{int(age_h*60)}分钟前"
            elif age_h < 24:
                time_tag = f"{int(age_h)}小时前"
            else:
                time_tag = f"{int(age_h/24)}天前"
            candidates.append((time_tag, title, link))
            if len(candidates) >= 3:
                break
        # 过滤：严格过滤，港股搜索词是公司英文名时允许降级放行
        results = []
        fallback = []
        for time_tag, title, link in candidates:
            if _is_relevant_news(title, _filter):
                zh = translate_to_zh(title)
                results.append((f"[{time_tag}] {zh if zh else title[:80]}", link, bool(zh)))
            else:
                title_lower = title.lower()
                is_generic = any(kw in title_lower for kw in _GENERIC_NEWS_KEYWORDS)
                if not is_generic:
                    zh = translate_to_zh(title)
                    fallback.append((f"[{time_tag}] {zh if zh else title[:80]}", link, bool(zh)))
                else:
                    logger.info(f"[yahoo] filtered: {title[:60]}")
        # 港股搜公司英文名时结果可能完全无关，不降级，直接返回严格过滤结果（空了再走 wallst）
        # 美股如果严格过滤后不足，才用 fallback 补充
        if len(results) < 3 and not is_hk_ticker(_filter):
            for item in fallback:
                if item not in results:
                    results.append(item)
                if len(results) >= 3:
                    break
    except Exception as e:
        logger.error(f"yahoo news error: {e}")
    return results


# 通用市场摘要新闻黑名单关键词
_GENERIC_NEWS_KEYWORDS = [
    "top gainers", "top losers", "most active", "dow jones",
    "s&p 500 stocks", "today's session", "today's trading session",
    "nasdaq stocks", "market movers", "after-hours movers",
    "premarket movers", "weekly recap", "market wrap",
    "stocks moving", "stocks are moving", "which stocks",
    "biggest movers", "biggest gainers", "biggest losers",
    "market recap", "market summary", "market update",
    "hot stocks", "watch list", "watchlist",
]

# ticker 公司关键词白名单
# 品牌深度关键词（用于东方财富新闻过滤，比 _TICKER_ALIASES 更全）
_BRAND_KEYWORDS: dict[str, list[str]] = {
    "0700.HK": ["tencent", "wechat", "weixin", "腾讯", "微信", "马化腾", "王者荣耀"],
    "9988.HK": ["alibaba", "alipay", "taobao", "tmall", "ant group", "阿里巴巴", "淘宝", "天猫", "支付宝", "蚂蚁"],
    "1810.HK": ["xiaomi", "redmi", "mijia", "小米", "雷军", "红米", "澎湃"],
    "3690.HK": ["meituan", "美团", "王兴"],
    "9618.HK": ["jd.com", "京东", "刘强东"],
    "9888.HK": ["baidu", "ernie", "百度", "李彦宏", "文心", "萝卜快跑"],
    "9999.HK": ["netease", "网易", "丁磊"],
    "1024.HK": ["kuaishou", "快手", "宿华"],
    "1211.HK": ["byd", "比亚迪", "王传福", "刀片电池", "海豹", "仰望", "汉ev", "唐dm"],
    "2015.HK": ["理想汽车", "理想l", "李想", "l6", "l7", "l8", "l9", "mega", "理想one", "li auto"],
    "9868.HK": ["xpeng", "小鹏", "何小鹏", "p7", "g6", "x9", "mona"],
    "9863.HK": ["nio", "蔚来", "李斌", "es6", "et7", "onvo", "乐道"],
    "2594.HK": ["zeekr", "极氪", "安聪慧"],
    "0941.HK": ["china mobile", "中国移动"],
    "0762.HK": ["china unicom", "中国联通"],
    "0728.HK": ["china telecom", "中国电信"],
    "0883.HK": ["cnooc", "中海油", "中国海洋石油"],
    "BABA":    ["alibaba", "阿里巴巴", "淘宝", "天猫", "支付宝", "蚂蚁"],
    "JD":      ["京东", "jd.com", "刘强东"],
    "BIDU":    ["百度", "baidu", "李彦宏", "文心", "萝卜快跑"],
    "LI":      ["理想汽车", "理想l", "李想", "l6", "l7", "l8", "l9", "mega", "li auto"],
    "XPEV":    ["小鹏", "xpeng", "何小鹏", "p7", "g6", "x9", "mona"],
    "NIO":     ["蔚来", "nio", "李斌", "es6", "et7", "onvo", "乐道"],
    "BILI":    ["bilibili", "b站", "哔哩哔哩", "陈睿"],
    "PDD":     ["拼多多", "temu", "pinduoduo", "多多买菜"],
    "TCOM":    ["携程", "trip.com", "梁建章"],
}


_TICKER_ALIASES: dict[str, list[str]] = {
    # 美股科技
    "NVDA": ["nvidia", "nvda", "jensen huang"],
    "AAPL": ["apple", "aapl", "tim cook", "iphone", "ipad", "mac"],
    "TSLA": ["tesla", "tsla", "elon musk", "cybertruck"],
    "MSFT": ["microsoft", "msft", "azure", "copilot", "satya nadella"],
    "GOOGL": ["google", "alphabet", "googl", "gemini", "youtube"],
    "GOOG":  ["google", "alphabet", "goog"],
    "AMZN": ["amazon", "amzn", "aws", "jeff bezos", "andy jassy"],
    "META":  ["meta", "facebook", "zuckerberg", "instagram", "whatsapp"],
    "NFLX": ["netflix", "nflx"],
    "AMD":   ["amd", "advanced micro", "lisa su"],
    "INTC": ["intel", "intc"],
    "COIN": ["coinbase", "coin "],
    "MSTR": ["microstrategy", "mstr", "michael saylor"],
    "PLTR": ["palantir", "pltr"],
    "HOOD": ["robinhood", "hood "],
    "UBER": ["uber"],
    "ABNB": ["airbnb"],
    "SNAP": ["snapchat", "snap "],
    "SPOT": ["spotify"],
    "SPY":  ["s&p 500", "sp500", "spdr"],
    "QQQ":  ["invesco", "nasdaq 100", "nasdaq-100"],
    # 美股中概股
    "BABA": ["alibaba", "baba", "alipay", "taobao", "tmall", "ant group"],
    "PDD":  ["pinduoduo", "temu", "pdd"],
    "JD":   ["jd.com", "jd.com", "jingdong"],
    "BIDU": ["baidu", "bidu", "ernie"],
    "NIO":  ["nio electric", "nio car", "weilai"],
    "XPEV": ["xpeng", "xiaopeng"],
    "LI":   ["li auto", "lixiang"],
    "BILI": ["bilibili", "bili "],
    "NTES": ["netease", "ntes"],
    "TCOM": ["trip.com", "ctrip"],
    "TAL":  ["tal education", "xueersi"],
    "EDU":  ["new oriental", "new oriental education"],
    "IQ":   ["iqiyi", "iq "],
    "VIPS": ["vipshop", "vips "],
    "YUMC": ["yum china", "kfc china", "pizza hut china"],
    # 港股
    "0700.HK": ["tencent", "wechat", "weixin", "honor of kings"],
    "9988.HK": ["alibaba", "alipay", "taobao", "tmall", "ant group"],
    "1810.HK": ["xiaomi", "redmi", "mijia"],
    "3690.HK": ["meituan", "dianping"],
    "9618.HK": ["jd.com", "jingdong", "jd logistics"],
    "9888.HK": ["baidu", "ernie bot", "apollo"],
    "9999.HK": ["netease", "netease games"],
    "1024.HK": ["kuaishou", "kwai"],
    "0992.HK": ["lenovo", "lenovo group"],
    "0268.HK": ["kingdee", "kingdee international"],
    "0285.HK": ["byd electronic"],
    "6618.HK": ["jd health"],
    "9961.HK": ["trip.com", "ctrip"],
    "0241.HK": ["ali health", "alibaba health"],
    "0780.HK": ["tongcheng travel"],
    "2518.HK": ["autohome"],
    "0522.HK": ["asm pacific", "asmpt"],
    "3888.HK": ["kingsoft", "kingsoft cloud"],
    "1347.HK": ["hua hong semiconductor", "hua hong"],
    "0981.HK": ["smic", "semiconductor manufacturing international"],
    "1211.HK": ["byd", "byd company"],
    "2015.HK": ["li auto", "lixiang"],
    "9868.HK": ["xpeng", "xpeng motors"],
    "9863.HK": ["nio inc", "weilai"],
    "2594.HK": ["zeekr"],
    "0836.HK": ["china resources power", "cr power"],
    "0916.HK": ["longyuan power"],
    "2380.HK": ["china power international"],
    "2208.HK": ["goldwind", "xinjiang goldwind"],
    "0005.HK": ["hsbc", "hsbc holdings"],
    "0011.HK": ["hang seng bank", "hang seng"],
    "0388.HK": ["hkex", "hong kong exchange", "hong kong exchanges"],
    "0939.HK": ["china construction bank", "ccb"],
    "1398.HK": ["icbc", "industrial commercial bank"],
    "1288.HK": ["agricultural bank", "agricultural bank of china"],
    "3988.HK": ["bank of china", "boc"],
    "2318.HK": ["ping an", "ping an insurance"],
    "2628.HK": ["china life", "china life insurance"],
    "1336.HK": ["new china life", "new china insurance"],
    "0966.HK": ["china taiping", "taiping insurance"],
    "1299.HK": ["aia", "aia group", "aia insurance"],
    "6881.HK": ["china galaxy", "china galaxy securities"],
    "6837.HK": ["haitong securities", "haitong"],
    "1109.HK": ["china resources land", "cr land"],
    "0688.HK": ["coli", "china overseas land"],
    "0017.HK": ["new world development"],
    "0012.HK": ["henderson land"],
    "0016.HK": ["sun hung kai", "shkp"],
    "0001.HK": ["ck hutchison", "cheung kong"],
    "2007.HK": ["country garden", "country garden holdings"],
    "3333.HK": ["evergrande", "china evergrande"],
    "0941.HK": ["china mobile"],
    "0762.HK": ["china unicom", "unicom"],
    "0728.HK": ["china telecom"],
    "0002.HK": ["clp", "clp holdings"],
    "0003.HK": ["towngas", "hong kong and china gas"],
    "0006.HK": ["power assets"],
    "0883.HK": ["cnooc"],
    "0386.HK": ["sinopec", "china petroleum"],
    "0857.HK": ["petrochina", "cnpc"],
    "1088.HK": ["china shenhua", "shenhua energy"],
    "3800.HK": ["gcl technology", "gcl solar"],
    "1177.HK": ["sino biopharmaceutical"],
    "1093.HK": ["cspc pharmaceutical", "cspc"],
    "2269.HK": ["wuxi biologics", "wuxi bio"],
    "6160.HK": ["beigene"],
    "0867.HK": ["china medical system", "cms"],
    "2196.HK": ["shanghai pharmaceuticals", "shanghai pharma"],
    "1548.HK": ["genscript biotech", "genscript"],
    "6998.HK": ["akeso", "akeso biotech"],
    "2382.HK": ["sunny optical"],
    "0027.HK": ["galaxy entertainment", "galaxy casino"],
    "1928.HK": ["sands china", "sands casino"],
    "0880.HK": ["sjm holdings", "grand lisboa"],
    "1128.HK": ["wynn macau", "wynn"],
    "6862.HK": ["haidilao", "haidilao hotpot"],
    "9901.HK": ["new oriental", "new oriental education"],
    "0291.HK": ["china resources beer", "snow beer"],
    "2020.HK": ["anta sports", "anta"],
    "1929.HK": ["chow tai fook", "ctf"],
    "6808.HK": ["sun art retail", "sun art", "auchan"],
    "0293.HK": ["cathay pacific"],
    "1055.HK": ["china southern airlines", "southern airlines"],
    "0670.HK": ["china eastern airlines", "eastern airlines"],
    "0753.HK": ["air china"],
    "2866.HK": ["cosco shipping", "cosco container"],
    "1199.HK": ["cosco ports", "cosco shipping ports"],
    "0083.HK": ["sino land"],
    "2800.HK": ["tracker fund", "hsi etf"],
    "2823.HK": ["ishares a50", "ftse a50", "china a50"],
    "3033.HK": ["gold etf", "hua xia gold"],
    "3032.HK": ["hs tech etf", "hang seng tech etf"],
    "3067.HK": ["nasdaq etf hk"],
}


def _is_relevant_news(title: str, ticker: str) -> bool:
    """过滤掉与目标 ticker 无关的通用市场新闻"""
    title_lower = title.lower()
    for kw in _GENERIC_NEWS_KEYWORDS:
        if kw in title_lower:
            return False
    ticker_upper = ticker.upper()
    aliases = _TICKER_ALIASES.get(ticker_upper, [])
    # 白名单：命中任一 alias 关键词
    for alias in aliases:
        if alias in title_lower:
            return True
    # 白名单：非港股时直接包含 ticker
    if not ticker_upper.endswith(".HK") and ticker_upper.lower() in title_lower:
        return True
    # 已知公司但未命中 → 过滤
    if ticker_upper in _TICKER_ALIASES:
        return False
    return True


def get_news_finnhub(ticker: str) -> list[tuple[str, str, bool]]:
    """Finnhub 公司新闻 - 用自己的 API key，过滤通用市场新闻，英文标题自动翻译"""
    results = []
    try:
        client = finnhub.Client(api_key=FINNHUB_API_KEY)
        today    = datetime.date.today()
        week_ago = today - datetime.timedelta(days=7)
        news_list = client.company_news(ticker, _from=str(week_ago), to=str(today))
        now_ts = int(time.time())
        for item in news_list[:20]:  # 多拉几条，过滤后再截取
            title  = item.get("headline", "").strip()
            link   = item.get("url", "")
            pub_ts = item.get("datetime", 0)
            if not title:
                continue
            if not _is_relevant_news(title, ticker):
                logger.info(f"[finnhub] filtered: {title[:60]}")
                continue
            age_h = (now_ts - pub_ts) / 3600 if pub_ts else 999
            if age_h > 72:
                continue
            if age_h < 1:
                time_tag = f"{int(age_h*60)}分钟前"
            elif age_h < 24:
                time_tag = f"{int(age_h)}小时前"
            else:
                time_tag = f"{int(age_h/24)}天前"
            zh = translate_to_zh(title)
            if zh:
                results.append((f"[{time_tag}] {zh}", link, True))
            else:
                results.append((f"[{time_tag}] {title[:80]}", link, False))
            if len(results) >= 3:
                break
    except Exception as e:
        logger.error(f"finnhub news error: {e}")
    return results


def get_news_eastmoney_hk(ticker: str) -> list[tuple[str, str, bool]]:
    """东方财富港股个股新闻 - 中文，专属个股，直连无需代理
    取30条后按公司关键词过滤，确保返回真正相关的新闻
    secid 格式: 116.{5位代码}，如 116.00700（腾讯）、116.01810（小米）
    """
    results = []
    try:
        code5 = ticker.split(".")[0].zfill(5)
        secid = f"116.{code5}"
        cmd = ["curl", "-s", "--max-time", "8", "--noproxy", "*",
               "-H", "User-Agent: Mozilla/5.0",
               f"https://np-listapi.eastmoney.com/comm/web/getListInfo?client=web&type=1&mTypeAndCode={secid}&pageSize=30&pageIndex=0&cb="]
        raw = subprocess.run(cmd, capture_output=True, timeout=10).stdout.decode("utf-8", errors="replace")
        d = json.loads(raw)
        items = (d.get("data") or {}).get("list") or []
        now_ts = int(time.time())

        # 获取该 ticker 的关键词列表（优先用品牌深度关键词表）
        ticker_upper = ticker.upper()
        brand_kws = _BRAND_KEYWORDS.get(ticker_upper, [])
        if brand_kws:
            keywords = set(k.lower() for k in brand_kws)
        else:
            aliases = _TICKER_ALIASES.get(ticker_upper, [])
            cn_name = CN_MAP.get(ticker_upper, "")
            keywords = set(a.lower() for a in aliases)
            if cn_name:
                keywords.add(cn_name.lower())
            en_name = TICKER_NAMES.get(ticker_upper, "").lower()
            if en_name:
                keywords.add(en_name)

        filtered = []
        fallback = []  # 无法判断相关性时备用
        seen_titles: set[str] = set()  # 去重

        for item in items:
            title = item.get("Art_Title", "").strip()
            link  = item.get("Art_Url", "") or item.get("Art_OriginUrl", "")
            show_time = item.get("Art_ShowTime", "")
            if not title:
                continue
            try:
                import datetime as _dt, calendar as _cal
                pub_dt = _dt.datetime.strptime(show_time, "%Y-%m-%d %H:%M:%S")
                pub_ts = int(_cal.timegm(pub_dt.timetuple())) - 8 * 3600
                age_h = (now_ts - pub_ts) / 3600
            except:
                age_h = 24
            if age_h > 72:
                continue
            if age_h < 1:
                time_tag = f"{int(age_h*60)}分钟前"
            elif age_h < 24:
                time_tag = f"{int(age_h)}小时前"
            else:
                time_tag = f"{int(age_h/24)}天前"

            # 去重：标题前20字相同的认为是重复
            title_key = title[:20]
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)

            entry = (f"[{time_tag}] {title[:80]}", link, False)
            title_lower = title.lower()

            if keywords and any(kw in title_lower for kw in keywords):
                filtered.append(entry)
            else:
                fallback.append(entry)

            if len(filtered) >= 3:
                break

        # 严格过滤结果优先；不足3条时用备用补到3条
        results = filtered[:3]
        if len(results) < 3:
            for item in fallback:
                if item not in results:
                    results.append(item)
                if len(results) >= 3:
                    break

        logger.info(f"[eastmoney_hk] {ticker}: {len(filtered)} related + {len(fallback)} fallback → {len(results)} returned")
    except Exception as e:
        logger.error(f"eastmoney_hk news error: {e}")
    return results

def get_news_wallst(ticker: str) -> list[tuple[str, str]]:
    """华尔街见闻快讯 - 中文，返回 (title, url)"""
    results = []
    try:
        cn_name = CN_MAP.get(ticker.upper(), ticker)
        raw     = curl_get(
            "https://api-one.wallstcn.com/apiv1/content/lives?channel=us-stock&limit=30",
            referer="https://wallstreetcn.com"
        )
        items = json.loads(raw).get("data", {}).get("items", [])
        for item in items:
            content = item.get("content_text", "").replace("\n", " ")[:55]
            art_id  = item.get("id", "")
            link    = f"https://wallstreetcn.com/articles/{art_id}"
            if not content:
                continue
            if cn_name in content or ticker.upper() in content:
                results.append((content, link, False))
            if len(results) >= 3:
                break
    except Exception as e:
        logger.error(f"wallst news error: {e}")
    return results


def get_news_binance(ticker: str) -> list[tuple[str, str, bool]]:
    """币安官方新闻 - 从 Binance PGC Feed 获取，筛选与 ticker 相关的条目
    API 实际结构: data.vos[]
      - title: 标题
      - webLink: 文章链接
      - date: Unix时间戳(秒)
      - tradingPairsV2[].code: 关联币种代码 (e.g. TSLA, AAPL)
      - tradingPairs: 老版字段，备用
    """
    results = []
    try:
        # strategy=10 以股票为主，strategy=5 覆盖加密货币，合并去重
        _proxy = _PROXY
        _proxy_args = ["--proxy", _proxy] if _proxy else []
        base_cmd = ["curl", "-s", "--max-time", "10",
                    "-H", "User-Agent: Mozilla/5.0",
                    "-H", "Accept: application/json"] + _proxy_args
        vos = []
        for strategy in ["10", "5"]:
            url = f"https://www.binance.com/bapi/composite/v4/friendly/pgc/feed/news/list?strategy={strategy}"
            raw = subprocess.run(base_cmd + [url], capture_output=True, text=True, timeout=12).stdout
            if not raw.strip():
                continue
            d = json.loads(raw)
            vos += (d.get("data") or {}).get("vos") or []

        # 目标币种匹配关键词
        t_upper = ticker.upper()
        # 币安 bStocks 命名规则：美股代码加 'B' 后缀 (e.g. TSLA → TSLAB)
        bstocks_code = t_upper + "B"
        cn_name = CN_MAP.get(t_upper, "")
        keywords = {t_upper.lower()}
        if cn_name:
            keywords.add(cn_name.lower())
        en_name = CN_TO_SEARCH.get(cn_name, "")
        if en_name:
            # 只取第一个单词避免汍刚就过滤掉无关内容
            keywords.add(en_name.split()[0].lower())

        now_ts = int(time.time())
        for item in vos:
            title  = (item.get("title") or "").strip()
            link   = item.get("webLink") or ""
            pub_ts = item.get("date") or 0
            if not title:
                continue

            # 币种匹配： tradingPairsV2[].code 包含 ticker 或 tickerB
            tp2_codes = {x.get("code", "").upper() for x in (item.get("tradingPairsV2") or [])}
            tp1_codes = {str(x).upper() for x in (item.get("tradingPairs") or [])}
            all_codes = tp2_codes | tp1_codes
            title_lower = title.lower()
            matched = (
                t_upper in all_codes
                or bstocks_code in all_codes
                or any(kw in title_lower for kw in keywords)
            )
            if not matched:
                continue
            # 再过一遍相关性过滤（Binance 有时给通用新闻打股票标签）
            if not _is_relevant_news(title, ticker):
                logger.info(f"[binance] filtered: {title[:60]}")
                continue

            # 时间戳如是毫秒转秒
            if pub_ts > 1e12:
                pub_ts = int(pub_ts / 1000)
            age_h = (now_ts - pub_ts) / 3600 if pub_ts else 999
            if age_h > 72:  # 超过3天不要
                logger.info(f"[binance] stale {age_h:.0f}h: {title[:50]}")
                continue
            if age_h < 1:
                time_tag = f"{int(age_h*60)}分钟前"
            elif age_h < 24:
                time_tag = f"{int(age_h)}小时前"
            else:
                time_tag = f"{int(age_h/24)}天前"

            # 英文标题自动翻译
            has_zh = any('\u4e00' <= c <= '\u9fff' for c in title)
            if not has_zh:
                title = translate_to_zh(title)[:80]

            display = f"[币安·{time_tag}] {title[:80]}"
            results.append((display, link, True))
            if len(results) >= 2:
                break
    except Exception as e:
        logger.warning(f"binance news error: {e}")
    return results


def get_news(ticker: str) -> list[tuple[str, str, bool]]:
    """新闻混合策略: 1条币安(30%) + 2条原有源(70%)；若无币安相关新闻则全用原有源"""
    logger.info(f"[news] fetching for {ticker}")

    # 港股：Yahoo 搜索用英文公司名（0700.HK 搜不到新闻）
    ticker_upper = ticker.upper()
    if is_hk_ticker(ticker):
        aliases = _TICKER_ALIASES.get(ticker_upper, [])
        if aliases:
            yahoo_query = aliases[0].capitalize()
        else:
            yahoo_query = ticker
            for cn, sym in CN_TO_TICKER.items():
                if sym == ticker_upper:
                    yahoo_query = CN_TO_SEARCH.get(cn, cn)
                    break
    else:
        yahoo_query = ticker

    # ── 并发拉取所有源 ──
    is_hk = is_hk_ticker(ticker)
    hk_equiv = _US_TO_HK.get(ticker_upper) if not is_hk else None  # 美股中概股的港股代码
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        f_binance  = pool.submit(get_news_binance,  ticker)
        f_finnhub  = pool.submit(get_news_finnhub,  ticker)
        f_yahoo    = pool.submit(get_news_yahoo,    yahoo_query, ticker if is_hk else None)
        f_wallst   = pool.submit(get_news_wallst,   ticker)
        # 港股用本身ticker，美股中概股用对应港股代码
        em_hk_target = ticker if is_hk else hk_equiv
        f_em_hk    = pool.submit(get_news_eastmoney_hk, em_hk_target) if em_hk_target else None
        binance_news = f_binance.result()
        finnhub_news = f_finnhub.result()
        yahoo_news   = f_yahoo.result()
        wallst_news  = f_wallst.result()
        em_hk_news   = f_em_hk.result() if f_em_hk else []

    logger.info(f"[news] binance={len(binance_news)} finnhub={len(finnhub_news)} "
                f"yahoo={len(yahoo_news)} wallst={len(wallst_news)} em_hk={len(em_hk_news)}")

    # ── 汇总：港股/中概股优先东方财富，纯美股走Finnhub ──
    legacy: list[tuple[str, str, bool]] = []
    seen_links: set[str] = set()
    if is_hk or hk_equiv:
        source_order = [em_hk_news, finnhub_news, yahoo_news, wallst_news]
    else:
        source_order = [finnhub_news, yahoo_news, wallst_news]
    for pool_list in source_order:
        for item in pool_list:
            link = item[1] if len(item) > 1 else ""
            if link and link in seen_links:
                continue
            if link:
                seen_links.add(link)
            legacy.append(item)

    # ── 混合策略 ──
    if binance_news:
        # 取1条币安 + 2条原有 = 3条
        final = binance_news[:1] + legacy[:2]
    else:
        # 无币安相关新闻，全用原有
        final = legacy[:3]

    logger.info(f"[news] final {len(final)} items for {ticker}")
    return final


def fmt(n: float) -> str:
    if n >= 1e12: return f"${n/1e12:.2f}T"
    if n >= 1e9:  return f"${n/1e9:.2f}B"
    if n >= 1e6:  return f"${n/1e6:.2f}M"
    return f"${n:,.0f}"


def he(s: str) -> str:
    """HTML 转义"""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


BINANCE_ACTIVITY_URL = "https://www.binance.com/en/activity"

# ticker → binance bstocks slug
BSTOCKS_SLUG: dict[str, str] = {
    "TSLA":  "tesla",
    "AAPL":  "apple",
    "NVDA":  "nvidia",
    "MSFT":  "microsoft",
    "GOOGL": "alphabet",
    "GOOG":  "alphabet",
    "AMZN":  "amazon",
    "META":  "meta",
    "NFLX":  "netflix",
    "AMD":   "advanced-micro-devices",
    "INTC":  "intel",
    "QCOM":  "qualcomm",
    "AVGO":  "broadcom",
    "ORCL":  "oracle",
    "ADBE":  "adobe",
    "CRM":   "salesforce",
    "CSCO":  "cisco",
    "IBM":   "ibm",
    "JPM":   "jpmorgan-chase",
    "BAC":   "bank-of-america",
    "GS":    "goldman-sachs",
    "MS":    "morgan-stanley",
    "V":     "visa",
    "MA":    "mastercard",
    "WMT":   "walmart",
    "COST":  "costco",
    "NKE":   "nike",
    "MCD":   "mcdonalds",
    "SBUX":  "starbucks",
    "KO":    "coca-cola",
    "DIS":   "disney",
    "BABA":  "alibaba",
    "PDD":   "pinduoduo",
    "JD":    "jd",
    "BIDU":  "baidu",
    "NIO":   "nio",
    "XPEV":  "xpeng",
    "LI":    "li-auto",
    "COIN":  "coinbase",
    "MSTR":  "microstrategy",
    "HOOD":  "robinhood",
    "PLTR":  "palantir",
    "UBER":  "uber",
    "ABNB":  "airbnb",
    "SPCX":  "spacex",
    "SPY":   "spy-etf",
    "QQQ":   "invesco-qqq-trust",
}


def bstocks_url(ticker: str) -> str:
    slug = BSTOCKS_SLUG.get(ticker.upper())
    if slug:
        return f"https://www.binance.com/en/price/{slug}-tokenized-bstocks"
    # 兜底：直接跳 bstocks 首页
    return "https://www.binance.com/en/bstocks"


def build_keyboard(ticker: str = "") -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🏦 美股开户", url=URL_OPEN_ACCOUNT)],
        [InlineKeyboardButton("📚 购买教程", url=URL_BUY_TUTORIAL)],
    ]
    if ticker:
        rows.append([InlineKeyboardButton("📈 看K线", url=bstocks_url(ticker))])
    return InlineKeyboardMarkup(rows)


def fmt_hkd(n: float) -> str:
    if n >= 1e12: return f"HK${n/1e12:.2f}T"
    if n >= 1e9:  return f"HK${n/1e9:.2f}B"
    if n >= 1e6:  return f"HK${n/1e6:.2f}M"
    return f"HK${n:,.0f}"


def build_message(data: dict, news: list[tuple[str, str]]) -> str:
    is_hk  = data.get("currency") == "HKD"
    ps     = "HK$" if is_hk else "$"
    arrow  = "🔴" if data["change"] < 0 else "🟢"
    sign   = "+" if data["change"] >= 0 else ""
    mc_str = (fmt_hkd(data["market_cap"]) if is_hk else fmt(data["market_cap"])) if data["market_cap"] else "N/A"
    pe_str = f"{data['pe_ratio']:.1f}x" if data["pe_ratio"] else "N/A"

    news_lines = ""
    for i, item in enumerate(news[:3], 1):
        title, link = item[0], item[1]
        is_translated = item[2] if len(item) > 2 else False
        if link and is_translated:
            news_lines += f'  {i}. {he(title)} <a href="{link}">[英文原文]</a>\n'
        elif link:
            news_lines += f'  {i}. <a href="{link}">{he(title)}</a>\n'
        else:
            news_lines += f"  {i}. {he(title)}\n"
    if not news_lines:
        news_lines = "  暂无相关新闻\n"

    return (
        f"📊 <b>{data['ticker']}</b> · {he(data['name'])}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{arrow} <b>{ps}{data['price']:.2f}</b>   "
        f"<code>{sign}{data['change']:.2f} ({sign}{data['change_pct']:.2f}%)</code>\n\n"
        f"💰 市值: <code>{he(mc_str)}</code>\n"
        f"📦 今日区间: <code>{ps}{data['low_day']:.2f} — {ps}{data['high_day']:.2f}</code>  振幅 <code>{data['amplitude']:.1f}%</code>\n"
        f"📅 52W区间: <code>{ps}{data['week52_low']:.2f} — {ps}{data['week52_high']:.2f}</code>\n"
        f"📐 P/E: <code>{he(pe_str)}</code>\n\n"
        f"📉 技术面\n"
        + (f"  {data['rsi_line']}\n"  if data.get('rsi_line')  else "")
        + (f"  {data['ma_line']}\n"   if data.get('ma_line')   else "")
        + (f"  {data['macd_line']}\n" if data.get('macd_line') else "")
        + (f"  {data['vol_line']}\n"  if data.get('vol_line')  else "")
        + f"  52周位置: <code>{data['pos_str']}</code>  {data['trend']}\n\n"
        f"📰 最新动态\n"
        f"{news_lines}"
    )


def cn_to_ticker_lookup(cn: str) -> str | None:
    """中文名 → ticker，先查本地表，再用 Finnhub 搜索兜底"""
    # 1. 直接命中本地表
    if cn in CN_TO_TICKER:
        return CN_TO_TICKER[cn]
    # 2. 查搜索关键词表 → Finnhub symbol_lookup
    search_kw = CN_TO_SEARCH.get(cn, cn)  # 没有映射就直接用中文名搜
    try:
        client = finnhub.Client(api_key=FINNHUB_API_KEY)
        results = client.symbol_lookup(search_kw)
        for item in results.get("result", []):
            sym = item.get("symbol", "")
            desc = item.get("description", "").upper()
            # 只取美股主板，排除期权/期货后缀
            if "." not in sym and len(sym) <= 5:
                logger.info(f"[lookup] {cn} → {sym} ({desc})")
                return sym
    except Exception as e:
        logger.warning(f"[lookup] finnhub search error: {e}")
    return None


# 美股中概股 → 对应港股代码（用于新闻拉取，港股东方财富新闻更丰富）
_US_TO_HK: dict[str, str] = {
    "BABA": "9988.HK",
    "JD":   "9618.HK",
    "BIDU": "9888.HK",
    "NTES": "9999.HK",
    "LI":   "2015.HK",
    "XPEV": "9868.HK",
    "NIO":  "9863.HK",
    "TCOM": "9961.HK",
    "BILI": "9626.HK",
    "EDU":  "9901.HK",
}


# ── 韩股提示（不在行情系统内，只给友好提示）────────────────────────────
_KR_HINTS: dict[str, str] = {
    "海力士": "SK Hynix", "SK海力士": "SK Hynix",
    "三星": "三星电子", "三星电子": "Samsung Electronics",
    "现代": "现代汽车", "현代汽车": "Hyundai Motor",
    "LG전자": "LG Electronics",
}


def extract_tickers(text: str) -> list[str]:
    """支持英文 #TSLA、加密 #BTC、港股 #0700.HK、中文 #腾讯 四种格式
    返回格式：加密货币前缀 'CRYPTO:' 区分股票"""
    found = []
    # 港股数字代码（如 #0700.HK 或 #700.HK）
    for t in re.findall(r"#(\d{1,5}\.HK)", text, re.IGNORECASE):
        num, suffix = t.upper().split(".")
        t_norm = num.zfill(4) + ".HK"
        if t_norm not in found:
            found.append(t_norm)
    # 先匹配中文（加密中文别名 + 股票中文名）
    for cn in re.findall(r"#([\u4e00-\u9fff][A-Za-z0-9\u4e00-\u9fff]{1,9})", text):
        # 先查加密别名
        csym = CRYPTO_CN_MAP.get(cn)
        if csym:
            key = f"CRYPTO:{csym}"
            if key not in found:
                found.append(key)
            continue
        # 再查股票
        ticker = cn_to_ticker_lookup(cn)
        if ticker and ticker not in found:
            found.append(ticker)
    # 匹配英文（加密白名单优先）
    for t in re.findall(r"#([A-Za-z0-9]{1,6})", text):
        t = t.upper()
        if t in _CRYPTO_SYMBOLS:
            key = f"CRYPTO:{t}"
            if key not in found:
                found.append(key)
        else:
            if t not in found:
                found.append(t)
    return found[:3]


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 欢迎使用行情机器人！\n\n"
        "发送 <b>#股票代码</b> 获取实时行情，例如：\n"
        "  美股：#AAPL  #TSLA  #NVDA\n"
        "  港股：#0700.HK  #腾讯  #小米\n"
        "  中文：#苹果  #特斯拉  #英伟达",
        parse_mode=ParseMode.HTML
    )


# ═══════════════════════════════════════════════════════════════
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Bot error: {context.error}", exc_info=context.error)


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """所有人可用的 /stats 命令，查看群组排行榜"""
    logger.info(f"[stats] uid={update.effective_user.id if update.effective_user else '?'} chat_type={update.effective_chat.type if update.effective_chat else '?'}")
    try:
        conn = sqlite3.connect(STATS_DB)
        total = conn.execute("SELECT COUNT(*) FROM queries").fetchone()[0]
        tickers = conn.execute(
            "SELECT ticker, COUNT(*) as cnt FROM queries GROUP BY ticker ORDER BY cnt DESC LIMIT 15"
        ).fetchall()
        chats = conn.execute(
            "SELECT chat_title, COUNT(*) as cnt FROM queries GROUP BY chat_id ORDER BY cnt DESC LIMIT 10"
        ).fetchall()
        today_cnt = conn.execute(
            "SELECT COUNT(*) FROM queries WHERE ts >= ?", (int(time.time()) - 86400,)
        ).fetchone()[0]
        conn.close()

        ticker_lines = "\n".join(f"  {t}  {c}次" for t, c in tickers) or "  暂无数据"
        chat_lines   = "\n".join(f"  {n}  {c}次" for n, c in chats)   or "  暂无数据"

        msg = (
            f"📊 <b>查询统计</b>\n"
            f"总计: <code>{total}</code> 次   今日: <code>{today_cnt}</code> 次\n\n"
            f"🔥 <b>热门股票（Top 15）</b>\n{ticker_lines}\n\n"
            f"💬 <b>活跃群组（Top 10）</b>\n{chat_lines}"
        )
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"统计查询失败: {e}")


async def _fetch_stock_cached(ticker: str, loop: asyncio.AbstractEventLoop) -> dict | None:
    """TTL 缓存 + 并发去重：同 ticker 同时多个请求只打一次真实 API"""
    now = time.time()
    # 先查缓存
    with _cache_lock:
        if ticker in _stock_cache:
            ts, cached = _stock_cache[ticker]
            if now - ts < STOCK_CACHE_TTL:
                logger.info(f"[cache] stock HIT {ticker} age={now-ts:.0f}s")
                return cached

    # 缓存失效——防重放：检查是否已有进行中的拉取
    async with _ticker_fetch_lock:
        if ticker in _ticker_fetch_events:
            evt = _ticker_fetch_events[ticker]
        else:
            evt = asyncio.Event()
            _ticker_fetch_events[ticker] = evt
            evt = None   # 标记自己是首个拉取者

    if evt is not None:
        # 非首个拉取者：等首个走完再拿缓存
        logger.info(f"[cache] stock WAIT {ticker} (deduplicate)")
        await evt.wait()
        with _cache_lock:
            if ticker in _stock_cache:
                _, cached = _stock_cache[ticker]
                return cached
        return None

    # 首个拉取者：真实请求 API
    try:
        data = await loop.run_in_executor(EXECUTOR, get_stock_data, ticker)
        with _cache_lock:
            _stock_cache[ticker] = (time.time(), data)
        return data
    finally:
        # 通知等待者并清除事件
        async with _ticker_fetch_lock:
            evt_to_set = _ticker_fetch_events.pop(ticker, None)
        if evt_to_set:
            evt_to_set.set()


async def _fetch_news_cached(ticker: str, loop: asyncio.AbstractEventLoop) -> list:
    """TTL 缓存新闻（并发去重逻辑同上）"""
    now = time.time()
    with _cache_lock:
        if ticker in _news_cache:
            ts, cached = _news_cache[ticker]
            if now - ts < NEWS_CACHE_TTL:
                logger.info(f"[cache] news HIT {ticker} age={now-ts:.0f}s")
                return cached
    result = await loop.run_in_executor(EXECUTOR, get_news, ticker)
    with _cache_lock:
        _news_cache[ticker] = (time.time(), result)
    return result


async def _send_with_retry(message, text: str, ticker: str):
    """Telegram 发送重试，正确处理 RetryAfter"""
    for attempt in range(4):
        try:
            await message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=build_keyboard(ticker)
            )
            return
        except RetryAfter as e:
            wait = e.retry_after + 1
            logger.warning(f"[flood] RetryAfter {wait}s for {ticker} (attempt {attempt+1})")
            await asyncio.sleep(wait)
        except Exception as e:
            if attempt < 3:
                logger.warning(f"[send] failed attempt {attempt+1}: {e}")
                await asyncio.sleep(2)
            else:
                logger.error(f"[send] gave up after 4 attempts: {e}")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        logger.info(f"[msg] no text: update={update.update_id} msg={update.message}")
        return
    logger.info(f"[msg] received: '{update.message.text[:80]}' from chat_id={update.message.chat_id}")
    tickers = extract_tickers(update.message.text)
    logger.info(f"[msg] extracted tickers: {tickers}")
    if not tickers:
        return

    loop = asyncio.get_running_loop()
    chat_id    = update.message.chat_id
    chat_title = update.message.chat.title or ""
    setting = {"autodel": False}

    for ticker in tickers:
        ticker = ticker.upper()
        await update.message.chat.send_action("typing")

        # ── 加密货币分流 ──
        if ticker.startswith("CRYPTO:"):
            symbol = ticker[7:]
            data = await loop.run_in_executor(EXECUTOR, get_crypto_data, symbol)
            news = await loop.run_in_executor(EXECUTOR, get_crypto_news, symbol)
            if int(chat_id) < 0:
                record_query(str(chat_id), chat_title, symbol)
            if not data:
                await update.message.reply_text(
                    f"❌ 未找到 <code>{symbol}</code> 的行情，请检查代码是否正确。",
                    parse_mode=ParseMode.HTML
                )
                continue
            msg = build_crypto_message(data, news)
            await _send_with_retry(update.message, msg, symbol)
            continue

        # ── 股票分流 ──
        # 行情和新闻并发拉取，并开缓存 + 去重
        data, news = await asyncio.gather(
            _fetch_stock_cached(ticker, loop),
            _fetch_news_cached(ticker, loop),
        )

        # 只统计群组查询（chat_id < 0 为群组，> 0 为私聊）
        if int(chat_id) < 0:
            record_query(str(chat_id), chat_title, ticker)

        if not data:
            await update.message.reply_text(
                f"❌ 未找到 <code>{ticker}</code> 的数据，请检查代码是否正确。",
                parse_mode=ParseMode.HTML
            )
            continue

        msg = build_message(data, news)
        for attempt in range(4):
            try:
                sent = await update.message.reply_text(
                    msg,
                    parse_mode=ParseMode.HTML,
                    reply_markup=build_keyboard(ticker)
                )
                pass  # autodel not in self-use version
                break
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
            except Exception as e:
                if attempt < 3:
                    await asyncio.sleep(2)
                else:
                    logger.error(f"[send] gave up: {e}")


def main():
    proxy = _PROXY
    proxy_kwargs = {"proxy": proxy} if proxy else {}
    req = HTTPXRequest(connection_pool_size=8, connect_timeout=15, read_timeout=60, write_timeout=30, pool_timeout=60, **proxy_kwargs)
    req_updates = HTTPXRequest(connection_pool_size=2, connect_timeout=15, read_timeout=60, write_timeout=30, pool_timeout=60, **proxy_kwargs)
    builder = (ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .request(req)
        .get_updates_request(req_updates)
    )
    if proxy:
        logger.info(f"Using proxy: {proxy}")

    # ── 启动前等旧连接释放 ──
    # stop_signals 默认监听 SIGTERM，launchd unload 时 PTB 会优雅关闭 getUpdates 连接
    # 但网络传播有延迟，等 5 秒确保 Telegram 服务端真的释放了旧连接
    _token = TELEGRAM_BOT_TOKEN
    _proxy_args = ["--proxy", proxy] if proxy else []
    logger.info("Waiting 5s for any previous session to fully close...")
    time.sleep(5)
    # 再主动调一次 getUpdates 抢占，确保没有其他实例
    _pre_start_ok = False
    for _attempt in range(8):
        try:
            _r = subprocess.run(
                ["curl", "-s", "--max-time", "10"] + _proxy_args +
                [f"https://api.telegram.org/bot{_token}/getUpdates?offset=-1&limit=1&timeout=0"],
                capture_output=True, text=True, timeout=12
            )
            import json as _json
            _d = _json.loads(_r.stdout)
            if _d.get("ok"):
                logger.info(f"Pre-start: session clear (attempt {_attempt+1}), starting polling")
                _pre_start_ok = True
                break
            elif _d.get("error_code") == 409:
                logger.warning(f"Pre-start: still 409 on attempt {_attempt+1}, waiting 15s...")
                time.sleep(15)
            else:
                logger.warning(f"Pre-start getUpdates unexpected: {_d}")
                _pre_start_ok = True  # 非 409 错误，允许继续尝试
                break
        except Exception as _e:
            logger.warning(f"Pre-start getUpdates error: {_e}, retrying in 10s...")
            time.sleep(10)
    if not _pre_start_ok:
        logger.error("Pre-start: still 409 after 8 attempts, giving up. launchd will retry.")
        import sys
        sys.exit(1)

    app = builder.post_init(_post_init).build()
    init_db()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)
    logger.info("Bot started — text-only mode.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()