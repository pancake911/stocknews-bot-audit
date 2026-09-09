# ============================================================
# bot_promo_audit.py — 对外推广版美股播报 Telegram Bot
# 审计版本（敏感信息已脱敏）
#
# 脱敏说明：
#   YOUR_PROMO_BOT_TOKEN     → Telegram Bot Token
#   YOUR_FINNHUB_API_KEY     → Finnhub 行情 API Key
#   YOUR_BLOCKBEATS_API_KEY  → BlockBeats 新闻 API Key
#   YOUR_ADMIN_TG_ID         → 管理员 Telegram 数字 ID
#   -100RANK_GROUP_1~10      → 排行榜群组 chat_id（第1~10名）
#   -100LUCKY_GROUP_1~3      → 幸运社区群组 chat_id
#   -100INTERNAL_GROUP_1     → 内部测试群 chat_id
#
# 审计关注点建议：
#   1. 命令权限控制（/award /collect 等管理员命令）
#   2. 用户输入校验（/submit_usdt 币安UID、/submit_merch 地址收集）
#   3. 数据库操作（award_submissions 表的写入逻辑）
#   4. 外部 API 调用（Finnhub / Yahoo / CoinGecko / Binance）
#   5. 推送逻辑（_get_push_targets / _binance_listing_poll_job）
# ============================================================

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

# ─── Finnhub 单例（避免每次请求重建 client）────────────────────────
_finnhub_client: finnhub.Client | None = None
_finnhub_client_lock = threading.Lock()

def _get_finnhub_client() -> finnhub.Client:
    global _finnhub_client
    if _finnhub_client is None:
        with _finnhub_client_lock:
            if _finnhub_client is None:
                _finnhub_client = finnhub.Client(api_key=FINNHUB_API_KEY)
    return _finnhub_client

# ─── TTL 缓存（防止高并发重复打 API）────────────────────────────────
_cache_lock    = threading.Lock()
_stock_cache: dict[str, tuple[float, dict]] = {}   # ticker → (ts, data)
_news_cache:  dict[str, tuple[float, list]] = {}   # ticker → (ts, news)
_tech_cache:  dict[str, tuple[float, dict]] = {}   # ticker → (ts, tech)  技术指标单独缓存
STOCK_CACHE_TTL = 180   # 3 分钟：行情数据
NEWS_CACHE_TTL  = 3600  # 60 分钟：新闻（改自 5 分钟，减少 API 请求）
TECH_CACHE_TTL  = 1800  # 30 分钟：技术指标（日线数据，日内变化缓慢）
_ticker_fetch_lock  = None   # 初始化在 main 里（需要事件循环）
_ticker_fetch_events: dict[str, asyncio.Event] = {}
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import RetryAfter
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest

# ─── 配置区 ──────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("PROMO_BOT_TOKEN", "YOUR_PROMO_BOT_TOKEN")
FINNHUB_API_KEY    = os.getenv("FINNHUB_API_KEY",    "YOUR_FINNHUB_KEY")
BINANCE_INVITE_URL = os.getenv("BINANCE_INVITE_URL", "https://www.binance.com")
# 底部按钮链接（待填写）
URL_OPEN_ACCOUNT  = os.getenv("URL_OPEN_ACCOUNT",  "https://www.binance.com")  # 美股开户
URL_BUY_TUTORIAL  = os.getenv("URL_BUY_TUTORIAL",  "https://www.binance.com")  # 购买教程
URL_GET_BONUS     = os.getenv("URL_GET_BONUS",     "https://www.binance.com")  # 领取福利
STATS_DB          = os.getenv("PROMO_STATS_DB", "stats_promo.db")              # 统计数据库
ADMIN_USER_ID     = int(os.getenv("ADMIN_USER_ID", "YOUR_ADMIN_TG_ID"))              # 管理员 TG ID
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
            ts         INTEGER,
            counted    INTEGER DEFAULT 1
        )
    """)
    # 迁移旧表：补充 counted 列
    try:
        conn.execute("ALTER TABLE queries ADD COLUMN counted INTEGER DEFAULT 1")
        conn.commit()
    except Exception:
        pass
    # 赛季配置表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # 群组设置表：自动删除 & 订阅
    conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_settings (
            chat_id        TEXT PRIMARY KEY,
            chat_title     TEXT,
            admin_user_id  TEXT,
            autodel        INTEGER DEFAULT 0,
            sub_daily      INTEGER DEFAULT 0,
            sub_binance    INTEGER DEFAULT 0
        )
    """)
    # 币安上新已推送记录（避免重复推）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS binance_pushed (
            article_id  INTEGER PRIMARY KEY
        )
    """)
    # 奖励收集表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS award_submissions (
            user_id       TEXT PRIMARY KEY,
            username      TEXT,
            full_name     TEXT,
            chat_id       TEXT,
            chat_title    TEXT,
            season_label  TEXT,
            rank          INTEGER,
            reward_usdt   INTEGER,
            address_type  TEXT,
            address       TEXT,
            binance_uid   TEXT,
            submitted_at  INTEGER
        )
    """)
    try:
        conn.execute("ALTER TABLE award_submissions ADD COLUMN merch_address TEXT DEFAULT ''")
        conn.commit()
    except Exception:
        pass
    # 奖励通知已发消息记录（用于撤回）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS award_sent_msgs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            target_id  TEXT,
            chat_id    TEXT,
            message_id INTEGER,
            sent_at    INTEGER
        )
    """)
    # 主动推送消息记录（每日新闻 + 币安公告）
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


def record_query(chat_id: str, chat_title: str, ticker: str):
    try:
        conn = sqlite3.connect(STATS_DB)
        # counted 逻辑：同群同ticker 10分钟内只计一分，每群每日上限500分
        now = int(time.time())
        day_start = now - (now % 86400)
        recent = conn.execute(
            "SELECT COUNT(*) FROM queries WHERE chat_id=? AND ticker=? AND ts>=? AND counted=1",
            (str(chat_id), ticker.upper(), now - 600)
        ).fetchone()[0]
        daily = conn.execute(
            "SELECT COUNT(*) FROM queries WHERE chat_id=? AND ts>=? AND counted=1",
            (str(chat_id), day_start)
        ).fetchone()[0]
        counted = 1 if (recent == 0 and daily < 500) else 0
        conn.execute(
            "INSERT INTO queries (chat_id, chat_title, ticker, ts, counted) VALUES (?,?,?,?,?)",
            (str(chat_id), chat_title or "私聊", ticker.upper(), now, counted)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"record_query error: {e}")

TICKER_NAMES = {
    "TSLA": "特斯拉", "AAPL": "苹果", "NVDA": "英伟达", "MSFT": "微软",
    "HXSCL": "SK海力士",
    "GOOGL": "谷歌", "AMZN": "亚马逊", "META": "Meta", "NFLX": "奈飞",
    "AMD": "AMD", "BABA": "阿里巴巴", "PDD": "拼多多", "JD": "京东",
    "BIDU": "百度", "NIO": "蔚来", "XPEV": "小鹏", "LI": "理想",
    "MSTR": "MicroStrategy", "COIN": "Coinbase", "HOOD": "Robinhood",
    "SPY": "标普500 ETF", "QQQ": "纳指100 ETF",
}


def _finnhub_curl(path: str) -> dict:
    """用 curl 直接调 Finnhub REST API，绕过 Python SSL 问题"""
    _proxy = os.getenv("HTTPS_PROXY", "") or os.getenv("https_proxy", "")
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
        _proxy = os.getenv("HTTPS_PROXY", "") or os.getenv("https_proxy", "")
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

        try:
            yahoo_vol = fut_vol.result(timeout=8)
            if yahoo_vol and yahoo_vol > 0:
                volume   = yahoo_vol
                turnover = volume * price
        except Exception as e:
            logger.debug(f"[yahoo_vol] result: {e}")

        return {
            "ticker":      ticker,
            "name":        name,
            "price":       price,
            "change":      change,
            "change_pct":  change_pct,
            "high_day":    high_day,
            "low_day":     low_day,
            "amplitude":   amplitude,
            "volume":      volume,
            "turnover":    turnover,
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
    """港股数据：腾讯财经 API（国内直连，有市值/PE/52周数据）"""
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
        mktcap_raw  = float(parts[37]) if parts[37] else 0
        pe_raw      = float(parts[39]) if parts[39] else None
        week52_high = float(parts[48]) if parts[48] else 0
        week52_low  = float(parts[49]) if parts[49] else 0

        market_cap  = mktcap_raw * 1e8

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
        client = _get_finnhub_client()

        # ── 三个 Finnhub API 并发请求（Python client 失败自动 curl fallback）──
        from concurrent.futures import as_completed
        def _fetch_quote():
            try:
                return client.quote(ticker)
            except Exception as e:
                logger.warning(f"quote python client failed ({e}), curl fallback")
                return _finnhub_curl(f"quote?symbol={ticker}")
        def _fetch_profile():
            try:
                return client.company_profile2(symbol=ticker)
            except Exception:
                return _finnhub_curl(f"stock/profile2?symbol={ticker}")
        def _fetch_metrics():
            try:
                return client.company_basic_financials(ticker, "all")
            except Exception:
                return _finnhub_curl(f"stock/metric?symbol={ticker}&metric=all")

        fut_quote   = EXECUTOR.submit(_fetch_quote)
        fut_profile = EXECUTOR.submit(_fetch_profile)
        fut_metrics = EXECUTOR.submit(_fetch_metrics)

        # 报价必须成功
        try:
            quote = fut_quote.result(timeout=10)
        except Exception as e:
            logger.error(f"quote fetch failed for {ticker}: {e}")
            return None

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
        volume     = quote.get("v", 0)   # Finnhub 免费版无此字段，后面从 tech 结果补
        turnover   = volume * price

        # ── 基本面（并发结果）──
        market_cap = 0
        name       = TICKER_NAMES.get(ticker, ticker)
        try:
            profile = fut_profile.result(timeout=6)
            market_cap = profile.get("marketCapitalization", 0) * 1e6
            # 中文名优先用 TICKER_NAMES，没有才用 profile 返回的英文名
            name = TICKER_NAMES.get(ticker, profile.get("name", ticker))
        except Exception as e:
            logger.warning(f"profile error (ignored): {e}")

        # ── 财务指标（并发结果）──
        week52_high = 0
        week52_low  = 0
        pe_ratio    = None
        try:
            metrics     = fut_metrics.result(timeout=6)
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
        # tech 顺带带回 yahoo_volume，直接复用，不再单独发一次 Yahoo 请求
        tech = get_technical_signals(ticker, client, price)

        # 从 tech 结果取 Yahoo 成交量（已在 get_technical_signals 内拉取）
        yahoo_vol = tech.get("yahoo_volume", 0)
        if yahoo_vol and yahoo_vol > 0:
            volume   = yahoo_vol
            turnover = volume * price

        return {
            "ticker":      ticker,
            "name":        name,
            "price":       price,
            "change":      change,
            "change_pct":  change_pct,
            "high_day":    high_day,
            "low_day":     low_day,
            "amplitude":   amplitude,
            "volume":      volume,
            "turnover":    turnover,
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
    """用 Yahoo Finance curl 拿日线，本地算 RSI/MA/MACD/量比。
    顺带返回 yahoo_volume 字段，供调用方复用，避免重复请求。
    结果带独立 TTL 缓存（TECH_CACHE_TTL=30min），日线数据变化慢。
    """
    # ── 命中缓存直接返回 ──
    now = time.time()
    with _cache_lock:
        if ticker in _tech_cache:
            ts, cached = _tech_cache[ticker]
            if now - ts < TECH_CACHE_TTL:
                logger.info(f"[cache] tech HIT {ticker} age={now-ts:.0f}s")
                return cached

    out = {"rsi_line": "", "macd_line": "", "ma_line": "", "vol_line": "", "yahoo_volume": 0}
    try:
        _proxy = os.getenv("HTTPS_PROXY", "") or os.getenv("https_proxy", "")
        _proxy_args = ["--proxy", _proxy] if _proxy else []
        cmd = ["curl", "-s", "--max-time", "10",
               "-H", "User-Agent: Mozilla/5.0"] + _proxy_args + [
               f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=3mo"]
        raw = subprocess.run(cmd, capture_output=True, text=True, timeout=12).stdout
        d = json.loads(raw)
        q = d["chart"]["result"][0]["indicators"]["quote"][0]
        closes  = [x for x in q.get("close", [])  if x is not None]
        volumes = [x for x in q.get("volume", []) if x is not None]
        # 顺带取最新成交量，供 get_stock_data 复用，避免重复请求
        if volumes:
            out["yahoo_volume"] = int(volumes[-1])
        # 取 meta 里的 regularMarketVolume 更准确
        try:
            meta_vol = d["chart"]["result"][0]["meta"].get("regularMarketVolume", 0)
            if meta_vol:
                out["yahoo_volume"] = int(meta_vol)
        except Exception:
            pass
        if len(closes) < 20:
            with _cache_lock:
                _tech_cache[ticker] = (time.time(), out)
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
        logger.warning(f"technical_signals error: {e}")
    # 写入技术指标缓存
    with _cache_lock:
        _tech_cache[ticker] = (time.time(), out)
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
    "闪迪": "SNDK",
    "西部数据": "WDC",
    "希捷": "STX",
    "台积电": "TSM", "台湾积电": "TSM",
    "美光": "MU", "美光科技": "MU",
    "安谋": "ARM", "ARM": "ARM",
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
    # SK海力士(000660.KS)/三星(005930.KS) 为韩股，Finnhub不支持，暂不加入
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
        _p = os.getenv("HTTPS_PROXY", "") or os.getenv("https_proxy", "")
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
    # 先查缓存
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
    """MyMemory 免费翻译接口（无代理可用）"""
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
    """Yahoo Finance 新闻 - 实时专业媒体，英文标题自动翻译中文
    filter_ticker: 用于相关性过滤的 ticker（港股时传原始 ticker，如 0700.HK）
    """
    _filter = filter_ticker or ticker
    import html as html_mod, email.utils
    results = []
    try:
        # query1/query2 轮试，其中一个限速时用另一个
        for host in ["query1", "query2"]:
            _proxy = os.getenv("HTTPS_PROXY", "") or os.getenv("https_proxy", "")
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
        # 过滤：先严格过滤，不足时降级放行（只过黑名单）
        results = []
        fallback = []
        for time_tag, title, link in candidates:
            if _is_relevant_news(title, _filter):
                zh = translate_to_zh(title)
                results.append((f"[{time_tag}] {zh if zh else title[:80]}", link, bool(zh)))
            else:
                # 黑名单过滤：仅过滤通用市场摘要，保留其余作为备用
                title_lower = title.lower()
                is_generic = any(kw in title_lower for kw in _GENERIC_NEWS_KEYWORDS)
                if not is_generic:
                    zh = translate_to_zh(title)
                    fallback.append((f"[{time_tag}] {zh if zh else title[:80]}", link, bool(zh)))
                else:
                    logger.info(f"[yahoo] filtered: {title[:60]}")
        # 港股搜公司英文名时，Yahoo 结果可能完全无关，不降级（让 wallst 兜底）
        # 美股严格过滤后不足才用 fallback 补充
        if len(results) < 3 and not is_hk_ticker(_filter):
            for item in fallback:
                if item not in results:
                    results.append(item)
                if len(results) >= 3:
                    break
    except Exception as e:
        logger.error(f"yahoo news error: {e}")
    return results


# 通用市场摘要新闻的关键词黑名单（与具体公司无关）
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

# ticker → 公司关键词列表（用于新闻相关性判断）
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
    "LI":   ["li auto", "li auto inc", "lixiang", "ideal auto", "理想汽车"],
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
    # 黑名单：明显是通用市场摘要
    for kw in _GENERIC_NEWS_KEYWORDS:
        if kw in title_lower:
            return False
    ticker_upper = ticker.upper()
    aliases = _TICKER_ALIASES.get(ticker_upper, [])
    # 白名单：标题直接提到 aliases 内任一关键词
    for alias in aliases:
        if alias in title_lower:
            return True
    # 白名单：标题提到纯 ticker（非港股）
    if not is_hk_ticker(ticker_upper) and ticker_upper.lower() in title_lower:
        return True
    # 中文公司名（TICKER_NAMES 里是中文，对英文新闻标题无效，跳过）
    # 已知公司未命中即过滤；未知 ticker 放行
    if ticker_upper in _TICKER_ALIASES:
        return False
    return True


def get_news_finnhub(ticker: str) -> list[tuple[str, str, bool]]:
    """Finnhub 公司新闻 - 用自己的 API key，英文标题自动翻译，过滤通用市场摘要"""
    results = []
    try:
        client = _get_finnhub_client()
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
            # 过滤通用市场新闻
            if not _is_relevant_news(title, ticker):
                logger.info(f"[finnhub] filtered generic news: {title[:60]}")
                continue
            age_h = (now_ts - pub_ts) / 3600 if pub_ts else 999
            if age_h > 72:  # 超过3天不要
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
    """币安 Square 新闻 - 匹配 ticker 对应的 bStocks 新闻，返回最多 1 条"""
    results = []
    try:
        cmd = [
            "curl", "-s", "--max-time", "8",
            "https://www.binance.com/bapi/composite/v4/friendly/pgc/feed/news/list?strategy=10"
        ]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout
        d = json.loads(out)
        vos = d.get("data", {}).get("vos", [])
        ticker_upper = ticker.upper()
        for item in vos:
            # tradingPairs 里的 code 是如 NVDAB、AAPLB，去掉末尾 B 对应原始 ticker
            pairs = item.get("tradingPairs") or []
            pairs += item.get("tradingPairsV2") or []
            matched = any(
                p.get("code", "").upper().rstrip("B") == ticker_upper
                or p.get("code", "").upper() == ticker_upper + "B"
                for p in pairs
            )
            if not matched:
                continue
            title   = item.get("title", "").strip()
            link    = item.get("webLink", "")
            pub_ts  = item.get("date", 0)
            # 再过一遍相关性过滤
            if title and not _is_relevant_news(title, ticker):
                logger.info(f"[binance] filtered: {title[:60]}")
                continue
            if not title:
                continue
            now_ts  = int(time.time())
            age_h   = (now_ts - pub_ts) / 3600 if pub_ts else 999
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
            break  # 只取 1 条
    except Exception as e:
        logger.error(f"binance news error: {e}")
    return results


def _get_yahoo_query(ticker: str) -> str:
    """港股用英文公司名搜 Yahoo，美股直接用 ticker"""
    if not is_hk_ticker(ticker):
        return ticker
    ticker_upper = ticker.upper()
    aliases = _TICKER_ALIASES.get(ticker_upper, [])
    if aliases:
        return aliases[0].capitalize()  # 如 "Tencent", "Xiaomi"
    for cn, sym in CN_TO_TICKER.items():
        if sym == ticker_upper:
            en = CN_TO_SEARCH.get(cn)
            if en:
                return en
    return ticker


def get_news(ticker: str) -> list[tuple[str, str, bool]]:
    """新闻策略：币安1条 + 其他2条；全部并发拉取，按优先级合并"""
    logger.info(f"[news] fetching for {ticker}")
    is_hk = is_hk_ticker(ticker)
    yahoo_query = _get_yahoo_query(ticker)
    hk_equiv = _US_TO_HK.get(ticker) if not is_hk else None
    logger.info(f"[news] yahoo_query={yahoo_query!r} is_hk={is_hk} hk_equiv={hk_equiv}")

    # 并发提交所有新闻源
    futures: dict[str, object] = {}
    futures["binance"] = EXECUTOR.submit(get_news_binance, ticker)
    if not is_hk:
        futures["finnhub"] = EXECUTOR.submit(get_news_finnhub, ticker)
    futures["yahoo"]   = EXECUTOR.submit(get_news_yahoo, yahoo_query, ticker if is_hk else None)
    futures["wallst"]  = EXECUTOR.submit(get_news_wallst, ticker)
    if is_hk:
        futures["em_hk"] = EXECUTOR.submit(get_news_eastmoney_hk, ticker)
    elif hk_equiv:
        futures["em_hk"] = EXECUTOR.submit(get_news_eastmoney_hk, hk_equiv)

    # 收集结果（最多等 8 秒）
    results_map: dict[str, list] = {}
    for name, fut in futures.items():
        try:
            results_map[name] = fut.result(timeout=8)
            logger.info(f"[news] {name} returned {len(results_map[name])} items")
        except Exception as e:
            logger.warning(f"[news] {name} failed: {e}")
            results_map[name] = []

    # 合并：币安 1 条优先，其余按优先级补到 2 条
    binance_news = results_map.get("binance", [])
    priority_order = ["em_hk", "finnhub", "yahoo", "wallst"] if not is_hk else ["em_hk", "yahoo", "wallst"]

    other: list = []
    seen: set[str] = set()
    for src in priority_order:
        for item in results_map.get(src, []):
            key = item[0][:30]
            if key not in seen:
                seen.add(key)
                other.append(item)
        if len(other) >= 3:
            break

    if binance_news:
        final = binance_news[:1] + other[:2]
    else:
        final = other[:3]

    logger.info(f"[news] final {len(final)} items for {ticker}")
    return final



# ── 加密货币模块 ────────────────────────────────────────
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
    "ZEC": "zcash", "DASH": "dash", "FTM": "fantom", "EGLD": "elrond-erd-2",
    "SAND": "the-sandbox", "MANA": "decentraland", "AXS": "axie-infinity",
    "IMX": "immutable-x", "CHZ": "chiliz", "MINA": "mina-protocol",
    "BAT": "basic-attention-token", "1INCH": "1inch", "SNX": "havven",
    "ENJ": "enjincoin", "ZIL": "zilliqa", "KAVA": "kava", "ROSE": "oasis-network",
    "OCEAN": "ocean-protocol", "FLOKI": "floki", "PYTH": "pyth-network",
    "STRK": "starknet", "OKB": "okb", "FLR": "flare-networks", "ARB": "arbitrum",
    "ALGO": "algorand",
}


def _curl_get_json(url: str) -> dict | list:
    """curl 请求返回 JSON，自动带代理"""
    _proxy = os.getenv("HTTPS_PROXY", "") or os.getenv("https_proxy", "")
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

    _proxy = os.getenv("HTTPS_PROXY", "") or os.getenv("https_proxy", "")
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
        _proxy = os.getenv("HTTPS_PROXY", "") or os.getenv("https_proxy", "")
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


def fmt_vol(n: float) -> str:
    """成交量格式化"""
    if not n: return "N/A"
    if n >= 1e8: return f"{n/1e8:.2f}亿股"
    if n >= 1e4: return f"{n/1e4:.1f}万股"
    return f"{n:.0f}股"

def fmt_turnover(n: float) -> str:
    """成交额格式化（美元）"""
    if not n: return "N/A"
    if n >= 1e9: return f"${n/1e9:.2f}B"
    if n >= 1e6: return f"${n/1e6:.1f}M"
    if n >= 1e3: return f"${n/1e3:.1f}K"
    return f"${n:.0f}"

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


def build_keyboard(ticker: str = ""):
    return None


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
    mc_str = (fmt_hkd(data["market_cap"]) if is_hk else fmt(data["market_cap"])) if data.get("market_cap") else "N/A"
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
        f"📊 成交量: <code>{fmt_vol(data.get('volume', 0))}</code>   成交额: <code>{fmt_turnover(data.get('turnover', 0))}</code>\n"
        f"📐 P/E: <code>{he(pe_str)}</code>\n\n"
        f"📉 技术面\n"
        + (f"  {data['rsi_line']}\n"  if data.get('rsi_line')  else "")
        + (f"  {data['ma_line']}\n"   if data.get('ma_line')   else "")
        + (f"  {data['macd_line']}\n" if data.get('macd_line') else "")
        + (f"  {data['vol_line']}\n"  if data.get('vol_line')  else "")
        + f"  52周位置: <code>{data['pos_str']}</code>  {data['trend']}\n\n"
        f"📰 最新动态\n"
        f"{news_lines}"
        f"———————————————————\n"
        f"📊 数据来源：<a href=\"https://www.binance.com/en/markets/coinInfo-bStocks\">币安</a>\n"
    )


def cn_to_ticker_lookup(cn: str) -> str | None:
    """中文名 → ticker，先查本地表，再用 Finnhub 搜索兜底"""
    # 1. 直接命中本地表
    if cn in CN_TO_TICKER:
        return CN_TO_TICKER[cn]
    # 2. 查搜索关键词表 → Finnhub symbol_lookup
    search_kw = CN_TO_SEARCH.get(cn, cn)  # 没有映射就直接用中文名搜
    try:
        client = _get_finnhub_client()
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
    """支持英文 #TSLA、港股 #0700.HK 和中文 #腾讯 三种格式"""
    found = []
    # 港股数字代码（如 #0700.HK 或 #700.HK）
    for t in re.findall(r"#(\d{1,5}\.HK)", text, re.IGNORECASE):
        num, suffix = t.upper().split(".")
        t_norm = num.zfill(4) + ".HK"
        if t_norm not in found:
            found.append(t_norm)
    # 先匹配中文
    for cn in re.findall(r"#([\u4e00-\u9fff][A-Za-z0-9\u4e00-\u9fff]{1,9})", text):
        ticker = cn_to_ticker_lookup(cn)
        if ticker and ticker not in found:
            found.append(ticker)
    # 再匹配英文（含加密币识别）
    for t in re.findall(r"#([A-Za-z]{1,5})", text):
        t = t.upper()
        if t in _CRYPTO_SYMBOLS:
            t = "CRYPTO:" + t
        if t not in found:
            found.append(t)
    return found[:3]


# ═══════════════════════════════════════════════════════════════
# 排行榜群名涂黑/截断
# ═══════════════════════════════════════════════════════════════

_TRUNCATE_WORDS = ["返佣", "返利", "返水", "OKX", "欧易", "欧意", "Gate", "芝鸻", "binance", "币安", "点位分享", "行情分析"]
_CENSOR_WORDS   = ["返佣", "返利", "返水"]

def censor_title(t: str) -> str:
    import re as _re
    if "-" in t:
        prefix, suffix = t.split("-", 1)
        if any(w.lower() in suffix.lower() for w in _TRUNCATE_WORDS):
            t = prefix.strip()
    for w in _CENSOR_WORDS:
        t = _re.sub(_re.escape(w), "█" * len(w), t, flags=_re.IGNORECASE)
    return t

# ═══════════════════════════════════════════════════════════════
# 新闻分类关键词
# ═══════════════════════════════════════════════════════════════

_MACRO_KW = ["美联储","CPI","通脁","利率","就业","GDP","降息","加息","财政","特朗普","关税","美傗","国傗","经济","衰退","鲍威尔","议息","非农","零售","贸易战","制裁","日元","欧央行","黄金","原油","石油","宏观"]
_STOCK_KW = ["股","纳斯达克","标普","道琼斯","英伟达","苹果","特斯拉","微软","谷歌","亚马逊","Meta","NYSE","IPO","收购","财报","营收","净利","NVIDIA","Apple","Tesla","美股","上市","华尔街","Hugging Face","OpenAI","AI芯片"]
_CRYPTO_KW = ["BTC","ETH","比特币","以太坊","代币","链上","矿工","魔表","DEX","DeFi","NFT","USDT","加密","巨鲸","币安","交易所","USDC","稳定币","山寨","Meme","空投","合约","Web3","区块链","Coinbase","Solana","SOL","HYPE","PONS"]

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

def _fetch_blockbeats_all() -> list:
    try:
        BLOCKBEATS_KEY = "YOUR_BLOCKBEATS_API_KEY"
        r = subprocess.run(
            ["curl", "-s", "--max-time", "15",
             "-H", f"api-key: {BLOCKBEATS_KEY}",
             "https://api-pro.theblockbeats.info/v1/newsflash/24h?lang=cn"],
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

_TOPIC_KW = ["美股", "股票", "行情", "资讯", "新闻", "财经", "market", "stock", "news", "finance"]

async def _get_forum_thread(app, chat_id: int):
    try:
        import urllib.request as _ur, json as _j
        proxy_url = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy", "")
        token = app.bot.token
        url = f"https://api.telegram.org/bot{token}/getForumTopics?chat_id={chat_id}&limit=100"
        if proxy_url:
            opener = _ur.build_opener(_ur.ProxyHandler({"https": proxy_url, "http": proxy_url}))
            resp = opener.open(url, timeout=8)
        else:
            resp = _ur.urlopen(url, timeout=8)
        data = _j.loads(resp.read())
        topics = data.get("result", {}).get("topics", [])
        if not topics:
            return None
        for topic in topics:
            name = (topic.get("name") or "").lower()
            if any(kw.lower() in name for kw in _TOPIC_KW):
                return topic.get("message_thread_id")
        return None
    except Exception as e:
        logger.debug(f"[forum] chat={chat_id}: {e}")
        return None

_member_count_cache: dict[str, tuple[float, int]] = {}  # cid → (ts, count)
_MEMBER_CACHE_TTL = 600  # 10分钟缓存

_push_targets_cache: dict[str, tuple[float, list]] = {}  # field → (ts, result)
_PUSH_TARGETS_CACHE_TTL = 600  # 整体结果缓存10分钟

async def _get_push_targets(app, field: str) -> list:
    now_ts = time.time()
    cached = _push_targets_cache.get(field)
    if cached and (now_ts - cached[0]) < _PUSH_TARGETS_CACHE_TTL:
        logger.debug(f"[push_targets] cache hit field={field} total={len(cached[1])}")
        return cached[1]
    manual = set(get_subscribed_chats(field))
    conn = sqlite3.connect(STATS_DB)
    rows = conn.execute(
        "SELECT DISTINCT chat_id FROM queries WHERE CAST(chat_id AS INTEGER) < 0"
    ).fetchall()
    conn.close()
    auto = set()
    now_ts = time.time()
    for (cid,) in rows:
        if cid in manual:
            auto.add(cid)
            continue
        # 使用缓存，避免每分钟对所有群实时查成员数
        cached = _member_count_cache.get(cid)
        if cached and (now_ts - cached[0]) < _MEMBER_CACHE_TTL:
            if cached[1] >= 50:
                auto.add(cid)
            continue
        try:
            count = await app.bot.get_chat_member_count(chat_id=int(cid))
            _member_count_cache[cid] = (now_ts, count)
            if count >= 50:
                auto.add(cid)
        except Exception as e:
            _member_count_cache[cid] = (now_ts, 0)  # 失败也缓存，避免重复请求
            logger.debug(f"[push_targets] {cid}: {e}")
        await asyncio.sleep(0.15)
    result = list(manual | auto)
    _push_targets_cache[field] = (now_ts, result)  # 缓存整体结果
    logger.info(f"[push_targets] field={field} total={len(result)}")
    return result

async def _daily_news_push_job(app):
    chats = await _get_push_targets(app, "sub_daily")
    if not chats:
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
    if total < 10:
        used = set(i["title"] for its in selected.values() for i in its)
        for item in all_items:
            if total >= 10: break
            if item["title"] not in used:
                selected["币圈"].append(item)
                used.add(item["title"]); total += 1
    if not any(selected.values()):
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
            lines.append(f'{idx}. <a href="{lk}">{he(i["title"])}</a>' if lk else f'{idx}. {he(i["title"])}')
            idx += 1
    lines.append("\n📊 数据来源：律动财经 · 币安")
    msg = "\n".join(lines)
    # 均匀分布在 60 秒内发完，避免短时间内触发 Telegram 429 限速
    n = len(chats)
    interval = min(60.0 / max(n, 1), 3.0)
    for chat_id in chats:
        for attempt in range(4):
            try:
                thread_id = await _get_forum_thread(app, int(chat_id))
                kwargs = {"message_thread_id": thread_id} if thread_id else {}
                sent = await app.bot.send_message(chat_id=int(chat_id), text=msg, parse_mode="HTML",
                                           disable_web_page_preview=True, **kwargs)
                if sent:
                    _save_push_msg("daily", int(chat_id), sent.message_id)
                break
            except RetryAfter as e:
                wait = e.retry_after + 1
                logger.warning(f"[daily_push] 429 RetryAfter {wait}s (attempt {attempt+1})")
                await asyncio.sleep(wait)
            except Exception as e:
                logger.warning(f"[daily_push] chat={chat_id}: {e}")
                break
        await asyncio.sleep(interval)

async def _auto_delete(bot_or_context, chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try:
        bot = getattr(bot_or_context, "bot", bot_or_context)
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.info(f"[autodel] deleted msg={message_id} chat={chat_id}")
    except Exception as e:
        logger.debug(f"[autodel] delete failed: {e}")

async def _binance_listing_poll_job(app):
    chats = await _get_push_targets(app, "sub_binance")
    if not chats:
        return
    _LISTING_KW = ["现货", "合约", "上线", "上市", "新增交易对", "永续合约", "交割合约"]
    try:
        r = subprocess.run(
            ["curl", "-s", "--proxy", "socks5://127.0.0.1:7890", "--max-time", "10",
             "-H", "lang: zh-CN",
             "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&pageSize=10&pageNo=1"],
            capture_output=True, text=True, timeout=12
        )
        d = json.loads(r.stdout)
        articles = []
        for cat in d.get("data", {}).get("catalogs", []):
            if cat.get("catalogId") == 48:
                articles = cat.get("articles", [])
                break
    except Exception as e:
        logger.debug(f"[binance_poll] 获取失败: {e}")
        return
    if not articles:
        return
    conn = sqlite3.connect(STATS_DB)
    for art in articles:
        aid   = art.get("id")
        title = art.get("title", "")
        if not aid or not title:
            continue
        if not any(kw in title for kw in _LISTING_KW):
            conn.execute("INSERT OR IGNORE INTO binance_pushed (article_id) VALUES (?)", (aid,))
            conn.commit()
            continue
        exists = conn.execute("SELECT 1 FROM binance_pushed WHERE article_id=?", (aid,)).fetchone()
        if exists:
            continue
        conn.execute("INSERT INTO binance_pushed (article_id) VALUES (?)", (aid,))
        conn.commit()
        url = f"https://www.binance.com/zh-CN/support/announcement/{art.get('code', '')}"
        msg = f"🔔 <b>币安上新公告</b>\n\n{he(title)}\n\n🔗 <a href=\"{url}\">查看详情</a>"
        n = len(chats)
        interval = min(60.0 / max(n, 1), 3.0)
        for chat_id in chats:
            for attempt in range(4):
                try:
                    thread_id = await _get_forum_thread(app, int(chat_id))
                    kwargs = {"message_thread_id": thread_id} if thread_id else {}
                    sent = await app.bot.send_message(chat_id=int(chat_id), text=msg, parse_mode="HTML",
                                                      disable_web_page_preview=False, **kwargs)
                    if sent:
                        _save_push_msg("binance", int(chat_id), sent.message_id)
                        asyncio.create_task(_auto_delete(app, int(chat_id), sent.message_id, 120))
                    break
                except RetryAfter as e:
                    wait = e.retry_after + 1
                    logger.warning(f"[binance_poll] 429 RetryAfter {wait}s (attempt {attempt+1})")
                    await asyncio.sleep(wait)
                except Exception as e:
                    logger.warning(f"[binance_poll] chat={chat_id}: {e}")
                    break
            await asyncio.sleep(interval)
    conn.close()

def _get_daily_pushed_date() -> str:
    """从 DB 读取今日推送记录，返回日期字符串 YYYY-MM-DD 或空字符串"""
    conn = sqlite3.connect(STATS_DB)
    row = conn.execute("SELECT value FROM config WHERE key='daily_pushed_date'").fetchone()
    conn.close()
    return row[0] if row else ""

def _set_daily_pushed_date(date_str: str):
    conn = sqlite3.connect(STATS_DB)
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('daily_pushed_date', ?)", (date_str,))
    conn.commit()
    conn.close()

async def _scheduler(app):
    while True:
        try:
            now = datetime.datetime.now()
            today_str = now.strftime("%Y-%m-%d")

            # 只在10:00整触发（精确1分钟窗口），DB记录防止重启后重复推
            if now.hour == 10 and now.minute == 0:
                if _get_daily_pushed_date() != today_str:
                    _set_daily_pushed_date(today_str)
                    logger.info("[scheduler] 10:00 daily news push")
                    await _daily_news_push_job(app)

            await _binance_listing_poll_job(app)
        except Exception as e:
            logger.warning(f"[scheduler] error: {e}")
        await asyncio.sleep(60)

async def _post_init(app):
    asyncio.create_task(_scheduler(app))

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 先检测是否获奖群主，是的话直接发奖励通知，不走普通 /start 流程
    if update.effective_chat and update.effective_chat.type == "private":
        handled = await _check_and_greet_award_winner(update, context)
        if handled:
            return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ 管理我的群组设置", callback_data="mg:open")],
    ])
    await update.message.reply_text(
        "👋 <b>欢迎使用美股行情机器人！</b>\n\n"
        "📊 <b>怎么查股票？</b>\n"
        "在任意群聊中发送 <code>#股票代码</code> 即可：\n"
        "  <code>#AAPL</code>  <code>#TSLA</code>  <code>#NVDA</code>\n"
        "  <code>#苹果</code>  <code>#特斯拉</code>  <code>#英伟达</code>\n"
        "  <code>#腾讯</code>  <code>#比特币</code>  <code>#BTC</code>\n\n"
        "📰 <b>主动推送服务</b>\n"
        "如果你是群主，可以为你的群开启：\n"
        "  • 每日早10点热点资讯日报\n"
        "  • 币安新币上线实时公告\n"
        "  • 股票播报2分钟后自动删除\n\n"
        "👇 点击下方按钮配置你的群组：",
        parse_mode="HTML",
        reply_markup=kb
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Bot error: {context.error}", exc_info=context.error)


# ═══════════════════════════════════════════════════════════════
# 群组设置辅助函数
# ═══════════════════════════════════════════════════════════════

def get_season_start() -> int:
    conn = sqlite3.connect(STATS_DB)
    row = conn.execute("SELECT value FROM config WHERE key='season_start'").fetchone()
    conn.close()
    return int(row[0]) if row else 0

def set_season_start(ts: int):
    conn = sqlite3.connect(STATS_DB)
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('season_start', ?)", (str(ts),))
    conn.commit()
    conn.close()

def get_chat_setting(chat_id: str) -> dict:
    conn = sqlite3.connect(STATS_DB)
    row = conn.execute(
        "SELECT autodel, sub_daily, sub_binance FROM chat_settings WHERE chat_id=?",
        (chat_id,)
    ).fetchone()
    conn.close()
    if row:
        return {"autodel": bool(row[0]), "sub_daily": bool(row[1]), "sub_binance": bool(row[2])}
    return {"autodel": False, "sub_daily": False, "sub_binance": False}

def upsert_chat_setting(chat_id: str, chat_title: str, admin_user_id: str, **kwargs):
    conn = sqlite3.connect(STATS_DB)
    conn.execute(
        "INSERT INTO chat_settings (chat_id, chat_title, admin_user_id) VALUES (?,?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET "
        "chat_title=CASE WHEN excluded.chat_title!='' THEN excluded.chat_title ELSE chat_title END, "
        "admin_user_id=excluded.admin_user_id",
        (chat_id, chat_title, admin_user_id)
    )
    for k, v in kwargs.items():
        conn.execute(f"UPDATE chat_settings SET {k}=? WHERE chat_id=?", (int(v), chat_id))
    conn.commit()
    conn.close()

def get_subscribed_chats(field: str) -> list:
    """field: sub_daily 或 sub_binance"""
    conn = sqlite3.connect(STATS_DB)
    rows = conn.execute(f"SELECT chat_id FROM chat_settings WHERE {field}=1").fetchall()
    conn.close()
    return [r[0] for r in rows]

# ═══════════════════════════════════════════════════════════════
# 功能1：/autodel 命令
# ═══════════════════════════════════════════════════════════════

async def autodel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("⚠️ /autodel 请在群组内使用")
        return
    try:
        member = await chat.get_member(user.id)
        if member.status not in ("creator", "administrator"):
            await update.message.reply_text("⚠️ 只有群主或管理员可以设置")
            return
    except Exception:
        await update.message.reply_text("⚠️ 无法验证权限，请确保 bot 是管理员")
        return

    args = context.args
    if not args or args[0].lower() not in ("on", "off"):
        setting = get_chat_setting(str(chat.id))
        status = "已开启" if setting["autodel"] else "未开启"
        await update.message.reply_text(
            f"🗑 自动删除状态：<b>{status}</b>\n\n"
            f"<code>/autodel on</code> — 开启（播报2分钟后自动删除）\n"
            f"<code>/autodel off</code> — 关闭",
            parse_mode="HTML"
        )
        return

    enable = args[0].lower() == "on"
    upsert_chat_setting(str(chat.id), chat.title or "", str(user.id), autodel=enable)
    if enable:
        await update.message.reply_text("✅ 已开启自动删除，播报消息将在2分钟后自动删除")
    else:
        await update.message.reply_text("✅ 已关闭自动删除，播报消息将永久保留")

# ═══════════════════════════════════════════════════════════════
# 群主管理面板 /manage /setup
# ═══════════════════════════════════════════════════════════════

def _build_manage_keyboard(chat_id: str) -> InlineKeyboardMarkup:
    s = get_chat_setting(chat_id)
    on, off = "✅", "⬜"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"{on if s['sub_daily'] else off} 每日热点新闻推送",
            callback_data=f"mg:{chat_id}:sub_daily"
        )],
        [InlineKeyboardButton(
            f"{on if s['sub_binance'] else off} 币安上新实时公告",
            callback_data=f"mg:{chat_id}:sub_binance"
        )],
        [InlineKeyboardButton(
            f"{on if s['autodel'] else off} 股票播报2分钟后自动删除",
            callback_data=f"mg:{chat_id}:autodel"
        )],
        [InlineKeyboardButton("🔙 切换群组", callback_data="mg:switch")],
    ])

async def manage_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        if update.message:
            await update.message.reply_text("⚠️ 请私聊机器人使用 /manage")
        return

    conn = sqlite3.connect(STATS_DB)
    managed = conn.execute(
        "SELECT chat_id, chat_title FROM chat_settings WHERE admin_user_id=?",
        (str(user.id),)
    ).fetchall()
    queried = conn.execute(
        "SELECT DISTINCT chat_id FROM queries WHERE CAST(chat_id AS INTEGER) < 0"
    ).fetchall()
    conn.close()

    managed_ids = {r[0] for r in managed}

    # 如果已有记录，直接快速响应，不再重新验证
    if managed:
        all_groups = list(managed)
    else:
        # 首次使用：并发检查所有群，看哪些群用户是管理员
        unknown = [(cid,) for (cid,) in queried if cid not in managed_ids]

        async def _check_admin(cid):
            try:
                member = await context.bot.get_chat_member(chat_id=int(cid), user_id=user.id)
                if member.status in ("creator", "administrator"):
                    chat_obj = await context.bot.get_chat(chat_id=int(cid))
                    title = chat_obj.title or cid
                    upsert_chat_setting(cid, title, str(user.id))
                    return (cid, title)
            except Exception:
                pass
            return None

        results = await asyncio.gather(*[_check_admin(cid) for (cid,) in unknown])
        extra = [r for r in results if r]
        all_groups = list(managed) + extra
    msg_obj = update.message or (update.callback_query.message if update.callback_query else None)
    if not all_groups:
        if msg_obj:
            await msg_obj.reply_text(
                "⚠️ 没有找到你管理的群组。\n\n"
                "请确认：\n"
                "1. 你是该群的群主或管理员\n"
                "2. 群里有人用过 #股票代码 查询过"
            )
        return

    if len(all_groups) == 1:
        cid, title = all_groups[0]
        upsert_chat_setting(cid, title, str(user.id))
        kb = _build_manage_keyboard(cid)
        txt = f"⚙️ <b>群组设置</b>\n\n📌 群组：<b>{he(title)}</b>\n\n点击选项切换开/关："
        if update.callback_query:
            await update.callback_query.edit_message_text(txt, parse_mode="HTML", reply_markup=kb)
        else:
            await msg_obj.reply_text(txt, parse_mode="HTML", reply_markup=kb)
    else:
        buttons = [
            [InlineKeyboardButton(f"📌 {t[:30]}", callback_data=f"mg_sel:{c}:{t[:20]}")]
            for c, t in all_groups
        ]
        txt = "⚙️ <b>请选择要管理的群组：</b>"
        if update.callback_query:
            await update.callback_query.edit_message_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await msg_obj.reply_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))

async def manage_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user  = update.effective_user
    if not query or not user:
        return
    await query.answer()
    data = query.data or ""

    if data.startswith("mg_sel:"):
        parts = data.split(":", 2)
        cid   = parts[1]
        title = parts[2] if len(parts) > 2 else cid
        upsert_chat_setting(cid, title, str(user.id))
        kb  = _build_manage_keyboard(cid)
        txt = f"⚙️ <b>群组设置</b>\n\n📌 群组：<b>{he(title)}</b>\n\n点击选项切换开/关："
        await query.edit_message_text(txt, parse_mode="HTML", reply_markup=kb)
        return

    if data in ("mg:switch", "mg:open"):
        await manage_command(update, context)
        return

    if data.startswith("mg:"):
        parts = data.split(":")
        if len(parts) < 3:
            return
        cid, field = parts[1], parts[2]
        try:
            member = await context.bot.get_chat_member(chat_id=int(cid), user_id=user.id)
            if member.status not in ("creator", "administrator"):
                await query.answer("⚠️ 你不是该群的管理员", show_alert=True)
                return
        except Exception:
            await query.answer("⚠️ 无法验证权限", show_alert=True)
            return

        s = get_chat_setting(cid)
        new_val = not s.get(field, False)
        upsert_chat_setting(cid, "", str(user.id), **{field: new_val})

        conn = sqlite3.connect(STATS_DB)
        row = conn.execute("SELECT chat_title FROM chat_settings WHERE chat_id=?", (cid,)).fetchone()
        conn.close()
        title = row[0] if row else cid

        label_map = {"sub_daily": "每日热点新闻推送", "sub_binance": "币安上新实时公告", "autodel": "股票播报自动删除"}
        status = "已开启 ✅" if new_val else "已关闭 ⬜"
        await query.answer(f"{label_map.get(field, field)} {status}")
        kb  = _build_manage_keyboard(cid)
        txt = f"⚙️ <b>群组设置</b>\n\n📌 群组：<b>{he(title)}</b>\n\n点击选项切换开/关："
        await query.edit_message_text(txt, parse_mode="HTML", reply_markup=kb)

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """所有人可用的 /stats 命令，查询当前赛季群组排行榜"""
    logger.info(f"[stats] uid={update.effective_user.id if update.effective_user else '?'} chat_type={update.effective_chat.type if update.effective_chat else '?'}")
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    try:
        season_start = get_season_start()
        conn = sqlite3.connect(STATS_DB)
        day_start = int(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        season_filter = max(season_start, 0)

        total_valid = conn.execute(
            "SELECT COUNT(*) FROM queries WHERE counted=1 AND ts>=?", (season_filter,)
        ).fetchone()[0]
        today_valid = conn.execute(
            "SELECT COUNT(*) FROM queries WHERE counted=1 AND ts>=? AND ts>=?",
            (day_start, season_filter)
        ).fetchone()[0]

        if season_start > 0:
            season_dt = datetime.datetime.fromtimestamp(season_start).strftime("%m月%d日 %H:%M")
            season_label = f"本赛季开始: {season_dt}"
        else:
            season_label = "统计全部历史数据"

        chats = conn.execute("""
            SELECT chat_title, COUNT(*) as score
            FROM queries
            WHERE counted=1 AND CAST(chat_id AS INTEGER) < 0 AND ts>=?
            GROUP BY chat_id
            ORDER BY score DESC
            LIMIT 10
        """, (season_filter,)).fetchall()

        tickers = conn.execute("""
            SELECT ticker, COUNT(*) as cnt
            FROM queries
            WHERE counted=1 AND ts>=?
            GROUP BY ticker
            ORDER BY cnt DESC
            LIMIT 10
        """, (season_filter,)).fetchall()

        conn.close()

        rank_lines = []
        for i, (title, score) in enumerate(chats):
            icon = medals[i] if i < len(medals) else f"{i+1}."
            rank_lines.append(f"{icon} {censor_title(title)}  <b>{score}</b>分")
        rank_str = "\n".join(rank_lines) or "  暂无数据"
        ticker_lines = "  " + "  ".join(f"#{t}({c}次)" for t, c in tickers) if tickers else "  暂无数据"

        msg = (
            f"📊 <b>活动排行榜（本赛季）</b>\n"
            f"<i>{season_label}</i>\n"
            f"有效总计: <code>{total_valid}</code>次   今日有效: <code>{today_valid}</code>次\n"
            f"<i>规则：同群同ticker 10分钟内只计1分，每群每日上限500分</i>\n\n"
            f"🏆 <b>群组排名 Top 10</b>\n{rank_str}\n\n"
            f"🔥 <b>热门股票</b>\n{ticker_lines}"
        )
        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"统计查询失败: {e}")


async def newseason_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理员专用：/newseason 开启新赛季（分数清零，历史保留）"""
    if not update.effective_user or update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ 仅管理员可用")
        return
    now_ts = int(time.time())
    set_season_start(now_ts)
    dt = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"[newseason] season reset at {dt} by uid={update.effective_user.id}")
    await update.message.reply_text(
        f"✅ <b>新赛季已开启！</b>\n"
        f"开始时间: <code>{dt}</code>\n"
        f"历史数据已保留，排行榜从零开始计分。",
        parse_mode="HTML"
    )


# ═══════════════════════════════════════════════════════════════
# 奖励发放功能
# ═══════════════════════════════════════════════════════════════

AWARD_SEASON_LABEL = "第二赛季"
AWARD_RANKINGS = [
    {"rank": 1,  "chat_id": -100RANK_GROUP_1, "chat_title": "MemeticMonk狗宝殿",      "reward_usdt": 100},
    {"rank": 2,  "chat_id": -100RANK_GROUP_2, "chat_title": "曼波猪脚饭",              "reward_usdt": 30},
    {"rank": 3,  "chat_id": -100RANK_GROUP_3, "chat_title": "天天X网",                 "reward_usdt": 30},
    {"rank": 4,  "chat_id": -100RANK_GROUP_4, "chat_title": "只要肝不死就往死里干",    "reward_usdt": 20},
    {"rank": 5,  "chat_id": -100RANK_GROUP_5, "chat_title": "比特币炒币交流群",        "reward_usdt": 20},
    {"rank": 6,  "chat_id": -100RANK_GROUP_6, "chat_title": "三建学园（方外之境）",    "reward_usdt": 20},
    {"rank": 7,  "chat_id": -100RANK_GROUP_7, "chat_title": "小董和她的富豪朋友们",    "reward_usdt": 20},
    {"rank": 8,  "chat_id": -100RANK_GROUP_8, "chat_title": "币小超社区",              "reward_usdt": 20},
    {"rank": 9,  "chat_id": -100RANK_GROUP_9, "chat_title": "八方社区",                "reward_usdt": 20},
    {"rank": 10, "chat_id": -100RANK_GROUP_10, "chat_title": "专心搞u，偶尔悟道",       "reward_usdt": 20},
]
AWARD_LUCKY = [
    {"chat_id": -100LUCKY_GROUP_1, "chat_title": "VM唯有搞钱社区"},
    {"chat_id": -100LUCKY_GROUP_2, "chat_title": "幣圈訊息一籮筐"},
    {"chat_id": -100LUCKY_GROUP_3, "chat_title": "脫貧致富搖錢鼠"},
]
RANK_ICONS = {1: "🥇", 2: "🥈", 3: "🥉"}
AWARD_POSTER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "award_poster_s2.jpg")

# 周边对话式收集状态 {user_id: {rank_info, step, merch_address, merch_name}}
_merch_pending: dict = {}


def _get_award_for_chat(chat_id: int):
    for r in AWARD_RANKINGS:
        if r["chat_id"] == chat_id:
            return r
    return None


def _save_submission(user_id, username, full_name, chat_id, chat_title,
                     rank, reward_usdt, address_type, address,
                     binance_uid, merch_address=""):
    conn = sqlite3.connect(STATS_DB)
    conn.execute("""
        INSERT OR REPLACE INTO award_submissions
        (user_id, username, full_name, chat_id, chat_title, season_label,
         rank, reward_usdt, address_type, address, binance_uid, merch_address, submitted_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (user_id, username, full_name, chat_id, chat_title,
          AWARD_SEASON_LABEL, rank, reward_usdt, address_type,
          address, binance_uid, merch_address, int(time.time())))
    conn.commit()
    conn.close()


async def _find_winner_rank(bot, user_id: int):
    """通过 Telegram API 查找用户是哪个排名群的群主
    
    优先用 getChatAdministrators（无需 bot 是群管理员），
    fallback 到 getChatMember（需要 bot 是管理员）。
    """
    for r in AWARD_RANKINGS:
        try:
            admins = await bot.get_chat_administrators(r["chat_id"])
            for admin in admins:
                if admin.user.id == user_id and admin.status == "creator":
                    return r
        except Exception:
            # fallback: 尝试 getChatMember（bot 是管理员时可用）
            try:
                member = await bot.get_chat_member(r["chat_id"], user_id)
                if member.status == "creator":
                    return r
            except Exception:
                continue
    return None


async def _check_and_greet_award_winner(update, context) -> bool:
    """私聊 /start 时检查是否为获奖群主，是则发奖励通知+选择按钮。返回 True 表示已处理。"""
    user = update.effective_user
    if not user:
        return False
    rank_info = await _find_winner_rank(context.bot, user.id)
    if not rank_info:
        return False

    rank = rank_info["rank"]
    icon = RANK_ICONS.get(rank, f"#{rank}")
    usdt = rank_info["reward_usdt"]
    title = rank_info["chat_title"]

    if rank <= 3:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"💰 领取 {usdt} USDT", callback_data=f"award:usdt:{rank}"),
        ]])
        choice_hint = f"奖励：<b>{usdt} USDT</b>"
    else:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(f"💰 领取 {usdt} USDT", callback_data=f"award:usdt:{rank}"),
            InlineKeyboardButton("🎒 币安双肩包", callback_data=f"award:merch:{rank}"),
        ]])
        choice_hint = f"奖励可选：<b>{usdt} USDT</b> 或 🎒 <b>币安品牌双肩包</b>"

    # 先发海报
    try:
        with open(AWARD_POSTER_PATH, "rb") as f:
            await update.message.reply_photo(photo=f)
    except Exception as e:
        logger.warning(f"[award] poster send failed: {e}")
    # 再发按钮消息
    await update.message.reply_text(
        f"🎉 <b>恭喜！{AWARD_SEASON_LABEL}排行榜奖励通知</b>\n\n"
        f"你的群 <b>{title}</b> 荣获 {icon} 第 {rank} 名\n"
        f"{choice_hint}\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"请点击下方按钮选择领奖方式 👇",
        parse_mode="HTML",
        reply_markup=kb
    )
    return True


async def _handle_award_callback(update, context) -> bool:
    """处理 award:usdt/merch 的 inline button 回调"""
    query = update.callback_query
    if not query or not query.data.startswith("award:"):
        return False
    await query.answer()
    parts = query.data.split(":")
    action = parts[1]
    rank = int(parts[2])
    rank_info = next((r for r in AWARD_RANKINGS if r["rank"] == rank), None)
    if not rank_info:
        await query.edit_message_text("⚠️ 排名信息未找到，请联系管理员")
        return True

    user = query.from_user
    user_id = str(user.id)
    icon = RANK_ICONS.get(rank, f"#{rank}")

    if action == "usdt":
        await query.edit_message_text(
            f"💰 <b>领取 USDT</b>\n\n"
            f"群组：{rank_info['chat_title']} {icon} 第 {rank} 名\n"
            f"奖励：<b>{rank_info['reward_usdt']} USDT</b>\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"请发送命令提交币安UID：\n"
            f"<code>/submit_usdt 币安UID</code>\n\n"
            f"示例：\n"
            f"<code>/submit_usdt 123456789</code>\n\n"
            f"⏰ 请在 <b>7天内</b> 提交，逾期视为放弃",
            parse_mode="HTML"
        )
    elif action == "merch":
        _merch_pending[user_id] = {"rank_info": rank_info, "step": "waiting_address"}
        await query.edit_message_text(
            f"🎒 <b>币安品牌双肩包 — 填写收货信息</b>\n\n"
            f"群组：{rank_info['chat_title']} {icon} 第 {rank} 名\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"第 1/3 步：请直接回复你的 <b>收货地址</b>\n"
            f"（省/市/区/街道详情）",
            parse_mode="HTML"
        )
    return True


async def _handle_merch_conversation(update, context) -> bool:
    """拦截私聊文字消息，处理周边实物地址收集对话。返回 True 表示已消费。"""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return False
    user_id = str(user.id)
    state = _merch_pending.get(user_id)
    if not state:
        return False

    text = update.message.text.strip()
    rank_info = state["rank_info"]
    step = state["step"]
    icon = RANK_ICONS.get(rank_info["rank"], f"#{rank_info['rank']}")

    if step == "waiting_address":
        state["merch_address"] = text
        state["step"] = "waiting_name"
        await update.message.reply_text(
            "✅ 地址已记录\n\n第 2/3 步：请回复 <b>收货人姓名</b>：",
            parse_mode="HTML"
        )
        return True

    elif step == "waiting_name":
        state["merch_name"] = text
        state["step"] = "waiting_phone"
        await update.message.reply_text(
            "✅ 姓名已记录\n\n第 3/3 步：请回复 <b>手机号</b>：",
            parse_mode="HTML"
        )
        return True

    elif step == "waiting_phone":
        state["merch_phone"] = text
        full_merch = (
            f"地址：{state['merch_address']} | "
            f"姓名：{state['merch_name']} | "
            f"电话：{state['merch_phone']}"
        )
        _save_submission(
            user_id=user_id,
            username=user.username or "",
            full_name=user.full_name or "",
            chat_id=str(rank_info["chat_id"]),
            chat_title=rank_info["chat_title"],
            rank=rank_info["rank"],
            reward_usdt=rank_info["reward_usdt"],
            address_type="周边-双肩包",
            address="",
            binance_uid="",
            merch_address=full_merch
        )
        del _merch_pending[user_id]
        await update.message.reply_text(
            f"✅ <b>收货信息提交成功！</b>\n\n"
            f"群组：{rank_info['chat_title']} {icon} 第 {rank_info['rank']} 名\n"
            f"地址：{state['merch_address']}\n"
            f"姓名：{state['merch_name']}\n"
            f"电话：{state['merch_phone']}\n\n"
            f"我们将在 <b>7个工作日内</b> 安排发货 📦",
            parse_mode="HTML"
        )
        logger.info(f"[award] merch submitted: uid={user_id} rank={rank_info['rank']} info={full_merch}")
        return True

    return False


def _save_push_msg(push_type: str, chat_id: int, message_id: int):
    """记录每日新闻/币安公告推送的 message_id，用于后续删除"""
    conn = sqlite3.connect(STATS_DB)
    conn.execute(
        "INSERT INTO push_sent_msgs (push_type, chat_id, message_id, sent_at) VALUES (?,?,?,?)",
        (push_type, str(chat_id), message_id, int(time.time()))
    )
    conn.commit()
    conn.close()


def _save_sent_msg(target_id: str, chat_id: str, message_id: int):
    conn = sqlite3.connect(STATS_DB)
    conn.execute(
        "INSERT INTO award_sent_msgs (target_id, chat_id, message_id, sent_at) VALUES (?,?,?,?)",
        (str(target_id), str(chat_id), message_id, int(time.time()))
    )
    conn.commit()
    conn.close()


async def _send_award_to_one(bot, rank_info: dict, status_lines: list):
    """对单个群发奖励通知，并存储 message_id"""
    chat_id = rank_info["chat_id"]
    chat_title = rank_info["chat_title"]
    rank = rank_info["rank"]
    icon = RANK_ICONS.get(rank, f"#{rank}")
    usdt = rank_info["reward_usdt"]
    choice_hint = f"奖励：{usdt} USDT" if rank <= 3 else f"奖励可选：{usdt} USDT 或 🎒 币安双肩包"

    try:
        admins = await bot.get_chat_administrators(chat_id)
        creator = next((a for a in admins if a.status == "creator"), None)
        if not creator:
            status_lines.append(f"⚠️ {icon} {chat_title}：未找到群主")
            return

        creator_id = creator.user.id
        creator_name = creator.user.full_name

        if rank <= 3:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"💰 领取 {usdt} USDT", callback_data=f"award:usdt:{rank}")]])
        else:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"💰 领取 {usdt} USDT", callback_data=f"award:usdt:{rank}"),
                                        InlineKeyboardButton("🎒 币安双肩包", callback_data=f"award:merch:{rank}")]])
        msg_text = (
            f"🎉 <b>恭喜！{AWARD_SEASON_LABEL}排行榜奖励通知</b>\n\n"
            f"你的群 <b>{chat_title}</b> 荣获 {icon} 第 {rank} 名\n"
            f"{choice_hint}\n\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"请点击下方按钮选择领奖方式 👇"
        )

        dm_sent = False
        try:
            # 先发海报
            try:
                with open(AWARD_POSTER_PATH, "rb") as f:
                    poster_sent = await bot.send_photo(chat_id=creator_id, photo=f)
                    _save_sent_msg(creator_id, creator_id, poster_sent.message_id)
            except Exception as pe:
                logger.warning(f"[award] poster to {creator_id} failed: {pe}")
            # 再发按钮消息
            sent = await bot.send_message(chat_id=creator_id, text=msg_text, parse_mode="HTML", reply_markup=kb)
            _save_sent_msg(creator_id, creator_id, sent.message_id)
            dm_sent = True
            status_lines.append(f"✅ {icon} {chat_title}：已私信群主 {creator_name}")
        except Exception:
            pass

        if not dm_sent:
            mention = f'<a href="tg://user?id={creator_id}">{creator_name}</a>'
            sent = await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🏆 {mention} 恭喜！\n"
                    f"你的群在{AWARD_SEASON_LABEL}排行榜中荣获 {icon} 第 {rank} 名\n"
                    f"{choice_hint}\n\n"
                    f"请私信 @stocknews_forbot 领取奖励 🎁"
                ),
                parse_mode="HTML"
            )
            _save_sent_msg(creator_id, chat_id, sent.message_id)
            status_lines.append(f"📢 {icon} {chat_title}：私信失败，已在群内@群主 {creator_name}")

    except Exception as e:
        status_lines.append(f"❌ {icon} {chat_title}：出错 {e}")


async def award_command(update, context):
    """管理员专用：/award — 向所有排名群+幸运社区群主发奖励通知"""
    if not update.effective_user or update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ 仅管理员可用")
        return
    bot = context.bot
    status_lines = []
    for rank_info in AWARD_RANKINGS:
        await _send_award_to_one(bot, rank_info, status_lines)
        await asyncio.sleep(1)
    # 幸运社区
    for lucky in AWARD_LUCKY:
        try:
            admins = await bot.get_chat_administrators(lucky["chat_id"])
            creator = next((a for a in admins if a.status == "creator"), None)
            if not creator:
                status_lines.append(f"⚠️ 🎁 {lucky['chat_title']}：未找到群主")
                continue
            creator_id = creator.user.id
            creator_name = creator.user.full_name
            lucky_msg = (
                f"🎁 <b>幸运社区奖励 — {AWARD_SEASON_LABEL}</b>\n\n"
                f"恭喜你的群 <b>{lucky['chat_title']}</b> 被选为本赛季幸运社区！\n"
                f"奖励：<b>币安品牌双肩包 ×1</b>\n\n"
                f"请私信我们确认收货信息，7天内有效 🎉"
            )
            dm_sent = False
            try:
                sent = await bot.send_message(chat_id=creator_id, text=lucky_msg, parse_mode="HTML")
                _save_sent_msg(creator_id, creator_id, sent.message_id)
                dm_sent = True
                status_lines.append(f"✅ 🎁 {lucky['chat_title']}：已私信群主 {creator_name}")
            except Exception:
                pass
            if not dm_sent:
                mention = f'<a href="tg://user?id={creator_id}">{creator_name}</a>'
                sent = await bot.send_message(
                    chat_id=lucky["chat_id"],
                    text=(f"🎁 {mention} 恭喜！\n你的群被选为{AWARD_SEASON_LABEL}幸运社区\n"
                          f"请私信 @stocknews_forbot 领取 <b>币安品牌双肩包</b> 🎉"),
                    parse_mode="HTML"
                )
                _save_sent_msg(creator_id, lucky["chat_id"], sent.message_id)
                status_lines.append(f"📢 🎁 {lucky['chat_title']}：已在群内@群主 {creator_name}")
        except Exception as e:
            status_lines.append(f"❌ 🎁 {lucky['chat_title']}：出错 {e}")
        await asyncio.sleep(1)
    await update.message.reply_text(
        f"✅ <b>/award 执行完毕</b>\n\n" + "\n".join(status_lines), parse_mode="HTML"
    )


async def award_test_command(update, context):
    """管理员专用：/award_test <chat_id> — 只对单个群测试，不影响其他群"""
    if not update.effective_user or update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ 仅管理员可用")
        return
    args = context.args
    if not args:
        lines = ["用法：<code>/award_test &lt;chat_id&gt;</code>", "", "当前排名群："]
        for r in AWARD_RANKINGS:
            lines.append(f"  {r['rank']}. {r['chat_title']} → <code>{r['chat_id']}</code>")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        return
    try:
        test_chat_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ chat_id 必须是数字")
        return
    rank_info = _get_award_for_chat(test_chat_id)
    if not rank_info:
        await update.message.reply_text(f"❌ chat_id {test_chat_id} 不在排名列表中")
        return
    status_lines = []
    await _send_award_to_one(context.bot, rank_info, status_lines)
    await update.message.reply_text(
        f"🧪 <b>测试发送结果</b>\n\n" + "\n".join(status_lines), parse_mode="HTML"
    )


async def award_recall_command(update, context):
    """管理员专用：/award_recall — 撤回所有已发的奖励通知消息"""
    if not update.effective_user or update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ 仅管理员可用")
        return
    bot = context.bot
    conn = sqlite3.connect(STATS_DB)
    rows = conn.execute("SELECT id, target_id, chat_id, message_id FROM award_sent_msgs").fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("📭 没有已记录的发送消息")
        return
    ok, fail = 0, 0
    for row_id, target_id, chat_id, message_id in rows:
        try:
            await bot.delete_message(chat_id=int(chat_id), message_id=message_id)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.3)
    # 清空记录
    conn = sqlite3.connect(STATS_DB)
    conn.execute("DELETE FROM award_sent_msgs")
    conn.commit()
    conn.close()
    await update.message.reply_text(
        f"✅ 撤回完成：成功 {ok} 条，失败 {fail} 条（已超48h或bot无权限）"
    )


async def submit_usdt_command(update, context):
    """群主私信提交币安UID：/submit_usdt 币安UID"""
    user = update.effective_user
    chat = update.effective_chat
    if not chat or chat.type != "private":
        await update.message.reply_text("⚠️ 请在私聊中提交")
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "❌ 格式错误，请发送：\n<code>/submit_usdt 币安UID</code>\n\n示例：<code>/submit_usdt 123456789</code>",
            parse_mode="HTML"
        )
        return

    binance_uid = args[0].strip()

    # 简单校验：币安UID 为纯数字
    if not binance_uid.isdigit():
        await update.message.reply_text(
            "⚠️ 币安UID 应为纯数字，请检查后重新提交",
            parse_mode="HTML"
        )
        return

    user_id = str(user.id)
    rank_info = await _find_winner_rank(context.bot, user.id)

    if not rank_info:
        await update.message.reply_text("⚠️ 未找到你的群主身份，请联系管理员")
        return

    _save_submission(
        user_id=user_id, username=user.username or "",
        full_name=user.full_name or "", chat_id=str(rank_info["chat_id"]),
        chat_title=rank_info["chat_title"], rank=rank_info["rank"],
        reward_usdt=rank_info["reward_usdt"], address_type="USDT",
        address="", binance_uid=binance_uid
    )
    icon = RANK_ICONS.get(rank_info["rank"], f"#{rank_info['rank']}")
    await update.message.reply_text(
        f"✅ <b>提交成功！</b>\n\n"
        f"群组：{rank_info['chat_title']}\n"
        f"排名：{icon} 第 {rank_info['rank']} 名\n"
        f"奖励：{rank_info['reward_usdt']} USDT\n"
        f"币安UID：<code>{binance_uid}</code>\n\n"
        f"我们将在 <b>7个工作日内</b> 发放，请留意到账通知 🙏",
        parse_mode="HTML"
    )
    logger.info(f"[award] USDT: uid={user_id} rank={rank_info['rank']} buid={binance_uid}")


async def submit_merch_command(update, context):
    """/submit_merch — 手动触发周边收货流程（按钮触发的走 callback，此命令备用）"""
    user = update.effective_user
    chat = update.effective_chat
    if not chat or chat.type != "private":
        await update.message.reply_text("⚠️ 请在私聊中使用")
        return
    user_id = str(user.id)
    rank_info = await _find_winner_rank(context.bot, user.id)
    if not rank_info:
        await update.message.reply_text("⚠️ 未找到你的群主身份，请联系管理员")
        return
    if rank_info["rank"] <= 3:
        await update.message.reply_text("⚠️ 1-3名奖励为固定USDT，请使用 /submit_usdt 提交")
        return
    _merch_pending[user_id] = {"rank_info": rank_info, "step": "waiting_address"}
    icon = RANK_ICONS.get(rank_info["rank"], f"#{rank_info['rank']}")
    await update.message.reply_text(
        f"🎒 <b>币安品牌双肩包 — 填写收货信息</b>\n\n"
        f"群组：{rank_info['chat_title']} {icon} 第 {rank_info['rank']} 名\n\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"第 1/3 步：请直接回复你的 <b>收货地址</b>\n（省/市/区/街道详情）",
        parse_mode="HTML"
    )


async def collect_command(update, context):
    """管理员专用：/collect — 汇总所有已提交的奖励"""
    if not update.effective_user or update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ 仅管理员可用")
        return

    conn = sqlite3.connect(STATS_DB)
    rows = conn.execute("""
        SELECT rank, chat_title, full_name, username, address_type, address,
               binance_uid, COALESCE(merch_address,''), submitted_at
        FROM award_submissions ORDER BY rank ASC
    """).fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text("📭 暂无提交记录")
        return

    lines = [f"📋 <b>{AWARD_SEASON_LABEL}奖励汇总</b> ({len(rows)}/{len(AWARD_RANKINGS)} 已提交)\n"]
    for rank, chat_title, full_name, username, addr_type, address, buid, merch_addr, ts in rows:
        icon = RANK_ICONS.get(rank, f"#{rank}")
        uname = f"@{username}" if username else full_name
        dt = datetime.datetime.fromtimestamp(ts).strftime("%m-%d %H:%M")
        if addr_type == "USDT":
            lines.append(
                f"{icon} <b>第{rank}名</b> {chat_title}\n"
                f"   群主：{uname}\n"
                f"   类型：USDT\n"
                f"   币安UID：<code>{buid}</code>\n"
                f"   时间：{dt}\n"
            )
        else:
            lines.append(
                f"{icon} <b>第{rank}名</b> {chat_title}\n"
                f"   群主：{uname}\n"
                f"   类型：{addr_type}\n"
                f"   收货：{merch_addr}\n"
                f"   时间：{dt}\n"
            )

    submitted_ranks = {r[0] for r in rows}
    missing = [r for r in AWARD_RANKINGS if r["rank"] not in submitted_ranks]
    if missing:
        lines.append("\n⏳ <b>未提交</b>")
        for r in missing:
            icon = RANK_ICONS.get(r["rank"], f"#{r['rank']}")
            lines.append(f"   {icon} 第{r['rank']}名 {r['chat_title']}")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def delete_push_command(update, context):
    """管理员专用：/delete_push [N小时]—删除最近N小时内所有主动推送的消息，默认24h"""
    if not update.effective_user or update.effective_user.id != ADMIN_USER_ID:
        await update.message.reply_text("⛔ 仅管理员可用")
        return
    args = context.args
    hours = int(args[0]) if args and args[0].isdigit() else 24
    since_ts = int(time.time()) - hours * 3600
    bot = context.bot
    conn = sqlite3.connect(STATS_DB)
    rows = conn.execute(
        "SELECT id, chat_id, message_id, push_type, sent_at FROM push_sent_msgs WHERE sent_at >= ? ORDER BY sent_at DESC",
        (since_ts,)
    ).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text(f"📭 最近 {hours} 小时内无推送记录")
        return
    await update.message.reply_text(f"🗑 开始删除最近 {hours} 小时的 {len(rows)} 条推送消息...")
    ok = fail = 0
    ids_to_delete = []
    for row_id, chat_id, message_id, push_type, sent_at in rows:
        try:
            await bot.delete_message(chat_id=int(chat_id), message_id=message_id)
            ok += 1
            ids_to_delete.append(row_id)
        except Exception as e:
            logger.debug(f"[delete_push] failed chat={chat_id} msg={message_id}: {e}")
            fail += 1
        await asyncio.sleep(0.05)
    # 清除已删成功的记录
    if ids_to_delete:
        conn = sqlite3.connect(STATS_DB)
        conn.execute(f"DELETE FROM push_sent_msgs WHERE id IN ({','.join('?'*len(ids_to_delete))})", ids_to_delete)
        conn.commit()
        conn.close()
    await update.message.reply_text(
        f"✅ 删除完成：成功 {ok} 条，失败 {fail} 条（已超 48h 或 bot 无权限）"
    )


async def _fetch_stock_cached(ticker: str, loop: asyncio.AbstractEventLoop) -> dict | None:
    """TTL 缓存 + 并发去重：同 ticker 同时多个请求只打一次真实 API"""
    global _ticker_fetch_lock
    if _ticker_fetch_lock is None:
        _ticker_fetch_lock = asyncio.Lock()

    # 1. 先查缓存（无锁）
    now = time.time()
    with _cache_lock:
        if ticker in _stock_cache:
            ts, cached = _stock_cache[ticker]
            if now - ts < STOCK_CACHE_TTL:
                logger.info(f"[cache] stock HIT {ticker} age={now-ts:.0f}s")
                return cached

    # 2. 加锁检查是否已有请求在飞
    async with _ticker_fetch_lock:
        # 再次检查缓存（加锁期间可能已被别人填充）
        now = time.time()
        with _cache_lock:
            if ticker in _stock_cache:
                ts, cached = _stock_cache[ticker]
                if now - ts < STOCK_CACHE_TTL:
                    logger.info(f"[cache] stock HIT2 {ticker} age={now-ts:.0f}s")
                    return cached

        # 已有同 ticker 请求在飞，等它的 Event
        if ticker in _ticker_fetch_events:
            evt = _ticker_fetch_events[ticker]
            logger.info(f"[cache] stock WAIT {ticker} (dedup)")
        else:
            evt = None
            new_evt = asyncio.Event()
            _ticker_fetch_events[ticker] = new_evt

    if evt is not None:
        # 等待已有请求完成
        await evt.wait()
        with _cache_lock:
            if ticker in _stock_cache:
                _, cached = _stock_cache[ticker]
                return cached
        return None

    # 3. 本请求负责真实拉取
    try:
        data = await loop.run_in_executor(EXECUTOR, get_stock_data, ticker)
        with _cache_lock:
            _stock_cache[ticker] = (time.time(), data)
        return data
    finally:
        async with _ticker_fetch_lock:
            evt_to_set = _ticker_fetch_events.pop(ticker, None)
        if evt_to_set:
            evt_to_set.set()


async def _fetch_news_cached(ticker: str, loop: asyncio.AbstractEventLoop) -> list:
    """TTL 缓存新闻"""
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


async def _send_with_retry(message, text: str) -> None:
    """Telegram 发送重试，处理 RetryAfter 和网络抖动"""
    for attempt in range(4):
        try:
            await message.reply_text(text, parse_mode=ParseMode.HTML)
            return
        except RetryAfter as e:
            wait = e.retry_after + 1
            logger.warning(f"[flood] RetryAfter {wait}s (attempt {attempt+1})")
            await asyncio.sleep(wait)
        except Exception as e:
            if attempt < 3:
                wait = 5 * (attempt + 1)
                logger.warning(f"[send] failed attempt {attempt+1}, retry in {wait}s: {e}")
                await asyncio.sleep(wait)
            else:
                logger.error(f"[send] gave up after 4 attempts: {e}")




# ═══════════════════════════════════════════════════════════════
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    # 私聊逻辑：周边对话进行中优先拦截
    # 注意：获奖群主检测已移到 /start 命令，此处不再重复检测（避免每条消息都遍历13个群）
    if update.effective_chat and update.effective_chat.type == "private":
        handled = await _handle_merch_conversation(update, context)
        if handled:
            return
    logger.info(f"[msg] from={update.message.from_user.id if update.message.from_user else '?'} chat={update.message.chat_id} text={repr(update.message.text[:50])}")
    tickers = extract_tickers(update.message.text)
    logger.info(f"[msg] extracted tickers: {tickers}")
    if not tickers:
        return

    loop = asyncio.get_running_loop()
    chat_id    = update.message.chat_id
    chat_title = update.message.chat.title or ""
    setting = get_chat_setting(str(chat_id)) if int(chat_id) < 0 else {"autodel": False}

    for ticker in tickers:
        ticker = ticker.upper()
        await update.message.chat.send_action("typing")

        # ── 加密货币分流 ──
        if ticker.startswith("CRYPTO:"):
            symbol = ticker[7:]
            loop2 = asyncio.get_running_loop()
            data = await loop2.run_in_executor(None, get_crypto_data, symbol)
            news = await loop2.run_in_executor(None, get_crypto_news, symbol)
            if int(chat_id) < 0:
                record_query(str(chat_id), chat_title, symbol)
            if not data:
                await update.message.reply_text(
                    f"❌ 未找到 <code>{symbol}</code> 的行情，请检查代码是否正确。",
                    parse_mode=ParseMode.HTML
                )
                continue
            msg = build_crypto_message(data, news)
            await _send_with_retry(update.message, msg)
            continue

        # 即时回复“查询中”，让用户知道 bot 已收到请求
        # 命中缓存时不发（直接马上出结果）
        _is_cached = False
        with _cache_lock:
            if ticker in _stock_cache:
                ts, _ = _stock_cache[ticker]
                if time.time() - ts < STOCK_CACHE_TTL:
                    _is_cached = True

        placeholder = None
        if not _is_cached:
            try:
                placeholder = await update.message.reply_text(
                    f"⏳ <code>{ticker}</code> 查询中，请稍候...",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass

        # 行情和新闻并发拉取（带 TTL 缓存 + 并发去重）
        data, news = await asyncio.gather(
            _fetch_stock_cached(ticker, loop),
            _fetch_news_cached(ticker, loop),
        )

        # 只统计群组查询（chat_id < 0 为群组，> 0 为私聊）
        if int(chat_id) < 0:
            record_query(str(chat_id), chat_title, ticker)

        if not data:
            err_msg = f"❌ 未找到 <code>{ticker}</code> 的数据，请检查代码是否正确。"
            if placeholder:
                try:
                    await placeholder.edit_text(err_msg, parse_mode=ParseMode.HTML)
                    continue
                except Exception:
                    pass
            await update.message.reply_text(err_msg, parse_mode=ParseMode.HTML)
            continue

        msg = build_message(data, news)
        # 有占位消息就编辑它，否则重新发
        if placeholder:
            for attempt in range(4):
                try:
                    await placeholder.edit_text(msg, parse_mode=ParseMode.HTML)
                    if setting["autodel"]:
                        asyncio.create_task(_auto_delete(context, chat_id, placeholder.message_id, 120))
                    break
                except RetryAfter as e:
                    await asyncio.sleep(e.retry_after + 1)
                except Exception as e:
                    if attempt < 3:
                        await asyncio.sleep(5 * (attempt + 1))
                    else:
                        logger.error(f"[send] edit gave up: {e}")
        else:
            for attempt in range(4):
                try:
                    sent = await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
                    if setting["autodel"] and sent:
                        asyncio.create_task(_auto_delete(context, chat_id, sent.message_id, 120))
                    break
                except RetryAfter as e:
                    await asyncio.sleep(e.retry_after + 1)
                except Exception as e:
                    if attempt < 3:
                        await asyncio.sleep(5 * (attempt + 1))
                    else:
                        logger.error(f"[send] gave up: {e}")


def main():
    # ── 进程锁：防止多实例同时运行 ──────────────────────────────────
    import fcntl, sys
    _lockfile_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bot_promo.lock")
    _lockfile = open(_lockfile_path, "w")
    try:
        fcntl.flock(_lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lockfile.write(str(os.getpid()))
        _lockfile.flush()
    except BlockingIOError:
        logger.error("Another bot_promo instance is already running. Exiting.")
        _lockfile.close()
        sys.exit(1)
    # ─────────────────────────────────────────────────────────────────

    proxy = os.getenv("HTTPS_PROXY", "") or os.getenv("https_proxy", "")
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
                _pre_start_ok = True
                break
        except Exception as _e:
            logger.warning(f"Pre-start getUpdates error: {_e}, retrying in 10s...")
            time.sleep(10)
    if not _pre_start_ok:
        logger.error("Pre-start: still 409 after 8 attempts, giving up. launchd will retry.")
        import sys
        sys.exit(1)

    app = (builder
        .post_init(_post_init)
        .build()
    )
    init_db()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("newseason", newseason_command))
    app.add_handler(CommandHandler("autodel", autodel_command))
    app.add_handler(CommandHandler("manage", manage_command))
    app.add_handler(CommandHandler("setup", manage_command))
    app.add_handler(CommandHandler("delete_push", delete_push_command))
    app.add_handler(CommandHandler("award", award_command))
    app.add_handler(CommandHandler("award_test", award_test_command))
    app.add_handler(CommandHandler("award_recall", award_recall_command))
    app.add_handler(CommandHandler("collect", collect_command))
    app.add_handler(CommandHandler("submit_usdt", submit_usdt_command))
    app.add_handler(CommandHandler("submit_merch", submit_merch_command))
    app.add_handler(CallbackQueryHandler(manage_callback, pattern=r"^mg[_:]"))
    app.add_handler(CallbackQueryHandler(
        lambda u, c: _handle_award_callback(u, c),
        pattern=r"^award:"
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)
    logger.info("Bot started — text-only mode.")
    app.run_polling(drop_pending_updates=True)  # 默认监听 SIGTERM，launchd stop 时优雅退出


if __name__ == "__main__":
    main()