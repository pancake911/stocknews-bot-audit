#!/bin/bash
# Bot 健康监控 - 常驻进程，每 2 分钟检查一次
# 核心检查：日志里最后一次 getUpdates 200 OK 的时间
# getMe 只是短请求，通了不代表 bot 在工作；只有 getUpdates 持续 200 才说明 bot 真正在收消息

SELF_BOT_TOKEN="YOUR_SELF_BOT_TOKEN"
PROMO_BOT_TOKEN="YOUR_PROMO_BOT_TOKEN"
ADMIN_ID="YOUR_ADMIN_TELEGRAM_ID"
PROXY="http://127.0.0.1:12000"
WORKDIR="/Users/xx/.openclaw-aisatoshi2026/workspace-aisatoshi2026/stock-bot"
LOG="$WORKDIR/monitor.log"

# getUpdates 超过这么多秒没出现 → 判定为断线
MAX_SILENCE_SECONDS=300  # 5 分钟

send_tg() {
    curl -s --proxy "$PROXY" \
        -X POST "https://api.telegram.org/bot${SELF_BOT_TOKEN}/sendMessage" \
        -d chat_id="$ADMIN_ID" \
        -d text="$1" \
        -d parse_mode="HTML" > /dev/null 2>&1
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG"
}

# 从 bot 的 error.log 里找最后一次 "getUpdates.*200 OK" 的时间戳，返回距今秒数
seconds_since_last_update() {
    local logfile="$1"
    # 取最后一条 getUpdates 200 OK 的时间戳行（格式：INFO:httpx:... "HTTP/1.1 200 OK"）
    # 日志行里没有时间戳，用文件 mtime 做兜底
    local last_line
    last_line=$(grep "getUpdates.*200 OK" "$logfile" 2>/dev/null | tail -1)
    if [ -z "$last_line" ]; then
        echo 99999
        return
    fi
    # 用 log 文件最后写入时间（getUpdates 200 几乎每30秒就有一条，文件 mtime 足够精确）
    local mtime
    mtime=$(stat -f "%m" "$logfile" 2>/dev/null)
    local now
    now=$(date +%s)
    echo $((now - mtime))
}

restart_bot() {
    local script="$1"
    local service="$2"
    pkill -f "$script" 2>/dev/null
    sleep 3
    rm -f "$WORKDIR/.${script%.py}.lock"
    launchctl kickstart -k "gui/$(id -u)/$service" > /dev/null 2>&1
    sleep 15
}

check_bot() {
    local name="$1"
    local script="$2"
    local service="$3"
    local errlog="$4"

    local silence
    silence=$(seconds_since_last_update "$errlog")

    if [ "$silence" -lt "$MAX_SILENCE_SECONDS" ]; then
        log "$name OK (last getUpdates ${silence}s ago)"
        return 0
    fi

    log "$name DEAD — getUpdates 已 ${silence}s 无响应，强制重启..."

    restart_bot "$script" "$service"

    # 重启后等 15 秒再检查
    local silence2
    silence2=$(seconds_since_last_update "$errlog")
    if [ "$silence2" -lt "$MAX_SILENCE_SECONDS" ]; then
        log "$name 重启成功"
        send_tg "⚠️ <b>Bot 监控</b>
<b>${name}</b> 长连接断开 ${silence}s，已自动重启 ✅
时间：$(date '+%m-%d %H:%M')"
    else
        log "$name 重启后仍无 getUpdates！"
        send_tg "🚨 <b>Bot 监控紧急告警</b>
<b>${name}</b> 重启后仍无响应！
时间：$(date '+%m-%d %H:%M')
需要人工介入！"
    fi
}

log "=== 监控启动（getUpdates 日志检测模式，每 2 分钟）==="

while true; do
    check_bot "自用版 (@BinanceStock_BOT)" \
        "bot.py" "com.stockbot" \
        "$WORKDIR/bot.error.log"

    check_bot "对外版 (@stocknews_forbot)" \
        "bot_promo.py" "com.stockbot.promo" \
        "$WORKDIR/bot_promo.error.log"

    tail -1000 "$LOG" > "${LOG}.tmp" && mv "${LOG}.tmp" "$LOG"

    sleep 120  # 每 2 分钟检查一次
done
