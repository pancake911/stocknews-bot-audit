#!/bin/bash
# Bot 健康监控脚本 - 每 5 分钟由 launchd 调用
# 真正的健康检查：调用 getMe API 验证 bot 能通信，而不只是看进程存在

SELF_BOT_TOKEN="8880493449:AAGQ8jhT9YRJOKQKAt_CQvUhrJ9NXtIrCWU"
PROMO_BOT_TOKEN="8899487203:AAFVnrL8nH1IAPdxvUM_402nIxfGYjwaUVI"
ADMIN_ID="5471917452"
PROXY="http://127.0.0.1:12000"
LOG="/Users/xx/.openclaw-aisatoshi2026/workspace-aisatoshi2026/stock-bot/monitor.log"
WORKDIR="/Users/xx/.openclaw-aisatoshi2026/workspace-aisatoshi2026/stock-bot"

send_tg() {
    local msg="$1"
    curl -s --proxy "$PROXY" \
        -X POST "https://api.telegram.org/bot${SELF_BOT_TOKEN}/sendMessage" \
        -d chat_id="$ADMIN_ID" \
        -d text="$msg" \
        -d parse_mode="HTML" \
        > /dev/null 2>&1
}

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG"
}

# 真正的健康检查：调用 getMe，5 秒内返回 ok:true 才算健康
bot_is_healthy() {
    local token="$1"
    local result
    result=$(curl -s --proxy "$PROXY" --max-time 5 \
        "https://api.telegram.org/bot${token}/getMe" 2>/dev/null)
    echo "$result" | grep -q '"ok":true'
}

restart_bot() {
    local script="$1"
    local service="$2"
    local lockfile="$WORKDIR/.${script%.py}.lock"
    rm -f "$lockfile"
    launchctl kickstart -k "gui/$(id -u)/$service" > /dev/null 2>&1
    sleep 10
}

check_bot() {
    local name="$1"
    local script="$2"
    local service="$3"
    local token="$4"

    # 第一步：API 健康检查（最重要，进程活着但不工作也能发现）
    if bot_is_healthy "$token"; then
        log "$name HEALTHY (API OK)"
        return 0
    fi

    log "$name UNHEALTHY — API 无响应，开始处理..."

    # 第二步：看进程是否存在
    if pgrep -f "$script" > /dev/null 2>&1; then
        # 进程在但 API 不通 → 僵死状态，强制 kill 再重启
        log "$name 进程存在但 API 不通，强制重启..."
        pkill -f "$script" 2>/dev/null
        sleep 3
    else
        log "$name 进程不存在，尝试重启..."
    fi

    # 第三步：重启
    restart_bot "$script" "$service"

    # 第四步：重启后再验证
    if bot_is_healthy "$token"; then
        log "$name 重启成功，API 恢复正常"
        send_tg "⚠️ <b>Bot 监控告警</b>
<b>${name}</b> 无响应，已自动重启 ✅
时间：$(date '+%m-%d %H:%M')"
    else
        log "$name 重启后 API 仍不通！"
        send_tg "🚨 <b>Bot 监控紧急告警</b>
<b>${name}</b> 无响应，自动重启后仍失败！
时间：$(date '+%m-%d %H:%M')
需要手动检查！"
    fi
}

# 检查自用版
check_bot "自用版 (@BinanceStock_BOT)" "bot.py" "com.stockbot" "$SELF_BOT_TOKEN"

# 检查对外版
check_bot "对外版 (@stocknews_forbot)" "bot_promo.py" "com.stockbot.promo" "$PROMO_BOT_TOKEN"

# 日志只保留最近 500 行
tail -500 "$LOG" > "${LOG}.tmp" && mv "${LOG}.tmp" "$LOG"
