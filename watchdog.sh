#!/bin/bash
# 看门狗: 保活 8800 上的实时股票监控服务
# 设计: 轻量常驻, 每 10s 检查 8800 端口, 一旦失联立即 nohup 拉起 app.py
# 由发布平台的自启动(AUTOSTART)拉起本脚本, 从而间接保活 app.py
set -u
PORT=8800
APP_DIR=/workspace/stock_monitor
LOG=/tmp/stock_app.log
WDLOG=/tmp/watchdog.log

listen() { ss -ltn 2>/dev/null | grep -q ":$PORT "; }

launch() {
  cd "$APP_DIR" || return
  nohup python3.11 app.py >> "$LOG" 2>&1 &
  echo "$(date '+%F %T') watchdog: launched app.py (pid $!)" >> "$WDLOG"
}

# 启动即先拉起一次(不等首轮循环), 保证探活尽快通过
if ! listen; then
  launch
fi

while true; do
  if ! listen; then
    echo "$(date '+%F %T') watchdog: port $PORT down, restarting..." >> "$WDLOG"
    launch
  fi
  sleep 10
done
