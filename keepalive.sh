#!/bin/bash
# 看门狗: 在沙箱活跃期间保持 8800 端口的监控服务存活。
# 每 20s 探活, 若 8800 无响应则自动重启 app.py(本地AI模型默认关闭, 内存约 70-90MB)。
# 用法: bash keepalive.sh   (后台: nohup bash keepalive.sh > keepalive.log 2>&1 &)
set -u
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"
PORT="${PORT:-8800}"
APP="app.py"
LOG="$DIR/keepalive.log"
exec 3>>"$LOG"
log(){ echo "[$(date '+%F %T')] $*" >&3; }
log "看门狗启动, 守护端口 $PORT (pid $$)"

while true; do
  if ! curl -s -o /dev/null --max-time 4 "http://127.0.0.1:$PORT/"; then
    log "⚠️ 端口 $PORT 无响应, 尝试重启..."
    pkill -f "python3.11 $APP" 2>/dev/null
    sleep 2
    PORT="$PORT" nohup python3.11 "$APP" >> "$DIR/monitor.log" 2>&1 &
    NPID=$!
    sleep 6
    if curl -s -o /dev/null --max-time 5 "http://127.0.0.1:$PORT/"; then
      log "✓ 已重启, 新 pid $NPID, 端口恢复"
    else
      log "❌ 重启后仍未就绪, 下一轮重试"
    fi
  fi
  sleep 20
done
