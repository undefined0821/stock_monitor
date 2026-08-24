#!/usr/bin/env bash
# 发布包装脚本 —— 关键防护: 每次发布前先从线上拉取运行时数据(持仓/回测历史),
# 写回本地后再调用发布脚本, 从而避免用陈旧基线覆盖用户在前端改的最新持仓。
#
# 用法:  bash deploy.sh
# 依赖: 线上服务可达(同一条公网链接); 本地已安装 python3.11 + node。
set -u

BASE_DIR="/workspace/stock_monitor"
URL="https://a95a9c559be00473f.app.workbuddy.link"
PORTFOLIO="$BASE_DIR/portfolio.json"
GAPLOG="$BASE_DIR/gapup_log.jsonl"
EXAMPLE="$BASE_DIR/portfolio.json.example"
PUBLISH_JS="/root/.codebuddy/skills/发布为应用/scripts/publish.js"

echo "==> [1/2] 同步线上运行时数据 (防止覆盖用户前端改动)..."

# —— 持仓: 合并线上 holdings 到本地(保留 closed_positions/settings/watchlist 等顶层字段) ——
tmp=$(mktemp)
if curl -s --max-time 20 "$URL/api/portfolio" -o "$tmp" && [ -s "$tmp" ]; then
  python3.11 - "$PORTFOLIO" "$EXAMPLE" "$tmp" <<'PY'
import json, sys
local_path, example_path, live_path = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    live = json.load(open(live_path))
    live_holdings = live.get("holdings", [])
    assert live_holdings, "线上 holdings 为空"
except Exception as e:
    print("  ! 解析线上持仓失败, 跳过同步:", e); sys.exit(0)
# 以本地(优先)或示例文件作为顶层结构来源, 仅覆盖 holdings
base = None
for p in (local_path, example_path):
    try:
        base = json.load(open(p)); break
    except Exception:
        continue
if base is None:
    base = {}
base["holdings"] = live_holdings
json.dump(base, open(local_path, "w"), ensure_ascii=False, indent=2)
print("  ✓ 持仓已同步为线上最新:", [h.get("code") for h in live_holdings])
PY
else
  echo "  ! 拉取线上持仓失败(服务可能未就绪), 将沿用本地 portfolio.json"
fi
rm -f "$tmp"

# —— 回测日志: 用 /api/gapup/log 的 records 重建(尽力而为, 保护基线/历史) ——
tmp2=$(mktemp)
if curl -s --max-time 20 "$URL/api/gapup/log" -o "$tmp2" && [ -s "$tmp2" ]; then
  python3.11 - "$GAPLOG" "$tmp2" <<'PY'
import json, sys
out_path, live_path = sys.argv[1], sys.argv[2]
try:
    d = json.load(open(live_path))
    recs = d.get("records", [])
    assert recs, "线上无回测记录"
except Exception as e:
    print("  ! 解析线上回测日志失败, 跳过同步:", e); sys.exit(0)
with open(out_path, "w", encoding="utf-8") as f:
    for r in recs:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
print("  ✓ 回测日志已同步:", len(recs), "条")
PY
else
  echo "  ! 拉取线上回测日志失败, 将沿用本地 gapup_log.jsonl"
fi
rm -f "$tmp2"

echo "==> [2/2] 调用发布脚本部署..."
cd "$BASE_DIR" || exit 1
node "$PUBLISH_JS" --dir "$BASE_DIR"
