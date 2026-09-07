# -*- coding: utf-8 -*-
"""生成自带数据的独立仪表盘 HTML（不依赖服务器即可查看当前快照）"""
import json, time
import app

snap = app.build_snapshot()
# 触发并等待涨停候选池构建(后台), 然后取结果
app._start_pool_build(force=True)
t0 = time.time()
while time.time() - t0 < 100:
    with app._CAND_LOCK:
        if app._CAND_POOL and not app._CAND_BUILDING:
            break
    time.sleep(3)
if app._CAND_POOL:
    snap["preopen"] = app._calib_preopen_view(app.scan_limit_up())
else:
    snap["preopen"] = {"time": "构建中", "rows": [], "note": "候选池构建中"}
# 指数预测
try:
    # v3.11.17: 与线上 /api/snapshot 同口径 —— 展示层必须经 _calib_*_view:
    # Platt 校准概率 + 按校准后概率重判方向 + 置信度重算 + 弱信号门控(观望)。
    # 此前直接用原始 index_forecast(), 静态快照与线上展示两套数字,
    # 且曾出现「校准后概率 22.4% 却仍判 看涨」的概率与方向自相矛盾数据。
    snap["idx_forecast"] = app._calib_idx_view(app.index_forecast())
except Exception:
    pass
# 尾盘预测
try:
    snap["close"] = app._calib_close_view(app.close_prediction(snap))
except Exception:
    pass
# 主题材拉/踩指数
try:
    snap["sector_drivers"] = app.detect_sector_drivers(snap)
except Exception:
    pass
# 尾盘高开潜力(全市场扫描, 较重, 等待后台构建完成)
try:
    app._start_gapup_build(force=True)
    t0 = time.time()
    while time.time() - t0 < 420:
        with app._GAPUP_LOCK:
            if not app._GAPUP_BUILDING and app.STATE.get("gapup"):
                break
        time.sleep(3)
    snap["gapup"] = app.STATE.get("gapup") or {"time": "构建中", "rows": [], "note": "构建中"}
except Exception:
    snap["gapup"] = None

html = app.DASHBOARD_HTML.replace("__VERSION__", app.VERSION)
old = 'load();setInterval(load,5000);'
data_js = json.dumps(snap, ensure_ascii=False).replace("</", "<\\/")
new = (
    'const EMBEDDED = ' + data_js + ';\n'
    'render(EMBEDDED);\n'
    "document.getElementById('status').textContent = '● 快照(' + EMBEDDED.beijing + ')';"
    'function tryLive(){fetch(\'/api/snapshot\').then(r=>r.json()).then(d=>{render(d);'
    "document.getElementById('status').textContent=d.trading?'● 实时交易中':'○ 非交易时段';}).catch(()=>{});}\n"
    'try{ setInterval(tryLive,5000); }catch(e){}\n'
)
assert old in html, "未找到脚本片段"
html = html.replace(old, new)

out = f"{app.BASE}/dashboard_snapshot.html"
with open(out, "w", encoding="utf-8") as f:
    f.write(html)
print("已生成:", out, "大小", len(html))
print("快照时间:", snap["beijing"])
print("涨停趋势:", [(r["code"], r["name"], r["prob"]) for r in snap["preopen"]["rows"]])
