# -*- coding: utf-8 -*-
"""生成自带数据的独立仪表盘 HTML — 从运行中的服务(127.0.0.1:8800)拉取实时快照数据。
这样拿到的 gapup/close/idx_forecast/preopen 等均是服务当前真实内存态，与实时面板一致。"""
import json, os
import urllib.request

PORT = 8800
BASE = f"http://127.0.0.1:{PORT}"

def _get(path):
    with urllib.request.urlopen(f"{BASE}{path}", timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

snap = _get("/api/snapshot")

# 读取服务端注入的 HTML 骨架(含主题/按钮/样式)，并替换版本号
html_src = None
try:
    with urllib.request.urlopen(BASE + "/", timeout=30) as r:
        html_src = r.read().decode("utf-8")
except Exception as e:
    print("拉取页面HTML失败:", e)

if html_src is None:
    raise SystemExit("无法获取服务端 HTML")

# 直接使用服务端返回的完整页面(它已经用 VERSION 渲染好了 __VERSION__)
html = html_src

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

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_snapshot.html")
with open(out, "w", encoding="utf-8") as f:
    f.write(html)
print("已生成:", out, "大小", len(html))
print("快照时间:", snap.get("beijing"))
g = snap.get("gapup") or {}
print("gapup:", f'{len(g.get("rows", []))} rows', g.get("time") or g.get("note") or "")
