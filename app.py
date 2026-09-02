# -*- coding: utf-8 -*-
"""
实时股票监控平台 v2
- 仅工作日(周一~周五)运行
- 9:25:02 起每分钟更新"开盘前涨停趋势": 自动扫描主板选出5只未持有强势股 + 涨停概率
- 持仓: 止盈点/压力位/止损/补仓点
- 最上方: 上证指数 1 小时后趋势预测(每2分钟), 并标概率
- 异动提醒置顶
数据底座: 腾讯财经实时行情 qt.gtimg.cn (真实数据)
"""
from flask import Flask, Response, jsonify, request
import requests
import json, re, math, time, threading, datetime, os, random, traceback, shutil, copy, sys
from concurrent.futures import ThreadPoolExecutor

# 以 `python app.py` 直接启动时, 本文件以 __main__ 身份执行; 而 scheduler.py / backtest.py
# 里的 `import app` 会再加载一份同名副本。两份副本各自持有一套模块级状态, 会造成两个问题:
#   ① 副本与 __main__ 的 HOLDINGS 互不相通 —— 前端 /api/portfolio 保存后只更新了 __main__
#      那份, 调度循环却用副本那份 build_snapshot, 5 秒内又把旧持仓写回 STATE["latest"],
#      表现为"改了持仓又变回去";
#   ② 副本重复执行初始化(重复读盘/重复建线程池), 且一旦子模块存在循环导入, 副本会拿到
#      半成品命名空间(见 backtest._app 的说明), 调度循环随即崩溃。
# 这里在加载任何子模块之前, 把 __main__ 登记为 'app', 保证 `import app` 拿到同一份实例。
# 正常以 `import app` 方式加载时(如 build_static.py / gunicorn), __name__ != '__main__', 本段跳过。
if __name__ == "__main__" and sys.modules.get("__main__") is not None:
    sys.modules.setdefault("app", sys.modules["__main__"])

from config import *  # 业务配置(板块/题材)
from core import *    # 共享核心: BASE/STATE/FCONFIG/SCFG/全局常量/时间工具
from market_data import *  # 行情数据层
from backtest import *     # 预测回测闭环
from calib import *     # 校准与调参(拆分自 app.py)
app = Flask(__name__)
VERSION = "v3.11.12"

# BASE: 跨平台——默认取脚本所在目录; 沙箱/旧部署兜底到 /workspace/stock_monitor

_CLASSIFY_LOCK = threading.Lock()
_CLASSIFY_CACHE = None


def load_classify_cache():
    global _CLASSIFY_CACHE
    if _CLASSIFY_CACHE is None:
        try:
            with open(CLASSIFY_CACHE_FILE, "r", encoding="utf-8") as f:
                _CLASSIFY_CACHE = json.load(f)
        except Exception:
            _CLASSIFY_CACHE = {}
    return _CLASSIFY_CACHE


def save_classify_cache(cache):
    try:
        with open(CLASSIFY_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def fetch_emweb_industry(code, market):
    """东方财富 F10 公司概况: 返回 EM2016 三级行业字符串(如 '医药生物-化学制药-化学原料药')。

    带短时重试(应对冷启动/瞬时网络抖动), 全部失败返回 None(由调用方决定是否重试/缓存)。"""
    m = (market or _market_prefix(code)).upper()
    url = f"https://emweb.securities.eastmoney.com/PC_HSF10/CompanySurvey/PageAjax?code={m}{code}"
    for _ in range(2):
        try:
            r = requests.get(url, timeout=8,
                             headers={"User-Agent": "Mozilla/5.0",
                                       "Referer": "https://emweb.eastmoney.com/"})
            r.encoding = "utf-8"
            d = r.json()
            jbzl = d.get("jbzl") or []
            if jbzl:
                return (jbzl[0].get("EM2016") or jbzl[0].get("INDUSTRYCSRC1") or "").strip()
            return None
        except Exception:
            import time as _t
            _t.sleep(1)
    return None


def match_theme(text):
    if not text:
        return None
    for tname, kws in THEME_KEYWORDS:
        for kw in kws:
            if kw and kw in text:
                return tname
    return None


def _clean_theme(theme):
    """归一化题材字段: null / 'None' / 'none' / 'null' / '未知' / '' 等假值转为空串,
    避免前端把脏值(如字符串 'None')当成有效题材渲染芯片。"""
    if theme is None:
        return ""
    t = str(theme).strip()
    if not t or t.lower() in ("none", "null", "未知", "暂无", "nan"):
        return ""
    return t


def auto_classify(code, market=None, name="", use_cache=True):
    """返回该股票归属的细分题材名(命中 THEMES 之一), 否则 None。结果写入缓存。

    use_cache=False 时强制重新拉取(用于后台补类重试); 仅当成功拿到行业时才写缓存,
    网络失败(industry=None)不缓存, 以便下次重试。"""
    code = str(code).strip()
    cache = load_classify_cache()
    if use_cache and code in cache and cache[code].get("theme") is not None:
        return cache[code]["theme"]
    industry = fetch_emweb_industry(code, market)
    text = f"{industry or ''} {name or ''}"
    theme = match_theme(text)
    if industry is not None:
        with _CLASSIFY_LOCK:
            cache[code] = {"name": name, "industry": industry, "theme": theme}
            save_classify_cache(cache)
    return theme

def enrich_holding(h, q):
    code = (h["market"] + h["code"]).upper()
    p = q.get(code)
    if not p or len(p) <= F["volratio"]:
        return {"name": h["name"], "code": h["code"], "market": h["market"], "error": "行情获取失败"}
    d = parse_row(p)
    price, cost, shares = d["price"], h["cost"], h["shares"]
    if price <= 0:
        return {"name": h["name"], "code": h["code"], "market": h["market"], "error": "无有效价格"}
    value = price * shares
    pnl = (price - cost) * shares
    pnl_pct = (price - cost) / cost * 100 if cost else 0
    # 当日盈亏口径: 分段计算(开盘前持仓 vs 今日加仓), 避免加仓抹除加仓前已有收益。
    #   open_shares = 今日开盘前持有的股数(加仓前基准);
    #     由系统在每日开盘时快照重置, 盘中加仓时自动把"加仓前股数"记为 open_shares。
    #   - 开盘前部分(<=open_shares): 昨日收盘已持有, 当日盈亏基于上一交易日收盘价(=昨收)
    #   - 今日加仓部分(>open_shares): 今日才买入, 基于加权成本(=cost, 即追高成本)
    #   这样加仓不会抹除加仓前 1200 股等的当日收益, 也不会把加仓亏损算成昨收收益。
    #   浮动盈亏(pnl)始终基于成本价, 与当日盈亏口径无关。
    try:
        os_ = max(0.0, min(float(h.get("open_shares") or 0), shares))
    except Exception:
        os_ = 0.0
    prevclose = d["prevclose"] if d["prevclose"] > 0 else cost
    open_part = (price - prevclose) * os_               # 开盘前持仓: 昨收基准
    add_part = (price - cost) * (shares - os_)          # 今日加仓: 成本基准
    day_pnl = open_part + add_part
    denom = prevclose * os_ + cost * (shares - os_)
    day_pnl_pct = day_pnl / denom * 100 if denom else 0
    if os_ <= 0:
        day_basis_label = "成本"      # 无开盘前持仓(今日首买/全加仓)
    elif os_ >= shares:
        day_basis_label = "昨收"      # 开盘前即持有全部, 当日未加仓
    else:
        day_basis_label = "混合"      # 开盘前部分+今日加仓, 分段计算

    stop_loss = h.get("stop_loss_pct", 8.0)
    add1 = h.get("add1_pct", 5.0)
    add2 = h.get("add2_pct", 10.0)
    take_pct = h.get("take_profit_pct", TAKE_PROFIT_PCT)
    press_pct = h.get("pressure_pct", PRESSURE_PCT)

    stop_price = round(cost * (1 - stop_loss / 100), 2)
    add1_price = round(cost * (1 - add1 / 100), 2)
    add2_price = round(cost * (1 - add2 / 100), 2)
    take_price = round(cost * (1 + take_pct / 100), 2)
    press_price = round(cost * (1 + press_pct / 100), 2)
    # 压力位也用近期最高价参考(若有更高)
    pressure = max(press_price, round(d["high"], 2)) if d["high"] > 0 else press_price

    dist_limit_up = (d["limit_up"] - price) / price * 100 if price and d["limit_up"] else 999

    anomalies = []
    if d["pct"] >= ANOM.get("strong_up_pct", 5):
        anomalies.append(("danger", f"强势拉升 +{d['pct']:.2f}%"))
    if d["pct"] <= ANOM.get("strong_down_pct", -5):
        anomalies.append(("danger", f"破位下跌 {d['pct']:.2f}%"))
    if d["amplitude"] >= ANOM.get("amplitude_pct", 8):
        anomalies.append(("warn", f"剧烈震荡 振幅{d['amplitude']:.2f}%"))
    if d["turnover"] >= ANOM.get("turnover_pct", 15):
        anomalies.append(("warn", f"高换手 {d['turnover']:.2f}%"))
    if 0 < d["volratio"] <= 10 and d["volratio"] >= ANOM.get("vol_ratio", 3):
        anomalies.append(("info", f"放量 量比{d['volratio']:.2f}"))
    if price <= stop_price:
        anomalies.append(("danger", f"⚠ 跌破止损线 {stop_price} → 撤离"))
    elif price <= add2_price:
        anomalies.append(("info", f"进入补仓区2 {add2_price} (成本-{add2:.0f}%)"))
    elif price <= add1_price:
        anomalies.append(("info", f"进入补仓区1 {add1_price} (成本-{add1:.0f}%)"))
    if price >= take_price:
        anomalies.append(("warn", f"✅ 触及止盈线 {take_price} (+{take_pct:.0f}%) → 可分批兑现"))
    if price >= pressure:
        anomalies.append(("info", f"触及压力位 {pressure} → 注意回落"))
    if 0 < dist_limit_up <= PREOPEN_CFG.get("strong_limit_dist_pct", 2):
        anomalies.append(("danger", f"逼近涨停 仅差{dist_limit_up:.2f}%"))

    buy_date = h.get("buy_date", "")

    return {
        "name": h.get("name") or d.get("name") or h.get("code", ""), "code": h["code"], "market": h["market"],
        "price": round(price, 2), "prevclose": round(d["prevclose"], 2),
        "open": round(d["open"], 2), "pct": round(d["pct"], 2),
        "high": round(d["high"], 2), "low": round(d["low"], 2),
        "cost": cost, "shares": shares,
        "value": round(value, 2), "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
        "day_pnl": round(day_pnl, 2), "day_pnl_pct": round(day_pnl_pct, 2),
        "day_basis": day_basis_label,

        "limit_up": round(d["limit_up"], 2), "limit_down": round(d["limit_down"], 2),
        "turnover": round(d["turnover"], 2), "amplitude": round(d["amplitude"], 2),
        "volratio": round(d["volratio"], 2) if 0 < d["volratio"] <= 10 else None,
        "float_mv": round(d["float_mv"], 1),
        "stop_price": stop_price, "add1_price": add1_price, "add2_price": add2_price,
        "take_price": take_price, "pressure": round(pressure, 2),
        "dist_limit_up": round(dist_limit_up, 2),
        "below_stop": price <= stop_price, "in_add": price <= add1_price,
        "at_take": price >= take_price, "at_pressure": price >= pressure,
        "anomalies": [{"level": l, "text": t} for l, t in anomalies],
        "sector_code": h.get("sector_code"), "sector_name": h.get("sector_name"),
        "theme": h.get("theme", ""),   # 题材自动归类结果(前端新增持仓保存时写入), 供卡片芯片使用
        "buy_date": buy_date,
    }


# ----------------------------- 上证指数 1小时预测 -----------------------------

def _market_context(snap=None):
    """市场级特征: 用于三个预测模块精度增强。含 宽度/板块均涨/小盘情绪/尾盘动向。"""
    if snap is None:
        try:
            snap = build_snapshot()
        except Exception:
            snap = {}
    idx = {i["code"]: i for i in snap.get("indices", [])}
    sh = idx.get("sh000001"); cyb = idx.get("sz399006"); retail = idx.get("sz399303")
    sh_pct = sh["pct"] if sh else 0.0
    cyb_pct = cyb["pct"] if cyb else 0.0
    retail_pct = retail["pct"] if retail else 0.0
    up = snap.get("sector_up_count", 0); dn = snap.get("sector_down_count", 0)
    total = up + dn
    breadth = round(up / total, 3) if total else 0.5
    sector_avg = snap.get("sector_avg", 0) or 0.0
    # 尾盘动向: 上证分时最后10分钟均价相对前20分钟均价的涨跌幅(%)
    late = 0.0
    try:
        mc = fetch_minute("sh000001")
        if len(mc) >= 20:
            prices = [x["p"] for x in mc]
            prev = sum(prices[-20:-10]) / 10.0
            last = sum(prices[-10:]) / 10.0
            late = (last - prev) / prev * 100.0 if prev else 0.0
    except Exception:
        late = 0.0
    return {"sh_pct": round(sh_pct, 2), "cyb_pct": round(cyb_pct, 2),
            "retail_pct": round(retail_pct, 2), "breadth": breadth,
            "breadth_up": up, "breadth_down": dn,
            "sector_avg": round(sector_avg, 2), "late": round(late, 3)}


def _confidence(prob, breadth, ai_prob=None, heuristic_prob=None):
    """置信度(0~1): 概率偏离中性 + 宽度极端度 + (启用AI时)启发式与AI方向一致性。"""
    dist = abs(prob - 50) / 50.0                      # 0~1, 越偏离50越果断
    ext = abs(breadth - 0.5) * 2.0                     # 0~1, 宽度越偏一侧越明确
    agree = 0.0
    if ai_prob is not None and heuristic_prob is not None:
        agree = 1.0 if (heuristic_prob >= 50) == (ai_prob >= 50) else 0.0
    conf = 0.5 * dist + 0.25 * ext + 0.25 * agree
    return round(min(1.0, max(0.0, conf)), 2)


def _late_pull(code):
    """尾盘拉升强度(%): 最后10分钟均价相对前20分钟均价的涨跌幅。正=尾盘抢筹。"""
    try:
        mkt = _market_prefix(code) + code
        mc = fetch_minute(mkt)
        if len(mc) >= 20:
            prices = [x["p"] for x in mc]
            prev = sum(prices[-20:-10]) / 10.0
            last = sum(prices[-10:]) / 10.0
            return (last - prev) / prev * 100.0 if prev else 0.0
    except Exception:
        pass
    return 0.0


def index_forecast(snap=None):
    """每2分钟预测上证1小时后方向。v2.8多因子: 动量/尾盘动向/日内位置/量能/宽度/小盘情绪 + 置信度。"""
    q = fetch_tencent(["sh000001"])
    d = parse_row(q.get("SH000001", []))
    if not d["price"]:
        return {"error": "指数数据获取失败"}
    pct = d["pct"]
    pos = 0.5
    if d["high"] > d["low"] > 0:
        pos = (d["price"] - d["low"]) / (d["high"] - d["low"])
    vr = d["volratio"] if 0 < d["volratio"] <= 10 else 1
    wb = d["weibi"] if 0 <= d["weibi"] <= 100 else 0
    ctx = _market_context(snap)
    late = ctx["late"]
    breadth = ctx["breadth"]
    retail = ctx["retail_pct"]
    # 多因子线性打分 (7维)
    score = 0.0
    score += pct * FCONFIG["idx_pct_w"]                          # ①当日涨幅动量
    score += late * FCONFIG["idx_late_w"]                         # ②尾盘动向 (强预测力)
    score += (pos - 0.5) * FCONFIG["idx_pos_w"]                   # ③日内位置
    score += (vr - 1) * FCONFIG["idx_vr_w"]                       # ④量比
    score += (wb / 100.0) * FCONFIG["idx_wb_w"]                   # ⑤委比
    score += (breadth - 0.5) * FCONFIG["idx_breadth_w"]           # ⑥宽度 (上涨板块占比)
    score += retail * FCONFIG["idx_retail_w"]                     # ⑦国证2000小盘情绪
    prob = 1 / (1 + math.exp(-score / FCONFIG["idx_sig"])) * 100
    # 三段式判定: 避免在概率接近50时强行判定涨跌(阈值可由 v3.11 自动调参覆盖)
    T = _MODULE_THRESHOLDS.get("idx_1h", 58)
    verdict = "看涨" if prob >= T else ("看跌" if prob <= 100 - T else "震荡")
    # v3.11.2: 分时图(可视化)改为实时拉取。ttl=0 绕过 MINUTE_CACHE_TTL 缓存,
    # 让图表随预测本体(IDX_FORECAST_SEC=5s)同步刷新; 预测特征(_market_context 的
    # 尾盘动向等)仍走缓存, 预测频率与 AI 融合节奏(IDX_AI_FUSE_SEC)均不变。
    chart = fetch_minute("sh000001", ttl=0)
    confidence = _confidence(prob, breadth)
    return {
        "time": beijing_now().strftime("%H:%M:%S"),
        "price": round(d["price"], 2), "pct": round(pct, 2),
        "high": round(d["high"], 2), "low": round(d["low"], 2),
        "prob": round(prob, 1), "verdict": verdict,
        "weibi": round(wb, 1), "vr": round(vr, 2), "pos": round(pos, 3),
        "late": round(late, 3), "breadth": breadth,
        "breadth_up": ctx["breadth_up"], "breadth_down": ctx["breadth_down"],
        "retail": round(retail, 2),
        "ai_used": False, "confidence": confidence,
        "chart": chart,
        "note": (f"v2.8多因子(动量+尾盘动向+日内位置+量能+宽度+小盘情绪), "
                 f"预测1小时后方向, 置信度{confidence:.2f}"),
    }


# 上证1小时预测: 异步 worker + AI 方向融合(避免阻塞调度主循环)
_IDX_BUILDING = False

def _build_idx_worker():
    global _IDX_BUILDING
    with LOCK:
        if _IDX_BUILDING:
            return
        _IDX_BUILDING = True
    try:
        base = index_forecast()
        if "error" not in base:
            ai = AIClient()
            heuristic_prob = base["prob"]
            # v3.11.7: 保留启发式原始概率, 供展示层按校准后概率重算置信度时还原"AI一致性"项
            base["heuristic_prob"] = heuristic_prob
            # v3.10: 预测本体已提频到5秒, 但远程AI较慢, AI融合按 IDX_AI_FUSE_SEC 降频,
            # 两次AI之间直接采用纯启发式结果(随行情5秒更新), 避免高频打爆AI接口。
            _now = beijing_now()
            last_ai = STATE.get("idx_ai_time")
            ai_due = (not last_ai) or (_now - last_ai).total_seconds() >= IDX_AI_FUSE_SEC
            if ai.available and ai_due:
                STATE["idx_ai_time"] = _now
            if ai.available and ai_due:
                feat = {"pct": base["pct"], "pos": base["pos"],
                        "weibi": base["weibi"], "vr": base["vr"],
                        "breadth": base.get("breadth", 0.5),
                        "sector_avg": 0.0,   # 由snap二次获取; index_forecast已含breadth
                        "retail": base.get("retail", 0.0),
                        "late": base.get("late", 0.0)}
                rd = ai.refine_direction("上证指数", feat)
                if rd:
                    ai_p = rd[1]
                    w = FCONFIG.get("ai_fuse_w", AI_FUSED_W)
                    base["prob"] = round((1 - w) * base["prob"] + w * ai_p, 1)
                    _T = _MODULE_THRESHOLDS.get("idx_1h", 58)
                    base["verdict"] = ("看涨" if base["prob"] >= _T
                                       else "看跌" if base["prob"] <= 100 - _T else "震荡")
                    base["ai_used"] = True
                    base["ai_prob"] = round(ai_p, 1)
                    base["confidence"] = _confidence(base["prob"], base.get("breadth", 0.5),
                                                     ai_prob=ai_p, heuristic_prob=heuristic_prob)
                    base["note"] = (base["note"] +
                                    f" ｜ 启发式{heuristic_prob:.1f}% + AI{ai_p:.1f}% "
                                    f"→ 融合(AI权重{w}){base['prob']:.1f}%")
            # v3.10: 落盘待回测(预测5秒一次, 落盘按 IDX_PRED_LOG_SEC 节流, 避免日志爆炸)
            try:
                _now2 = beijing_now()
                last_lp = STATE.get("idx_predlog_time")
                if (not last_lp or (_now2 - last_lp).total_seconds() >= IDX_PRED_LOG_SEC):
                    STATE["idx_predlog_time"] = _now2
                    # 落盘方向与线上展示层同口径(校准后概率 + 弱信号门控为「观望」);
                    # 但 prob 存原始值(不套校准), 校准统一在展示/_recompute_pred_stats 套用, 避免二次校准与拟合反馈漂移
                    _sv = _idx_gated_verdict(base)
                    log_prediction(
                        "idx_1h",
                        {"prob": round(base["prob"], 1), "verdict": _sv,
                         "price": base["price"], "key": "上证指数",
                         "feats": {"pct": base.get("pct", 0), "late": base.get("late", 0),
                                   "pos": base.get("pos", 0.5), "vr": base.get("vr", 1),
                                   "wb": base.get("weibi", 0), "breadth": base.get("breadth", 0.5),
                                   "retail": base.get("retail", 0)}},
                        _add_trading_minutes(_now2, 60))
            except Exception:
                traceback.print_exc()
            with LOCK:
                STATE["idx_forecast"] = base
    except Exception as e:
        print("[idx] worker failed:", e, flush=True)
    finally:
        with LOCK:
            _IDX_BUILDING = False

def _start_idx_build():
    threading.Thread(target=_build_idx_worker, daemon=True).start()


# ----------------------------- 9:25:02 涨停趋势扫描 -----------------------------
def _normalize_holdings(raw):
    """简化持仓配置: 只需 code/cost/shares/buy_date, 其余自动生成默认值。"""
    out = []
    for h in raw:
        code = str(h.get("code", "")).strip()
        if not code:
            continue
        out.append({
            "code": code,
            "market": h.get("market") or _market_prefix(code),
            "shares": float(h.get("shares", 0) or 0),
            "cost": float(h.get("cost", 0) or 0),
            "buy_date": str(h.get("buy_date", "")).strip(),
            "last_buy_date": str(h.get("last_buy_date", "")).strip(),
            # 今日开盘前持有股数(加仓前基准): 用于当日盈亏分段计算,
            # 由系统在每日开盘重置 + 盘中加仓时自动记录, 无需手动维护。
            "open_shares": float(h.get("open_shares", 0) or 0),
            "open_date": str(h.get("open_date", "")).strip(),
            "name": str(h.get("name", "")).strip(),
            # 题材自动归类结果(前端新增持仓保存时写入, 启动后台线程补齐存量); 为空则回退 STOCK_THEMES/STOCK_SECTOR
            "theme": _clean_theme(h.get("theme")),
            # 板块归属: 优先用 portfolio.json 显式设置, 否则回退到内置 STOCK_SECTOR 表
            "sector_code": str(h.get("sector_code", "")).strip()
                            or _SECTOR_NAME_TO_CODE.get(STOCK_SECTOR.get(str(h.get("code", "")).strip(), ""), ""),
            "sector_name": str(h.get("sector_name", "")).strip()
                           or STOCK_SECTOR.get(str(h.get("code", "")).strip(), ""),
            "stop_loss_pct": float(h.get("stop_loss_pct", 8.0)),
            "add1_pct": float(h.get("add1_pct", 5.0)),
            "add2_pct": float(h.get("add2_pct", 10.0)),
            "take_profit_pct": float(h.get("take_profit_pct", TAKE_PROFIT_PCT)),
            "pressure_pct": float(h.get("pressure_pct", PRESSURE_PCT)),
        })
    return out


HOLDINGS = _normalize_holdings(HOLDINGS_RAW)


def reload_holdings():
    """运行时热重载持仓配置(前端编辑保存后调用, 无需重启服务)。"""
    global HOLDINGS
    with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    with _HOLD_LOCK:
        HOLDINGS = _normalize_holdings(cfg.get("holdings", []))


def persist_holdings():
    """将当前 HOLDINGS(含 open_shares/open_date 等运行时字段)写回 portfolio.json。"""
    with _HOLD_LOCK:
        rows = [dict(h) for h in HOLDINGS]
    try:
        with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        cfg = {}
    cfg["holdings"] = rows
    tmp = PORTFOLIO_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, PORTFOLIO_PATH)
    reload_holdings()


def reset_open_basis_if_new_day():
    """每日开盘基准重置: 检测到新交易日时, 把各持仓的 open_shares 重置为当前 shares。

    这样"开盘前持有量"始终以当天开盘为锚; 用户盘中加仓时再单独把 open_shares
    记为加仓前股数(见 /api/portfolio 保存逻辑), 实现当日盈亏分段计算。
    今天已手动指定的 open_shares(如加仓后的中贝)因 open_date==今天不会被覆盖。"""
    global HOLDINGS
    today = beijing_now().date().strftime("%Y-%m-%d")
    if getattr(reset_open_basis_if_new_day, "_date", None) == today:
        return
    reset_open_basis_if_new_day._date = today
    changed = False
    with _HOLD_LOCK:
        for h in HOLDINGS:
            if h.get("open_date") != today:
                h["open_shares"] = h["shares"]
                h["open_date"] = today
                changed = True
    if changed:
        try:
            persist_holdings()
        except Exception:
            pass


def _backfill_themes():
    """后台补齐持仓题材归类, 并周期性保活。

    阶段一: 启动后最多 3 轮尝试补齐存量持仓(网络异常重试)。
    阶段二: 之后每 5 分钟扫描一次, 归类任何"新增但未归类"的持仓——
             覆盖前端保存时恰逢网络抖动导致未归类的情形, 确保最终自动出芯片。
    仅处理既无 theme 字段、又不在手工 STOCK_THEMES 表的持仓。
    关键修复: 直接基于内存 HOLDINGS 原地归类并 persist_holdings 落盘,
    不再读取可能已过期的 portfolio.json 再整体写回, 避免与 /api/portfolio 保存
    形成 read-modify-write 竞争、把用户刚改的持仓覆盖掉。"""
    import time

    def _classify_once(use_cache):
        # 先快照待归类持仓(不加锁), 网络归类后再原地写回(加锁), 缩短锁持有时间
        with _HOLD_LOCK:
            targets = [(h, str(h.get("code", "")).strip())
                      for h in HOLDINGS
                      if str(h.get("code", "")).strip()
                      and not h.get("theme") and not STOCK_THEMES.get(str(h.get("code", "")).strip())]
        if not targets:
            return False
        updates = {}
        for h, code in targets:
            try:
                th = auto_classify(code, h.get("market"), h.get("name", ""), use_cache=use_cache)
            except Exception:
                th = None
            if th:
                updates[code] = th
        if not updates:
            return False
        with _HOLD_LOCK:
            for h in HOLDINGS:
                c = str(h.get("code", "")).strip()
                if c in updates:
                    h["theme"] = updates[c]
        persist_holdings()
        return True

    # 阶段一: 启动补齐(重试)
    for attempt in range(3):
        try:
            if not _classify_once(use_cache=(attempt > 0)):
                break
            time.sleep(60)
        except Exception as e:
            print("[backfill] 题材补齐异常:", e)
            time.sleep(30)
    # 阶段二: 周期性保活(应对 POST 时网络抖动未归类的持仓)
    while True:
        time.sleep(300)
        try:
            _classify_once(use_cache=False)
        except Exception as e:
            print("[backfill] 周期扫描异常:", e)


def _seed_gapup_baseline():
    """服务首次启动且 gapup_log.jsonl 缺失时的兜底播种。
    v3.7.1 重要修正: 不再写入带占位特征(close_pos=0.5)的 manual_baseline 假基线——
    那会与「尾盘高开潜力」实时推荐显示不一致且污染优化样本。基线即「最近一次 14:52
    自动扫描的 auto 记录」, 由调度器自然产生(真实盘口特征)。此处仅在完全无记录时,
    用 STATE['gapup'] 实时推荐(seed 时取 features 子对象真实字段)兜底, 确保特征真实。"""
    now = beijing_now()
    rows = []
    with LOCK:
        g = STATE.get("gapup")
        if g and isinstance(g, dict) and g.get("rows"):
            rows = g["rows"]
    if not rows:
        print("[init] gapup_log.jsonl 缺失且无实时推荐, 跳过种子(待 14:52 自动扫描生成真实基线)", flush=True)
        return
    stocks = []
    for c in rows[:5]:
        f = c.get("features") if isinstance(c.get("features"), dict) else {}
        if not f:   # 顶层的 close_pos/late_pull 等才是实时扫描真实字段
            f = c
        stocks.append({
            "code": c["code"], "name": c.get("name", ""),
            "prob": c.get("prob", 0.0), "pct": c.get("pct", 0.0),
            "late_pull": c.get("late_pull", 0.0),
            "features": {
                "range_pos": c.get("close_pos", f.get("range_pos", 0.5)),
                "pct": c.get("pct", f.get("pct", 0.0)),
                "weibi": c.get("weibi", f.get("weibi", 0)),
                "volratio": c.get("volratio", f.get("volratio", 1)),
                "turnover": c.get("turnover", f.get("turnover", 0)),
                "late_pull": c.get("late_pull", f.get("late_pull", 0.0)),
                "breadth": f.get("breadth", 0.5), "retail": f.get("retail", 0),
                "idx_late": f.get("idx_late", 0),
            },
        })
    rec = {"date": now.strftime("%Y-%m-%d"), "scan_time": now.strftime("%H:%M:%S"),
           "source": "manual_baseline", "stocks": stocks, "verified": False}
    with open(GAPUP_LOG, "w", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"[init] gapup_log.jsonl 缺失, 已用实时推荐播种基线({len(stocks)}只, 真实特征)", flush=True)


def _ensure_runtime_data():
    """启动自愈: 确保运行时数据文件存在, 缺失则用模板/基线初始化, 避免空文件致服务异常。
    注意: 这些文件属用户数据, 正常发布由 deploy.sh 保证与线上一致, 此处仅兜底。"""
    if not os.path.exists(PORTFOLIO_PATH):
        example = PORTFOLIO_PATH + ".example"
        if os.path.exists(example):
            shutil.copyfile(example, PORTFOLIO_PATH)
            print(f"[init] portfolio.json 缺失, 已用示例模板初始化", flush=True)
    if not os.path.exists(GAPUP_LOG):
        _seed_gapup_baseline()
    # 2.5) 加载概率校准参数(Platt scaling), 保证重启后推荐概率延续上次校准结果
    _load_gapup_calib()
    _load_pred_calib()
    # v3.11.0: 加载并应用自动调参结果(权重/阈值), 保证重启后延续上次调参
    _apply_pred_tune()
    # 3) 加载调优后的权重(若存在)覆盖默认 gu_*(部分字典即可, gap_up_score 会合并到 FCONFIG)
    global GAPUP_WEIGHT_OVERRIDE
    if os.path.exists(GAPUP_TUNED):
        try:
            tw = json.load(open(GAPUP_TUNED, encoding="utf-8"))
            if isinstance(tw, dict) and tw:
                GAPUP_WEIGHT_OVERRIDE = {k: float(v) for k, v in tw.items()}
                print(f"[init] 已加载调优权重: {len(tw)} 项", flush=True)
        except Exception:
            pass
    # 2.6) 启动即按当前 pred_log + 已加载的校准参数重算回测统计, 避免重启后面板读到旧缓存文件
    try:
        _recompute_pred_stats()
    except Exception:
        pass





# 候选池缓存: 后台线程构建, API即时返回
_CAND_POOL = []
_CAND_POOL_DATE = None
_CAND_BUILDING = False
_CAND_LOCK = threading.Lock()


def _prefilter_universe():
    """返回主板(排除持仓/ST/观察池)候选代码列表"""
    held = {h["code"] for h in HOLDINGS}
    held |= {w["code"] for w in WATCHLIST}
    return {c: n for c, n in UNIVERSE.items()
            if c not in held and "ST" not in n.upper()}


def _count_consec_limitups(bars):
    """从最近一个已完成交易日往前数连续涨停天数(主板±10%)。返回 int。"""
    if len(bars) < 2:
        return 0
    n = 0
    for i in range(len(bars) - 1, 0, -1):
        prev_close = bars[i - 1]["close"]
        if prev_close <= 0:
            break
        limit_up = round(prev_close * 1.10, 2)
        if bars[i]["close"] >= limit_up - 0.02:   # 允许 2 分钱舍入
            n += 1
        else:
            break
    return n


def _score_preopen(c, resonance, scfg):
    """涨停概率打分(v2.7+可配): 一字板/量比/涨幅/委比 + 流通市值区间 + 妖股连板加分 + 板块共振。"""
    score = 0.0
    score += (15 - c["dist_limit_up"]) * scfg.get("limitup_weight", 0.6)
    vr = c["volratio"] if 0 < c["volratio"] <= 10 else 1
    score += (vr - 1) * scfg.get("vr_weight", 1.5)
    score += min(c["pct"], 9) * scfg.get("pct_weight", 0.8)
    wb = c["weibi"] if 0 <= c["weibi"] <= 100 else 0
    score += (wb / 100) * scfg.get("weibi_weight", 2.0)
    fmv = c.get("float_mv", 50) or 50
    yao = c.get("yao", False)
    # 流通市值区间: 30-80亿最优; >100亿大象票强降权(妖股豁免, 不拘泥市值)
    if 30 <= fmv <= 80:
        score += scfg.get("fv_optimal_bonus", 5.0)
    elif fmv > scfg.get("fv_large_thresh", 100):
        if not yao:
            score += scfg.get("fv_large_penalty", -30.0)
    elif fmv > 80:
        score += scfg.get("fv_mid_penalty", -5.0)
    # 妖股(连板基因): 放宽共振要求, 连板加分 + 小盘额外加分
    if yao:
        yd = c.get("yao_days", 0)
        score += min(yd * scfg.get("yao_consec_bonus", 15), scfg.get("yao_consec_cap", 45))
        if fmv < scfg.get("yao_smallcap_thresh", 30):
            score += scfg.get("yao_smallcap_bonus", 5.0)
    # 板块共振(全局因子)
    score += (resonance or 0) * scfg.get("resonance_scale", 0.1)
    return 1 / (1 + math.exp(-score / scfg.get("sig_scale", 6.0))) * 100


def _build_pool_worker(force):
    """后台全市场粗筛, 构建候选池。v2.7增强: 一字板权重下调+流通市值区间+板块共振+妖股连板旁路。"""
    global _CAND_POOL, _CAND_POOL_DATE, _CAND_BUILDING
    with _CAND_LOCK:
        if _CAND_BUILDING:
            return
        _CAND_BUILDING = True
    try:
        today = beijing_now().strftime("%Y-%m-%d")
        uni = _prefilter_universe()
        codes = [(_market_prefix(c) + c) for c in uni]
        # v3.0优化: 一次性拉全部, fetch_tencent 内部 batch+并发
        all_quotes = fetch_tencent(codes)
        # 拉板块数据用于"板块共振"打分(避免一字板陷阱)
        sec_q = fetch_tencent([c for c, _ in SECTOR_BOARDS])
        sectors_pct = []
        for c, _nm in SECTOR_BOARDS:
            d = parse_row(sec_q.get(c.upper(), []))
            if d["price"] > 0:
                sectors_pct.append(d["pct"])
        sec_avg = sum(sectors_pct) / len(sectors_pct) if sectors_pct else 0
        sec_up_ratio = (sum(1 for p in sectors_pct if p > 0) / len(sectors_pct)) if sectors_pct else 0
        sec_max = max(sectors_pct) if sectors_pct else 0
        # 板块共振分(可配): 平均上涨>resonance_min_avg且上涨板块占比>resonance_min_up_ratio → 强共振; 否则弱共振; 龙头板块>resonance_leader_thresh额外加
        resonance_bonus = 0.0
        if sec_avg > SCFG["resonance_min_avg"] and sec_up_ratio > SCFG["resonance_min_up_ratio"]:
            resonance_bonus = SCFG["resonance_strong"]
        elif sec_avg > 0 or sec_up_ratio > 0.55:
            resonance_bonus = SCFG["resonance_weak"]
        if sec_max > SCFG["resonance_leader_thresh"]:
            resonance_bonus += SCFG["resonance_leader"]   # 龙头板块拉动
        cands = []
        for code, name in uni.items():
            p = all_quotes.get((_market_prefix(code) + code).upper())
            if not p or len(p) <= F["volratio"]:
                continue
            d = parse_row(p)
            if d["price"] <= 0 or d["limit_up"] <= 0:
                continue
            if d["float_mv"] and d["float_mv"] < SCFG["min_float_mv"]:
                continue
            if d["price"] < SCFG["min_price"]:
                continue
            dist = (d["limit_up"] - d["price"]) / d["price"] * 100
            if dist > 15:
                continue
            cands.append({"code": code, "name": name, "price": d["price"],
                          "pct": d["pct"], "dist_limit_up": dist,
                          "limit_up": d["limit_up"], "volratio": d["volratio"],
                          "turnover": d["turnover"], "weibi": d["weibi"],
                          "float_mv": d["float_mv"], "yao": False, "yao_days": 0})
        # 初始评分(不含妖股) + 排序, 取 Top(用于拉 K 线做妖股连板检测, 避免全市场拉 K 线)
        for c in cands:
            c["_resonance"] = resonance_bonus
            c["prob"] = _score_preopen(c, resonance_bonus, SCFG)
        cands.sort(key=lambda x: (x["prob"], -x["dist_limit_up"]), reverse=True)
        # 妖股检测: 对 Top80 拉日K线, 数截至昨日的连续涨停天数(连板基因)
        yao_min = SCFG["yao_min_consec"]
        for c in cands[:80]:
            try:
                bars = _fetch_kline(c["code"], 12)
                yd = _count_consec_limitups(bars)
                if yd >= yao_min:
                    c["yao"] = True
                    c["yao_days"] = yd
            except Exception:
                pass
        # 含妖股的最终重新打分 + 排序
        for c in cands:
            c["prob"] = _score_preopen(c, resonance_bonus, SCFG)
        # AI 精修涨停概率(首扫一次, 缓存到候选, 不拖慢每分钟更新)
        try:
            ai = AIClient()
            if ai.available:
                res = ai.refine_limitup(cands[:8])
                if res:
                    for c in cands[:8]:
                        if c["code"] in res[1]:
                            c["ai_prob"] = res[1][c["code"]]
        except Exception:
            pass
        for c in cands:
            c["blend_prob"], c["blend_model"] = _blend_limitup(c)
        cands.sort(key=lambda x: (x["prob"], -x["dist_limit_up"]), reverse=True)
        with _CAND_LOCK:
            _CAND_POOL, _CAND_POOL_DATE = cands, today
            STATE["preopen_alerted"] = set()   # 每日重置炸板/红开告警去重
            STATE["preopen_reeval_last"] = None
        # 构建完成后自动写入 STATE["preopen"], 供快照/前端展示
        top = cands[:5]
        try:
            with LOCK:
                STATE["preopen"] = {
                    "time": beijing_now().strftime("%H:%M:%S"),
                    "rows": [{"code": c["code"], "name": c["name"], "price": round(c["price"], 2),
                              "pct": round(c["pct"], 2), "limit_up": round(c["limit_up"], 2),
                              "dist_limit_up": round(c["dist_limit_up"], 2),
                              "prob": c["blend_prob"], "model": c["blend_model"],
                              "volratio": c["volratio"], "float_mv": c["float_mv"],
                              "yao": c.get("yao", False), "yao_days": c.get("yao_days", 0)} for c in top],
                    "note": "9:25:02首扫全市场主板(排除持仓/ST, 市值≥37亿, 价≥8), 之后每分钟从候选池重排; 涨停概率已融入AI模型权重" + str(AI_FUSED_W),
                }
                STATE["preopen_date"] = today
        except Exception:
            pass
        # v3.10: 落盘待回测(盘前涨停预测 → 当日收盘验证是否真涨停)
        try:
            vat = today + " 15:05:00"
            for c in top:
                log_prediction("preopen_limitup",
                               {"prob": c.get("blend_prob"), "verdict": "看涨",
                                "qcode": _market_prefix(c["code"]) + c["code"],
                                "key": c["name"], "pct": c.get("pct"),
                                "dist": c.get("dist_limit_up"),
                                "feats": {"dist_limit_up": c.get("dist_limit_up", 15),
                                          "vr": c.get("volratio", 1), "pct": c.get("pct", 0),
                                          "wb": c.get("weibi", 0), "fmv": c.get("float_mv", 50),
                                          "yao": bool(c.get("yao", False)), "yao_days": c.get("yao_days", 0),
                                          "resonance": c.get("_resonance", 0)}}, vat)
            print(f"[pred-log] 盘前涨停预测落盘 {len(top)}条", flush=True)
        except Exception:
            traceback.print_exc()
    finally:
        with _CAND_LOCK:
            _CAND_BUILDING = False


def _start_pool_build(force=False):
    """异步触发候选池构建(不阻塞)"""
    threading.Thread(target=_build_pool_worker, args=(force,), daemon=True).start()


def _post_open_filter():
    """9:30开盘后动态校验(可重复运行, 由调度按 reeval_interval 触发):
    - 炸板的(距涨停>broken_dist_thresh%)从rows移除, 顺位由候选池下一名补上;
    - 红开的(涨幅≥red_open_pct%且距涨停≤broken_dist_thresh%)标记🔥可买;
    - 异动提醒里追加炸板/红开记录(按 code:event 去重, 不重复刷屏);
    - 妖股(连板基因)标记保留, 不因其无板块共振而被误剔除。"""
    global _CAND_POOL
    today = beijing_now().strftime("%Y-%m-%d")
    if _CAND_POOL_DATE != today or not _CAND_POOL:
        return
    cur = STATE.get("preopen")
    if not cur or not cur.get("rows"):
        return
    rows = cur["rows"]
    seen = {r["code"] for r in rows}
    # 拉全部候选的最新盘口
    all_codes = [(_market_prefix(c["code"]) + c["code"]) for c in _CAND_POOL]
    fresh = {}
    for i in range(0, len(all_codes), 8):
        try:
            fresh.update(fetch_tencent(all_codes[i:i + 8]))
        except Exception:
            continue
    bdt = SCFG["broken_dist_thresh"]
    rop = SCFG["red_open_pct"]
    alerted = STATE.setdefault("preopen_alerted", set())
    def _decorate(row):
        c_code = _market_prefix(row["code"]) + row["code"]
        p = fresh.get(c_code.upper())
        if not p or len(p) <= F["volratio"]:
            return row, False, False
        d = parse_row(p)
        if d["price"] <= 0:
            return row, False, False
        base_dist = row.get("dist_limit_up")   # 9:25:02/9:29 基线(继位行为None)
        row["price"] = round(d["price"], 2)
        row["pct"] = round(d["pct"], 2)
        row["limit_up"] = round(d["limit_up"], 2)
        fresh_dist = round((d["limit_up"] - d["price"]) / d["price"] * 100, 2)
        row["dist_limit_up"] = fresh_dist
        # 炸板判定: 相对基线跌幅(原始行, 避免误杀本就不在涨停位的妖股) / 绝对(继位行)
        if base_dist is None:
            is_broken = fresh_dist > bdt
        else:
            is_broken = (fresh_dist - base_dist) > bdt
        is_red_open = d["pct"] >= rop and fresh_dist <= bdt
        row["red_open"] = is_red_open
        row["broken"] = is_broken
        return row, is_red_open, is_broken
    def _yao_tag(r):
        return ("【🔥妖股·%d连板】" % r.get("yao_days", 0)) if r.get("yao") else ""
    # 1) 处理现有 rows(炸板剔除 + 红开告警, 按 code:event 去重)
    new_rows = []
    for r in rows:
        r2, is_red, is_broken = _decorate(dict(r))
        if is_broken:
            key = f"{r2['code']}:broken"
            if key not in alerted:
                alerted.add(key)
                STATE.setdefault("alerts", []).append({
                    "time": beijing_now().strftime("%H:%M:%S"),
                    "name": r2["name"], "level": "danger",
                    "text": f"⚠️ 炸板剔除: {r2['name']} 距涨停{r2['dist_limit_up']:.2f}%, 涨幅{r2['pct']:+.2f}%{_yao_tag(r2)}"
                })
            continue
        if is_red and not r2.get("is_succesor"):
            key = f"{r2['code']}:red"
            if key not in alerted:
                alerted.add(key)
                STATE.setdefault("alerts", []).append({
                    "time": beijing_now().strftime("%H:%M:%S"),
                    "name": r2["name"], "level": "up",
                    "text": f"🔥 红开可买: {r2['name']} 涨幅{r2['pct']:+.2f}%, 距涨停{r2['dist_limit_up']:.2f}%{_yao_tag(r2)}"
                })
        new_rows.append(r2)
    # 2) 顺位补齐到 5 个(从候选池补, 保留妖股标记)
    if len(new_rows) < 5:
        for c in _CAND_POOL:
            if len(new_rows) >= 5:
                break
            if c["code"] in seen:
                continue
            seen.add(c["code"])
            r2, is_red, is_broken = _decorate({
                "code": c["code"], "name": c["name"],
                "prob": c.get("blend_prob", round(c["prob"], 1)),
                "model": c.get("blend_model", "启发式"),
                "volratio": c["volratio"], "float_mv": c["float_mv"],
                "yao": c.get("yao", False), "yao_days": c.get("yao_days", 0),
            })
            if is_broken:
                continue
            r2["is_succesor"] = True
            if is_red:
                key = f"{c['code']}:red"
                if key not in alerted:
                    alerted.add(key)
                    STATE.setdefault("alerts", []).append({
                        "time": beijing_now().strftime("%H:%M:%S"),
                        "name": r2["name"], "level": "up",
                        "text": f"🔥 红开可买(继位): {r2['name']} 涨幅{r2['pct']:+.2f}%, 距涨停{r2['dist_limit_up']:.2f}%{_yao_tag(r2)}"
                    })
            new_rows.append(r2)
    STATE["alerts"] = STATE["alerts"][-60:]
    cur["rows"] = new_rows
    if "动态炸板校验" not in cur.get("note", ""):
        cur["note"] = cur.get("note", "") + " ｜ 9:30后每30s动态炸板校验"
    cur["time"] = beijing_now().strftime("%H:%M:%S")
    STATE["preopen"] = cur
    n_red = sum(1 for r in new_rows if r.get("red_open"))
    n_brk = len([r for r in rows if r.get("broken")]) + max(0, len(rows) - len(new_rows) - sum(1 for r in rows if r.get("broken")))
    print(f"[open-filter] 当前炸板剔除后剩{len(new_rows)}只, 红开{n_red}只", flush=True)


# 9:25:02-9:30 独立快扫线程: 每次刷新Top30盘口并重新计算AI权重概率(真正提速)
_PREOPEN_FAST_BUILDING = False


def _fast_preopen_scan():
    """从_CAND_POOL取Top30, 拉最新盘口, 重算启发式+AI融合, 写回STATE['preopen']。"""
    global _CAND_POOL, _PREOPEN_FAST_BUILDING
    if _PREOPEN_FAST_BUILDING:
        return
    _PREOPEN_FAST_BUILDING = True
    try:
        today = beijing_now().strftime("%Y-%m-%d")
        if _CAND_POOL_DATE != today or not _CAND_POOL:
            return
        pool = list(_CAND_POOL)
        top30 = pool[:30]
        # 1) 拉Top30最新盘口
        codes = [(_market_prefix(c["code"]) + c["code"]) for c in top30]
        fresh = {}
        batch = 8
        for i in range(0, len(codes), batch):
            try:
                fresh.update(fetch_tencent(codes[i:i + batch]))
            except Exception:
                continue
        # 2) 刷新Top30字段 + 重算启发式概率(v2.7: 一字板权重下调+流通市值打分)
        for c in top30:
            p = fresh.get((_market_prefix(c["code"]) + c["code"]).upper())
            if not p or len(p) <= F["volratio"]:
                continue
            d = parse_row(p)
            if d["price"] <= 0 or d["limit_up"] <= 0:
                continue
            c["price"] = d["price"]; c["pct"] = d["pct"]
            c["limit_up"] = d["limit_up"]
            c["dist_limit_up"] = (d["limit_up"] - d["price"]) / d["price"] * 100
            c["volratio"] = d["volratio"]; c["turnover"] = d["turnover"]
            c["weibi"] = d["weibi"]
            c["prob"] = _score_preopen(c, c.get("_resonance", 0), SCFG)   # 含妖股连板加分
        # 3) 重新按概率排序
        top30.sort(key=lambda x: (x["prob"], -x["dist_limit_up"]), reverse=True)
        # 4) 写回候选池(Top30已刷新, 之后保持)
        with _CAND_LOCK:
            _CAND_POOL = top30 + _CAND_POOL[30:]
        # 5) 每次重新计算AI权重概率(Top5, 真正用模型重算, 非缓存)
        try:
            ai = AIClient()
            if ai.available:
                res = ai.refine_limitup(top30[:5])
                if res:
                    for c in top30[:5]:
                        if c["code"] in res[1]:
                            c["ai_prob"] = res[1][c["code"]]
        except Exception:
            pass
        # 6) 重新融合(规则+AI) + 写STATE
        for c in top30[:5]:
            c["blend_prob"], c["blend_model"] = _blend_limitup(c)
        with LOCK:
            STATE["preopen"] = {
                "time": beijing_now().strftime("%H:%M:%S"),
                "rows": [{"code": c["code"], "name": c["name"], "price": round(c["price"], 2),
                          "pct": round(c["pct"], 2), "limit_up": round(c["limit_up"], 2),
                          "dist_limit_up": round(c["dist_limit_up"], 2),
                          "prob": c.get("blend_prob", round(c["prob"], 1)),
                          "model": c.get("blend_model", "启发式"),
                          "volratio": c["volratio"], "float_mv": c["float_mv"],
                          "yao": c.get("yao", False), "yao_days": c.get("yao_days", 0)} for c in top30[:5]],
                "note": (f"9:25:02首扫全市场主板, 独立线程每{PREOPEN_FAST_SEC}s刷新Top30盘口"
                         f"+重新计算AI权重概率(融合权重{AI_FUSED_W})"),
            }
    finally:
        _PREOPEN_FAST_BUILDING = False


def _preopen_fast_loop():
    """独立daemon线程: 9:25:02-9:30期间, 每PREOPEN_FAST_SEC秒执行_fast_preopen_scan。"""
    while True:
        try:
            now = beijing_now()
            in_win = datetime.time(9, 25, 2) <= now.time() < datetime.time(9, 30)
            today = now.strftime("%Y-%m-%d")
            ready = _CAND_POOL_DATE == today and bool(_CAND_POOL)
            if in_win and ready and not _PREOPEN_FAST_BUILDING:
                _fast_preopen_scan()
        except Exception:
            traceback.print_exc()
        time.sleep(PREOPEN_FAST_SEC)


def _blend_limitup(c):
    """涨停概率: 规则与 AI 融合。返回 (prob, model)。"""
    if c.get("ai_prob") is not None and AI_FUSED_W > 0:
        p = (1 - AI_FUSED_W) * c["prob"] + AI_FUSED_W * c["ai_prob"]
        return round(p, 1), "AI"
    return round(c["prob"], 1), "启发式"


def scan_limit_up():
    """从候选池取前3只未持有强势股; 若无候选则异步触发构建"""
    now = beijing_now()
    today = now.strftime("%Y-%m-%d")
    global _CAND_POOL, _CAND_POOL_DATE
    if _CAND_POOL_DATE != today or not _CAND_POOL:
        _start_pool_build()
        return {"time": now.strftime("%H:%M:%S"), "rows": [], "building": True,
                "note": "候选池构建中(首次约需1分钟), 请稍候自动更新…"}
    top = _CAND_POOL[:5]
    return {
        "time": now.strftime("%H:%M:%S"),
        "rows": [{"code": c["code"], "name": c["name"], "price": round(c["price"], 2),
                  "pct": round(c["pct"], 2), "limit_up": round(c["limit_up"], 2),
                  "dist_limit_up": round(c["dist_limit_up"], 2),
                  "prob": c.get("blend_prob", round(c["prob"], 1)),
                  "model": c.get("blend_model", "启发式"),
                  "volratio": c["volratio"],
                  "float_mv": c["float_mv"],
                  "yao": c.get("yao", False), "yao_days": c.get("yao_days", 0)} for c in top],
        "note": "9:25:02首扫全市场主板(排除持仓/ST, 市值≥37亿, 价≥8), 之后每分钟从候选池重排; 涨停概率为统计打分, 非保证",
    }


# ----------------------------- 尾盘次日概率 -----------------------------
def nextday_prob(h, snap, ctx):
    # v3.11: 权重参数化到 FCONFIG stk_*(默认与旧硬编码一致, 行为不变); 返回 feats 供自动调参重打分
    W = FCONFIG
    factors = []
    sh = next((i for i in snap["indices"] if i["code"] == "sh000001"), None)
    cyb = next((i for i in snap["indices"] if i["code"] == "sz399006"), None)
    sh_pct = sh["pct"] if sh else 0
    cyb_pct = cyb["pct"] if cyb else 0
    sec = next((s for s in snap["sectors"] if s["code"] == h.get("sector_code")), None)
    sec_pct = sec["pct"] if sec else 0
    pct_pos = 1 if h.get("pct", 0) >= 0 else -1
    high, low, price = h.get("high", 0), h.get("low", 0), h.get("price", 0)
    rng = high - low
    lower = (price - low) / rng if rng > 0 else 0.5
    upper = (high - price) / rng if rng > 0 else 0.5
    turnover = h.get("turnover", 0)
    pnl_pct = h.get("pnl_pct", 0)
    breadth = ctx.get("breadth", 0.5)
    retail_pct = ctx.get("retail_pct", 0)
    late = ctx.get("late", 0)
    c1 = sh_pct * W["stk_sh_w"] + cyb_pct * W["stk_cyb_w"]
    c2 = sec_pct * W["stk_sec_w"]
    c3 = W["stk_posmag"] if pct_pos >= 0 else -W["stk_posmag"]
    c4 = (lower - upper) * W["stk_yin_w"]
    c5 = W["stk_amt_w"] if turnover >= 10 else 0
    c6 = -W["stk_pnl_neg"] if pnl_pct > 8 else (W["stk_pnl_pos"] if pnl_pct < -8 else 0)
    c7 = (breadth - 0.5) * W["stk_breadth_w"]
    c8 = retail_pct * W["stk_retail_w"]
    c9 = late * W["stk_late_w"]
    score = c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9
    factors.append(("大盘", round(c1, 2), f"上证{sh_pct:+.2f}% 创业板{cyb_pct:+.2f}%"))
    factors.append((f"板块({h.get('sector_name','-')})", round(c2, 2), f"{sec_pct:+.2f}%"))
    factors.append(("当日阴阳", round(c3, 2), "收阳" if pct_pos >= 0 else "收阴"))
    factors.append(("影线", round(c4, 2), f"下影{lower*100:.0f}% 上影{upper*100:.0f}%"))
    factors.append(("量能", round(c5, 2), f"换手{turnover:.1f}%"))
    factors.append(("持仓位置", round(c6, 2), f"{pnl_pct:+.2f}%"))
    factors.append(("宽度", round(c7, 2), f"上涨板块占比{breadth:.2f}"))
    factors.append(("小盘", round(c8, 2), f"国证2000{retail_pct:+.2f}%"))
    factors.append(("尾盘动向", round(c9, 2), f"上证尾盘{late:+.2f}%"))
    prob = 1 / (1 + math.exp(-score / W["stk_sig"])) * 100
    T = _MODULE_THRESHOLDS.get("close_stock", 60)
    verdict = "偏多" if prob >= T else ("偏空" if prob <= 100 - T else "震荡")
    feats = {"sh_pct": sh_pct, "cyb_pct": cyb_pct, "sec_pct": sec_pct, "pct_pos": pct_pos,
             "lower": lower, "upper": upper, "turnover": turnover, "pnl_pct": pnl_pct,
             "breadth": breadth, "retail_pct": retail_pct, "late": late}
    return {"name": h["name"], "code": h["code"], "prob": round(prob, 1),
            "verdict": verdict, "factors": factors, "feats": feats}


def close_prediction(snap):
    """尾盘预测: 大盘明日方向 + 各持仓个股明日方向。
    v2.8多因子: 加入板块均涨/宽度/小盘/尾盘动向, 输出置信度。"""
    ctx = _market_context(snap)
    sh_pct = ctx["sh_pct"]; cyb_pct = ctx["cyb_pct"]
    breadth = ctx["breadth"]; late = ctx["late"]; retail = ctx["retail_pct"]
    sector_avg = ctx["sector_avg"]
    mscore = 0.0
    mscore += sh_pct * FCONFIG["cl_sh_w"]
    mscore += cyb_pct * FCONFIG["cl_cyb_w"]
    mscore += sector_avg * FCONFIG["cl_sec_w"]
    mscore += (breadth - 0.5) * FCONFIG["cl_breadth_w"]    # 宽度: 上涨板块占比
    mscore += retail * FCONFIG["cl_retail_w"]                # 国证2000小盘情绪
    mscore += late * FCONFIG["cl_late_w"]                    # 尾盘动向: 强预测力
    mprob = 1 / (1 + math.exp(-mscore / FCONFIG["cl_sig"])) * 100
    confidence = _confidence(mprob, breadth)
    _T = _MODULE_THRESHOLDS.get("close_market", 58)
    market = {"prob": round(mprob, 1),
              "verdict": ("偏多" if mprob >= _T else "偏空" if mprob <= 100 - _T else "震荡"),
              "sh_pct": sh_pct, "cyb_pct": cyb_pct,
              "sector_avg": sector_avg, "breadth": breadth,
              "breadth_up": ctx["breadth_up"], "breadth_down": ctx["breadth_down"],
              "retail": retail, "late": late, "confidence": confidence}
    stocks = [nextday_prob(h, snap, ctx) for h in snap["holdings"] if not h.get("error")]
    return {"time": beijing_now().strftime("%H:%M:%S"), "market": market,
            "stocks": stocks,
            "note": ("v2.8多因子(大盘/板块/宽度/小盘/尾盘动向+个股阴阳/影线/量能/持仓位置), "
                     f"置信度{confidence:.2f}, 非投资建议")}


# 尾盘预测: 异步 worker + AI 大盘方向融合(避免阻塞调度主循环)
_CLOSE_BUILDING = False

def _build_close_worker():
    global _CLOSE_BUILDING
    with LOCK:
        if _CLOSE_BUILDING:
            return
        _CLOSE_BUILDING = True
    try:
        snap = build_snapshot()
        base = close_prediction(snap)
        ai = AIClient()
        m = base["market"]
        heuristic_prob = m["prob"]
        m["heuristic_prob"] = heuristic_prob   # 供 _calib_close_view 重算置信度时还原"AI一致性"项
        if ai.available:
            sh = next((i for i in snap["indices"] if i["code"] == "sh000001"), None)
            sh_pct = sh["pct"] if sh else 0
            feat = {"pct": sh_pct,
                    "pos": min(1.0, max(0.0, 0.5 + sh_pct / 10.0)),
                    "weibi": 0, "vr": 1,
                    "breadth": m.get("breadth", 0.5),
                    "retail": m.get("retail", 0),
                    "late": m.get("late", 0)}
            rd = ai.refine_direction("大盘(上证)", feat)
            if rd:
                ai_p = rd[1]
                w = FCONFIG.get("ai_fuse_w", AI_FUSED_W)
                m["prob"] = round((1 - w) * m["prob"] + w * ai_p, 1)
                _T = _MODULE_THRESHOLDS.get("close_market", 58)
                m["verdict"] = ("偏多" if m["prob"] >= _T
                                else "偏空" if m["prob"] <= 100 - _T else "震荡")
                m["ai_prob"] = round(ai_p, 1)
                m["confidence"] = _confidence(m["prob"], m.get("breadth", 0.5),
                                             ai_prob=ai_p, heuristic_prob=heuristic_prob)
                base["ai_used"] = True
                base["note"] = (base["note"] +
                                f" ｜ 启发式{heuristic_prob:.1f}% + AI{ai_p:.1f}% "
                                f"→ 融合(AI权重{w}){m['prob']:.1f}%")
        # v3.10: 落盘待回测(大盘1条 + 每只持仓1条, 次日收盘后回填真实涨跌)
        # base_close 记录预测时的最新价(14:50≈收盘), 供实时行情兜底时校验基准对齐
        ctx = _market_context(snap)   # v3.11.5: worker 作用域需显式计算 ctx(close_prediction 内部为局部变量)
        try:
            _now2 = beijing_now()
            nd = _next_trading_day(_now2)
            vat = nd.strftime("%Y-%m-%d") + " 15:05:00"
            sh_px = next((i.get("price") for i in snap.get("indices", [])
                          if i.get("code") == "sh000001"), None)
            _cm = round(_apply_pred_calib("close_market", m["prob"]), 1)
            _vm = _close_verdict("close_market", _cm)
            log_prediction("close_market",
                           {"prob": round(m["prob"], 1), "verdict": _vm,
                            "qcode": "sh000001", "key": "大盘(上证)",
                            "base_close": sh_px,
                            "feats": {"sh_pct": ctx["sh_pct"], "cyb_pct": ctx["cyb_pct"],
                                      "sector_avg": ctx["sector_avg"], "breadth": ctx["breadth"],
                                      "retail": ctx["retail_pct"], "late": ctx["late"]}}, vat)
            hpx = {h.get("code"): h.get("price") for h in snap.get("holdings", [])}
            for s in base.get("stocks", []):
                code = s.get("code")
                if not code:
                    continue
                _cs = round(_apply_pred_calib("close_stock", s.get("prob")), 1)
                _vs = _close_verdict("close_stock", _cs)
                log_prediction("close_stock",
                               {"prob": round(s.get("prob"), 1), "verdict": _vs,
                                "qcode": _market_prefix(code) + code,
                                "key": s.get("name", code),
                                "base_close": hpx.get(code),
                                "feats": s.get("feats", {})}, vat)
            print(f"[pred-log] 尾盘预测落盘: 大盘1 + 个股{len(base.get('stocks', []))}条", flush=True)
        except Exception:
            traceback.print_exc()
        with LOCK:
            STATE["close"] = base
    except Exception as e:
        print("[close] worker failed:", e, flush=True)
    finally:
        with LOCK:
            _CLOSE_BUILDING = False

def _start_close_build():
    threading.Thread(target=_build_close_worker, daemon=True).start()


# ----------------------------- 快照 -----------------------------
def build_snapshot():
    reset_open_basis_if_new_day()  # 每日开盘基准重置(新交易日才生效)
    now = beijing_now()
    wd = is_weekday(now)
    trading, phase = trading_phase(now)
    hcodes = [(h["market"] + h["code"]).lower() for h in HOLDINGS]
    icodes = [c for c, _ in INDICES]
    scodes = [c for c, _ in SECTOR_BOARDS]
    wcodes = [(w["market"] + w["code"]).lower() for w in WATCHLIST]
    # 细分题材成分股(用于卡片芯片 + 题材拉/踩指数)
    tcodes = [(_market_prefix(c) + c).lower() for c in STOCK_THEMES]
    q = fetch_tencent(list(dict.fromkeys(hcodes + icodes + scodes + wcodes + tcodes)))

    # 逐只 enriched, 单只异常不影响整体快照(避免一只持仓数据异常导致整页冻结、编辑不可见)
    holdings = []
    for h in HOLDINGS:
        try:
            holdings.append(enrich_holding(h, q))
        except Exception as _e:
            print("[snapshot] enrich_holding 跳过异常持仓 %s: %s" % (h.get("code"), _e))
            try:
                holdings.append(dict(h))
            except Exception:
                pass
    indices = []
    for c, nm in INDICES:
        d = parse_row(q.get(c.upper(), []))
        if d["price"]:
            indices.append({"name": nm, "code": c, "price": round(d["price"], 2),
                            "pct": round(d["pct"], 2), "high": round(d["high"], 2),
                            "low": round(d["low"], 2)})
    sectors = []
    for c, nm in SECTOR_BOARDS:
        d = parse_row(q.get(c.upper(), []))
        if d["price"]:
            sectors.append({"name": nm, "code": c, "pct": round(d["pct"], 2)})
    sectors.sort(key=lambda x: x["pct"], reverse=True)
    _sec_pct_by_name = {s["name"]: s["pct"] for s in sectors}

    # 细分题材涨跌: 题材 = 成分股当日平均涨跌幅(实时, 来自腾讯行情);
    # 比 12 个中证行业指数更贴近个股关联主线(如 CPO/PCB/纸业/白酒 等)。
    themes = []
    for tname, members in THEMES:
        ps = []
        for m in members:
            d = parse_row(q.get((_market_prefix(m) + m).upper(), []))
            if d["price"] and d.get("pct") is not None:
                ps.append(d["pct"])
        if ps:
            themes.append({"name": tname, "code": "TH:" + tname,
                           "pct": round(sum(ps) / len(ps), 2), "n": len(ps)})
    themes.sort(key=lambda x: x["pct"], reverse=True)
    _theme_by_code = {t["code"]: t for t in themes}
    _theme_rank = {t["code"]: i + 1 for i, t in enumerate(themes)}  # 1=最强

    # 为每只持仓附加"所属题材涨跌 + 在题材中的强弱排名", 供卡片角落芯片展示。
    # 优先细分子题(任一持仓命中题材即自动显示芯片); 未命中题材时回退宽泛行业(STOCK_SECTOR)。
    for h in holdings:
        if h.get("error"):
            continue
        tlist = _clean_theme(h.get("theme")) or STOCK_THEMES.get(h["code"])
        if not tlist:
            broad = STOCK_SECTOR.get(h["code"])
            tlist = [broad] if broad else None
        if not tlist:
            continue
        tname = tlist[0] if isinstance(tlist, (list, tuple)) else tlist
        tc = "TH:" + tname
        tt = _theme_by_code.get(tc)
        if tt is not None:
            h["theme_name"] = tname
            h["theme_pct"] = tt["pct"]
            h["theme_rank"] = _theme_rank.get(tc)
            h["theme_total"] = len(themes)
        elif tname in _sec_pct_by_name:
            h["theme_name"] = tname
            h["theme_pct"] = _sec_pct_by_name[tname]
            h["theme_rank"] = None
            h["theme_total"] = None
    # 为每只持仓附加"所属板块(中证行业)今日涨跌幅", 供卡片角落板块芯片展示(板块联动)。
    for h in holdings:
        if h.get("error"):
            continue
        sn = h.get("sector_name")
        h["sector_pct"] = _sec_pct_by_name.get(sn) if sn else None
    up = [s for s in sectors if s["pct"] > 0]
    avg = sum(s["pct"] for s in sectors) / len(sectors) if sectors else 0
    bias = ("多数板块上涨" if avg > 0.3
            else "多数板块下跌" if avg < -0.3 else "板块分化")

    # 散户今日平均盈亏(以国证2000小盘股指数当日涨跌幅近似)
    rd = parse_row(q.get(RETAIL_INDEX[0].upper(), []))
    retail_pnl = {"name": RETAIL_INDEX[1], "price": round(rd["price"], 2),
                  "pct": round(rd["pct"], 2)} if rd["price"] else None

    total_value = sum(h.get("value", 0) for h in holdings if not h.get("error"))
    total_pnl = sum(h.get("pnl", 0) for h in holdings if not h.get("error"))
    total_cost = sum(h.get("cost", 0) * h.get("shares", 0) for h in holdings if not h.get("error"))
    total_day_pnl = sum(h.get("day_pnl", 0) for h in holdings if not h.get("error"))
    total_prev_value = sum(h.get("prevclose", 0) * h.get("shares", 0) for h in holdings if not h.get("error"))
    total_day_pnl_pct = round(total_day_pnl / total_prev_value * 100, 2) if total_prev_value else 0

    # 已清仓: 计算累计已实现盈亏与今日已实现盈亏(close_date==今天)
    closed_total = sum((c.get("realized_pnl") or 0) for c in CLOSED)
    closed_today = sum((c.get("realized_pnl") or 0) for c in CLOSED
                       if c.get("close_date") == now.strftime("%Y-%m-%d"))

    return {
        "beijing": now.strftime("%Y-%m-%d %H:%M:%S"), "weekday": now.weekday(),
        "trading": trading, "phase": phase, "is_weekday": wd,
        "indices": indices, "sectors": sectors, "themes": themes,
        "sector_up_count": len(up), "sector_down_count": len(sectors) - len(up),
        "sector_avg": round(avg, 2), "sector_bias": bias,
        "retail_pnl": retail_pnl,
        "holdings": holdings,
        "total_value": round(total_value, 2), "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl / total_cost * 100, 2) if total_cost else 0,
        "total_day_pnl": round(total_day_pnl, 2), "total_day_pnl_pct": total_day_pnl_pct,
        "idx_forecast": STATE["idx_forecast"],
        "preopen": STATE["preopen"], "close": STATE["close"],
        "alerts": STATE["alerts"][-10:],   # 异动提醒最多显示10条最新
        "closed_positions": CLOSED,        # 已清仓列表(数据保留, 前端不展示)
        "closed_total_pnl": round(closed_total, 2),  # 累计已实现盈亏(数据保留, 前端不展示)
        "closed_today_pnl": round(closed_today, 2),  # 今日已实现盈亏(数据保留, 前端不展示)
    }


# ----------------------------- 主题材拉/踩指数 -----------------------------
def detect_sector_drivers(snap):
    """识别哪些细分题材在拉抬/压制大盘指数(每10分钟检测一次)。

    逻辑: 以主要指数(上证)涨跌幅为基准, 计算各细分题材(成分股平均涨跌)相对大盘的偏离度。
    偏离明显为正 = 拉指数(贡献上行), 偏离明显为负 = 踩指数(拖累下行)。
    题材比 12 个中证行业更细(如 CPO/PCB/纸业/白酒 等), 更贴近个股主线。
    """
    idx = next((i for i in snap["indices"] if i["code"] == "sh000001"), None)
    base = idx["pct"] if idx else 0
    themes = snap.get("themes", [])
    if not themes:
        return {"error": "题材数据缺失"}
    TH = 0.8  # 相对大盘的明显异动阈值(百分点)
    rows = []
    for s in themes:
        dev = round(s["pct"] - base, 2)
        rows.append({"name": s["name"], "code": s["code"],
                     "pct": s["pct"], "dev": dev})
    pullers = sorted([r for r in rows if r["dev"] >= TH], key=lambda x: -x["dev"])[:3]
    pressers = sorted([r for r in rows if r["dev"] <= -TH], key=lambda x: x["dev"])[:3]
    move = "明显上行" if base > 0.6 else ("明显下行" if base < -0.6 else "区间震荡")
    return {
        "time": beijing_now().strftime("%H:%M:%S"),
        "index_name": idx["name"] if idx else "上证指数",
        "index_pct": round(base, 2),
        "move": move,
        "pullers": pullers,
        "pressers": pressers,
        "threshold": TH,
        "note": f"以{idx['name'] if idx else '上证指数'}涨跌幅为基准, 细分题材(成分股平均涨跌)相对大盘偏离≥{TH}%视为明显异动; 拉=正向拉动指数, 踩=负向压制指数",
    }


# ----------------------------- 尾盘高开潜力(下个交易日开盘) -----------------------------
# 可选 AI 精修客户端: 配置 OpenAI 兼容接口后, 平台算法可调用更强模型提升准确度;
# 未配置或调用失败时自动降级为启发式模型(保证功能始终可用、无需密钥)。
# 本地 AI 模型自动探测(优先较大模型, 回退较小模型)
def _detect_local_model():
    for name in ("Qwen2.5-1.5B-Instruct", "Qwen2.5-0.5B-Instruct"):
        p = os.path.join(BASE, "models", name)
        if os.path.isfile(os.path.join(p, "model.safetensors")) or \
           os.path.isfile(os.path.join(p, "pytorch_model.bin")):
            return p
    return None
LOCAL_MODEL_DIR = _detect_local_model()
# AI 在融合概率中的权重(0-1)。启发式与本地 AI 概率按 (1-W):W 融合, 防止小模型极端值破坏排序
AI_FUSED_W = float(os.environ.get("AI_FUSE_W", "0.45"))

# 本地 AI 模型进程级缓存(避免每次扫描重复加载 ~3GB 权重)
_AI_MDL = None
_AI_TOK = None
_AI_EOS = None
_AI_LOCK = threading.Lock()   # v3.0: 本地模型推理全局互斥锁, 防多线程并发访问导致崩溃
_AI_LOCAL_LOCK = threading.Lock()


def _load_local_once():
    global _AI_MDL, _AI_TOK, _AI_EOS
    if _AI_MDL is not None:
        return True
    with _AI_LOCAL_LOCK:
        if _AI_MDL is not None:
            return True
        if not LOCAL_MODEL_DIR:
            return False
        try:
            import torch
            from transformers import AutoModelForCausalLM, AutoTokenizer
            _AI_TOK = AutoTokenizer.from_pretrained(LOCAL_MODEL_DIR)
            _AI_MDL = AutoModelForCausalLM.from_pretrained(LOCAL_MODEL_DIR, dtype=torch.float32)
            _AI_MDL.eval()
            _AI_EOS = _AI_MDL.config.eos_token_id
            print(f"[AI] local model loaded: {LOCAL_MODEL_DIR}", flush=True)
            return True
        except Exception as e:
            print("[AI] local model load failed:", e, flush=True)
            return False


class AIClient:
    """本地 transformers 推理优先(无需 API key); 若配置 OpenAI 兼容接口则远程兜底。"""

    def __init__(self):
        # 本地模型默认关闭: 沙箱 CPU 推理极慢且占用 ~6GB 内存(1.5B float32),
        # 易卡死/被 OOM 杀掉。需显式 LOCAL_AI=1 才启用。无远程 key 时回退纯启发式。
        self.local_dir = LOCAL_MODEL_DIR
        self.use_local = LOCAL_MODEL_DIR is not None and \
            str(os.environ.get("LOCAL_AI", "0")).lower() in ("1", "true", "on")
        self.use_remote = bool(AI_ENABLED and AI_KEY)
        self.available = self.use_local or self.use_remote
        self.model = AI_MODEL
        self.backend = "local" if self.use_local else ("remote" if self.use_remote else "none")

    def _ensure_local(self):
        return _load_local_once()

    def _ask_one(self, c):
        """单只候选: 让模型输出 0-100 整数高开概率。v2.8: 输入加入尾盘拉升/宽度/板块。"""
        sys_p = ("你是A股量化研究助手。根据单只主板股票当日盘口与市场尾盘环境, 估计它下个交易日开盘"
                 "相对今日收盘高开的概率(0-100, 越高越可能高开)。"
                 "收盘位置高/尾盘最后几分钟拉升/涨幅适中(2-6%)/委比为正/市场宽度偏多 → 偏高(60-95); "
                 "尾盘杀跌/收在低位/涨幅为负或过高(>9%已透支)/委比为负/宽度偏空 → 偏低(8-40); 中间给 40-65。"
                 "只输出一个整数, 不要解释或标点。")
        usr = (f"股票 {c['name']}({c['code']}): 当日涨幅{c['pct']:.2f}%, 收盘位置{c['close_pos']:.2f}, "
               f"委比{c['weibi']:.0f}%, 量比{c['volratio']:.2f}, 换手{c['turnover']:.2f}%, 振幅{c['amplitude']:.2f}%, "
               f"尾盘拉升{c.get('late_pull', 0):.2f}%, 市场宽度{c.get('breadth', 0.5):.2f}, 板块均涨{c.get('sector_avg', 0):.2f}%。"
               f"只输出高开概率整数。")
        try:
            import torch
            prompt = _AI_TOK.apply_chat_template(
                [{"role": "system", "content": sys_p}, {"role": "user", "content": usr}],
                tokenize=False, add_generation_prompt=True)
            ids = _AI_TOK(prompt, return_tensors="pt").input_ids
            with _AI_LOCK:   # v3.0: 模型推理串行化
                with torch.no_grad():
                    out = _AI_MDL.generate(ids, max_new_tokens=10, do_sample=False,
                                           eos_token_id=_AI_EOS, pad_token_id=_AI_EOS)
            txt = _AI_TOK.decode(out[0][ids.shape[1]:], skip_special_tokens=True).strip()
            m = re.search(r"\d{1,3}(?:\.\d+)?", txt)
            if not m:
                return None
            return max(0.0, min(100.0, float(m.group())))
        except Exception:
            return None

    def _refine_remote(self, cands):
        items = [{"code": c["code"], "name": c["name"], "pct": c["pct"],
                  "close_pos": c["close_pos"], "weibi": c["weibi"],
                  "volratio": c["volratio"], "turnover": c["turnover"],
                  "amplitude": c["amplitude"]} for c in cands]
        sys_p = ("你是A股量化研究助手。给定若干主板股票当日盘口特征, 请预测它们"
                 "在下个交易日开盘相对今日收盘高开的概率(0-100, 越高越可能高开)。"
                 "只返回一个JSON数组, 每项形如 {\"code\":\"600XXX\",\"prob\":数值}, 不要任何多余文字或解释。")
        usr = "请预测下列股票下个交易日开盘高开概率:\n" + json.dumps(items, ensure_ascii=False)
        try:
            r = requests.post(self.base + "/chat/completions",
                              headers={"Authorization": f"Bearer {self.key}",
                                       "Content-Type": "application/json"},
                              json={"model": self.model,
                                    "messages": [{"role": "system", "content": sys_p},
                                                 {"role": "user", "content": usr}],
                                    "temperature": 0.2,
                                    "response_format": {"type": "json_object"}},
                              timeout=45)
            obj = r.json()
            txt = obj["choices"][0]["message"]["content"]
            parsed = json.loads(txt)
            arr = parsed if isinstance(parsed, list) else parsed.get("probs") or parsed.get("data") or []
            out = {}
            for it in arr:
                try:
                    out[str(it["code"])] = max(0.0, min(100.0, float(it["prob"])))
                except Exception:
                    pass
            return out if out else None
        except Exception:
            return None

    def _ask_batch(self, cands):
        """v3.0: 本地模型批量推理——把全部候选放进一个 prompt, 单次生成所有概率。
        相比逐只 _ask_one(每只一次完整生成), 大幅缩减耗时。返回 {code: prob} 或 None。"""
        try:
            import torch
            if not self._ensure_local():
                return None
            items = [{"code": c["code"], "name": c["name"], "pct": c["pct"],
                      "close_pos": c.get("close_pos", 0.5), "weibi": c["weibi"],
                      "volratio": c["volratio"], "turnover": c["turnover"],
                      "amplitude": c["amplitude"], "late_pull": c.get("late_pull", 0),
                      "breadth": c.get("breadth", 0.5)} for c in cands]
            sys_p = ("你是A股量化研究助手。给定若干主板股票当日盘口与市场尾盘环境, 预测它们"
                     "下个交易日开盘相对今日收盘高开的概率(0-100, 越高越可能高开)。"
                     "只返回一个JSON数组, 每项形如 {\"code\":\"600XXX\",\"prob\":数值}, 不要任何多余文字或解释。")
            usr = "请预测下列股票下个交易日开盘高开概率:\n" + json.dumps(items, ensure_ascii=False)
            prompt = _AI_TOK.apply_chat_template(
                [{"role": "system", "content": sys_p}, {"role": "user", "content": usr}],
                tokenize=False, add_generation_prompt=True)
            ids = _AI_TOK(prompt, return_tensors="pt").input_ids
            with _AI_LOCK:   # v3.0: 模型推理串行化, 防多线程并发崩溃
                with torch.no_grad():
                    out = _AI_MDL.generate(ids, max_new_tokens=256, do_sample=False,
                                           eos_token_id=_AI_EOS, pad_token_id=_AI_EOS)
            txt = _AI_TOK.decode(out[0][ids.shape[1]:], skip_special_tokens=True).strip()
            m = re.search(r"\[.*\]", txt, re.S)
            if not m:
                return None
            parsed = json.loads(m.group())
            res = {}
            for it in (parsed if isinstance(parsed, list) else parsed.get("probs") or []):
                try:
                    res[str(it["code"])] = max(0.0, min(100.0, float(it["prob"])))
                except Exception:
                    pass
            return res if res else None
        except Exception:
            return None

    def refine_gapup(self, cands):
        """返回 (backend, {code: prob}) 或 None。只走远程AI(HTTP带timeout稳定), 本地模型沙箱CPU推理不稳定已跳过。
        v3.0: 失败自动回退启发式, 绝不卡死。"""
        if self.use_remote:
            r = self._refine_remote(cands)
            if r:
                return ("remote", r)
        return None

    def _ask_prompt(self, sys_p, usr):
        """通用: 让模型对给定任务输出 0-100 整数概率。返回 float 或 None。"""
        try:
            import torch
            if not self._ensure_local():
                return None
            prompt = _AI_TOK.apply_chat_template(
                [{"role": "system", "content": sys_p}, {"role": "user", "content": usr}],
                tokenize=False, add_generation_prompt=True)
            ids = _AI_TOK(prompt, return_tensors="pt").input_ids
            with _AI_LOCK:   # v3.0: 模型推理串行化
                with torch.no_grad():
                    out = _AI_MDL.generate(ids, max_new_tokens=10, do_sample=False,
                                           eos_token_id=_AI_EOS, pad_token_id=_AI_EOS)
            txt = _AI_TOK.decode(out[0][ids.shape[1]:], skip_special_tokens=True).strip()
            m = re.search(r"\d{1,3}(?:\.\d+)?", txt)
            if not m:
                return None
            return max(0.0, min(100.0, float(m.group())))
        except Exception as e:
            print("[AI] _ask_prompt failed:", e, flush=True)
            return None

    def refine_limitup(self, cands):
        """涨停概率精修: 返回 (backend, {code: prob}) 或 None。本地优先。
        v2.7增强: prompt加入流通市值/板块共振/封单稳定性等上下文, 减少一字板陷阱。"""
        sys_p = ("你是A股短线打板助手。根据单只主板股票开盘前盘口特征与板块环境, 估计它当日真封板的概率(0-100)。"
                 "打分要点(综合考虑):\n"
                 "①距涨停越近/涨幅越高/委比为正/量比活跃 → 偏高; 但仅靠一字板≠强势, 若孤立一字板(无板块共振)反而陷阱概率高;\n"
                 "②流通市值30-80亿为最优区间(资金可控+题材热度), >100亿大象票封板难度大应降分;\n"
                 "③板块共振: 当日多板块上涨+龙头板块>2% → 整体环境好, 提分; 无板块共振的孤板谨慎;\n"
                 "④妖股(连板基因): 若个股已有≥2连板, 说明资金高度认可/独立行情强, 即使无板块共振也应给予较高概率(60-95), 但需提示连板持续性与炸板风险;\n"
                 "⑤距涨停0%但量比<1/委比为负 → 大概率诱多陷阱, 给5-25低分;\n"
                 "⑥距涨停1-3%且量比>1.5+板块共振 → 真强势, 给70-95;\n"
                 "⑦中间特征给40-65。\n"
                 "只输出一个整数(0-100), 不要解释或标点。")
        out = {}
        for c in cands:
            fmv = c.get("float_mv", 50) or 50
            yao = c.get("yao", False)
            yao_hint = ""
            if yao:
                yao_hint = (f" 该票已有{c['yao_days']}连板(妖股基因/独立行情强), "
                            f"请重点评估连板持续性, 即便无板块共振也勿过度压低分数。")
            usr = (f"股票 {c['name']}({c['code']}): 现价{c['price']:.2f}, 距涨停{c['dist_limit_up']:.2f}%, "
                   f"涨幅{c['pct']:.2f}%, 委比{c['weibi']:.0f}%, 量比{c['volratio']:.2f}, "
                   f"流通市值{fmv:.0f}亿, 板块共振强度{(c.get('_resonance') or 0):.0f}/33。{yao_hint}"
                   f"只输出涨停概率整数。")
            p = self._ask_prompt(sys_p, usr)
            if p is not None:
                out[c["code"]] = p
        return ("local", out) if out else None

    def refine_direction(self, name, feat):
        """指数/大盘方向判断: 返回 (backend, prob_up 0-100) 或 None。prob_up>=50 视为看涨。
        v2.8: 输入增加 市场宽度/板块均涨/小盘情绪/尾盘动向, 让AI拥有完整环境。"""
        sys_p = ("你是A股大盘研判助手。根据指数当日盘口与市场宽度, 估计它未来1小时内继续上涨的概率(0-100)。"
                 "综合判断: 涨幅高/日内位置高/量比为正/尾盘拉升/市场宽度偏多(上涨板块占比>0.5)/小盘情绪好 → 偏高(60-95); "
                 "下跌/位置低/量比负/尾盘杀跌/宽度偏空(上涨板块占比<0.4) → 偏低(8-40); 中性给40-65。"
                 "若宽度与指数方向矛盾(如指数涨但多数板块跌), 应下调概率。只输出一个整数, 不要解释或标点。")
        breadth = feat.get("breadth", 0.5)
        usr = (f"{name}: 当日涨幅{feat['pct']:.2f}%, 日内位置{feat['pos']:.2f}(0最低~1最高), "
               f"委比{feat['weibi']:.0f}%, 量比{feat['vr']:.2f}, "
               f"市场宽度(上涨板块占比){breadth:.2f}, 国证2000(小盘){feat.get('retail', 0):.2f}%, "
               f"尾盘动向{feat.get('late', 0):.2f}%(正=尾盘拉升)。"
               f"只输出上涨概率整数。")
        p = self._ask_prompt(sys_p, usr)
        return ("local", p) if p is not None else None


# ---------------- v3.10: 概率校准(Platt scaling) ----------------
# 问题: sigmoid(score/gu_sig) 只是单调变换, gu_sig 是拍脑袋定的, 输出并非真实概率。
# 实测: 宣称 66.9% 概率, 实际高开率仅 40%(严格≥0.5% 仅 20%), 校准偏差 +26.9pp。
# 方案: 用已验证样本 (score, 是否真高开) 拟合 Platt scaling: P = 1/(1+exp(A*score+B)),
# 让输出概率名副其实。样本不足时自动降级不启用, 避免小样本过拟合。
# _GAPUP_CALIB / _PRED_CALIB 已迁至 core.py(共享校准状态, 供 calib 与 app 共用)

def gap_up_raw_score(d, ctx=None, late_pull=0.0):
    """v3.10: 返回原始线性分数 score(未经 sigmoid), 供概率校准(Platt scaling)使用。
    与 gap_up_score 共用同一套打分逻辑, 保证校准时用的分数与出推荐的分数一致。"""
    W = {**FCONFIG, **(GAPUP_WEIGHT_OVERRIDE or {})}  # 覆盖权重合并到默认, 保证键齐全
    if d["price"] <= 0 or d["high"] <= d["low"]:
        return None
    rng = d["high"] - d["low"]
    range_pos = (d["price"] - d["low"]) / rng if rng > 0 else 0.5
    pct = d["pct"]
    weibi = d["weibi"] if 0 <= d["weibi"] <= 100 else 0
    vr = d["volratio"] if 0 < d["volratio"] <= 10 else 1
    to = d["turnover"]
    peak = W.get("gu_parab_peak", 4.0)
    score = 0.0
    score += (range_pos - 0.5) * W["gu_pos_w"]      # ①收在高位 = 尾盘强势
    score += -((pct - peak) ** 2) / (7.0 / W["gu_parab_w"])  # ②涨幅偏好peak附近
    score += (weibi / 100.0) * W["gu_wb_w"]         # ③委比正向
    score += max(0.0, min(vr, 4.0) - 1.0) * W["gu_vr_w"]  # ④量比活跃但不过热
    score -= max(0.0, vr - 4.0) * 0.3                    # ④'放量过热(追高)轻微惩罚
    score += min(max(to - 3.0, 0.0), 12.0) * W["gu_to_w"] # ⑤换手充足
    score += late_pull * W["gu_latepull_w"]         # ⑥尾盘拉升 (强预测力)
    if ctx:
        score += (ctx.get("breadth", 0.5) - 0.5) * W["gu_breadth_w"]
        score += ctx.get("retail_pct", 0) * W["gu_retail_w"]
        score += ctx.get("late", 0) * W["gu_idxlate_w"]
    # ⑥'收盘位置与尾盘拉升协同: 收在高位且尾盘仍抢筹 → 隔夜高开更稳(乘性增强)
    if late_pull > 0:
        score += (range_pos - 0.5) * min(late_pull, 2.0) * 0.8
    if d["amplitude"] > 12:                               # 振幅过大疑似冲高回落
        score -= (d["amplitude"] - 12) * 0.05
    return score


def gap_up_score(d, ctx=None, late_pull=0.0):
    """启发式: 下个交易日开盘高开概率(0-100)。v2.8加入 尾盘拉升/宽度/小盘/大盘尾盘动向。
    v3.4: 支持权重覆盖(_GAPUP_WEIGHT_OVERRIDE), 供调优时临时替换 gu_* 权重。
    v3.10: 内部复用 gap_up_raw_score, 并应用 Platt scaling 校准(若已拟合)。"""
    W = {**FCONFIG, **(GAPUP_WEIGHT_OVERRIDE or {})}  # 覆盖权重合并到默认, 保证键齐全
    s = gap_up_raw_score(d, ctx, late_pull)
    if s is None:
        return 0.0
    p = 1 / (1 + math.exp(-s / W["gu_sig"])) * 100
    return round(_apply_gapup_calib(s, p), 1)


_GAPUP_BUILDING = False
_GAPUP_LOCK = threading.Lock()


def _build_gapup_worker(force=False):
    """尾盘扫描主板(排除涨停): 选高开概率最高的5支。
    v2.8: 第一遍启发式粗排, 第二遍对Top15拉分时算late_pull并重算, AI精修Top10, 输出置信度。"""
    global _GAPUP_BUILDING
    with _GAPUP_LOCK:
        if _GAPUP_BUILDING:
            return
        _GAPUP_BUILDING = True
    try:
        today = beijing_now().strftime("%Y-%m-%d")
        print(f"[gapup] worker start, uni={len(_prefilter_universe())}", flush=True)
        ctx = _market_context()                  # 宽度/小盘/大盘尾盘动向
        print(f"[gapup] ctx ok, breadth={ctx['breadth']}", flush=True)
        uni = _prefilter_universe()
        codes = [(_market_prefix(c) + c) for c in uni]
        t0 = time.time()
        # v3.0优化: 一次性拉全部, fetch_tencent 内部 batch=40 + 线程池并发8, 大幅提速
        q = fetch_tencent(codes)
        print(f"[gapup] universe done in {time.time()-t0:.0f}s, got {len(q)}", flush=True)
        cands = []
        for code, name in uni.items():
            p = q.get((_market_prefix(code) + code).upper())
            if not p or len(p) <= F["volratio"]:
                continue
            d = parse_row(p)
            if d["price"] <= 0 or d["limit_up"] <= 0:
                continue
            if d["float_mv"] and d["float_mv"] < SCAN.get("min_float_mv", 37):
                continue
            if d["price"] < SCAN.get("min_price", 8):
                continue
            dist = (d["limit_up"] - d["price"]) / d["price"] * 100 if d["price"] else 999
            if dist <= 0.3:                      # 排除涨停/近似涨停
                continue
            rng = d["high"] - d["low"]
            close_pos = (d["price"] - d["low"]) / rng if rng > 0 else 0.5
            prob = gap_up_score(d, ctx, late_pull=0.0)
            cands.append({"code": code, "name": name, "price": d["price"], "pct": d["pct"],
                          "limit_up": d["limit_up"], "dist_limit_up": round(dist, 2),
                          "weibi": d["weibi"] if 0 <= d["weibi"] <= 100 else 0,
                          "volratio": d["volratio"] if 0 < d["volratio"] <= 10 else 1,
                          "turnover": d["turnover"], "amplitude": d["amplitude"],
                          "close_pos": round(close_pos, 3),
                          "late_pull": 0.0,
                          # v3.10: 保存原始线性分数(供概率校准); None 表示数据无效
                          "score": gap_up_raw_score(d, ctx, late_pull=0.0),
                          "prob": prob, "model": "启发式"})
        cands.sort(key=lambda x: x["prob"], reverse=True)
        top = cands[:15]
        print(f"[gapup] top15 ready, fetch late_pull...", flush=True)
        # 第二遍: 对Top15拉分时算尾盘拉升并重算 (v3.0: 共享线程池并行拉分时提速)
        def _lp_of(c):
            try:
                return _late_pull(c["code"])
            except Exception:
                return 0.0
        lps = list(_fetch_pool(6).map(_lp_of, top))
        for c, lp in zip(top, lps):
            c["late_pull"] = round(lp, 3)
            # 构造满足 gap_up_score 前置检查的 d: 用 high=1, low=0, price=close_pos
            # (使 range_pos = close_pos, 与第一遍的 c["close_pos"] 保持一致)
            d_dummy = {"price": c["close_pos"], "high": 1.0, "low": 0.0,
                       "pct": c["pct"], "weibi": c["weibi"], "volratio": c["volratio"],
                       "turnover": c["turnover"], "amplitude": c["amplitude"]}
            c["prob"] = gap_up_score(d_dummy, ctx, late_pull=lp)
            # v3.10: 同步更新分数, 与最终 prob 对应(校准时用同一份 score)
            _s = gap_up_raw_score(d_dummy, ctx, late_pull=lp)
            if _s is not None:
                c["score"] = _s
        top.sort(key=lambda x: x["prob"], reverse=True)
        print(f"[gapup] refine (heuristic + optional remote AI)...", flush=True)
        # AI 精修: 本地1.5B模型在沙箱CPU推理不稳定(会卡死), 故只尝试远程AI(timeout兜底), 失败回退启发式
        ai = AIClient()
        res = None
        if ai.use_remote:   # 仅远程AI精修(HTTP带timeout, 稳定); 本地模型跳过避免卡死
            rich = []
            for c in top[:5]:
                c2 = dict(c)
                c2["breadth"] = ctx["breadth"]
                c2["sector_avg"] = ctx["sector_avg"]
                rich.append(c2)
            res = ai.refine_gapup(rich)
        if res:
            src, refined = res
            w = FCONFIG.get("ai_fuse_w", AI_FUSED_W)
            for c in top:
                if c["code"] in refined:
                    ai_p = refined[c["code"]]
                    c["heuristic_prob"] = c["prob"]
                    c["ai_prob"] = round(ai_p, 1)
                    c["prob"] = round((1 - w) * c["prob"] + w * ai_p, 1)
                    c["model"] = "AI"
            top.sort(key=lambda x: x["prob"], reverse=True)
        final = top[:5]
        # 置信度: 偏离50 + late_pull强度(尾部抢筹越强越自信)
        for c in final:
            c["confidence"] = round(min(1.0, abs(c["prob"] - 50) / 50.0 * 0.7
                                       + min(2.0, abs(c.get("late_pull", 0))) / 2.0 * 0.3), 2)
        with LOCK:
            bname = ("本地AI模型" if (res and res[0] == "local")
                     else "远程AI模型" if (res and res[0] == "remote") else "启发式")
            w = FCONFIG.get("ai_fuse_w", AI_FUSED_W)
            STATE["gapup"] = {
                "time": beijing_now().strftime("%H:%M:%S"),
                "rows": [{"code": c["code"], "name": c["name"], "price": round(c["price"], 2),
                          "pct": round(c["pct"], 2), "dist_limit_up": c["dist_limit_up"],
                          "weibi": c["weibi"], "volratio": c["volratio"],
                          "late_pull": c.get("late_pull", 0),
                          "prob": c["prob"], "model": c["model"],
                          "heuristic_prob": round(c.get("heuristic_prob", c["prob"]), 1),
                          "ai_prob": c.get("ai_prob"),
                          "confidence": c.get("confidence", 0)} for c in final],
                "ai_used": bool(res),
                "backend": bname,
                "context": {"breadth": ctx["breadth"], "sector_avg": ctx["sector_avg"],
                            "retail": ctx["retail_pct"], "late": ctx["late"]},
                "note": (f"v2.8多因子(收盘位置/涨幅偏好/委比/量能/换手/尾盘拉升/宽度/小盘/大盘尾盘), "
                         f"{bname}精修(AI权重{w}), 非投资建议"),
            }
            STATE["gapup_date"] = today
        # v3.4: 记录当日推荐(记住5只), 供下一交易日开盘后回测验证
        _log_gapup_record({
            "date": today,
            "scan_time": beijing_now().strftime("%H:%M:%S"),
            "source": "auto",
            "stocks": [{
                "code": c["code"], "name": c["name"], "prob": c["prob"],
                "pct": c["pct"], "late_pull": c.get("late_pull", 0),
                # v3.10: 记录原始线性分数, 供 Platt scaling 概率校准使用
                "score": c.get("score"),
                "features": {
                    "range_pos": c.get("close_pos", 0.5), "pct": c["pct"],
                    "weibi": c.get("weibi", 0), "volratio": c.get("volratio", 1),
                    "turnover": c.get("turnover", 0), "late_pull": c.get("late_pull", 0),
                    "breadth": ctx.get("breadth", 0.5), "retail": ctx.get("retail_pct", 0),
                    "idx_late": ctx.get("late", 0),
                },
            } for c in final],
            "verified": False,
        })
        print(f"[gapup] done, {len(final)} rows", flush=True)
    except Exception:
        traceback.print_exc()
    finally:
        with _GAPUP_LOCK:
            _GAPUP_BUILDING = False


def _start_gapup_build(force=False):
    threading.Thread(target=_build_gapup_worker, args=(force,), daemon=True).start()


def scan_gap_up(force=False):
    now = beijing_now()
    today = now.strftime("%Y-%m-%d")
    if force or STATE["gapup_date"] != today or not STATE.get("gapup"):
        _start_gapup_build(force=force)
        return {"time": now.strftime("%H:%M:%S"), "rows": [], "building": True,
                "note": "高开潜力扫描中(全市场主板, 约2分钟), 请稍候自动更新…"}
    return STATE["gapup"]


# ----------------------------- 尾盘高开潜力: 回测闭环 + 权重自优化(v3.4) -----------------------------
class _WeightOverride:
    """临时覆盖 FCONFIG 的 gu_* 权重, 供 optimize 调优时重算分数用。"""
    def __init__(self, weights):
        self.weights = weights
        self.prev = None
    def __enter__(self):
        global GAPUP_WEIGHT_OVERRIDE
        self.prev = GAPUP_WEIGHT_OVERRIDE
        GAPUP_WEIGHT_OVERRIDE = self.weights
        return self
    def __exit__(self, *a):
        global GAPUP_WEIGHT_OVERRIDE
        GAPUP_WEIGHT_OVERRIDE = self.prev


# 生效的权重覆盖(None = 用 FCONFIG 默认)。启动时可从 gapup_weights_tuned.json 加载;
# optimize 调优时由 _WeightOverride 临时覆盖。必须定义在模块级, 供 _WeightOverride 引用。
GAPUP_WEIGHT_OVERRIDE = None


def _prev_trading_day(dt):
    """返回 dt 之前最近工作日(周一~五), 跳过周末。"""
    d = dt.date() if hasattr(dt, "date") else dt
    while True:
        d = d - datetime.timedelta(days=1)
        if datetime.datetime(d.year, d.month, d.day).weekday() < 5:
            return d


def _log_gapup_record(rec):
    """写入/覆盖一条交易日推荐记录到 gapup_log.jsonl。

    v3.11.6 修复: 同日同 source 改为**覆盖**(取当日最新一次预测), 而非跳过 —
    原实现用「跳过」去重, 导致手动重新预测(force=True)只更新面板 STATE['gapup'],
    却不更新回测源 gapup_log, 使「尾盘高开潜力」面板与「高开待回测」数据不一致
    (面板显示手动列表, 实际回测/次日验证的却是每日扫描列表)。覆盖后二者始终指向同一条当日推荐。"""
    try:
        recs = _load_gapup_log()
        key = (rec.get("date"), rec.get("source"))
        for i, r in enumerate(recs):
            if (r.get("date"), r.get("source")) == key:
                recs[i] = rec          # 覆盖: 以当日最新预测为准
                break
        else:
            recs.append(rec)          # 不存在则新增
        with open(GAPUP_LOG, "w", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    except Exception:
        traceback.print_exc()


def _verify_gapup_open(target_date=None):
    """核心回测: 取待回测记录的5只, 抓真实开盘价 vs 昨收, 判定是否高开, 写回并累计统计。"""
    try:
        rec = _find_verify_target(target_date)
        if not rec:
            print("[gapup-verify] 无待回测记录", flush=True)
            return None
        if rec.get("verified"):
            print(f"[gapup-verify] {rec.get('date')} 已验证, 跳过", flush=True)
            return rec
        # 注意: fetch_tencent 必须传小写市场前缀(sh/sz), 腾讯接口不认大写的 SH/SZ(会返回 PNONE_MATCH 导致抓不到开盘价)
        codes = [_market_prefix(s["code"]) + s["code"] for s in rec["stocks"]]
        q = fetch_tencent(codes)
        actual = []
        for s in rec["stocks"]:
            key = (_market_prefix(s["code"]) + s["code"]).upper()  # fetch 返回时 key 被转大写
            p = q.get(key)
            if not p:
                actual.append({"code": s["code"], "name": s["name"], "open": None,
                               "prevclose": None, "gap_pct": None, "is_gap_up": None})
                continue
            d = parse_row(p)
            prev, op = d["prevclose"], d["open"]
            if prev and prev > 0 and op > 0:
                gap = (op - prev) / prev * 100
                # v3.10: 命中判定改为"有意义的高开"(默认 gap>=0.5%), 原为 op>prev 导致
                # +0.01% 的噪声级波动也算命中, 虚增命中率。严格判定用于主指标,
                # 同时保留 is_gap_any(>0) 以便对照展示宽松命中率。
                actual.append({"code": s["code"], "name": s["name"], "open": round(op, 2),
                               "prevclose": round(prev, 2), "gap_pct": round(gap, 2),
                               "is_gap_up": gap >= GAPUP_MIN_GAP_PCT,
                               "is_gap_any": op > prev})
            else:
                actual.append({"code": s["code"], "name": s["name"], "open": op,
                               "prevclose": prev, "gap_pct": None, "is_gap_up": None})
        rec["verified"] = True
        rec["verified_at"] = beijing_now().strftime("%Y-%m-%d %H:%M:%S")
        rec["actual"] = actual
        recs = _load_gapup_log()
        for i, r in enumerate(recs):
            if r.get("date") == rec.get("date") and r.get("source") == rec.get("source"):
                recs[i] = rec
                break
        with open(GAPUP_LOG, "w", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        stats = _accumulate_stats(rec)
        ng = sum(1 for a in actual if a.get("is_gap_up"))
        print(f"[gapup-verify] {rec.get('date')} 验证完成: 高开 {ng}/{len(actual)}", flush=True)
        # v3.11.1: 接入统一自动调参引擎(权重自调, AUC目标); 样本不足则记录待激活
        try:
            auto_tune_module("gapup")
        except Exception:
            traceback.print_exc()
        # v3.10: 每验证一条就重拟合概率校准, 让概率随样本积累自动收敛到真实水平
        try:
            cb = fit_gapup_calib()
            if cb.get("ok"):
                print(f"[gapup-calib] A={cb['A']} B={cb['B']} n={cb['n']} "
                      f"预测均值={cb['pred_mean']}% 实际={cb['actual_rate']*100:.1f}%", flush=True)
            else:
                print(f"[gapup-calib] 跳过: {cb.get('reason')}", flush=True)
        except Exception:
            traceback.print_exc()
        return rec
    except Exception:
        traceback.print_exc()
        return None


def _calib_idx_view(payload):
    """上证1小时预测的实时概率校准(复制后改, 不动 STATE)。

    v3.11.7 修复「概率低 / 置信度低却仍展示方向」:
      1) 一致性: 概率经 Platt 校准后, verdict 与 confidence 一律按【校准后】概率重算。
         旧逻辑只在原始概率上判方向, 而校准又只改了 prob, 于是出现
         「显示 13.2% 却判定看涨」这种概率与方向自相矛盾的输出。
      2) 门控: 置信度低于 idx_min_conf 时不再给出方向性判定, 改显示"观望"并置灰概率,
         避免把弱信号包装成可执行的涨跌判断。
    仅作用于展示层 —— STATE 仍存原始概率与原始 verdict(调度/AI融合需要); 但落盘(idx_1h)
    经 _idx_gated_verdict 把 verdict 按校准后概率重判(含观望门控), 与展示层同口径; 落盘 prob
    仍存原始值(校准在展示/_recompute_pred_stats 统一套用, 避免二次校准与拟合反馈漂移), 回测指标一致。
    """
    if not isinstance(payload, dict) or "prob" not in payload:
        return payload
    p = dict(payload)
    raw_prob = float(p.get("prob", 0) or 0)
    prob = round(_apply_pred_calib("idx_1h", raw_prob), 1)
    p["raw_prob"] = round(raw_prob, 1)        # 保留校准前概率, 便于排查与对比
    p["prob"] = prob
    # ① 按校准后概率重判方向(阈值同样走语义护栏, 保证 >50)
    T = _clamp_threshold("idx_1h",
                         _MODULE_THRESHOLDS.get("idx_1h", _TUNE_SPEC["idx_1h"]["def_thr"]))
    verdict = "看涨" if prob >= T else ("看跌" if prob <= 100 - T else "震荡")
    p["verdict"] = verdict
    # ② 按校准后概率重算置信度(dist 项随概率变化; AI 一致性项沿用原始启发式概率)
    conf = _confidence(prob, p.get("breadth", 0.5),
                       ai_prob=p.get("ai_prob"),
                       heuristic_prob=p.get("heuristic_prob"))
    p["confidence"] = conf
    # ③ 弱信号门控: 方向性判定需同时满足「有方向」且「置信度达标」
    min_conf = float(FCONFIG.get("idx_min_conf", 0.35))
    actionable = verdict in ("看涨", "看跌") and conf >= min_conf
    p["min_conf"] = min_conf
    p["actionable"] = actionable
    p["display_verdict"] = verdict if actionable else ("震荡" if verdict == "震荡" else "观望")
    if not actionable:
        p["note"] = (p.get("note", "") +
                     f" ｜ 置信度{conf:.2f} < {min_conf:.2f}, 信号不足, 仅观望不判方向")
    return p


def _idx_gated_verdict(base):
    """与 _calib_idx_view 完全一致的「方向」裁决, 专供落盘/回测使用:
    先按 Platt 校准后概率重判 看涨/看跌/震荡, 再对弱信号(置信度<idx_min_conf)门控为「观望」。
    使回测总览的 idx_1h 方向与线上实时展示层保持同一套口径。
    注: 置信度与 _calib_idx_view 一样基于校准后概率重算(不读 base 里缓存的 confidence), 保证两处逐字节一致。"""
    raw_prob = float(base.get("prob", 0) or 0)
    prob = round(_apply_pred_calib("idx_1h", raw_prob), 1)
    T = _clamp_threshold("idx_1h",
                         _MODULE_THRESHOLDS.get("idx_1h", _TUNE_SPEC["idx_1h"]["def_thr"]))
    verdict = "看涨" if prob >= T else ("看跌" if prob <= 100 - T else "震荡")
    conf = _confidence(prob, base.get("breadth", 0.5),
                       ai_prob=base.get("ai_prob"),
                       heuristic_prob=base.get("heuristic_prob"))
    min_conf = float(FCONFIG.get("idx_min_conf", 0.35))
    actionable = verdict in ("看涨", "看跌") and conf >= min_conf
    return verdict if actionable else ("震荡" if verdict == "震荡" else "观望")


def _close_verdict(module, cal_prob):
    """按【校准后】概率重判收盘模块方向(偏多/偏空/震荡), 与 _calib_close_view 同一口径。
    收盘模块无弱信号门控(无 close_min_conf 配置), 故不转「观望」。"""
    T = _clamp_threshold(module, _MODULE_THRESHOLDS.get(module, _TUNE_SPEC[module]["def_thr"]))
    return "偏多" if cal_prob >= T else ("偏空" if cal_prob <= 100 - T else "震荡")


def _calib_close_view(payload):
    """尾盘预测(大盘+个股)的实时概率校准(复制后改, 不动 STATE)。
    修复「校准后概率与方向自相矛盾」: prob 经 Platt 校准后, verdict 一律按【校准后】概率
    重判(偏多/偏空/震荡), 与 _calib_idx_view 同一思路; 旧逻辑只校准 prob 不复判 verdict,
    会出现「显示校准后48%却判定偏多」的矛盾。收盘模块无弱信号门控(无 close_min_conf 配置),
    故不转「观望」; 落盘(见 _build_close_worker)也用同一函数, 保证回测总览方向与线上展示层一致。"""
    if not isinstance(payload, dict):
        return payload
    p = copy.deepcopy(payload)
    if isinstance(p.get("market"), dict):
        m = p["market"]
        m["raw_prob"] = round(m.get("prob", 0) or 0, 1)
        m["prob"] = round(_apply_pred_calib("close_market", m["prob"]), 1)
        m["verdict"] = _close_verdict("close_market", m["prob"])
        m["confidence"] = _confidence(m["prob"], m.get("breadth", 0.5),
                                      ai_prob=m.get("ai_prob"),
                                      heuristic_prob=m.get("heuristic_prob"))
    for s in (p.get("stocks") or []):
        if isinstance(s, dict):
            s["raw_prob"] = round(s.get("prob", 0) or 0, 1)
            s["prob"] = round(_apply_pred_calib("close_stock", s["prob"]), 1)
            s["verdict"] = _close_verdict("close_stock", s["prob"])
    return p


def _calib_preopen_view(payload):
    """盘前涨停预测的实时概率校准(复制后改, 不动 STATE)。"""
    if not isinstance(payload, dict):
        return payload
    p = copy.deepcopy(payload)
    for c in (p.get("rows") or []):
        if isinstance(c, dict):
            c["prob"] = round(_apply_pred_calib("preopen_limitup", c.get("prob", 0)), 1)
    return p


# ---------------- v3.10: 本地日线库 (P2) ----------------
# 问题: 此前所有历史K线都靠实时调 _fetch_kline 现抓, 既慢又无本地沉淀,
#       导致"回测/统计/连板基因"等依赖历史的逻辑永远只能看最近 12 天, 且无法验证。
# 方案: 每个交易日收盘后(15:05)把当日完整日线(OHLCV)落盘 daily_bars.jsonl,
#       并按"保留天数 + 体积上限"双重阈值定期清理, 防止长期运行把磁盘写满。
# 格式: 每行一天 {"date":"2026-08-30","ts":"...","bars":{"sh000001":{"o":..,"h":..,"l":..,"c":..,"v":..}}}

def _daily_universe():
    """日线库收录范围: 指数 + 持仓 + 自选 + 当日涨停候选 + 尾盘高开推荐。
    DAILY_UNIVERSE_LIMIT>0 时截断(0=不限制)。"""
    codes = [c for c, _ in INDICES]
    try:
        codes += [(h["market"] + h["code"]).lower() for h in HOLDINGS]
    except Exception:
        pass
    try:
        codes += [(w["market"] + w["code"]).lower() for w in WATCHLIST]
    except Exception:
        pass
    try:
        for c in (_CAND_POOL or [])[:30]:
            codes.append(_market_prefix(c["code"]) + c["code"])
    except Exception:
        pass
    try:
        for c in (STATE.get("gapup") or {}).get("rows", [])[:10]:
            codes.append(_market_prefix(c["code"]) + c["code"])
    except Exception:
        pass
    codes = [c.lower() for c in codes if c]
    codes = list(dict.fromkeys(codes))
    if DAILY_UNIVERSE_LIMIT and len(codes) > DAILY_UNIVERSE_LIMIT:
        codes = codes[:DAILY_UNIVERSE_LIMIT]
    return codes


# _load_daily() 已下沉到 backtest.py(本文件通过 `from backtest import *` 复用),
# 原因见 backtest._load_daily 的文档字符串: 模块加载顺序导致本文件定义的名字无法被回测层引用。

def _daily_size_mb(recs):
    return sum(len(json.dumps(r, ensure_ascii=False)) + 1 for r in recs) / 1024.0 / 1024.0


def capture_daily_bars():
    """收盘后抓取当日完整日线落库, 并顺带执行定期清理(保留天数/体积上限双阈值)。"""
    try:
        now = beijing_now()
        today = now.strftime("%Y-%m-%d")
        codes = _daily_universe()
        if not codes:
            return {"ok": False, "reason": "无收录标的"}
        ymd = now.strftime("%Y%m%d")
        bars, got, stale = {}, 0, 0
        for i in range(0, len(codes), 40):
            q = fetch_tencent(codes[i:i + 40])
            for k, row in q.items():
                # 关键: 校验行情日期。休市/节假日时腾讯仍返回上一交易日的收盘数据,
                # 若不校验就会把上一交易日的K线错记成今天的, 日线库从此失真。
                qday = str(row[30])[:8] if len(row) > 30 else ""
                if qday and qday != ymd:
                    stale += 1
                    continue
                d = parse_row(row)
                if not d.get("price") or d["price"] <= 0:
                    continue
                bars[k.lower()] = {"o": d["open"], "h": d["high"], "l": d["low"],
                                   "c": d["price"], "v": d.get("vol", 0),
                                   "amt": d.get("amount_wan", 0)}
                got += 1
        if not bars:
            return {"ok": False, "reason": f"无当日行情(可能休市, 过期行情{stale}只)"}
        recs = [r for r in _load_daily() if r.get("date") != today]
        recs.append({"date": today, "ts": now.strftime("%Y-%m-%d %H:%M:%S"), "bars": bars})
        recs.sort(key=lambda r: r.get("date", ""))
        # --- 定期清理: 先按保留天数, 再按体积上限 ---
        note = []
        if len(recs) > DAILY_KEEP_DAYS:
            drop = len(recs) - DAILY_KEEP_DAYS
            recs = recs[drop:]
            note.append(f"超保留天数({DAILY_KEEP_DAYS}天), 丢弃最旧{drop}天")
        guard = 0
        while _daily_size_mb(recs) > DAILY_MAX_MB and len(recs) > 30 and guard < 500:
            recs = recs[1:]
            guard += 1
        if guard:
            note.append(f"超体积上限({DAILY_MAX_MB}MB), 再丢弃最旧{guard}天")
        tmp = DAILY_BARS + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        os.replace(tmp, DAILY_BARS)
        print(f"[daily-bars] {today} 落库 {got}只(跳过过期{stale}只), 累计 {len(recs)}天, "
              f"{_daily_size_mb(recs):.2f}MB" + (f" | 清理: {'; '.join(note)}" if note else ""),
              flush=True)
        return {"ok": True, "date": today, "codes": got, "stale_skipped": stale,
                "days": len(recs), "size_mb": round(_daily_size_mb(recs), 3), "cleaned": note}
    except Exception:
        traceback.print_exc()
        return {"ok": False, "reason": "异常", "trace": traceback.format_exc()}


def daily_bars_info():
    recs = _load_daily()
    if not recs:
        return {"days": 0, "codes": 0, "size_mb": 0, "from": None, "to": None}
    return {"days": len(recs), "codes": len(recs[-1].get("bars", {})),
            "size_mb": round(_daily_size_mb(recs), 3),
            "from": recs[0].get("date"), "to": recs[-1].get("date"),
            "keep_days": DAILY_KEEP_DAYS, "max_mb": DAILY_MAX_MB}


def get_daily_bars(code, days=60):
    """从本地日线库取某标的最近 N 天日线(升序)。本地没有时回退到在线 _fetch_kline。"""
    key = code.lower()
    if not key.startswith(("sh", "sz")):
        key = _market_prefix(code) + code
    out = []
    for r in _load_daily()[-days:]:
        b = (r.get("bars") or {}).get(key)
        if b:
            out.append({"date": r["date"], "open": b.get("o"), "close": b.get("c"),
                        "high": b.get("h"), "low": b.get("l"), "vol": b.get("v")})
    return out


# ----------------------------- 选股池(v3.11.13) -----------------------------
# 每日定时(默认 14:30)全市场扫描主板, 严格按用户给定条件筛选:
#   (1) 过去 lookback(5) 根K线中, 至少 below_need(4) 根满足 MA短(3日线) < MA长(7日线)
#       —— 即前期均线被压制/粘合在下方;
#   (2) 当根 MA短 从下往上上穿 MA长(前一根 MA短<MA长, 且当根 MA短>=MA长)。
#   加分项: SKDJ 的 K 落在 20 左右(默认 15~25) 且 K 向上金叉 D -> 显著加分, 优先推荐。
#   输出: 按分数降序取 TopN(默认3), 含 名称/代码/题材/分数; 无满足项时 rows 为空(前端显示"无")。

def _ma(vals, n):
    """简单移动平均; 数据不足返回 None。"""
    if len(vals) < n or n <= 0:
        return None
    return sum(vals[-n:]) / float(n)


def _skdj_series(bars, n=9, m1=3, m2=3):
    """SKDJ(慢速随机指标)的 K/D 序列, 与 bars 等长; 前置不足处填 None。

    RSV = (C - LLV(L,n)) / (HHV(H,n) - LLV(L,n)) * 100   (区间为0时取 50 中值)
    K   = ((m1-1)*K_prev + RSV) / m1
    D   = ((m2-1)*D_prev + K)   / m2
    初值 K=D=50(行业惯例)。
    """
    ks, ds = [], []
    k_prev, d_prev = 50.0, 50.0
    for i in range(len(bars)):
        if i + 1 < n:
            ks.append(None); ds.append(None); continue
        win = bars[i + 1 - n:i + 1]
        try:
            hh = max(b["high"] for b in win)
            ll = min(b["low"] for b in win)
        except (KeyError, TypeError):
            ks.append(None); ds.append(None); continue
        rng = hh - ll
        rsv = 50.0 if rng <= 0 else (bars[i]["close"] - ll) / rng * 100.0
        k_prev = ((m1 - 1) * k_prev + rsv) / float(m1)
        d_prev = ((m2 - 1) * d_prev + k_prev) / float(m2)
        ks.append(k_prev); ds.append(d_prev)
    return ks, ds


def _pool_theme(code, name):
    """本地解析题材, 全程不发起网络请求(全市场扫描若逐股联网会慢到不可用)。
    优先级: STOCK_THEMES 静态表 > 名称关键词 match_theme > 已缓存的分类结果。
    统一返回【字符串】(无则空串) —— STOCK_THEMES 的值是列表, 需取首个, 否则前端会渲染成 ["白酒"]。"""
    t = STOCK_THEMES.get(str(code))
    if t:
        return t[0] if isinstance(t, (list, tuple)) else str(t)
    t = match_theme(name or "")
    if t:
        return t[0] if isinstance(t, (list, tuple)) else str(t)
    try:
        c = load_classify_cache().get(str(code)) or {}
        th = c.get("theme") or ""
        return th[0] if isinstance(th, (list, tuple)) else str(th)
    except Exception:
        return ""


def _pool_eval(bars, code, name, price, pct):
    """按选股池条件评估单只股票。返回 (是否命中, 详情dict|None)。"""
    p = PCFG
    ns, nl = int(p["ma_short"]), int(p["ma_long"])
    look, need = int(p["lookback"]), int(p["below_need"])
    closes = [b["close"] for b in bars]
    # 至少需要: 窗口(look) + 当根 + 前一根, 且当根能算出 MA长 -> look + nl + 1
    if len(closes) < look + nl + 1:
        return False, None
    ma_s, ma_l = [], []
    for i in range(len(closes)):
        ma_s.append(_ma(closes[:i + 1], ns))
        ma_l.append(_ma(closes[:i + 1], nl))
    i = len(closes) - 1                      # 当根(最新)索引
    if None in (ma_s[i], ma_l[i], ma_s[i - 1], ma_l[i - 1]):
        return False, None
    # 条件(2): 当根从下往上上穿 —— 前一根 MA短<MA长, 当根 MA短>=MA长
    if not (ma_s[i - 1] < ma_l[i - 1] and ma_s[i] >= ma_l[i]):
        return False, None
    # 条件(1): 当根之前的 look 根里, 至少 need 根 MA短 在 MA长 下方
    below = sum(1 for j in range(i - look, i)
                if ma_s[j] is not None and ma_l[j] is not None and ma_s[j] < ma_l[j])
    if below < need:
        return False, None
    # ---- SKDJ ----
    ks, ds = _skdj_series(bars, int(p["skdj_n"]), int(p["skdj_m1"]), int(p["skdj_m2"]))
    k, d = ks[i], ds[i]
    kp, dp = ks[i - 1], ds[i - 1]
    cross = (None not in (k, d, kp, dp) and kp <= dp and k > d)
    near = (k is not None and float(p["skdj_low"]) <= k <= float(p["skdj_high"]))
    # ---- 打分(越高越靠前) ----
    score = 0.0
    spread = ((ma_s[i] - ma_l[i]) / ma_l[i] * 100.0) if ma_l[i] else 0.0
    score += min(max(spread, 0.0) * float(p["w_cross"]), float(p["w_cross_cap"]))
    if cross and near:                       # SKDJ 低位金叉: 主加分, 越贴近20越高
        score += float(p["w_skdj_cross"]) + max(0.0, 5.0 - abs(k - 20.0)) * 2.0
    elif cross:                              # 金叉但 K 不在低位区间
        score += float(p["w_skdj_other_cross"])
    elif near:                               # K 在低位但未金叉
        score += float(p["w_skdj_near"])
    score += (below - need) * float(p["w_extra_below"])   # 压制越充分略加分
    return True, {
        "code": code, "name": name, "theme": _pool_theme(code, name),
        "market": _market_prefix(code) or "sh",
        "price": round(price, 2) if price else None,
        "pct": round(pct, 2) if pct is not None else None,
        "score": round(score, 1),
        "ma_short": round(ma_s[i], 3), "ma_long": round(ma_l[i], 3),
        "spread_pct": round(spread, 3), "below_bars": below,
        "skdj_k": None if k is None else round(k, 1),
        "skdj_d": None if d is None else round(d, 1),
        "skdj_cross": bool(cross), "skdj_low": bool(near),
    }


def _build_stock_pool(force=False):
    """后台全市场扫描主板, 产出选股池 TopN, 写入 STATE['stock_pool']。

    两阶段: 先批量拉实时行情做轻量预筛(剔除停牌/无有效价), 再并发抓日K算指标,
    避免对全市场(数千只)盲目逐只抓K线。全程在后台线程执行, 不阻塞调度循环。
    """
    with LOCK:
        if STATE.get("stock_pool_scanning"):
            return
        STATE["stock_pool_scanning"] = True
    t0 = time.time()
    try:
        today = beijing_now().strftime("%Y-%m-%d")
        p = PCFG
        uni = _prefilter_universe()          # 主板(排除持仓/自选/ST)
        codes = list(uni.keys())
        # 阶段1: 批量实时行情预筛
        # 注意: 腾讯行情接口大小写敏感 —— 请求必须传小写代码(大写会返回 v_pnone_match 无效响应),
        # 而 fetch_tencent 返回的 key 是大写, 故查表时要 .upper()
        qmap = {(_market_prefix(c) + c).lower(): c for c in codes}
        quotes = fetch_tencent(list(qmap.keys()))
        cands = []
        for qk, c in qmap.items():
            row = quotes.get(qk.upper()) or quotes.get(qk)
            if not row:
                continue
            d = parse_row(row)
            if d["price"] <= 0:              # 停牌/无有效价
                continue
            amt = (num(d.get("amount_wan")) or 0) * 10000
            if p.get("min_amount") and amt < float(p["min_amount"]):
                continue
            cands.append((c, uni[c], d["price"], d["pct"], amt))
        if p.get("max_scan") and len(cands) > int(p["max_scan"]):
            cands.sort(key=lambda x: -(x[4] or 0))
            cands = cands[:int(p["max_scan"])]

        # 阶段2: 并发抓日K(含当根)并评估
        min_bars = int(p["lookback"]) + int(p["ma_long"]) + 1
        bars_n = max(int(p["bars"]), min_bars + 2)

        def _work(item):
            c, nm, pr, pctv, _amt = item
            try:
                bars = _fetch_kline(c, days=bars_n, include_today=True)
                if len(bars) < min_bars:
                    return None
                ok, info = _pool_eval(bars, c, nm, pr, pctv)
                return info if ok else None
            except Exception:
                return None

        hits = []
        workers = max(1, int(p["workers"]))
        if cands:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pool") as ex:
                for info in ex.map(_work, cands):
                    if info:
                        hits.append(info)
        hits.sort(key=lambda x: -x["score"])
        top = hits[:max(1, int(p["top_n"]))]
        # v3.11.13: TopN 题材补全 —— 仅对最终入围的命中股, 若本地题材为空则联网
        # (东方财富行业+名称自动识别)补全; 只 N 只, 快且写缓存, 不影响全市场扫描热路径。
        for x in top:
            if not x.get("theme"):
                try:
                    x["theme"] = auto_classify(x["code"], x.get("market"), x.get("name")) or ""
                except Exception:
                    pass
        with LOCK:
            STATE["stock_pool"] = {
                "date": today,
                "time": beijing_now().strftime("%H:%M:%S"),
                "rows": top,
                "scanned": len(cands),
                "matched": len(hits),
                "note": (f"主板扫描 {len(cands)} 只 · 命中 {len(hits)} 只 · "
                         f"取分数最高 {len(top)} 只"),
                "seconds": round(time.time() - t0, 1),
            }
            STATE["stock_pool_date"] = today
    except Exception:
        traceback.print_exc()
    finally:
        with LOCK:
            STATE["stock_pool_scanning"] = False


# 休眠窗口策略(用户授权): 每日 16:00–次日 09:00 允许沙箱平台自动休眠以省资源。
# 唤醒由平台在收到用户新指令时自动完成(无需额外操作)。本调度器在非交易时段不主动
# 保持连接/不做重活(各预测模块仅在交易时段触发), 不会阻止平台休眠。
# 若用户在休眠期发来指令, 平台唤醒沙箱后, 看门狗(watchdog.sh, 由发布 AUTOSTART 拉起)
# 会自动拉起本服务, 公网链接恢复可用。
@app.route("/")
def index():
    return Response(DASHBOARD_HTML.replace("__VERSION__", VERSION), mimetype="text/html")


@app.route("/api/snapshot")
def api_snapshot():
    with LOCK:
        if STATE["latest"] is None:
            # v3.0: build_snapshot 用超时线程保护, 避免偶发卡住导致接口挂起
            box = {}
            def _build():
                try:
                    box["snap"] = build_snapshot()
                except Exception as e:
                    box["err"] = str(e)
            t = threading.Thread(target=_build, daemon=True)
            t.start()
            t.join(timeout=10)
            if "snap" in box:
                STATE["latest"] = box["snap"]
            elif "err" in box:
                return jsonify({"error": box["err"]})
            else:
                # 超时未完成: 返回空快照兜底, 避免挂起
                return jsonify({"error": "build_snapshot timeout", "beijing": beijing_now().strftime("%Y-%m-%d %H:%M:%S")})
        data = dict(STATE["latest"])
        # 动态合并最新状态(涨停趋势/指数预测/预警), 前端5秒轮询即可看到
        data["preopen"] = _calib_preopen_view(STATE["preopen"])
        data["idx_forecast"] = _calib_idx_view(STATE["idx_forecast"])
        data["close"] = _calib_close_view(STATE["close"])
        data["sector_drivers"] = STATE["sector_drivers"]
        data["gapup"] = STATE["gapup"]
    data["server_time"] = beijing_now().strftime("%Y-%m-%d %H:%M:%S")
    return jsonify(data)


@app.route("/api/preopen")
def api_preopen():
    with LOCK:
        if STATE["preopen"]:
            return jsonify(_calib_preopen_view(STATE["preopen"]))
    # 无缓存: 异步触发构建并返回构建中状态
    r = scan_limit_up()
    if r.get("building"):
        with LOCK:
            STATE["preopen"] = r
        return jsonify(r)
    with LOCK:
        STATE["preopen"] = r
    return jsonify(r)


@app.route("/api/close")
def api_close():
    _start_close_build()
    with LOCK:
        cur = STATE["close"]
    return jsonify(_calib_close_view(cur) if cur else {"building": True,
                                    "note": "尾盘预测计算中(含AI模型融合), 请稍候自动更新…"})


@app.route("/api/idx")
def api_idx():
    # v3.11.8: 必须为 datetime(调度循环 now-last 做时间减法), 旧代码写成 strftime 字符串会
    # 导致 scheduler_loop 每轮 TypeError 崩溃; 置为 now 既修复类型又保持节流语义。
    STATE["idx_forecast_time"] = beijing_now()
    _start_idx_build()
    with LOCK:
        cur = STATE["idx_forecast"]
    return jsonify(_calib_idx_view(cur) if cur else {"building": True,
                                    "note": "上证预测计算中(含AI模型融合), 请稍候自动更新…"})


@app.route("/api/stock_pool")
def api_stock_pool():
    """v3.11.13 选股池: ?rescan=1 后台强制重新扫描(全市场主板, 约1~3分钟)。"""
    if request.args.get("rescan") in ("1", "true"):
        threading.Thread(target=_build_stock_pool, kwargs={"force": True}, daemon=True).start()
    with LOCK:
        cur = STATE.get("stock_pool")
    if cur:
        return jsonify(cur)
    return jsonify({"date": None, "rows": [], "scanned": 0, "matched": 0,
                    "scanning": bool(STATE.get("stock_pool_scanning")),
                    "note": "尚未扫描（每交易日 14:30 自动执行，可点「立即扫描」）"})


@app.route("/api/pred_stats")
def api_pred_stats():
    """v3.10: 各预测模块的命中率/校准偏差回测统计。?refresh=1 强制重算。"""
    s = _recompute_pred_stats() if request.args.get("refresh") in ("1", "true") else load_pred_stats()
    # v3.11.0: 附加各模块自动调参状态(阈值/权重), 供面板展示
    s["tune"] = {m: _PRED_TUNE.get(m) for m in _TUNE_SPEC}
    s["tune_meta"] = {"min_thr": MIN_TUNE_THRESH, "min_w": MIN_TUNE_WEIGHT,
                      "gapup_min": GAPUP_MIN_OPT_SAMPLES}
    # v3.11.1: 把尾盘高开潜力(gapup) 的命中率/校准状态桥接进统一面板, 作为第5个模块行
    st = _load_stats()
    gcal = _GAPUP_CALIB
    ghr = st.get("hit_rate")
    gap_avg_pred = st.get("avg_pred")
    s["modules"]["gapup"] = {
        "label": "尾盘高开潜力", "n": st.get("total", 0),
        "hit_rate": ghr, "avg_pred": gap_avg_pred,
        "bias_pp": (gap_avg_pred - ghr * 100)
                   if (ghr is not None and gap_avg_pred is not None) else None,
        "calib": ({"n": gcal.get("n", 0), "applied": gcal.get("n", 0) >= GAPUP_MIN_CALIB_SAMPLES}
                  if gcal.get("A") is not None else {"n": 0, "applied": False}),
        "by_verdict": {}, "recent": [],
    }
    return jsonify(s)


@app.route("/api/pred_verify")
def api_pred_verify():
    """手动触发一次预测回测回填(调试用)。"""
    return jsonify({"done": verify_predictions(), "stats": load_pred_stats()})


@app.route("/api/tune", methods=["POST"])
def api_tune():
    """v3.11.0: 手动触发全部模块自动调参(阈值+权重)。达到样本阈值才生效, 否则保持待激活。"""
    out = auto_tune_all()
    return jsonify({"ok": True, "tuned": {m: {
        "threshold": (r.get("threshold") or {}).get("status", "ok"),
        "weights": (r.get("weights") or {}).get("status", "ok")} for m, r in out.items()}})


@app.route("/api/tune_reset", methods=["POST"])
def api_tune_reset():
    """v3.11.0: 清除自动调参结果, 恢复默认权重/阈值。"""
    global _PRED_TUNE
    _PRED_TUNE = {}
    try:
        if os.path.exists(PRED_TUNE):
            os.remove(PRED_TUNE)
    except Exception:
        pass
    for k in list(FCONFIG.keys()):
        if k in _TUNE_W_DEFAULT:
            FCONFIG[k] = _TUNE_W_DEFAULT[k]
    for k in list(SCFG.keys()):
        if k in _SCFG_W_DEFAULT:
            SCFG[k] = _SCFG_W_DEFAULT[k]
    for m in _TUNE_SPEC:
        _MODULE_THRESHOLDS[m] = _TUNE_SPEC[m]["def_thr"]
    # v3.11.1: gapup 权重覆盖(由 optimize_gapup_weights 落盘 GAPUP_TUNED)一并恢复默认
    global GAPUP_WEIGHT_OVERRIDE
    GAPUP_WEIGHT_OVERRIDE = None
    try:
        if os.path.exists(GAPUP_TUNED):
            os.remove(GAPUP_TUNED)
    except Exception:
        pass
    try:
        _recompute_pred_stats()
    except Exception:
        pass
    return jsonify({"ok": True, "msg": "已恢复默认权重与阈值"})


@app.route("/api/daily_bars")
def api_daily_bars():
    """v3.10: 本地日线库状态。?capture=1 手动触发当日落库; ?code=600522&days=60 取日线。"""
    code = request.args.get("code")
    if code:
        try:
            days = int(request.args.get("days", 60))
        except ValueError:
            days = 60
        return jsonify({"code": code, "bars": get_daily_bars(code, days)})
    if request.args.get("capture") in ("1", "true"):
        return jsonify(capture_daily_bars())
    return jsonify(daily_bars_info())


@app.route("/api/drivers")
def api_drivers():
    try:
        snap = build_snapshot()
        r = detect_sector_drivers(snap)
        with LOCK:
            STATE["sector_drivers"] = r
            STATE["sector_drivers_time"] = beijing_now()
        return jsonify(r)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/gapup")
def api_gapup():
    # ?force=1 表示用户点击"立即检测"按钮: 强制重新触发一次全市场扫描
    force = request.args.get("force") in ("1", "true")
    r = scan_gap_up(force=force)
    if r.get("building"):
        with LOCK:
            STATE["gapup"] = r
        return jsonify(r)
    with LOCK:
        STATE["gapup"] = r
    return jsonify(r)


@app.route("/api/gapup/log")
def api_gapup_log():
    """返回回测历史记录 + 累计统计, 供前端「高开回测」卡片渲染。"""
    recs = _load_gapup_log()
    recs.sort(key=lambda r: r.get("date", ""), reverse=True)
    stats = _load_stats()
    tuned = None
    if os.path.exists(GAPUP_TUNED):
        try:
            tuned = json.load(open(GAPUP_TUNED, encoding="utf-8"))
        except Exception:
            pass
    return jsonify({
        "records": recs[:30],
        "stats": stats,
        "tuned_weights": tuned,
        "min_opt_samples": GAPUP_MIN_OPT_SAMPLES,
    })


@app.route("/api/gapup/optimize", methods=["POST"])
def api_gapup_optimize():
    """手动触发权重调优(需累积足够样本)。"""
    return jsonify(optimize_gapup_weights())


@app.route("/api/portfolio", methods=["GET", "POST"])
def api_portfolio():
    """持仓前端编辑接口。
    GET : 返回当前内存持仓(仅可编辑字段 code/cost/shares), 用于初始化编辑表单。
    POST: 保存编辑结果, 仅接受 code/cost/shares; 原子写回 portfolio.json 并热重载,
          其余字段(closed_positions/settings/watchlist 等)保持不变, 无需重启服务。"""
    global HOLDINGS
    if request.method == "GET":
        with _HOLD_LOCK:
            # 返回完整持仓字典(含 buy_date/name/阈值等非编辑字段), 便于发布前从线上同步还原;
            # 前端编辑面板仅取 code/cost/shares, 多余字段会被忽略。
            rows = [dict(h) for h in HOLDINGS]
        return jsonify({"holdings": rows})
    # POST: 前端编辑后保存
    try:
        payload = request.get_json(force=True)
        raw = payload.get("holdings", [])
        # 读取现有持仓, 保存时按 code 保留原有哪些非编辑字段(如 buy_date/name/sector/阈值等)
        with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        existing = {str(h.get("code", "")): h for h in cfg.get("holdings", [])}
        new_holdings, seen = [], set()
        for h in raw:
            code = str(h.get("code", "")).strip()
            if not code or code in seen:
                continue
            seen.add(code)
            cost = float(h.get("cost", 0) or 0)
            shares = float(h.get("shares", 0) or 0)
            if cost < 0 or shares < 0:
                return jsonify({"ok": False, "error": f"股票 {code} 的成本/股数不能为负"}), 400
            # 仅覆盖可编辑的三个字段, 其余原有字段原样保留
            item = dict(existing.get(code, {}))
            item["code"] = code
            item["cost"] = round(cost, 3)
            item["shares"] = round(shares, 2)
            # 每次保存(新增 / 改成本 / 加仓)都代表"今天有买入动作",
            # 故把最后买入日(last_buy_date)写为今天, 使当日盈亏基于成本(而非昨收)。
            # 首次买入时若没有 buy_date 也补记为今天(首买日)。
            today_str = beijing_now().strftime("%Y-%m-%d")
            item.setdefault("buy_date", today_str)
            item["last_buy_date"] = today_str
            item["open_date"] = today_str
            # 加仓检测: 若本次保存股数 > 原持仓股数, 把"加仓前股数"记为 open_shares,
            # 使当日盈亏分段(开盘前部分按昨收, 加仓部分按成本), 不抹除加仓前收益。
            # 首买(无原有持仓) open_shares=0(全按成本); 未加仓则保留原 open_shares。
            if code not in existing:
                item["open_shares"] = 0.0
            else:
                old_shares = float(existing[code].get("shares", 0) or 0)
                if shares > old_shares:
                    item["open_shares"] = round(old_shares, 2)
            new_holdings.append(item)
        # 题材自动归类: 对缺失题材且不在手工 STOCK_THEMES 表的持仓(含新增),
        # 通过东方财富行业 + 名称自动识别, 写回 theme 字段(前端新增即自动显示芯片)。
        # 归类仅在保存时触发, 失败不影响保存; 分类网络异常时交由启动后台线程重试。
        try:
            need = [h for h in new_holdings
                    if not (h.get("theme") or STOCK_THEMES.get(h["code"]))]
            if need:
                codes = [h["code"] for h in need]
                qn = fetch_tencent([(_market_prefix(c) + c) for c in codes])
                for h in need:
                    up = (_market_prefix(h["code"]) + h["code"]).upper()
                    nm = h.get("name") or parse_row(qn.get(up, [])).get("name", "")
                    if nm:
                        h["name"] = nm
                    h["theme"] = auto_classify(h["code"], h.get("market"), nm)
        except Exception:
            pass
        if not new_holdings:
            return jsonify({"ok": False, "error": "至少保留一只持仓(股票代码不能为空)"}), 400
        # 仅替换 holdings, 保留其余顶层字段(closed_positions/settings/watchlist 等)
        cfg["holdings"] = new_holdings
        tmp = PORTFOLIO_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        os.replace(tmp, PORTFOLIO_PATH)   # 原子写, 避免半截文件
        # 热重载内存, 下一轮快照(≤5s)即生效
        with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
            cfg2 = json.load(f)
        with _HOLD_LOCK:
            HOLDINGS = _normalize_holdings(cfg2.get("holdings", []))
        STATE["latest"] = None   # 强制下一轮 /api/snapshot 用新持仓重建, 编辑即时可见
        return jsonify({"ok": True, "count": len(HOLDINGS)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ----------------------------- 前端 -----------------------------
DASHBOARD_HTML = open(os.path.join(BASE, "templates", "dashboard.html"), encoding="utf-8").read()  # 前端模板已抽至 templates/dashboard.html



if __name__ == "__main__":
    from scheduler import scheduler_loop  # 延迟导入, 避免与 scheduler 的 import app 形成循环导入
    _ensure_runtime_data()   # 兜底: 缺失的运行时数据文件用模板/基线初始化
    # 启动独立快扫线程(9:25:02-9:30期间, 每PREOPEN_FAST_SEC秒刷新Top30+重算AI)
    threading.Thread(target=_preopen_fast_loop, daemon=True).start()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    threading.Thread(target=_backfill_themes, daemon=True).start()  # 后台补齐存量持仓题材
    print(f"监控平台 v2 启动: http://localhost:{PORT}")
    app.run(host="0.0.0.0", port=PORT, threaded=True)


