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
import json, re, math, time, threading, datetime, os, random, traceback, shutil
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, Response, jsonify, request

import requests

BASE = "/workspace/stock_monitor"
PORT = int(os.environ.get("PORT", 8800))
VERSION = "v3.8.0"
PORTFOLIO_PATH = f"{BASE}/portfolio.json"
_HOLD_LOCK = threading.Lock()   # 持仓配置热重载锁(前端编辑保存后无需重启)

# 尾盘高开潜力: 回测闭环 + 权重自优化(v3.4)
GAPUP_LOG = f"{BASE}/gapup_log.jsonl"          # 每行一条交易日推荐记录(记住5只)
GAPUP_STATS = f"{BASE}/gapup_stats.json"       # 累计回测统计(命中率等)
GAPUP_TUNED = f"{BASE}/gapup_weights_tuned.json"  # 调优后的 gu_* 权重(启动时加载覆盖默认)
GAPUP_MIN_OPT_SAMPLES = 10                     # 至少累计10只已验证样本才允许自动调权(配合正则防过拟合)
GAPUP_OPT_MULTIPLIERS = [0.6, 0.8, 1.25, 1.5, 2.0]  # 坐标上升尝试的权重乘子
GAPUP_OPT_REG = 0.02          # L2 正则强度(向默认权重回拉, 抑制小样本过拟合)
GAPUP_OPT_CAL = 0.3           # 校准惩罚强度(预测均值 vs 实际高开率)
GAPUP_WEIGHT_OVERRIDE = None                    # 调优时临时覆盖 FCONFIG 的 gu_* 权重

with open(f"{BASE}/portfolio.json", "r", encoding="utf-8") as f:
    CFG = json.load(f)
HOLDINGS_RAW = CFG["holdings"]
WATCHLIST = CFG.get("watchlist", [])
CLOSED = CFG.get("closed_positions", [])  # 已清仓(含已实现盈亏, 按用户要求不展示, 仅保留数据)
SET = CFG.get("settings", {})
POLL = SET.get("poll_interval_sec", 5)
ANOM = SET.get("anomaly", {})
PREOPEN_CFG = SET.get("preopen", {})
PREOPEN_FAST_SEC = int(SET.get("preopen_fast_sec", 3))  # 9:25:02-9:30快扫间隔(秒), 独立线程每次重算AI
TAKE_PROFIT_PCT = SET.get("take_profit_pct", 15.0)      # 止盈线(成本上浮)
PRESSURE_PCT = SET.get("pressure_pct", 25.0)            # 压力位(成本上浮)
SCAN = SET.get("scan", {})                               # 涨停扫描条件
# 涨停扫描可调阈值(默认值, 可被 portfolio.json 的 settings.scan 覆盖)
_SCAN_DEFAULTS = {
    "min_float_mv": 37, "min_price": 8,
    "limitup_weight": 0.6, "vr_weight": 1.5, "pct_weight": 0.8, "weibi_weight": 2.0, "sig_scale": 6.0,
    "resonance_strong": 25.0, "resonance_weak": 10.0, "resonance_leader": 8.0,
    "resonance_min_avg": 0.3, "resonance_min_up_ratio": 0.6, "resonance_leader_thresh": 2.0, "resonance_scale": 0.1,
    "fv_optimal_min": 30, "fv_optimal_max": 80, "fv_optimal_bonus": 5.0,
    "fv_mid_penalty": -5.0, "fv_large_thresh": 100, "fv_large_penalty": -30.0,
    "broken_dist_thresh": 2.0, "red_open_pct": 5.0,
    "reeval_from": "09:30", "reeval_to": "09:35", "reeval_interval": 30,
    "yao_min_consec": 2, "yao_consec_bonus": 15, "yao_consec_cap": 45,
    "yao_smallcap_thresh": 30, "yao_smallcap_bonus": 5.0,
}
SCFG = {**_SCAN_DEFAULTS, **SCAN}                        # 生效的扫描配置(默认合并用户覆盖)
# 预测模块可调权重(默认值, 可被 portfolio.json 的 settings.forecast 覆盖)
# v2.8: 三个预测模块统一引入 宽度/尾盘动向/小盘情绪 多因子 + 置信度校准
_FORECAST_DEFAULTS = {
    # 上证指数1小时趋势预测
    "idx_pct_w": 2.2, "idx_late_w": 1.8, "idx_pos_w": 2.0, "idx_vr_w": 1.0,
    "idx_wb_w": 1.5, "idx_breadth_w": 3.0, "idx_retail_w": 0.8, "idx_sig": 6.0,
    # 尾盘预测(大盘明日方向)
    "cl_sh_w": 1.8, "cl_cyb_w": 1.0, "cl_sec_w": 1.2, "cl_breadth_w": 6.0,
    "cl_retail_w": 1.0, "cl_late_w": 2.5, "cl_sig": 6.0,
    # 尾盘高开潜力
    "gu_pos_w": 3.5, "gu_parab_w": 1.0, "gu_wb_w": 2.2, "gu_vr_w": 0.6,
    "gu_to_w": 0.12, "gu_latepull_w": 1.5, "gu_breadth_w": 3.0,
    "gu_retail_w": 0.6, "gu_idxlate_w": 1.2, "gu_sig": 3.0, "gu_parab_peak": 4.0,
    # AI 融合权重(与上一次保持一致, 可被 settings.forecast.ai_fuse_w 覆盖)
    "ai_fuse_w": float(os.environ.get("AI_FUSE_W", "0.45")),
}
FORECAST_CFG = SET.get("forecast", {})
FCONFIG = {**_FORECAST_DEFAULTS, **FORECAST_CFG}            # 生效的预测配置
# v3.4: 若存在调优后的 gu_* 权重, 启动时加载覆盖默认(由回测闭环自动生成)
if os.path.exists(GAPUP_TUNED):
    try:
        _tw = json.load(open(GAPUP_TUNED, encoding="utf-8"))
        for _k, _v in _tw.items():
            FCONFIG[_k] = _v
        print(f"[init] 已加载调优权重: {list(_tw.keys())}", flush=True)
    except Exception:
        pass
# 可选 AI 精修: 配置 OpenAI 兼容接口后, 平台算法可调用更强模型提升预测准确度
AI_CFG = SET.get("ai_assist", {})
AI_ENABLED = str(os.environ.get("AI_ASSIST", AI_CFG.get("enabled", "0"))).lower() in ("1", "true", "on")
AI_BASE = os.environ.get("OPENAI_API_BASE", AI_CFG.get("base_url", "https://api.openai.com/v1")).rstrip("/")
AI_KEY = os.environ.get("OPENAI_API_KEY", AI_CFG.get("api_key", ""))
AI_MODEL = os.environ.get("OPENAI_MODEL", AI_CFG.get("model", "gpt-4o-mini"))

HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"}
# 腾讯字段索引(0-based, 实测)
F = dict(name=1, code=2, price=3, prevclose=4, open=5, vol=6,
         time=30, chg=31, pct=32, high=33, low=34,
         amount_wan=37, turnover=38, amplitude=43,
         float_mv=44, limit_up=47, limit_down=48, weibi=49, volratio=50)

# 大盘指数
INDICES = [
    ("sh000001", "上证指数"), ("sz399001", "深证成指"), ("sz399006", "创业板指"),
    ("sh000300", "沪深300"), ("sh000905", "中证500"), ("sh000852", "中证1000"),
    ("sz399303", "国证2000"),
]
# 散户平均盈亏代表指数(小盘股, 最贴近散户持仓结构)
RETAIL_INDEX = ("sz399303", "国证2000")
# 行业板块(中证行业指数)
SECTOR_BOARDS = [
    ("sz399928", "能源"), ("sz399929", "原材料"), ("sz399930", "工业"),
    ("sz399931", "可选消费"), ("sz399932", "主要消费"), ("sz399933", "医药卫生"),
    ("sz399934", "金融地产"), ("sz399935", "信息技术"), ("sz399936", "电信业务"),
    ("sz399937", "公用事业"), ("sz399975", "证券公司"), ("sz399997", "中证白酒"),
]

# 股票→所属板块(中证行业指数)归属表: 腾讯个股行情不含"所属行业"字段, 且 portfolio.json
# 里用户持仓多未填 sector, 故在此做轻量归属, 供持仓卡片角落展示"所属板块涨跌"。
# key=股票代码(不带市场前缀), value=SECTOR_BOARDS 里的板块名。新增持仓时在此补充即可。
STOCK_SECTOR = {
    "600522": "信息技术",   # 中天科技(通信设备/光纤)
    "603123": "可选消费",   # 翠微股份(商业零售)
    "002657": "信息技术",   # 中科金财(金融科技/软件)
    "000049": "工业",       # 德赛电池(电池/电子制造)
    "002401": "信息技术",   # 中远海科(智能交通/软件)
    "600475": "公用事业",   # 华光环能(环保能源/热电)
    "600613": "医药卫生",   # 神奇制药(制药)
}
_SECTOR_NAME_TO_CODE = {nm: c for c, nm in SECTOR_BOARDS}

STATE = {
    "latest": None, "last_update": None, "trading": False,
    "preopen": None, "preopen_date": None, "close": None, "close_date": None,
    "idx_forecast": None, "idx_forecast_time": None,
    "sector_drivers": None, "sector_drivers_time": None,
    "gapup": None, "gapup_date": None,
    "alerts": [], "is_weekday": True,
}
LOCK = threading.Lock()


def beijing_now():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=8)


def _parse_hhmm(s):
    """'09:30' -> datetime.time(9,30)"""
    h, m = str(s).split(":")
    return datetime.time(int(h), int(m))


def is_weekday(dt):
    return dt.weekday() < 5


def trading_phase(dt):
    """返回 (在交易窗口内, 阶段名)"""
    if not is_weekday(dt):
        return False, "周末/节假日(不运行)"
    t = dt.time()
    if datetime.time(9, 15) <= t < datetime.time(9, 25):
        return True, "集合竞价"
    if datetime.time(9, 25) <= t < datetime.time(9, 30):
        return True, "竞价结束·待开盘"
    if datetime.time(9, 30) <= t < datetime.time(11, 30):
        return True, "连续竞价"
    if datetime.time(13, 0) <= t < datetime.time(15, 0):
        return True, "连续竞价"
    if datetime.time(11, 30) <= t < datetime.time(13, 0):
        return False, "午间休市"
    if datetime.time(15, 0) <= t < datetime.time(16, 0):
        return False, "收盘后"
    return False, "非交易时段"


# ----------------------------- 行情抓取 -----------------------------
_TENCENT_SESSION = None
_FETCH_POOL = None
def _tencent_session():
    global _TENCENT_SESSION
    if _TENCENT_SESSION is None:
        s = requests.Session()
        s.headers.update(HEADERS)
        _TENCENT_SESSION = s
    return _TENCENT_SESSION

def _fetch_pool(max_workers=8):
    """v3.0: 全局共享线程池, 避免每次 fetch 新建导致线程堆积。"""
    global _FETCH_POOL
    if _FETCH_POOL is None:
        _FETCH_POOL = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="fetch")
    return _FETCH_POOL

def _fetch_chunk(chunk):
    """拉取一批代码, 返回 {CODE: fields}。失败返回 {}。
    v3.0: 用流式读取+整体超时, 防止腾讯接口响应挂起阻塞请求。"""
    out = {}
    url = "https://qt.gtimg.cn/q=" + ",".join(chunk)
    try:
        s = _tencent_session()
        with s.get(url, timeout=(3, 6), stream=True) as r:
            r.encoding = "gbk"
            txt = r.text
        for line in txt.strip().split(";"):
            line = line.strip()
            if not line or "=" not in line or '"' not in line:
                continue
            key = line.split("=")[0].replace("v_", "").upper()
            val = line.split('"')[1]
            if not val or "none_match" in val:
                continue
            out[key] = val.split("~")
    except Exception:
        pass
    return out

def fetch_tencent(codes, batch=40, max_workers=8):
    """codes: list of 'sh600522' 等, 返回 {CODE: fields}。
    v3.0优化: 加大批量(batch=40) + 全局共享线程池并发 + Session连接复用 + 整体读超时,
    全市场扫描从 ~170s 大幅缩短且不会因响应挂起阻塞。"""
    if not codes:
        return {}
    uniq = list(dict.fromkeys(codes))
    chunks = [uniq[i:i + batch] for i in range(0, len(uniq), batch)]
    out = {}
    if len(chunks) <= 1:
        out.update(_fetch_chunk(chunks[0]) if chunks else {})
    else:
        ex = _fetch_pool(max_workers)
        for res in ex.map(_fetch_chunk, chunks):
            out.update(res)
    return out


def num(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


# 全市场代码表(缓存)
def load_universe():
    """返回 {code: name} 主板池; 无缓存则尝试akshare拉取"""
    path = f"{BASE}/all_a_codes.json"
    uni = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for r in json.load(f):
                c = r["code"]
                if c.startswith(("600", "601", "603", "605", "000", "001", "002")):
                    uni[c] = r["name"].replace(" ", "")
    if not uni:
        try:
            import warnings
            warnings.filterwarnings("ignore")
            import akshare as ak
            df = ak.stock_info_a_code_name()
            for _, row in df.iterrows():
                c = str(row["code"])
                if c.startswith(("600", "601", "603", "605", "000", "001", "002")):
                    uni[c] = str(row["name"]).replace(" ", "")
            with open(path, "w", encoding="utf-8") as f:
                json.dump([{"code": c, "name": n} for c, n in uni.items()], f, ensure_ascii=False)
        except Exception:
            pass
    return uni


UNIVERSE = load_universe()


# ----------------------------- 持仓计算 -----------------------------
def parse_row(p):
    """把腾讯字段数组解析为dict, 容错"""
    def g(i):
        return p[i] if i < len(p) else ""
    return {
        "name": g(F["name"]), "code": g(F["code"]),
        "price": num(g(F["price"])), "prevclose": num(g(F["prevclose"])),
        "open": num(g(F["open"])), "pct": num(g(F["pct"])),
        "high": num(g(F["high"])), "low": num(g(F["low"])),
        "turnover": num(g(F["turnover"])), "amplitude": num(g(F["amplitude"])),
        "limit_up": num(g(F["limit_up"])), "limit_down": num(g(F["limit_down"])),
        "weibi": num(g(F["weibi"])), "volratio": num(g(F["volratio"])),
        "float_mv": num(g(F["float_mv"])),  # 单位: 亿
        "amount_wan": num(g(F["amount_wan"])),
    }


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
    # 当日盈亏: 隔夜持仓基于昨收; 当日新买入持仓基于买入成本
    #   区分原因: 今日才买入的持仓, 昨日收盘时并未持有, 用昨收算当日盈亏无意义,
    #   应以其实际买入成本(=cost)为基准。buy_date 为空或早于今日时按隔夜持仓处理(沿用昨收)。
    _bd = None
    if h.get("buy_date"):
        try:
            _bd = datetime.datetime.strptime(h["buy_date"], "%Y-%m-%d").date()
        except Exception:
            _bd = None
    if _bd is not None and _bd == beijing_now().date():
        prev = cost  # 今日新买入: 以买入成本为基准
    else:
        prev = d["prevclose"] if d["prevclose"] > 0 else cost
    day_pnl = (price - prev) * shares
    day_pnl_pct = (price - prev) / prev * 100 if prev else 0

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

    # 持仓时长: 按买入日期自动计算天数
    buy_date = h.get("buy_date", "")
    holding_days = None
    if buy_date:
        try:
            bd = datetime.datetime.strptime(buy_date, "%Y-%m-%d").date()
            holding_days = (beijing_now().date() - bd).days
        except Exception:
            holding_days = None

    return {
        "name": h.get("name") or d.get("name") or h.get("code", ""), "code": h["code"], "market": h["market"],
        "price": round(price, 2), "prevclose": round(d["prevclose"], 2),
        "open": round(d["open"], 2), "pct": round(d["pct"], 2),
        "high": round(d["high"], 2), "low": round(d["low"], 2),
        "cost": cost, "shares": shares,
        "value": round(value, 2), "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
        "day_pnl": round(day_pnl, 2), "day_pnl_pct": round(day_pnl_pct, 2),
        "day_basis": "成本" if (_bd is not None and _bd == beijing_now().date()) else "昨收",
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
        "buy_date": buy_date, "holding_days": holding_days,
    }


# ----------------------------- 上证指数 1小时预测 -----------------------------
def fetch_minute(code):
    """腾讯分时数据, 返回 [{'t':'HHMM','p':price},...]
    v3.0: 流式读取+整体超时, 防止分时接口挂起阻塞。"""
    try:
        url = f"https://ifzq.gtimg.cn/appstock/app/minute/query?code={code}"
        with requests.get(url, headers=HEADERS, timeout=(3, 6), stream=True) as r:
            d = r.json()
        data = d.get("data", {}).get(code, {}).get("data", {})
        rows = data.get("data", []) if isinstance(data, dict) else []
        out = []
        for row in rows:
            parts = row.split()
            if len(parts) >= 2:
                try:
                    out.append({"t": parts[0], "p": float(parts[1])})
                except Exception:
                    pass
        return out
    except Exception:
        return []


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
    # 三段式判定: 避免在概率接近50时强行判定涨跌
    verdict = "看涨" if prob >= 58 else ("看跌" if prob <= 42 else "震荡")
    chart = fetch_minute("sh000001")
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
            if ai.available:
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
                    base["verdict"] = ("看涨" if base["prob"] >= 58
                                       else "看跌" if base["prob"] <= 42 else "震荡")
                    base["ai_used"] = True
                    base["ai_prob"] = round(ai_p, 1)
                    base["confidence"] = _confidence(base["prob"], base.get("breadth", 0.5),
                                                     ai_prob=ai_p, heuristic_prob=heuristic_prob)
                    base["note"] = (base["note"] +
                                    f" ｜ 启发式{heuristic_prob:.1f}% + AI{ai_p:.1f}% "
                                    f"→ 融合(AI权重{w}){base['prob']:.1f}%")
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
def _market_prefix(code):
    return "sh" if code.startswith("6") else "sz"


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
            "name": str(h.get("name", "")).strip(),
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


def _fetch_kline(code, days=12):
    """返回最近 days 个交易日的 [date,open,close,high,low] 列表(前复权), 失败返回 []。
    用于妖股检测: 数截至昨日的连续涨停天数(连板基因)。"""
    mkt = _market_prefix(code) + code
    end = beijing_now().strftime("%Y-%m-%d")
    start = (beijing_now() - datetime.timedelta(days=days * 2)).strftime("%Y-%m-%d")
    url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
           f"?param={mkt},day,{start},{end},{days},qfq")
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        j = r.json()
        node = (j.get("data") or {}).get(mkt) or {}
        bars = node.get("qfqday") or node.get("day") or []
        out = []
        for b in bars:
            try:
                if b[0] == end:        # 排除当日(盘中不完整)
                    continue
                out.append({"date": b[0], "open": float(b[1]), "close": float(b[2]),
                            "high": float(b[3]), "low": float(b[4])})
            except (ValueError, IndexError, TypeError):
                continue
        return out
    except Exception:
        return []


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
    score = 0.0
    factors = []
    sh = next((i for i in snap["indices"] if i["code"] == "sh000001"), None)
    cyb = next((i for i in snap["indices"] if i["code"] == "sz399006"), None)
    sh_pct = sh["pct"] if sh else 0
    cyb_pct = cyb["pct"] if cyb else 0
    c1 = sh_pct * 1.5 + cyb_pct * 1.0
    score += c1
    factors.append(("大盘", round(c1, 2), f"上证{sh_pct:+.2f}% 创业板{cyb_pct:+.2f}%"))
    sec = next((s for s in snap["sectors"] if s["code"] == h.get("sector_code")), None)
    sec_pct = sec["pct"] if sec else 0
    c2 = sec_pct * 1.2
    score += c2
    factors.append((f"板块({h.get('sector_name','-')})", round(c2, 2), f"{sec_pct:+.2f}%"))
    c3 = 1.5 if h.get("pct", 0) >= 0 else -1.5
    score += c3
    factors.append(("当日阴阳", round(c3, 2), "收阳" if h.get("pct", 0) >= 0 else "收阴"))
    high, low, price = h.get("high", 0), h.get("low", 0), h.get("price", 0)
    rng = high - low
    if rng > 0:
        lower = (price - low) / rng
        upper = (high - price) / rng
        c4 = (lower - upper) * 1.5
        score += c4
        factors.append(("影线", round(c4, 2), f"下影{lower*100:.0f}% 上影{upper*100:.0f}%"))
    turnover = h.get("turnover", 0)
    c5 = 0.5 if turnover >= 10 else 0
    score += c5
    factors.append(("量能", round(c5, 2), f"换手{turnover:.1f}%"))
    pnl_pct = h.get("pnl_pct", 0)
    c6 = -0.8 if pnl_pct > 8 else (0.8 if pnl_pct < -8 else 0)
    score += c6
    factors.append(("持仓位置", round(c6, 2), f"{pnl_pct:+.2f}%"))
    # v2.8新增: 宽度/小盘/大盘尾盘动向 作为横截面因子(对所有持仓施加相同的环境偏移)
    c7 = (ctx.get("breadth", 0.5) - 0.5) * 3.0
    score += c7
    factors.append(("宽度", round(c7, 2), f"上涨板块占比{ctx.get('breadth', 0.5):.2f}"))
    c8 = ctx.get("retail_pct", 0) * 0.6
    score += c8
    factors.append(("小盘", round(c8, 2), f"国证2000{ctx.get('retail_pct', 0):+.2f}%"))
    c9 = ctx.get("late", 0) * 1.2
    score += c9
    factors.append(("尾盘动向", round(c9, 2), f"上证尾盘{c9/1.2:+.2f}%"))
    prob = 1 / (1 + math.exp(-score / 5.0)) * 100
    verdict = "偏多" if prob >= 60 else ("偏空" if prob <= 40 else "震荡")
    return {"name": h["name"], "code": h["code"], "prob": round(prob, 1),
            "verdict": verdict, "factors": factors}


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
    market = {"prob": round(mprob, 1),
              "verdict": ("偏多" if mprob >= 58 else "偏空" if mprob <= 42 else "震荡"),
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
                m["verdict"] = ("偏多" if m["prob"] >= 58
                                else "偏空" if m["prob"] <= 42 else "震荡")
                m["ai_prob"] = round(ai_p, 1)
                m["confidence"] = _confidence(m["prob"], m.get("breadth", 0.5),
                                             ai_prob=ai_p, heuristic_prob=heuristic_prob)
                base["ai_used"] = True
                base["note"] = (base["note"] +
                                f" ｜ 启发式{heuristic_prob:.1f}% + AI{ai_p:.1f}% "
                                f"→ 融合(AI权重{w}){m['prob']:.1f}%")
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
    now = beijing_now()
    wd = is_weekday(now)
    trading, phase = trading_phase(now)
    hcodes = [(h["market"] + h["code"]).lower() for h in HOLDINGS]
    icodes = [c for c, _ in INDICES]
    scodes = [c for c, _ in SECTOR_BOARDS]
    wcodes = [(w["market"] + w["code"]).lower() for w in WATCHLIST]
    q = fetch_tencent(list(dict.fromkeys(hcodes + icodes + scodes + wcodes)))

    holdings = [enrich_holding(h, q) for h in HOLDINGS]
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
    # 为每只持仓附加"所属板块涨跌 + 在全板块中的强弱排名", 供卡片角落芯片展示
    _sec_by_code = {s["code"]: s for s in sectors}
    _sec_rank = {s["code"]: i + 1 for i, s in enumerate(sectors)}  # 1=最强
    for h in holdings:
        if h.get("error") or not h.get("sector_code"):
            continue
        sc = _sec_by_code.get(h["sector_code"])
        if sc:
            h["sector_pct"] = sc["pct"]
            h["sector_rank"] = _sec_rank.get(h["sector_code"])
            h["sector_total"] = len(sectors)
    up = [s for s in sectors if s["pct"] > 0]
    avg = sum(s["pct"] for s in sectors) / len(sectors) if sectors else 0
    bias = ("资金偏流入/板块上升" if avg > 0.3
            else "资金偏流出/板块下降" if avg < -0.3 else "板块分化/震荡")

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
        "indices": indices, "sectors": sectors,
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
    """识别哪些主题材在拉抬/压制大盘指数(每10分钟检测一次)。

    逻辑: 以主要指数(上证)涨跌幅为基准, 计算各行业板块相对大盘的偏离度。
    偏离明显为正 = 拉指数(贡献上行), 偏离明显为负 = 踩指数(拖累下行)。
    """
    idx = next((i for i in snap["indices"] if i["code"] == "sh000001"), None)
    base = idx["pct"] if idx else 0
    sectors = snap.get("sectors", [])
    if not sectors:
        return {"error": "板块数据缺失"}
    TH = 0.8  # 相对大盘的明显异动阈值(百分点)
    rows = []
    for s in sectors:
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
        "note": f"以{idx['name'] if idx else '上证指数'}涨跌幅为基准, 板块相对大盘偏离≥{TH}%视为明显异动; 拉=正向拉动指数, 踩=负向压制指数",
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


def gap_up_score(d, ctx=None, late_pull=0.0):
    """启发式: 下个交易日开盘高开概率(0-100)。v2.8加入 尾盘拉升/宽度/小盘/大盘尾盘动向。
    v3.4: 支持权重覆盖(_GAPUP_WEIGHT_OVERRIDE), 供调优时临时替换 gu_* 权重。"""
    W = {**FCONFIG, **(GAPUP_WEIGHT_OVERRIDE or {})}  # 覆盖权重合并到默认, 保证键齐全
    if d["price"] <= 0 or d["high"] <= d["low"]:
        return 0.0
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
    return round(1 / (1 + math.exp(-score / W["gu_sig"])) * 100, 1)


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
    """追加一条交易日推荐记录到 gapup_log.jsonl, 同日同 source 不重复写。"""
    try:
        if os.path.exists(GAPUP_LOG):
            with open(GAPUP_LOG, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        old = json.loads(line)
                    except Exception:
                        continue
                    if old.get("date") == rec.get("date") and old.get("source") == rec.get("source"):
                        return  # 已记录, 跳过
        with open(GAPUP_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        traceback.print_exc()


def _load_gapup_log():
    recs = []
    if os.path.exists(GAPUP_LOG):
        with open(GAPUP_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    recs.append(json.loads(line))
                except Exception:
                    continue
    return recs


def _find_verify_target(target_date=None):
    """返回待回测记录(dict)或 None。优先 target_date; 否则最近一个「上一交易日及更早」未验证且有 stocks 的记录。
    注: 仅回测 date < 今天 的记录, 保证「今日推荐 → 下一交易日开盘验证」的语义, 避免当天误回测。"""
    recs = _load_gapup_log()
    if target_date:
        for r in recs:
            if r.get("date") == target_date and r.get("stocks"):
                return r
        return None
    today = beijing_now().strftime("%Y-%m-%d")
    cands = [r for r in recs if not r.get("verified") and r.get("stocks")
             and r.get("date", "") < today]
    if not cands:
        return None
    cands.sort(key=lambda r: r.get("date", ""), reverse=True)
    return cands[0]


def _load_stats():
    if os.path.exists(GAPUP_STATS):
        try:
            with open(GAPUP_STATS, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"total": 0, "gap_up": 0, "hit_rate": 0.0, "rank_hits": {},
            "avg_pred": 0.0, "avg_actual": 0.0, "recent": [], "optimizations": []}


def _save_stats(stats):
    with open(GAPUP_STATS, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def _accumulate_stats(rec=None):
    """基于全量已验证记录重算累计统计(幂等, 不增量累加, 杜绝重复计数)。

    v3.7.3 修复: 旧实现用 stats[\"total\"] += 并对 recent 做 _all_recs + [rec] 追加,
    而 _verify_gapup_open 先写日志文件再调本函数, 导致传入 rec 与文件内记录重复 →
    recent 出现两条相同记录(页面「累计」看似翻倍); 且增量累加在重验证时会重复计数。
    现改为: total/gap_up 由全量日志确定性重算, recent 按 (date,source) 去重。"""
    # 保留已有调权历史(由 optimize_gapup_weights 单独写入, 不可被本函数清掉)
    old = _load_stats()
    stats = {"total": 0, "gap_up": 0, "hit_rate": 0.0, "rank_hits": {},
             "avg_pred": 0.0, "avg_actual": 0.0, "recent": [],
             "optimizations": old.get("optimizations", [])}
    recs = _load_gapup_log()
    # 兜底: 若传入 rec 已验证但尚未落盘(理论上 _verify_gapup_open 已先写文件), 合并且不重复
    if rec and rec.get("verified") and rec.get("actual") and rec.get("stocks"):
        if not any(r.get("date") == rec.get("date") and r.get("source") == rec.get("source")
                   for r in recs):
            recs = recs + [rec]
    all_pred, all_act = [], []
    rank_hits, rank_tot = {}, {}
    for r in recs:
        if not (r.get("verified") and r.get("actual")):
            continue
        for i, s in enumerate(r.get("stocks", [])):
            a = next((x for x in r.get("actual", []) if x.get("code") == s.get("code")), None)
            if not a:
                continue
            if a.get("is_gap_up") is None:
                continue  # 开盘数据缺失(抓取失败)不计入命中率分母, 避免拉低准确率
            stats["total"] += 1
            if a.get("is_gap_up"):
                stats["gap_up"] += 1
            all_pred.append(s.get("prob", 0))
            all_act.append(a.get("gap_pct", 0) or 0)
            rank = i + 1
            rank_tot[rank] = rank_tot.get(rank, 0) + 1
            if a.get("is_gap_up"):
                rank_hits[rank] = rank_hits.get(rank, 0) + 1
    stats["hit_rate"] = round(stats["gap_up"] / stats["total"], 4) if stats["total"] else 0.0
    if all_pred:
        stats["avg_pred"] = round(sum(all_pred) / len(all_pred), 2)
        stats["avg_actual"] = round(sum(all_act) / len(all_act), 2)
        stats["rank_hits"] = {str(k): round(rank_hits.get(k, 0) / rank_tot[k], 4)
                              for k in sorted(rank_tot)}
    # 最近验证明细(按 date+source 去重, 避免同记录重复展示)
    seen, recent = set(), []
    for r in recs:
        if not (r.get("verified") and r.get("actual")):
            continue
        key = (r.get("date"), r.get("source"))
        if key in seen:
            continue
        seen.add(key)
        recent.append({
            "date": r.get("date"), "verified_at": r.get("verified_at"),
            "stocks": [{
                "code": s.get("code"), "name": s.get("name"), "prob": s.get("prob"),
                "gap_pct": next((a.get("gap_pct") for a in r.get("actual", []) if a.get("code") == s.get("code")), None),
                "is_gap_up": next((a.get("is_gap_up") for a in r.get("actual", []) if a.get("code") == s.get("code")), None),
            } for s in r.get("stocks", [])],
        })
    recent.sort(key=lambda x: x.get("verified_at", ""), reverse=True)
    stats["recent"] = recent[:10]
    _save_stats(stats)
    return stats


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
                actual.append({"code": s["code"], "name": s["name"], "open": round(op, 2),
                               "prevclose": round(prev, 2), "gap_pct": round(gap, 2),
                               "is_gap_up": op > prev})
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
        if stats.get("total", 0) >= GAPUP_MIN_OPT_SAMPLES:
            optimize_gapup_weights()
        return rec
    except Exception:
        traceback.print_exc()
        return None


def _gapup_auc(scores, ys):
    """Mann-Whitney AUC: 正样本预测分高于负样本的比例(含平局0.5)。衡量排序判别力。"""
    pos = [s for s, y in zip(scores, ys) if y == 1]
    neg = [s for s, y in zip(scores, ys) if y == 0]
    if not pos or not neg:
        return 0.5
    c = 0.0
    tot = 0
    for pp in pos:
        for nn in neg:
            tot += 1
            if pp > nn:
                c += 1.0
            elif pp == nn:
                c += 0.5
    return c / tot


def optimize_gapup_weights():
    """基于已验证样本, 坐标上升调优 gu_* 权重, 提升高开判定判别力(AUC)。
    v3.7: 目标 = AUC − L2正则(向默认回拉, 防小样本过拟合) − 校准惩罚(预测均值≈实际高开率)。
    可调项新增 gu_sig(概率标定)、gu_parab_peak(涨幅甜点); 搜索有界, 避免权重跑飞。"""
    try:
        recs = [r for r in _load_gapup_log()
                if r.get("verified") and r.get("actual") and r.get("stocks")]
        samples = []
        for r in recs:
            amap = {a["code"]: a for a in r.get("actual", [])}
            for s in r.get("stocks", []):
                a = amap.get(s["code"])
                if not a or a.get("is_gap_up") is None or not s.get("features"):
                    continue
                samples.append((s["features"], 1 if a["is_gap_up"] else 0))
        if len(samples) < GAPUP_MIN_OPT_SAMPLES:
            return {"ok": False, "reason": f"样本不足({len(samples)}/{GAPUP_MIN_OPT_SAMPLES}), 暂不调权",
                    "samples": len(samples)}
        ys = [y for _, y in samples]
        if sum(ys) == 0 or sum(ys) == len(ys):
            return {"ok": False, "reason": "样本标签单一(全高开/全未高开), 无法区分, 暂不调权",
                    "samples": len(samples)}
        tune_keys = ["gu_pos_w", "gu_parab_w", "gu_wb_w", "gu_vr_w", "gu_to_w",
                     "gu_latepull_w", "gu_breadth_w", "gu_retail_w", "gu_idxlate_w",
                     "gu_parab_peak", "gu_sig"]
        base = dict(FCONFIG)
        # 各参数搜索边界(防止过拟合跑飞)
        lo = {k: 0.3 * base[k] for k in tune_keys}
        hi = {k: 2.5 * base[k] for k in tune_keys}
        lo["gu_sig"], hi["gu_sig"] = 1.5, 10.0
        lo["gu_parab_peak"], hi["gu_parab_peak"] = 1.0, 8.0
        gap_rate = sum(ys) / len(ys)

        def _obj(weights):
            scores = []
            for feat, _ in samples:
                d = {"price": feat.get("range_pos", 0.5), "high": 1.0, "low": 0.0,
                     "pct": feat.get("pct", 0), "weibi": feat.get("weibi", 0),
                     "volratio": feat.get("volratio", 1), "turnover": feat.get("turnover", 0),
                     "amplitude": 0}
                ctx = {"breadth": feat.get("breadth", 0.5), "retail_pct": feat.get("retail", 0),
                       "sector_avg": 0, "late": feat.get("idx_late", 0)}
                with _WeightOverride(weights):
                    scores.append(gap_up_score(d, ctx, late_pull=feat.get("late_pull", 0)))
            auc = _gapup_auc(scores, ys)
            reg = sum(((weights[k] - base[k]) / base[k]) ** 2 for k in tune_keys)
            cal = (sum(scores) / len(scores) / 100.0 - gap_rate) ** 2  # 预测均值(0-1)对齐实际高开率
            return auc - GAPUP_OPT_REG * reg - GAPUP_OPT_CAL * cal

        best = dict(base)
        for _ in range(4):
            improved = False
            for k in tune_keys:
                cur = best[k]
                best_obj = _obj(best)
                for m in GAPUP_OPT_MULTIPLIERS:
                    cand = dict(best)
                    cand[k] = max(lo[k], min(hi[k], cur * m))
                    obj = _obj(cand)
                    if obj > best_obj + 1e-9:
                        best_obj = obj
                        best[k] = cand[k]
                        improved = True
            if not improved:
                break
        # 计算 AUC 前后(用于前端展示判别力提升)
        def _auc_only(weights):
            scores = []
            for feat, _ in samples:
                d = {"price": feat.get("range_pos", 0.5), "high": 1.0, "low": 0.0,
                     "pct": feat.get("pct", 0), "weibi": feat.get("weibi", 0),
                     "volratio": feat.get("volratio", 1), "turnover": feat.get("turnover", 0),
                     "amplitude": 0}
                ctx = {"breadth": feat.get("breadth", 0.5), "retail_pct": feat.get("retail", 0),
                       "sector_avg": 0, "late": feat.get("idx_late", 0)}
                with _WeightOverride(weights):
                    scores.append(gap_up_score(d, ctx, late_pull=feat.get("late_pull", 0)))
            return _gapup_auc(scores, ys)
        auc_before = round(_auc_only(base), 4)
        auc_after = round(_auc_only(best), 4)
        result = {"ok": True, "samples": len(samples), "gap_rate": round(gap_rate, 3),
                  "before": {k: round(base[k], 4) for k in tune_keys},
                  "after": {k: round(best[k], 4) for k in tune_keys},
                  "auc_before": auc_before, "auc_after": auc_after,
                  "objective_before": round(_obj(base), 4),
                  "objective_after": round(_obj(best), 4),
                  "at": beijing_now().strftime("%Y-%m-%d %H:%M:%S")}
        with open(GAPUP_TUNED, "w", encoding="utf-8") as f:
            json.dump({k: best[k] for k in tune_keys}, f, ensure_ascii=False, indent=2)
        stats = _load_stats()
        stats.setdefault("optimizations", []).append(result)
        stats["optimizations"] = stats["optimizations"][-10:]
        _save_stats(stats)
        print(f"[gapup-opt] 调权完成: AUC {auc_before}->{auc_after} | 目标 {result['objective_before']}->{result['objective_after']}", flush=True)
        return result
    except Exception:
        traceback.print_exc()
        return {"ok": False, "reason": "调权异常"}


# ----------------------------- 调度器 -----------------------------
def detect_alerts(snap):
    out = []
    for h in snap["holdings"]:
        if h.get("error"):
            continue
        for a in h.get("anomalies", []):
            if a["level"] in ("danger", "warn"):
                out.append({"time": snap["beijing"], "name": h["name"],
                            "code": h["code"], "level": a["level"], "text": a["text"]})
    return out


# 休眠窗口策略(用户授权): 每日 16:00–次日 09:00 允许沙箱平台自动休眠以省资源。
# 唤醒由平台在收到用户新指令时自动完成(无需额外操作)。本调度器在非交易时段不主动
# 保持连接/不做重活(各预测模块仅在交易时段触发), 不会阻止平台休眠。
# 若用户在休眠期发来指令, 平台唤醒沙箱后, 看门狗(watchdog.sh, 由发布 AUTOSTART 拉起)
# 会自动拉起本服务, 公网链接恢复可用。
def scheduler_loop():
    while True:
        try:
            now = beijing_now()
            if not is_weekday(now):
                # 周末: 保留最近数据但不刷新
                time.sleep(600)
                continue
            wd, phase = True, trading_phase(now)[1]
            trading = trading_phase(now)[0]
            snap = build_snapshot()
            with LOCK:
                STATE["trading"] = trading
                STATE["is_weekday"] = True
                for a in detect_alerts(snap):
                    if not any(x["name"] == a["name"] and x["text"] == a["text"]
                               for x in STATE["alerts"][-20:]):
                        STATE["alerts"].append(a)
                STATE["alerts"] = STATE["alerts"][-60:]
                STATE["latest"] = snap
                STATE["last_update"] = now

                # 指数1小时预测: 每2分钟(异步 worker + AI 融合, 不阻塞调度循环)
                if now.time() >= datetime.time(9, 15):
                    last = STATE["idx_forecast_time"]
                    if not last or (now - last).total_seconds() >= 120:
                        STATE["idx_forecast_time"] = now
                        _start_idx_build()

                # 主题材拉/踩指数: 每10分钟检测
                if now.time() >= datetime.time(9, 15):
                    last = STATE["sector_drivers_time"]
                    if not last or (now - last).total_seconds() >= 600:
                        try:
                            STATE["sector_drivers"] = detect_sector_drivers(snap)
                            STATE["sector_drivers_time"] = now
                        except Exception:
                            pass

                today = now.strftime("%Y-%m-%d")
                # 9:25:02触发候选池首扫(后台构建); 之后由独立快扫线程_preopen_fast_loop
                # 每PREOPEN_FAST_SEC秒刷新Top30盘口+重新计算AI权重概率(不拖主循环)
                if now.time() >= datetime.time(9, 25, 2) and now.time() < datetime.time(9, 30):
                    if STATE["preopen_date"] != today:
                        STATE["preopen_date"] = today
                        _start_pool_build()
                # 9:30-9:35 开盘后动态炸板校验: 每 reeval_interval 秒重判炸板/红开/继位(可重复, 去重告警)
                rf = _parse_hhmm(SCFG["reeval_from"]); rt = _parse_hhmm(SCFG["reeval_to"])
                if rf <= now.time() < rt:
                    last = STATE.get("preopen_reeval_last")
                    if last is None or (now - last).total_seconds() >= SCFG["reeval_interval"]:
                        STATE["preopen_reeval_last"] = now
                        try:
                            _post_open_filter()
                        except Exception:
                            traceback.print_exc()
                # 14:50 尾盘预测(异步 worker + AI 融合, 不阻塞调度循环)
                if now.time() >= datetime.time(14, 50) and STATE["close_date"] != today:
                    STATE["close_date"] = today
                    _start_close_build()

                # 尾盘高开潜力: 14:52 起自动扫描主板(每次交易日一次, 收盘前8分钟)
                if now.time() >= datetime.time(14, 52):
                    if STATE["gapup_date"] != today:
                        STATE["gapup_date"] = today
                        _start_gapup_build()

                # v3.4: 开盘后回测上一交易日推荐是否高开(每天09:30后跑一次, 后台线程不阻塞)
                if now.time() >= datetime.time(9, 30) and STATE.get("gapup_verify_date") != today:
                    STATE["gapup_verify_date"] = today
                    threading.Thread(target=_verify_gapup_open, daemon=True).start()

            if trading:
                time.sleep(POLL)
            else:
                time.sleep(30)
        except Exception:
            traceback.print_exc()
            time.sleep(15)


# ----------------------------- Flask -----------------------------
# 启动独立快扫线程(9:25:02-9:30期间, 每PREOPEN_FAST_SEC秒刷新Top30+重算AI)
threading.Thread(target=_preopen_fast_loop, daemon=True).start()

app = Flask(__name__)


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
        data["preopen"] = STATE["preopen"]
        data["idx_forecast"] = STATE["idx_forecast"]
        data["close"] = STATE["close"]
        data["sector_drivers"] = STATE["sector_drivers"]
        data["gapup"] = STATE["gapup"]
    data["server_time"] = beijing_now().strftime("%Y-%m-%d %H:%M:%S")
    return jsonify(data)


@app.route("/api/preopen")
def api_preopen():
    with LOCK:
        if STATE["preopen"]:
            return jsonify(STATE["preopen"])
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
    return jsonify(cur if cur else {"building": True,
                                    "note": "尾盘预测计算中(含AI模型融合), 请稍候自动更新…"})


@app.route("/api/idx")
def api_idx():
    STATE["idx_forecast_time"] = beijing_now().strftime("%Y-%m-%d")
    _start_idx_build()
    with LOCK:
        cur = STATE["idx_forecast"]
    return jsonify(cur if cur else {"building": True,
                                    "note": "上证预测计算中(含AI模型融合), 请稍候自动更新…"})


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
            # 仅覆盖可编辑的三个字段, 其余原有字段(含 buy_date)原样保留
            item = dict(existing.get(code, {}))
            item["code"] = code
            item["cost"] = round(cost, 3)
            item["shares"] = round(shares, 2)
            new_holdings.append(item)
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
        return jsonify({"ok": True, "count": len(HOLDINGS)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


# ----------------------------- 前端 -----------------------------
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>实时持仓监控平台 __VERSION__</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--line:#30363d;--up:#ef5350;--down:#26a69a;
--txt:#e6edf3;--mut:#8b949e;--gold:#e3b341;--blue:#58a6ff;--row-line:#21262d;--feed-bg:#11161d;--chart-bg:#0b0f14}
/* 浅色主题 */
[data-theme="light"]{--bg:#f6f8fa;--card:#ffffff;--line:#d0d7de;--up:#d1242f;--down:#1a7f37;
--txt:#1f2328;--mut:#656d76;--gold:#9a6700;--blue:#0969da;--row-line:#eaeef2;--feed-bg:#f0f3f6;--chart-bg:#eef1f4}
*{box-sizing:border-box}
body{margin:0;color:var(--txt);font-size:14px;line-height:1.5;-webkit-font-smoothing:antialiased;
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;
  background:radial-gradient(1100px 520px at 85% -8%,rgba(88,166,255,.07),transparent 60%),var(--bg)}
header{padding:13px 22px;border-bottom:1px solid var(--line);display:flex;flex-wrap:wrap;gap:14px;align-items:center;
  position:sticky;top:0;z-index:20;background:color-mix(in srgb,var(--bg) 82%,transparent);backdrop-filter:blur(10px)}
header h1{font-size:18px;margin:0;font-weight:700;letter-spacing:.3px}
.tag{font-size:12px;color:var(--mut);padding:3px 10px;border:1px solid var(--line);border-radius:20px}
.tag.live{color:var(--up);border-color:var(--up);background:rgba(239,83,80,.08)}
.wrap{max-width:1280px;margin:0 auto;padding:20px 18px 64px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:16px}
.card{position:relative;overflow:hidden;background:var(--card);border:1px solid var(--line);border-radius:14px;
  padding:16px;transition:transform .15s ease,box-shadow .15s ease,border-color .15s ease;box-shadow:0 1px 2px rgba(0,0,0,.18)}
.card:hover{transform:translateY(-2px);box-shadow:0 8px 22px rgba(0,0,0,.30);border-color:#3d4756}
.card::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--line)}
.card.up::before{background:var(--up)}.card.down::before{background:var(--down)}
.card h3{margin:0 0 2px;font-size:15px;font-weight:600;display:flex;justify-content:space-between;align-items:flex-start;gap:8px}
.code{color:var(--mut);font-size:12px;font-weight:normal;letter-spacing:.3px}
.px{font-size:30px;font-weight:700;line-height:1.1;font-variant-numeric:tabular-nums}
.pct{font-size:15px;font-weight:600;margin-left:8px;font-variant-numeric:tabular-nums}
.up{color:var(--up)}.down{color:var(--down)}.flat{color:var(--mut)}
.sector-chip{display:inline-flex;align-items:center;gap:5px;flex-shrink:0;font-size:11px;line-height:1;
  padding:5px 9px;border-radius:999px;border:1px solid var(--line);background:var(--row-line);color:var(--mut);white-space:nowrap}
.sector-chip .sp{font-weight:700;font-variant-numeric:tabular-nums}
.sector-chip .rk{opacity:.65;font-size:10px}
.grp{margin:12px 0 2px;font-size:11px;letter-spacing:1.5px;color:var(--mut);text-transform:uppercase;border-top:1px solid var(--row-line);padding-top:9px}
.row{display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--row-line);font-size:13px}
.row .lbl{color:var(--mut)}
.row:last-of-type{border-bottom:none}
.row span:last-child{font-variant-numeric:tabular-nums;font-weight:500}
.kpi{display:flex;gap:12px;flex-wrap:wrap;margin:4px 0 18px}
.kpi div{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 16px;min-width:118px;flex:1;box-shadow:0 1px 2px rgba(0,0,0,.15)}
.kpi div{color:var(--mut);font-size:12px}
.kpi b{display:block;font-size:22px;font-weight:700;color:var(--txt);font-variant-numeric:tabular-nums;margin-top:2px}
.section{margin:30px 0 12px;font-size:16px;font-weight:600;letter-spacing:.3px;border-left:4px solid var(--blue);padding-left:12px}
.badge{display:inline-block;font-size:12px;padding:2px 8px;border-radius:6px;margin:2px 4px 2px 0}
.b-yao{background:rgba(255,107,107,.16);color:#ff6b6b;border:1px solid #ff6b6b}
.b-danger{background:rgba(239,83,80,.18);color:var(--up)}
.b-warn{background:rgba(227,179,65,.18);color:var(--gold)}
.b-info{background:rgba(88,166,255,.18);color:var(--blue)}
.b-muted{background:var(--row-line);color:var(--mut)}
.b-up{background:rgba(239,83,80,.18);color:var(--up)}
.b-down{background:rgba(38,166,154,.18);color:var(--down)}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:7px 8px;text-align:right;border-bottom:1px solid var(--row-line)}
th{color:var(--mut);font-weight:500}
td:first-child,th:first-child{text-align:left}
.bar{height:8px;border-radius:4px;background:var(--down);display:inline-block;vertical-align:middle}
.bar.up{background:var(--up)}
.feed{max-height:320px;overflow:auto}
.feed .it{padding:7px 10px;border-left:3px solid var(--line);margin:6px 0;background:var(--feed-bg);border-radius:0 6px 6px 0;font-size:13px}
.feed .it.danger{border-color:var(--up)}
.feed .it.warn{border-color:var(--gold)}
.feed .it.info{border-color:var(--blue)}
.feed .t{color:var(--mut);font-size:11px}
.note{color:var(--mut);font-size:12px;margin-top:6px;line-height:1.5}
.prob{font-size:24px;font-weight:700}
.gauge{display:inline-block;font-size:22px;font-weight:700}
.eye{cursor:pointer;background:var(--card);color:var(--txt);border:1px solid var(--line);border-radius:8px;font-size:16px;padding:4px 10px;line-height:1}
.eye:hover{border-color:var(--blue)}
.masked{color:var(--mut);letter-spacing:2px}
.minichart{width:100%;height:130px;margin-top:10px;border-radius:8px;background:var(--chart-bg)}
</style></head>
<body>
<header>
  <h1>📊 实时持仓监控平台</h1>
  <span id="clock" class="tag">--</span>
  <span id="status" class="tag off">--</span>
  <span class="tag" id="phase">--</span>
  <button id="eyeBtn" class="eye" title="隐藏/显示金额">👁</button>
  <button id="themeBtn" class="eye" title="切换白天/夜间主题">🌙</button>
</header>
<div class="wrap">

  <!-- 0. 隐私提示 -->
  <div id="privacyTip" class="note" style="margin:6px 0 0;display:none">🔒 已隐藏金额（持仓市值/盈亏/成本）。点击右上角 👁 恢复显示。</div>

  <!-- 1. 异动提醒置顶 -->
  <div class="section" style="margin-top:6px">🚨 异动提醒（实时）</div>
  <div class="feed" id="feed" style="max-height:180px"><div class="note">暂无预警</div></div>

  <!-- 2. 上证指数1小时趋势预测 -->
  <div class="section">📈 上证指数 1 小时后趋势预测 <span class="code">每2分钟更新 · 含AI权重</span></div>
  <div class="card" id="idxforecast"><div class="note">加载中…</div>
    <button onclick="manual('idx')" style="margin-top:10px;background:var(--blue);color:#06121f;border:none;padding:7px 14px;border-radius:6px;cursor:pointer">手动预测</button>
  </div>

  <!-- 3. 9:25:02 开盘前涨停趋势(自动扫描5只未持有) -->
  <div class="section">🔥 开盘前涨停趋势 <span class="code">9:25:02首扫, 持续到9:29 每分钟扫描主板, 自动选5只未持有</span></div>
  <div class="card" id="preopen"><div class="note">尚未生成（交易日 09:25:02 起每分钟自动更新；可点按钮手动触发）</div>
    <button onclick="manual('preopen')" style="margin-top:10px;background:var(--blue);color:#06121f;border:none;padding:7px 14px;border-radius:6px;cursor:pointer">手动扫描</button>
  </div>

  <!-- 4. 我的持仓 -->
  <div class="section" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
    <span>💼 我的持仓（撤离/补仓/止盈/压力位）</span>
    <button onclick="enterEdit()" style="margin-left:auto;background:var(--blue);color:#06121f;border:none;padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600">✏️ 管理持仓</button>
  </div>
  <div class="kpi">
    <div>总市值<b id="tv">--</b></div>
    <div>浮动盈亏<b id="tp">--</b></div>
    <div>收益率<b id="tpp">--</b></div>
    <div>当日总盈亏<b id="td">--</b></div>
    <div>板块强弱<b id="sb">--</b></div>
  </div>
  <div class="grid" id="holdings"></div>
  <div class="card" id="pfEditor" style="display:none">
    <h3>✏️ 编辑持仓 <span class="note" style="font-weight:400">仅可改 股票代码 / 成本 / 股数，其余字段由系统按行情自动计算（无需持有时间 / 购买时间）</span></h3>
    <div id="pfRows"></div>
    <div style="margin-top:12px;display:flex;gap:10px;flex-wrap:wrap">
      <button onclick="pfAdd()" style="background:var(--card);color:var(--txt);border:1px solid var(--line);padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px">➕ 新增一行</button>
      <button onclick="pfSave()" style="background:var(--up);color:#fff;border:none;padding:6px 16px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600">💾 保存</button>
      <button onclick="pfCancel()" style="background:var(--card);color:var(--mut);border:1px solid var(--line);padding:6px 14px;border-radius:6px;cursor:pointer;font-size:13px">取消</button>
    </div>
    <div id="pfMsg" class="note" style="margin-top:8px"></div>
  </div>

  <!-- 5. 大盘 & 板块 -->
  <div class="section">🌐 大盘 & 板块资金动向</div>
  <div class="grid">
    <div class="card"><h3>三大指数</h3><table id="idx"></table></div>
    <div class="card"><h3>行业板块涨跌 <span class="code" id="sb2"></span></h3>
      <div id="sectors" style="max-height:300px;overflow:auto"></div>
      <div class="note" id="bias"></div>
    </div>
    <div class="card" id="retailCard" style="border-color:var(--gold)"><h3>散户今日平均盈亏</h3><div id="retail"><div class="note">加载中…</div></div></div>
    <div class="card" id="driversCard"><h3>主题材拉/踩指数 <span class="code">每10分钟检测</span></h3><div id="drivers"><div class="note">加载中…</div></div>
      <button onclick="manual('drivers')" style="margin-top:8px;background:var(--blue);color:#06121f;border:none;padding:5px 12px;border-radius:6px;cursor:pointer;font-size:12px">立即检测</button>
    </div>
  </div>

  <!-- 6. 尾盘预测 -->
  <div class="section" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
    <span>🔮 尾盘预测：下个交易日涨跌概率</span>
    <span class="code">交易日 14:50 自动预测 · 可随时手动刷新</span>
    <button id="closeRefreshBtn" onclick="manual('close')" style="background:var(--blue);color:#06121f;border:none;padding:6px 16px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;margin-left:auto">🎯 立即预测</button>
    <span id="closeRefreshTip" class="code">点击立即重新计算一次尾盘预测，一般需 10~30 秒</span>
  </div>
  <div class="card" id="close"><div class="note">尚未生成（交易日 14:50 自动预测；可点上方按钮手动触发）</div></div>

  <!-- 7. 尾盘高开潜力 -->
  <div class="section" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
    <span>🚀 尾盘高开潜力（下个交易日开盘）</span>
    <span class="code">尾盘前8分钟自动扫描主板(排除涨停), 选5支高开概率最高</span>
    <button id="gapupBtn" onclick="manual('gapup')" style="background:var(--blue);color:#06121f;border:none;padding:6px 16px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;margin-left:auto">🎯 立即检测</button>
    <span id="gapupTip" class="code">点击立即全市场扫描主板，约需 1~3 分钟</span>
  </div>
  <div class="card" id="gapup"><div class="note">尚未生成（交易日 14:52 起自动扫描；也可随时点击上方按钮立即检测）</div></div>
  <!-- 7.1 高开回测(v3.4) -->
  <div class="section" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
    <span>📊 高开回测（推荐→开盘实测）</span>
    <span class="code">交易日记录推荐, 次日开盘自动验证是否高开, 累积命中率并自优化权重</span>
    <button onclick="gapOpt()" style="background:var(--blue);color:#06121f;border:none;padding:6px 16px;border-radius:6px;cursor:pointer;font-size:13px;font-weight:600;margin-left:auto">⚙️ 手动调优权重</button>
    <span id="gapOptTip" class="code"></span>
  </div>
  <div class="card" id="gapverify"><div class="note">加载中…</div></div>
  <div class="note">数据来源：腾讯财经实时行情（真实数据）。本平台为个人监控与异动辅助工具，所有预测/概率均基于规则与统计模型，<b>不构成投资建议</b>。仅在交易日(周一~周五)运行。</div>
  <div class="note" style="text-align:center;margin-top:22px">实时持仓监控平台 <b id="ver">__VERSION__</b> ｜ 数据来源：腾讯财经实时行情（真实数据）</div>
</div>
<script>
const fmt=(n,d=2)=>n==null||isNaN(n)?'--':Number(n).toLocaleString('zh-CN',{minimumFractionDigits:d,maximumFractionDigits:d});
const cls=p=>p>0?'up':(p<0?'down':'flat');
const sign=p=>(p>0?'+':'')+fmt(p);
const probcls=p=>p>=60?'up':(p<=40?'down':'flat');
let showMoney=true;
function toggleMoney(){showMoney=!showMoney;
  document.getElementById('eyeBtn').textContent=showMoney?'👁':'🙈';
  document.getElementById('privacyTip').style.display=showMoney?'none':'block';
  load();}
document.getElementById('eyeBtn').addEventListener('click',toggleMoney);
// 主题切换：dark / light，默认跟随系统；选择持久化到 localStorage
const THEME_KEY='wb_theme';
function applyTheme(t){
  const real=(t==='light')?'light':'dark';
  document.documentElement.setAttribute('data-theme',real);
  document.getElementById('themeBtn').textContent=(real==='light')?'☀️':'🌙';
  document.getElementById('themeBtn').title=(real==='light')?'切换到夜间主题':'切换到白天主题';
}
(function(){
  let saved=null;
  try{saved=localStorage.getItem(THEME_KEY);}catch(e){}
  if(saved==='light'||saved==='dark'){applyTheme(saved);}
  else{
    const mq=window.matchMedia&&window.matchMedia('(prefers-color-scheme: light)');
    applyTheme(mq&&mq.matches?'light':'dark');
  }
})();
function toggleTheme(){
  const cur=document.documentElement.getAttribute('data-theme')==='light'?'light':'dark';
  const next=(cur==='light')?'dark':'light';
  applyTheme(next);
  try{localStorage.setItem(THEME_KEY,next);}catch(e){}
}
document.getElementById('themeBtn').addEventListener('click',toggleTheme);
function drawMinute(chart,pct){
  const c=document.getElementById('idxChart');
  if(!c||!chart||chart.length<2){if(c){c.parentNode.removeChild(c);}return;}
  const ctx=c.getContext('2d');
  const W=c.width=Math.max(c.clientWidth||600,300),H=c.height=130;
  const pts=chart.map(x=>x.p);
  const mn=Math.min(...pts),mx=Math.max(...pts),rg=(mx-mn)||1;
  ctx.clearRect(0,0,W,H);
  // 网格
  ctx.strokeStyle='#21262d';ctx.lineWidth=1;
  for(let i=1;i<4;i++){ctx.beginPath();ctx.moveTo(0,H*i/4);ctx.lineTo(W,H*i/4);ctx.stroke();}
  // 中线(昨收参考=首点与末点均值近似)
  ctx.strokeStyle='#30363d';ctx.beginPath();ctx.moveTo(0,H/2);ctx.lineTo(W,H/2);ctx.stroke();
  // 分时线
  ctx.beginPath();
  const step=W/(pts.length-1);
  pts.forEach((p,i)=>{const x=i*step,y=H-((p-mn)/rg)*(H-10)-5;i?ctx.lineTo(x,y):ctx.moveTo(x,y);});
  ctx.strokeStyle=(pct>=0?'#ef5350':'#26a69a');ctx.lineWidth=2;ctx.stroke();
  // 填充
  ctx.lineTo(W,H);ctx.lineTo(0,H);ctx.closePath();
  ctx.fillStyle=(pct>=0?'rgba(239,83,80,.08)':'rgba(38,166,154,.08)');ctx.fill();
  // 标注
  ctx.fillStyle='#8b949e';ctx.font='10px sans-serif';
  ctx.fillText('高 '+fmt(mx,1),6,12);ctx.fillText('低 '+fmt(mn,1),6,H-4);
}
function render(d){
  document.getElementById('clock').textContent=d.beijing+' '+['一','二','三','四','五','六','日'][d.weekday];
  const st=document.getElementById('status');
  if(!d.is_weekday){st.textContent='○ 周末/休市(不运行)';st.className='tag off';}
  else {st.textContent=d.trading?'● 交易中':'○ 非交易时段';st.className='tag '+(d.trading?'live':'off');}
  document.getElementById('phase').textContent=d.phase||'--';
  document.getElementById('tv').textContent=showMoney?fmt(d.total_value)+'元':'***';
  document.getElementById('tv').className=showMoney?'':'masked';
  const tp=document.getElementById('tp');
  tp.textContent=showMoney?sign(d.total_pnl)+'元':'***';tp.className=showMoney?cls(d.total_pnl):'masked';
  const tpp=document.getElementById('tpp');
  tpp.textContent=showMoney?sign(d.total_pnl_pct)+'%':'***';tpp.className=showMoney?cls(d.total_pnl_pct):'masked';
  const td=document.getElementById('td');
  td.textContent=showMoney?sign(d.total_day_pnl)+'元 ('+sign(d.total_day_pnl_pct)+'%)':'***';
  td.className=showMoney?cls(d.total_day_pnl):'masked';
  document.getElementById('sb').textContent=d.sector_bias||'--';

  // 异动提醒(置顶)
  const F=document.getElementById('feed');
  if(d.alerts&&d.alerts.length){F.innerHTML=d.alerts.slice().reverse().slice(0,10).map(a=>`<div class="it ${a.level}"><span class="t">${a.time}</span> <b>${a.name}</b> ${a.text}</div>`).join('');}
  else F.innerHTML='<div class="note">暂无预警</div>';

  // 指数1小时预测 + 分时图
  const ifEl=document.getElementById('idxforecast');
  if(d.idx_forecast){const f=d.idx_forecast;
    const up=f.verdict==='看涨';
    const conf = f.confidence!=null ? f.confidence : 0;
    const confPct = Math.round(conf*100);
    const confBadge = conf>=0.65 ? 'b-up' : (conf>=0.4 ? 'b-info' : 'b-muted');
    const breadth = f.breadth!=null ? f.breadth : 0.5;
    const late = f.late!=null ? f.late : 0;
    const retail = f.retail!=null ? f.retail : 0;
    ifEl.innerHTML=`<div style="display:flex;align-items:baseline;gap:16px;flex-wrap:wrap">
      <span>现价 <b class="px ${cls(f.pct)}">${fmt(f.price)}</b> <span class="pct ${cls(f.pct)}">${sign(f.pct)}%</span></span>
      <span>1小时后<b class="gauge ${up?'up':'down'}"> ${fmt(f.prob,1)}%</b> <b class="${up?'up':'down'}">${f.verdict}</b></span>
      <span class="note">更新 ${f.time} ｜ 日内高${fmt(f.high)} 低${fmt(f.low)} ｜ 置信度<span class="badge ${confBadge}">${confPct}%</span></span></div>
      <div style="display:flex;gap:14px;flex-wrap:wrap;margin:6px 0;font-size:13px;opacity:0.85">
        <span>📊 宽度(上涨板块占比): <b>${(breadth*100).toFixed(0)}%</b></span>
        <span>🚀 尾盘动向: <b class="${cls(late)}">${late>=0?'+':''}${late.toFixed(2)}%</b></span>
        <span>🧩 小盘(国证2000): <b class="${cls(retail)}">${retail>=0?'+':''}${retail.toFixed(2)}%</b></span>
      </div>
      <canvas id="idxChart" class="minichart"></canvas>
      <div class="note">${f.note}${f.ai_used?' ｜ <b style="color:var(--info)">AI模型已参与</b>':''}</div>`;
    drawMinute(f.chart, f.pct);
  } else ifEl.innerHTML='<div class="note">指数预测未生成(9:15后每2分钟更新)</div>';

  // 9:25:02 涨停扫描
  const po=document.getElementById('preopen');
  if(d.preopen){    let r='<div class="note">'+d.preopen.time+' 更新 ｜ '+d.preopen.note+'</div><table><tr><th>代码</th><th>名称</th><th>现价</th><th>涨停价</th><th>距涨停</th><th>涨停概率</th><th>模型</th><th>状态</th></tr>';
    d.preopen.rows.forEach(x=>{
      let yao = x.yao ? `<span class="badge b-yao">🔥妖·${x.yao_days}连板</span> ` : '';
      let badge='<span class="badge b-muted">观察</span>';
      if(x.broken){badge='<span class="badge b-down">⚠️ 炸板</span>';}
      else if(x.red_open){badge='<span class="badge b-up">🔥 可买</span>';}
      else if(x.is_succesor){badge='<span class="badge b-info">继位</span>';}
      r+=`<tr><td>${x.code}</td><td>${x.name}</td><td>${fmt(x.price)}</td><td>${fmt(x.limit_up)}</td><td class="${cls(-x.dist_limit_up)}">${fmt(x.dist_limit_up)}%</td><td class="prob ${probcls(x.prob)}">${fmt(x.prob,1)}%</td><td><span class="badge b-${x.model==='AI'?'info':'muted'}">${x.model||'启发式'}</span></td><td>${yao}${badge}</td></tr>`;
    });
    r+='</table>';po.innerHTML=r;}
  // 持仓 (支持隐私隐藏金额)
  const H=document.getElementById('holdings');H.innerHTML='';
  const mask=v=>showMoney?v:'***';
  const mval=v=>showMoney?fmt(v)+'元':'***';
  // 已清仓统计模块(按用户要求不展示): 数据保留在 snapshot 中, 仅不渲染卡片
  (d.holdings||[]).forEach(h=>{
    if(h.error){H.innerHTML+='<div class="card"><h3>'+h.name+' <span class="code">'+h.code+'</span></h3><div class="note">'+h.error+'</div></div>';return;}
    const pc=cls(h.pct);
    // 角落板块芯片: 所属板块名 + 板块涨跌幅 + 全板块强弱排名(板块整体涨跌, 非成分股家数比例)
    const secChip = h.sector_name ? `<span class="sector-chip" title="所属板块行情涨跌">${h.sector_name} <span class="sp ${cls(h.sector_pct)}">${sign(h.sector_pct)}%</span>${h.sector_rank?'<span class="rk">'+h.sector_rank+'/'+(h.sector_total||'')+'</span>':''}</span>` : '';
    let badges=(h.anomalies||[]).map(a=>'<span class="badge b-'+a.level+'">'+a.text+'</span>').join('');
    H.innerHTML+=`<div class="card ${pc}">
      <h3><span>${h.name} <span class="code">${h.market.toUpperCase()}${h.code}</span></span>${secChip}</h3>
      <div style="margin:2px 0 4px"><span class="px ${pc}">${fmt(h.price)}</span><span class="pct ${pc}">${sign(h.pct)}%</span></div>
      <div class="grp">盈亏概览</div>
      <div class="row"><span class="lbl">持仓市值</span><span class="${showMoney?'':'masked'}">${mval(h.value)}</span></div>
      <div class="row"><span class="lbl">浮动盈亏</span><span class="${showMoney?'':'masked'} ${cls(h.pnl)}">${showMoney?sign(h.pnl)+'元':mask(0)} (${showMoney?sign(h.pnl_pct)+'%':mask(0)})</span></div>
      <div class="row"><span class="lbl">当日盈亏</span><span class="${showMoney?'':'masked'} ${cls(h.day_pnl)}">${showMoney?sign(h.day_pnl)+'元':mask(0)} (${showMoney?sign(h.day_pnl_pct)+'%':mask(0)})<span class="note" style="margin-left:6px">·基${h.day_basis}</span></span></div>
      <div class="row"><span class="lbl">成本 / 股数</span><span class="${showMoney?'':'masked'}">${showMoney?fmt(h.cost,3):'***'} / ${h.shares}股</span></div>
      <div class="row"><span class="lbl">📅 持仓时长</span><span>${h.holding_days!=null?h.holding_days+' 天':'--'}${h.buy_date?' · '+h.buy_date:''}</span></div>
      <div class="grp">风控线</div>
      <div class="row"><span class="lbl">🛑 止损线</span><span class="down">≤ ${fmt(h.stop_price)}</span></div>
      <div class="row"><span class="lbl">🟢 补仓区</span><span class="up">≤ ${fmt(h.add1_price)} / ${fmt(h.add2_price)}</span></div>
      <div class="row"><span class="lbl">✅ 止盈线</span><span class="up">≥ ${fmt(h.take_price)}</span></div>
      <div class="row"><span class="lbl">⛰ 压力位</span><span class="up">≈ ${fmt(h.pressure)}</span></div>
      <div class="grp">盘面</div>
      <div class="row"><span class="lbl">涨停 / 跌停</span><span>${fmt(h.limit_up)} / ${fmt(h.limit_down)}</span></div>
      <div class="row"><span class="lbl">换手 / 振幅</span><span>${fmt(h.turnover)}% / ${fmt(h.amplitude)}%</span></div>
      <div style="margin-top:10px">${badges||'<span class="badge b-muted">正常</span>'}</div>
    </div>`;
  });
  // 指数
  let it='<tr><th>指数</th><th>点位</th><th>涨跌幅</th></tr>';
  (d.indices||[]).forEach(i=>{it+=`<tr><td>${i.name}</td><td>${fmt(i.price)}</td><td class="${cls(i.pct)}">${sign(i.pct)}%</td></tr>`;});
  document.getElementById('idx').innerHTML=it;
  // 板块
  document.getElementById('sb2').textContent='↑'+d.sector_up_count+' ↓'+d.sector_down_count+' 均值'+sign(d.sector_avg)+'%';
  let s='';
  (d.sectors||[]).forEach(x=>{const w=Math.min(Math.abs(x.pct)*6,100);s+=`<div style="margin:5px 0"><span style="display:inline-block;width:64px">${x.name}</span><span class="bar ${x.pct>=0?'up':''}" style="width:${w}px"></span> <span class="${cls(x.pct)}">${sign(x.pct)}%</span></div>`;});
  document.getElementById('sectors').innerHTML=s;
  document.getElementById('bias').textContent=d.sector_bias;
  // 散户今日平均盈亏(国证2000近似) —— 紧凑版
  const rt=document.getElementById('retail');
  if(d.retail_pnl){const r=d.retail_pnl;const rc=cls(r.pct);
    rt.innerHTML=`<div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">
      <span class="pct ${rc}" style="font-size:20px;font-weight:700">${sign(r.pct)}%</span>
      <span class="badge b-${r.pct>=0?'up':'down'}">散户平均${r.pct>=0?'盈利':'亏损'} ≈ ${sign(r.pct)}%</span>
      <span class="note" style="margin:0">${r.name} ${fmt(r.price)}</span>
    </div>
    <div class="note">以国证2000(小盘股)当日涨跌幅近似散户平均盈亏, 仅供参考</div>`;
  } else rt.innerHTML='<div class="note">散户平均盈亏数据未获取</div>';
  // 主题材拉/踩指数
  const dv=document.getElementById('drivers');
  if(d.sector_drivers){const dd=d.sector_drivers;
    const pull=(dd.pullers&&dd.pullers.length)?dd.pullers.map(s=>`<span class="badge b-up">▲ ${s.name} ${sign(s.pct)}% ·偏离+${fmt(s.dev)}%</span>`).join(' '):'<span class="badge b-muted">暂无明显拉动板块</span>';
    const pres=(dd.pressers&&dd.pressers.length)?dd.pressers.map(s=>`<span class="badge b-down">▼ ${s.name} ${sign(s.pct)}% ·偏离${fmt(s.dev)}%</span>`).join(' '):'<span class="badge b-muted">暂无明显压制板块</span>';
    dv.innerHTML=`<div class="note">${dd.index_name} ${sign(dd.index_pct)}% ｜ ${dd.move} ｜ ${dd.time} 更新</div>
      <div style="margin:8px 0"><b class="up">🟢 拉指数</b><br>${pull}</div>
      <div style="margin:8px 0"><b class="down">🔴 踩指数</b><br>${pres}</div>
      <div class="note">${dd.note}</div>`;
  } else dv.innerHTML='<div class="note">暂未检测(9:15后每10分钟更新, 可点按钮立即检测)</div>';
  // 尾盘
  if(d.close){let r=`<div class="note">${d.close.time} 生成 ｜ ${d.close.note}${d.close.ai_used?' ｜ <b style="color:var(--info)">AI模型已参与</b>':''}</div>`;
    const m=d.close.market;
    const cm = m.confidence!=null ? m.confidence : 0;
    const cmPct = Math.round(cm*100);
    const cmBadge = cm>=0.65 ? 'b-up' : (cm>=0.4 ? 'b-info' : 'b-muted');
    const mBr = m.breadth!=null ? m.breadth : 0.5;
    const mLt = m.late!=null ? m.late : 0;
    const mRt = m.retail!=null ? m.retail : 0;
    r+=`<div class="row" style="border:none"><span>大盘明日看涨概率</span><span class="prob ${probcls(m.prob)}">${fmt(m.prob,1)}% (${m.verdict}) <span class="badge ${cmBadge}" style="margin-left:6px">置信度${cmPct}%</span></span></div>`;
    r+=`<div style="display:flex;gap:14px;flex-wrap:wrap;margin:4px 0 8px;font-size:13px;opacity:0.85">
        <span>📊 宽度: <b>${(mBr*100).toFixed(0)}%</b></span>
        <span>🚀 尾盘动向: <b class="${cls(mLt)}">${mLt>=0?'+':''}${mLt.toFixed(2)}%</b></span>
        <span>🧩 小盘(国证2000): <b class="${cls(mRt)}">${mRt>=0?'+':''}${mRt.toFixed(2)}%</b></span>
      </div>`;
    r+='<table><tr><th>持仓</th><th>概率</th><th>倾向</th></tr>';
    d.close.stocks.forEach(x=>{r+=`<tr><td>${x.name}</td><td class="prob ${probcls(x.prob)}">${fmt(x.prob,1)}%</td><td>${x.verdict}</td></tr>`;});
    r+='</table>';document.getElementById('close').innerHTML=r;}
  // 尾盘高开潜力
  const gu=document.getElementById('gapup');
  if(d.gapup){const g=d.gapup;
    let r=`<div class="note">${g.time} 更新 ｜ ${g.ai_used?'已用AI模型精修':'启发式模型'} ｜ ${g.note}</div><table><tr><th>排名</th><th>代码</th><th>名称</th><th>现价</th><th>当日%</th><th>委比</th><th>尾盘拉升</th><th>高开概率</th><th>置信度</th><th>模型</th></tr>`;
    (g.rows||[]).forEach((x,i)=>{
      const lp = x.late_pull!=null ? x.late_pull : 0;
      const conf = x.confidence!=null ? x.confidence : 0;
      const cPct = Math.round(conf*100);
      const cBadge = conf>=0.65 ? 'b-up' : (conf>=0.4 ? 'b-info' : 'b-muted');
      r+=`<tr><td>${i+1}</td><td>${x.code}</td><td>${x.name}</td><td>${fmt(x.price)}</td><td class="${cls(x.pct)}">${sign(x.pct)}%</td><td class="${cls(x.weibi)}">${fmt(x.weibi)}%</td><td class="${cls(lp)}">${lp>=0?'+':''}${lp.toFixed(2)}%</td><td class="prob ${probcls(x.prob)}">${fmt(x.prob,1)}%</td><td><span class="badge ${cBadge}">${cPct}%</span></td><td><span class="badge b-${x.model==='AI'?'info':'muted'}">${x.model}</span></td></tr>`;
    });
    r+='</table>';gu.innerHTML=r;
  } else gu.innerHTML='<div class="note">尚未生成（交易日 14:52 起自动检测, 可点按钮立即检测）</div>';
}

// 高开回测(v3.4): 渲染历史记录 + 命中率
function renderGapVerify(d){
  const box=document.getElementById('gapverify');
  if(!box) return;
  const s=d.stats||{};
  const recs=d.records||[];
  let r='';
  const hr=s.hit_rate!=null?Math.round(s.hit_rate*100):0;
  const tot=s.total||0;
  const opts=s.optimizations||[];
  const lastOpt=opts.length?opts[opts.length-1]:null;
  r+=`<div class="note">累计回测 <b>${tot}</b> 只 ｜ 高开命中率 <b class="${cls(hr)}">${hr}%</b> ｜ 平均预测 <b>${fmt(s.avg_pred,1)}%</b> ｜ 平均实际高开 <b class="${cls(s.avg_actual)}">${fmt(s.avg_actual,2)}%</b> ｜ 自动调权阈值 ≥${d.min_opt_samples}只</div>`;
  // v3.7: 判别力 AUC 展示(权重自优化目标)
  if(lastOpt){
    const up=lastOpt.auc_after>=lastOpt.auc_before;
    r+=`<div class="sub">🎯 权重自优化(纯启发式·无AI): 判别力 AUC <b>${fmt(lastOpt.auc_before,3)}→${fmt(lastOpt.auc_after,3)}</b> ${up?'📈':'📉'} ｜ ${lastOpt.samples}样本 ｜ ${lastOpt.at?lastOpt.at.slice(5,16):''}</div>`;
  } else {
    r+=`<div class="sub">🎯 权重自优化(纯启发式): 累计≥${d.min_opt_samples}只已验证样本后, 自动以 AUC−正则−校准 为目标调权</div>`;
  }
  const pending=recs.filter(x=>!x.verified).slice(0,1);
  if(pending.length){
    const p=pending[0];
    // 待回测 = 最近一次 14:52 自动扫描记录(auto), 与「尾盘高开潜力」实时刷新结果天然不同步,
    // 这是回测闭环语义(T日推荐→T+1开盘实测), 不以"当前实时推荐"为准, 避免误导。
    const tag=p.source==='manual_baseline'?'·基线(历史推荐)':(p.source==='auto'?'·T日14:52推荐':'');
    r+=`<div class="sub">🕒 待回测（${p.date}${tag}）：${p.stocks.map(x=>x.code+' '+x.name+'('+fmt(x.prob,1)+'%)').join('、')}</div>`;
    r+=`<div class="sub" style="font-size:12px;opacity:.8">ℹ️ 待回测为最近一次 14:52 自动扫描结果，与上方「尾盘高开潜力」实时刷新不同步属正常（回测语义：T日推荐 → T+1开盘实测）</div>`;
  }
  const recent=s.recent||[];
  if(recent.length){
    r+='<table><tr><th>验证日</th><th>代码</th><th>名称</th><th>预测概率</th><th>实际高开%</th><th>结果</th></tr>';
    recent.forEach(rec=>{
      (rec.stocks||[]).forEach(x=>{
        const gp=x.gap_pct;
        const hit=x.is_gap_up;
        r+=`<tr><td>${rec.verified_at?rec.verified_at.slice(0,10):rec.date}</td><td>${x.code}</td><td>${x.name}</td><td class="prob ${probcls(x.prob)}">${fmt(x.prob,1)}%</td><td class="${cls(gp)}">${gp==null?'--':(gp>=0?'+':'')+fmt(gp,2)+'%'}</td><td>${hit==null?'--':(hit?'✅ 高开':'❌ 未高开')}</td></tr>`;
      });
    });
    r+='</table>';
  } else {
    r+='<div class="note">尚无验证数据。下一交易日 09:30 后自动回测当前推荐的 5 只是否高开。</div>';
  }
  // 调权历史(最近3次)
  if(opts.length){
    r+='<div class="sub" style="margin-top:6px">📋 调权历史:</div>';
    opts.slice(-3).reverse().forEach(o=>{
      r+=`<div class="sub" style="font-size:12px;opacity:.85">· ${o.at?o.at.slice(5,16):''} AUC ${fmt(o.auc_before,3)}→${fmt(o.auc_after,3)} ｜ ${o.samples}样本</div>`;
    });
  }
  box.innerHTML=r;
}

function gapOpt(){
  const tip=document.getElementById('gapOptTip');
  if(tip)tip.textContent='调权中(需≥样本阈值)…';
  fetch('/api/gapup/optimize',{method:'POST'}).then(r=>r.json()).then(res=>{
    if(tip){
      if(res.ok){
        tip.textContent=`✅ 已调权(${res.samples}样本): AUC ${res.auc_before}→${res.auc_after}`;
      } else {
        tip.textContent=`ℹ️ ${res.reason||'样本不足'}`;
      }
    }
    setTimeout(load,800);
  }).catch(()=>{if(tip)tip.textContent='调权失败';});
}

function load(){fetch('/api/snapshot').then(r=>r.json()).then(render).catch(()=>{});fetch('/api/gapup/log').then(r=>r.json()).then(renderGapVerify).catch(()=>{});}
function manual(t){
  if(t==='close'){
    const b=document.getElementById('closeRefreshBtn');
    const tip=document.getElementById('closeRefreshTip');
    if(b&&!b.disabled){
      b.disabled=true;b.style.opacity=.6;b.textContent='⏳ 预测中…';
      if(tip)tip.textContent='正在立即重新计算大盘+各持仓方向（含AI融合），请稍候…';
      fetch('/api/'+t).then(r=>r.json()).then(()=>{
        setTimeout(()=>{load();if(b){b.disabled=false;b.style.opacity=1;b.textContent='🎯 立即预测';}
          if(tip)tip.textContent='点击立即重新计算一次尾盘预测，一般需 10~30 秒';},800);
      }).catch(()=>{if(b){b.disabled=false;b.style.opacity=1;b.textContent='🎯 立即预测';}});
    }
  }else if(t==='gapup'){
    const b=document.getElementById('gapupBtn');
    const tip=document.getElementById('gapupTip');
    if(b&&!b.disabled){
      b.disabled=true;b.style.opacity=.6;b.textContent='⏳ 扫描中…';
      if(tip)tip.textContent='正在立即全市场扫描主板（约需 1~3 分钟），请稍候自动更新…';
      fetch('/api/gapup?force=1').then(r=>r.json()).then(()=>{
        setTimeout(()=>{load();if(b){b.disabled=false;b.style.opacity=1;b.textContent='🎯 立即检测';}
          if(tip)tip.textContent='点击立即全市场扫描主板，约需 1~3 分钟';},800);
      }).catch(()=>{if(b){b.disabled=false;b.style.opacity=1;b.textContent='🎯 立即检测';}});
    }
  }else{fetch('/api/'+t).then(r=>r.json()).then(()=>load());}
}
// ---- 持仓前端编辑(仅 code/cost/shares, 保存走 /api/portfolio) ----
let editing=false, pfEdit=[];
function enterEdit(){
  editing=true;
  document.getElementById('pfEditor').style.display='block';
  const msg=document.getElementById('pfMsg'); if(msg) msg.textContent='';
  fetch('/api/portfolio').then(r=>r.json()).then(d=>{
    pfEdit=(d.holdings||[]).map(h=>({code:String(h.code), cost:h.cost, shares:h.shares}));
    renderPfRows();
  }).catch(()=>{pfEdit=[];renderPfRows();});
}
function renderPfRows(){
  const box=document.getElementById('pfRows');
  if(!box) return;
  if(!pfEdit.length){box.innerHTML='<div class="note">暂无持仓，点「➕ 新增一行」添加</div>';return;}
  let h='<table style="width:100%;border-collapse:collapse"><tr><th style="text-align:left;padding:4px 8px">股票代码</th><th style="text-align:left;padding:4px 8px">成本</th><th style="text-align:left;padding:4px 8px">股数</th><th style="padding:4px 8px"></th></tr>';
  pfEdit.forEach((x,i)=>{
    h+=`<tr>
      <td style="padding:4px 8px"><input id="pfCode${i}" value="${x.code}" style="width:96px;background:var(--bg);color:var(--txt);border:1px solid var(--line);border-radius:5px;padding:5px 7px;font-size:13px" oninput="pfUpd(${i},'code',this.value)"></td>
      <td style="padding:4px 8px"><input id="pfCost${i}" type="number" step="0.001" value="${Number(x.cost||0).toFixed(3)}" style="width:96px;background:var(--bg);color:var(--txt);border:1px solid var(--line);border-radius:5px;padding:5px 7px;font-size:13px" oninput="pfUpd(${i},'cost',this.value)"></td>
      <td style="padding:4px 8px"><input id="pfShares${i}" type="number" step="1" value="${x.shares}" style="width:96px;background:var(--bg);color:var(--txt);border:1px solid var(--line);border-radius:5px;padding:5px 7px;font-size:13px" oninput="pfUpd(${i},'shares',this.value)"></td>
      <td style="padding:4px 8px"><button onclick="pfDel(${i})" style="background:transparent;color:var(--down);border:1px solid var(--down);border-radius:5px;padding:4px 9px;cursor:pointer;font-size:12px">🗑 删除</button></td>
    </tr>`;
  });
  h+='</table>';
  box.innerHTML=h;
}
function pfUpd(i,f,v){ if(pfEdit[i]) pfEdit[i][f]=v; }
function pfAdd(){ pfEdit.push({code:'',cost:0,shares:0}); renderPfRows(); }
function pfDel(i){ pfEdit.splice(i,1); renderPfRows(); }
function pfCancel(){ editing=false; const e=document.getElementById('pfEditor'); if(e) e.style.display='none'; const m=document.getElementById('pfMsg'); if(m) m.textContent=''; }
function pfSave(){
  // 从输入框读取最新值(防止 oninput 漏抓)
  pfEdit.forEach((x,i)=>{
    const c=document.getElementById('pfCode'+i), co=document.getElementById('pfCost'+i), s=document.getElementById('pfShares'+i);
    if(c) x.code=c.value; if(co) x.cost=parseFloat(co.value)||0; if(s) x.shares=parseFloat(s.value)||0;
  });
  const rows=pfEdit.map(x=>({code:(x.code||'').trim(), cost:parseFloat(x.cost)||0, shares:parseFloat(x.shares)||0})).filter(x=>x.code);
  const msg=document.getElementById('pfMsg');
  if(!rows.length){ if(msg) msg.textContent='请至少保留一只持仓，或填写股票代码'; return; }
  if(msg) msg.textContent='保存中…';
  fetch('/api/portfolio',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({holdings:rows})})
    .then(r=>r.json()).then(d=>{
      if(d.ok){ if(msg) msg.textContent='✅ 已保存（'+d.count+' 只），约 5 秒内刷新行情'; editing=false; const e=document.getElementById('pfEditor'); if(e) e.style.display='none'; load(); }
      else { if(msg) msg.textContent='❌ 保存失败：'+(d.error||'未知错误'); }
    }).catch(e=>{ if(msg) msg.textContent='❌ 保存失败：'+e; });
}
load();setInterval(load,5000);
</script>
</body></html>"""


if __name__ == "__main__":
    _ensure_runtime_data()   # 兜底: 缺失的运行时数据文件用模板/基线初始化
    threading.Thread(target=scheduler_loop, daemon=True).start()
    print(f"监控平台 v2 启动: http://localhost:{PORT}")
    app.run(host="0.0.0.0", port=PORT, threaded=True)
