# -*- coding: utf-8 -*-
"""共享核心层: 全局状态(BASE/STATE/FCONFIG/SCFG)、路径与业务常量、时间工具、共享小工具。
由 app.py 拆分而来, 各子模块用 `from core import *` 引入, 避免循环依赖。"""

import json, re, math, time, threading, datetime, os, random, traceback, shutil, copy
__all__ = ['AI_BASE', 'AI_CFG', 'AI_ENABLED', 'AI_KEY', 'AI_MODEL', 'ANOM', 'BASE', 'CLASSIFY_CACHE_FILE', 'CLOSED', 'DAILY_BARS', 'DAILY_KEEP_DAYS', 'DAILY_MAX_MB', 'DAILY_UNIVERSE_LIMIT', 'DAILY_WARMUP_DAYS', 'DAILY_WARMUP_WORKERS', 'F', 'FCONFIG', 'FORECAST_CFG', 'GAPUP_CALIB', 'GAPUP_LOG', 'GAPUP_MIN_CALIB_SAMPLES', 'GAPUP_MIN_GAP_PCT', 'GAPUP_MIN_OPT_SAMPLES', 'GAPUP_OPT_CAL', 'GAPUP_OPT_MULTIPLIERS', 'GAPUP_OPT_REG', 'GAPUP_STATS', 'GAPUP_TUNED', 'GAPUP_WEIGHT_OVERRIDE', 'HEADERS', 'HOLDINGS_RAW', 'IDX_AI_FUSE_SEC', 'IDX_FORECAST_SEC', 'INDICES', 'LOCK', 'MINUTE_CACHE_TTL', 'POLL', 'PCFG', 'POOL', 'PORT', 'PORTFOLIO_PATH', 'PRED_CALIB', 'PRED_LOG', 'PRED_MIN_CALIB_SAMPLES', 'PRED_STATS', 'PREOPEN_CFG', 'PREOPEN_FAST_SEC', 'PRESSURE_PCT', 'RETAIL_INDEX', 'SCAN', 'SCFG', 'SET', 'STATE', 'TAKE_PROFIT_PCT', 'WATCHLIST', '_CALIB_A_RANGE', '_CALIB_MAX_ABS_B', '_FETCH_POOL', '_FORECAST_DEFAULTS', '_MINUTE_CACHE', '_GAPUP_CALIB', '_PRED_CALIB', '_HERE', '_HOLD_LOCK', '_POOL_DEFAULTS', '_SCAN_DEFAULTS', '_TENCENT_SESSION', '_market_prefix', '_parse_hhmm', 'beijing_now', 'is_weekday', 'num', 'trading_phase']

# BASE: 跨平台——默认取脚本所在目录; 沙箱/旧部署兜底到 /workspace/stock_monitor
_HERE = os.path.dirname(os.path.abspath(__file__))
BASE = _HERE if os.path.isdir(_HERE) else "/workspace/stock_monitor"
PORT = int(os.environ.get("PORT", 8800))
PORTFOLIO_PATH = f"{BASE}/portfolio.json"
CLASSIFY_CACHE_FILE = f"{BASE}/stock_classify.json"
_HOLD_LOCK = threading.Lock()   # 持仓配置热重载锁(前端编辑保存后无需重启)
LOCK = threading.Lock()   # 快照/STATE 读写互斥(调度循环写、接口读)

# 尾盘高开潜力: 回测闭环 + 权重自优化(v3.4)
GAPUP_LOG = f"{BASE}/gapup_log.jsonl"          # 每行一条交易日推荐记录(记住5只)
GAPUP_STATS = f"{BASE}/gapup_stats.json"       # 累计回测统计(命中率等)
GAPUP_TUNED = f"{BASE}/gapup_weights_tuned.json"  # 调优后的 gu_* 权重(启动时加载覆盖默认)
GAPUP_MIN_OPT_SAMPLES = 10                     # 至少累计10只已验证样本才允许自动调权(配合正则防过拟合)
GAPUP_OPT_MULTIPLIERS = [0.6, 0.8, 1.25, 1.5, 2.0]  # 坐标上升尝试的权重乘子
GAPUP_OPT_REG = 0.02          # L2 正则强度(向默认权重回拉, 抑制小样本过拟合)
GAPUP_OPT_CAL = 0.3           # 校准惩罚强度(预测均值 vs 实际高开率)
GAPUP_WEIGHT_OVERRIDE = None                    # 调优时临时覆盖 FCONFIG 的 gu_* 权重
# v3.10: 高开"命中"判定阈值(%). 原为 >0 即算命中, 导致 +0.01% 噪声级波动也计入,
# 现要求有意义的高开幅度; 同时保留宽松(>0)与严格(>=阈值)双命中率指标。
GAPUP_MIN_GAP_PCT = 0.5
# v3.10: Platt scaling 概率校准文件(把原始 score 映射为真实高开概率)
GAPUP_CALIB = f"{BASE}/gapup_calib.json"
GAPUP_MIN_CALIB_SAMPLES = 12   # 少于该已验证样本数则不启用校准(防小样本过拟合)

# v3.10: 上证指数1小时预测刷新频率(秒)。预测本体每5秒随行情刷新,
# 但 AI 融合与分时图降频, 避免高频打爆接口。
IDX_FORECAST_SEC = 5          # 预测本体刷新间隔(秒)
IDX_AI_FUSE_SEC = 120         # AI 方向融合最小间隔(秒), 远程AI较慢故降频
MINUTE_CACHE_TTL = 30         # 分时数据缓存秒数(一次抓取供多处复用)

# v3.10: 本地日线库(自建历史K线, 突破"沙箱无历史数据"限制)
DAILY_BARS = f"{BASE}/daily_bars.jsonl"   # 每行一条 {code,date,open,high,low,close,vol,...}
DAILY_KEEP_DAYS = 250         # 日线保留天数(约1年交易日), 超出自动清理
DAILY_MAX_MB = 400            # 日线库文件大小上限(MB), 超限则清理最旧数据
                            # v3.12: 全主板(3151只)各保留 ~35 根日K供选股池零网络扫描,
                            # 体积需覆盖 全主板×35天; 400MB 足够(实测约 200-300MB)。
DAILY_UNIVERSE_LIMIT = 0      # 0=全市场; >0 则只存前N只(按代码序), 用于限制体积
# v3.12: 选股池本地日线库预热参数
DAILY_WARMUP_DAYS = 35        # 预热时每只股票抓取的历史K线根数(需覆盖 MA长+lookback+SKDJ 预热)
DAILY_WARMUP_WORKERS = 60     # 预热并发抓取线程数(全主板 ~3151 只, 一次抓齐约1-2分钟)

# v3.10: 通用预测回测闭环(上证1小时/尾盘大盘/尾盘个股/开盘前涨停)
PRED_LOG = f"{BASE}/pred_log.jsonl"
PRED_STATS = f"{BASE}/pred_stats.json"
# v3.10.1: 预测概率自动校准(Platt scaling, 与 gapup 同款): 4 个 P1 模块(上证1小时/尾盘大盘/尾盘个股/盘前涨停)
# 的概率输出随验证样本积累自动收敛到真实命中率。校准参数按模块分别持久化, 避免互相干扰。
PRED_CALIB = f"{BASE}/pred_calib.json"
# v3.11.7: 12 条样本对 Platt scaling 而言过少(业界通常要求 50+), 实测 idx_1h 用 15 条
# 样本拟合出的校准把整个概率分布压垮(见下方 _CALIB_MAX_ABS_B 注释), 故门槛提高到 30。
PRED_MIN_CALIB_SAMPLES = 30
# v3.11.7: Platt 校准参数护栏。B 是 logit 空间的平移量, 样本稀少时极易被拟合出极端值:
# 实测 idx_1h 仅 15 条样本就拟合出 B=-1.7598, 把中性 50% 压到 14.7%、把强多头 72% 压到 30.7%,
# 使"看涨"在数学上几乎不可能触发(需原始概率>86%), 表现为"概率永远很低"。
# 故限制中性点(prob=50)校准后落在 [30%, 70%] 区间内, 即 |B| <= ln(0.7/0.3) ≈ 0.847。
_CALIB_MAX_ABS_B = 0.85
_CALIB_A_RANGE = (0.5, 2.0)      # 斜率合理区间, 防止 A 退化或爆炸

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

# ---- 选股池(v3.11.13): 每日定时全市场扫描主板, 找出
#      「过去N根K线里有M根 MA短 在 MA长 下方(前期充分粘合/压制)
#        + 当根 MA短 从下往上上穿 MA长」的标的;
#      若同时出现 SKDJ 的 K≈20 且向上金叉 D, 则加分并优先推荐。
#      默认阈值可被 portfolio.json 的 settings.stock_pool 覆盖。
_POOL_DEFAULTS = {
    "scan_hhmm": "14:25",            # 每日自动扫描时刻(收盘前35分钟, 留足尾盘交易时间)
    "top_n": 3,                      # 推荐数量
    "ma_short": 3,                   # 短期均线(3日线)
    "ma_long": 7,                    # 长期均线(7日线)
    "lookback": 5,                   # 「过去N根K线」窗口(不含当根)
    "below_need": 4,                 # 窗口内至少 N 根 MA短 < MA长
    "skdj_n": 9,                     # SKDJ 的 RSV 周期
    "skdj_m1": 3,                    # K 的平滑周期
    "skdj_m2": 3,                    # D 的平滑周期
    # v3.12: 打分权重 w_* 与 SKDJ 低位区间 skdj_low/high 已内嵌进加密策略模块
    # (strategy_pool.blob, 机密配方), 不在源码/配置明文出现。如需调参:
    #   1) 改 strategy_pool.py 的 _SECRET_WEIGHTS
    #   2) 运行 python build_strategy.py 重新加密
    #   3) 删除明文 strategy_pool.py
    "workers": 24,                   # 并发抓取日K的线程数
    "bars": 30,                      # 每只抓取的日K根数(需覆盖 MA长+窗口+SKDJ 预热)
    "max_scan": 0,                   # 最多扫描只数(0=全部主板, 按成交额降序截断)
    "min_amount": 0,                 # 最低成交额(元)预筛, 0=不筛
    # v3.12 提速预筛: 用实时行情直接砍掉明显不符标的, 减少需跑指标计算的数量
    "pref_min_price": 5.0,           # 现价低于该值直接剔除(低价垃圾股, 极难满足均线上穿形态)
    "pref_min_float_mv": 20.0,       # 流通市值(亿)低于该值直接剔除
    "pref_min_amount": 20000000.0,   # 当日成交额(元)低于该值剔除(流动性差, 成交额小则量能不足)
}
POOL = SET.get("stock_pool", {})
PCFG = {**_POOL_DEFAULTS, **POOL}                        # 生效的选股池配置
# 预测模块可调权重(默认值, 可被 portfolio.json 的 settings.forecast 覆盖)
# v2.8: 三个预测模块统一引入 宽度/尾盘动向/小盘情绪 多因子 + 置信度校准
_FORECAST_DEFAULTS = {
    # 上证指数1小时趋势预测
    "idx_pct_w": 2.2, "idx_late_w": 1.8, "idx_pos_w": 2.0, "idx_vr_w": 1.0,
    "idx_wb_w": 1.5, "idx_breadth_w": 3.0, "idx_retail_w": 0.8, "idx_sig": 6.0,
    # v3.11.7: 上证1小时「给出方向性判定」所需的最低置信度(展示层门控)。
    # 低于此值只显示"观望", 不再把弱信号包装成涨跌判断。
    # 可用 portfolio.json 的 settings.forecast.idx_min_conf 覆盖。
    # 注意: 不要加进 _TUNE_W_DEFAULT —— 那会被自动调参器当作可调权重优化掉。
    "idx_min_conf": 0.35,
    # 尾盘预测(大盘明日方向)
    "cl_sh_w": 1.8, "cl_cyb_w": 1.0, "cl_sec_w": 1.2, "cl_breadth_w": 6.0,
    "cl_retail_w": 1.0, "cl_late_w": 2.5, "cl_sig": 6.0,
    # 尾盘个股次日(close_stock, v3.11 参数化; 默认与旧硬编码一致, 行为不变)
    "stk_sh_w": 1.5, "stk_cyb_w": 1.0, "stk_sec_w": 1.2, "stk_yin_w": 1.5,
    "stk_amt_w": 0.5, "stk_posmag": 1.5, "stk_pnl_pos": 0.8, "stk_pnl_neg": 0.8,
    "stk_breadth_w": 3.0, "stk_retail_w": 0.6, "stk_late_w": 1.2, "stk_sig": 5.0,
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

STATE = {
    "latest": None, "last_update": None, "trading": False,
    "preopen": None, "preopen_date": None, "close": None, "close_date": None,
    "idx_forecast": None, "idx_forecast_time": None, "idx_ai_time": None,
    "sector_drivers": None, "sector_drivers_time": None,
    "gapup": None, "gapup_date": None,
    "alerts": [], "is_weekday": True,
    # v3.10: 预测回测闭环 + 本地日线库
    "gapup_verify_date": None, "pred_verify_time": None,
    "idx_predlog_time": None, "daily_bars_date": None,
    # v3.11.13: 选股池(每日定时扫描主板 MA上穿+SKDJ低位金叉)
    "stock_pool": None, "stock_pool_date": None, "stock_pool_scanning": False,
    # v3.12: 全主板日线库预热状态(收盘后15:12触发, 每日一次)
    "daily_warmup_date": None,
}

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
_MINUTE_CACHE = {}  # 分时数据缓存(dict code->(ts, rows)); fetch_minute 复用, 降低接口压力

# ---------------- v3.10: 概率校准(Platt scaling) 共享状态 ----------------
# 问题: sigmoid(score/gu_sig) 只是单调变换, gu_sig 是拍脑袋定的, 输出并非真实概率。
# 实测: 宣称 66.9% 概率, 实际高开率仅 40%(严格≥0.5% 仅 20%), 校准偏差 +26.9pp。
# 方案: 用已验证样本 (score, 是否真高开) 拟合 Platt scaling: P = 1/(1+exp(A*score+B)),
# 让输出概率名副其实。样本不足时自动降级不启用, 避免小样本过拟合。
_GAPUP_CALIB = {"A": None, "B": None, "n": 0, "fitted_at": None}
# v3.10.1: 各 P1 模块的概率校准参数(按模块独立), 形如 {"idx_1h": {"A":1.0,"B":..,"n":..}, ...}
_PRED_CALIB = {}

def num(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


# 全市场代码表(缓存)

def _market_prefix(code):
    return "sh" if code.startswith("6") else "sz"


