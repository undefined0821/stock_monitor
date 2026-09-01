# -*- coding: utf-8 -*-
"""预测回测闭环: 落盘/统计/验证/告警。由 app.py 拆分。"""

import json, re, math, time, threading, datetime, os, random, traceback, copy
from core import *
import requests
# 行情层(market_data 只依赖 core, 不反向依赖本模块, 直接导入安全)。
# 本模块多处要用 fetch_tencent/parse_row 做实时兜底, 而 `from core import *` 并不含它们,
# 不显式导入会在运行到实时兜底分支时 NameError。
from market_data import fetch_tencent, parse_row

__all__ = ['IDX_PRED_LOG_SEC', 'LIMITUP_HIT_PCT', 'PRED_MODULES', '_accumulate_stats', '_actual_any', '_actual_hit', '_add_trading_minutes', '_day_pct', '_day_pct_local', '_find_verify_target', '_gapup_auc', '_kline_bars_range', '_live_day_pct', '_live_next_day_return', '_load_daily', '_load_gapup_log', '_load_pred_log', '_load_stats', '_next_day_return', '_next_day_return_local', '_next_trading_day', '_recompute_pred_stats', '_save_pred_log', '_save_stats', '_verdict_hit', '_verify_one_pred', 'detect_alerts', 'load_pred_stats', 'log_prediction', 'optimize_gapup_weights', 'verify_predictions']


def _calib():
    """延迟引用 calib 模块(只在函数体内 import)。

    calib.py 顶层有 `from backtest import *`, 若本模块再在顶层 `import calib`
    就构成循环导入(与 backtest↔app 同款问题), 故校准/调参相关的三个符号统一走
    函数内延迟 import: fit_pred_calib / auto_tune_module / _apply_pred_calib。
    """
    import calib
    return calib


def _app():
    """延迟引用 app 模块(只在函数体内 import)。

    ⚠️ 切勿改回模块顶层 `import app`。app.py 顶层有 `from backtest import *`,
    两者互引会在 `python app.py` 启动时形成循环导入:
      1) __main__(app.py) 执行到 `from backtest import *` → backtest 开始加载;
      2) backtest 顶层 `import app` → 系统里还没有 'app' → 再加载一份 app.py 副本;
      3) 副本执行到 `from backtest import *` 时, backtest 还停在 import app 那一行,
         连 __all__ 都还没定义 → 副本拿到的 backtest 命名空间是空的,
         detect_alerts / verify_predictions / log_prediction / load_pred_stats
         / PRED_MODULES 等全部丢失。
      4) scheduler.py 的 `import app` 拿到的正是这份残缺副本, 于是
         `app.detect_alerts(snap)` 每轮抛 AttributeError, 整个调度循环形同停摆:
         STATE["idx_forecast"] 永不生成 → 上证指数分时图消失; 题材拉踩/盘前扫描/
         尾盘预测/高开潜力/日线落库/预测回填 全部不再自动触发。
    改为函数体内延迟 import, 循环被彻底打断(加载 backtest 时不再触碰 app)。
    """
    import app
    return app


def _load_daily():
    """读取本地日线库, 返回按日期升序的记录列表 [{'date','ts','bars':{mktcode:{o,h,l,c,v,amt}}}]。

    说明: 该数据源同时被回测层(本模块的 *_local 系列)与 app.py 的日线接口使用。
    原先定义在 app.py, 但 app.py 顶层 `from backtest import *` 发生在本模块加载之后,
    本模块拿不到 app 的名字, 运行到此处会 NameError。故下沉到本模块并加入 __all__,
    由 app.py 星号导入复用, 两边共用一份实现。"""
    out = []
    if not os.path.exists(DAILY_BARS):
        return out
    try:
        with open(DAILY_BARS, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    out.append(json.loads(ln))
                except Exception:
                    continue
    except Exception:
        pass
    out.sort(key=lambda r: r.get("date", ""))
    return out


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



def _actual_hit(a):
    """v3.10: 按当前阈值 GAPUP_MIN_GAP_PCT 重新判定"严格命中"。
    历史记录的 is_gap_up 是按旧阈值(>0)存的, 改阈值后需重算才能保持口径一致。
    返回 True/False, 或 None(开盘数据缺失, 不计入分母)。"""
    gp = a.get("gap_pct")
    if gp is None:
        return None
    return gp >= GAPUP_MIN_GAP_PCT



def _actual_any(a):
    """宽松命中: 开盘价 > 昨收(即 gap>0), 含噪声级微幅高开, 仅作对照。"""
    gp = a.get("gap_pct")
    if gp is None:
        return None
    return gp > 0



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
             # v3.10: 双命中率。hit_rate=严格(高开>=GAPUP_MIN_GAP_PCT), hit_rate_any=宽松(>0)
             "gap_any": 0, "hit_rate_any": 0.0,
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
            # v3.10: 按当前阈值重算命中(历史记录可能是旧阈值), None=开盘数据缺失跳过
            hit = _actual_hit(a)
            if hit is None:
                continue  # 开盘数据缺失(抓取失败)不计入命中率分母, 避免拉低准确率
            any_hit = _actual_any(a)
            stats["total"] += 1
            if hit:
                stats["gap_up"] += 1
            if any_hit:
                stats["gap_any"] += 1
            all_pred.append(s.get("prob", 0))
            all_act.append(a.get("gap_pct", 0) or 0)
            rank = i + 1
            rank_tot[rank] = rank_tot.get(rank, 0) + 1
            if hit:
                rank_hits[rank] = rank_hits.get(rank, 0) + 1
    stats["hit_rate"] = round(stats["gap_up"] / stats["total"], 4) if stats["total"] else 0.0
    stats["hit_rate_any"] = round(stats["gap_any"] / stats["total"], 4) if stats["total"] else 0.0
    stats["min_gap_pct"] = GAPUP_MIN_GAP_PCT
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
                with _app()._WeightOverride(weights):
                    scores.append(_app().gap_up_score(d, ctx, late_pull=feat.get("late_pull", 0)))
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
                with _app()._WeightOverride(weights):
                    scores.append(_app().gap_up_score(d, ctx, late_pull=feat.get("late_pull", 0)))
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


# ---------------- v3.10: 通用预测回测闭环 (P1) ----------------
# 问题: 除"尾盘高开潜力"外, 其余预测模块(上证1小时/尾盘大盘次日/尾盘个股次日/盘前涨停)
# 全部只出预测、从不回收真实结果 —— 没有命中率, 概率是否可信无从验证, 更无法校准。
# 方案: 统一落盘 pred_log.jsonl, 到期自动抓真实行情回填, 按模块累计:
#   - 命中率 hit_rate(方向是否判对)
#   - 校准偏差 bias = 平均预测概率 - 实际发生率(衡量概率是否名副其实)
# 统一落盘后, 后续 Platt 校准可直接复用 gapup 的同款做法推广到各模块。

PRED_MODULES = {
    "idx_1h":          {"label": "上证1小时方向", "horizon": "1h",        "flat": 0.15},
    "close_market":    {"label": "尾盘大盘次日", "horizon": "next_day",   "flat": 0.30},
    "close_stock":     {"label": "尾盘个股次日", "horizon": "next_day",   "flat": 0.50},
    "preopen_limitup": {"label": "盘前涨停预测", "horizon": "today_close", "flat": 0.0},
}
IDX_PRED_LOG_SEC = 900          # 上证预测每5秒刷新, 落盘记录按15分钟节流, 避免日志爆炸
LIMITUP_HIT_PCT = 9.8           # 主板涨停判定(留0.2pp容差, 覆盖价格舍入)



def _load_pred_log():
    out = []
    if not os.path.exists(PRED_LOG):
        return out
    try:
        with open(PRED_LOG, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    out.append(json.loads(ln))
                except Exception:
                    continue
    except Exception:
        pass
    return out



def _save_pred_log(recs):
    tmp = PRED_LOG + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, PRED_LOG)



def _add_trading_minutes(t, mins):
    """在交易时间内推进 N 个交易分钟, 自动跳过午休(11:30-13:00), 超过15:00截断到15:00。
    用于计算"1小时后"的真实到期时刻 —— 直接加60分钟会落进午休或收盘后, 抓不到有效价格。"""
    day = t.date()
    cur = datetime.datetime.combine(day, t.time())
    end = datetime.datetime.combine(day, datetime.time(15, 0))
    lunch_s = datetime.datetime.combine(day, datetime.time(11, 30))
    lunch_e = datetime.datetime.combine(day, datetime.time(13, 0))
    left = float(mins)
    guard = 0
    while left > 0 and guard < 20:
        guard += 1
        if lunch_s <= cur < lunch_e:
            cur = lunch_e
            continue
        if cur >= end:
            cur = end
            break
        nxt = lunch_s if cur < lunch_s else end
        avail = (nxt - cur).total_seconds() / 60.0
        if avail <= 0:
            cur = nxt
            continue
        step = min(left, avail)
        cur += datetime.timedelta(minutes=step)
        left -= step
    return cur.strftime("%Y-%m-%d %H:%M:%S")



def _next_trading_day(t):
    """返回下一个工作日(交易日)的 datetime(仅跳过周末, 法定节假日由回测时顺延)。"""
    d = t + datetime.timedelta(days=1)
    while d.weekday() >= 5:
        d += datetime.timedelta(days=1)
    return d



def log_prediction(module, pred, verify_at, items=None):
    """落盘一条待回测预测。同模块+同到期时刻+同key只保留一条(幂等, 防重复刷新写爆日志)。"""
    if module not in PRED_MODULES:
        return None
    now = beijing_now()
    key = str(pred.get("key") or pred.get("verdict") or "")
    rec = {
        "id": f"{module}_{now.strftime('%Y%m%d%H%M%S')}_{abs(hash(key)) % 100000}",
        "module": module, "date": now.strftime("%Y-%m-%d"),
        "ts": now.strftime("%Y-%m-%d %H:%M:%S"),
        "verify_at": verify_at, "verified": False,
        "key": key, "pred": pred, "actual": None,
    }
    try:
        recs = _load_pred_log()
        for r in recs:
            if (r.get("module") == module and r.get("key") == key
                    and r.get("verify_at") == verify_at):
                return None          # 已记录, 跳过
        recs.append(rec)
        # 日志体积保护: 超过 3000 条时丢弃已验证的最旧记录
        if len(recs) > 3000:
            keep, dropped = [], 0
            for r in recs:
                if len(recs) - dropped <= 3000 or not r.get("verified"):
                    keep.append(r)
                else:
                    dropped += 1
            recs = keep
        _save_pred_log(recs)
        return rec["id"]
    except Exception:
        traceback.print_exc()
        return None



def _verdict_hit(verdict, ret, flat_band):
    """方向命中判定: 多/空看符号, 震荡看是否落在阈值带内。
    观望(弱信号未判方向)为非方向性预测, 返回 None —— 不计入方向命中率分子/分母。"""
    if verdict in ("看涨", "偏多"):
        return ret > 0
    if verdict in ("看跌", "偏空"):
        return ret < 0
    if verdict == "观望":
        return None
    return abs(ret) <= flat_band



def _kline_bars_range(code, start, end, n=20):
    """按显式日期区间抓日线(前复权), 供回测按日期回溯 —— 与验证执行时刻无关,
    即使沙箱休眠跨天后才回测, 也不会拿错基准收盘价。失败返回 []。"""
    mkt = _market_prefix(code) + code
    try:
        url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
               f"?param={mkt},day,{start},{end},{n},qfq")
        r = requests.get(url, headers=HEADERS, timeout=8)
        node = (r.json().get("data") or {}).get(mkt) or {}
        bars = node.get("qfqday") or node.get("day") or []
        out = []
        for b in bars:
            try:
                out.append({"date": b[0], "open": float(b[1]), "close": float(b[2]),
                            "high": float(b[3]), "low": float(b[4])})
            except (ValueError, IndexError, TypeError):
                continue
        return out
    except Exception:
        return []



def _next_day_return(code, pred_date):
    """取 pred_date 次一交易日的涨跌幅(次日收盘 vs pred_date收盘)。"""
    try:
        end = (datetime.datetime.strptime(pred_date, "%Y-%m-%d")
               + datetime.timedelta(days=12)).strftime("%Y-%m-%d")
    except ValueError:
        return None
    bars = _kline_bars_range(code, pred_date, end)
    for i, b in enumerate(bars):
        if b["date"] == pred_date and i + 1 < len(bars):
            base, nxt = b["close"], bars[i + 1]
            if base > 0:
                return {"ret": round((nxt["close"] - base) / base * 100, 3),
                        "price": nxt["close"], "date": nxt["date"]}
    return None



def _day_pct(code, date):
    """取 date 当日涨跌幅(相对其前一根K线收盘), 用于涨停判定, 与执行时刻无关。"""
    try:
        start = (datetime.datetime.strptime(date, "%Y-%m-%d")
                 - datetime.timedelta(days=20)).strftime("%Y-%m-%d")
        end = (datetime.datetime.strptime(date, "%Y-%m-%d")
               + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    except ValueError:
        return None
    bars = _kline_bars_range(code, start, end)
    for i, b in enumerate(bars):
        if b["date"] == date and i >= 1 and bars[i - 1]["close"] > 0:
            prev = bars[i - 1]["close"]
            return {"ret": round((b["close"] - prev) / prev * 100, 3),
                    "price": b["close"], "date": b["date"]}
    return None


# --- 本地日线库版回测数据源(数据由每日15:05采集落库) ---

def _next_day_return_local(code, pred_date):
    bars = [r for r in _load_daily() if (r.get("bars") or {}).get(code)]
    for i, b in enumerate(bars):
        if b["date"] == pred_date and i + 1 < len(bars):
            base = b["bars"][code]["c"]
            c1 = bars[i + 1]["bars"][code]["c"]
            if base and base > 0 and c1:
                return {"ret": round((c1 - base) / base * 100, 3), "price": c1,
                        "date": bars[i + 1]["date"], "src": "local"}
    return None



def _day_pct_local(code, date):
    bars = [r for r in _load_daily() if (r.get("bars") or {}).get(code)]
    for i, b in enumerate(bars):
        if b["date"] == date and i >= 1:
            prev = bars[i - 1]["bars"][code]["c"]
            c = b["bars"][code]["c"]
            if prev and prev > 0 and c:
                return {"ret": round((c - prev) / prev * 100, 3), "price": c,
                        "date": date, "src": "local"}
    return None


# --- 实时行情兜底(仅在能证明基准对齐时采信) ---

def _live_next_day_return(code, rec):
    """实时行情算次一交易日涨跌。仅当行情的昨收 ≈ 预测时记录的基准价(预测日14:50价≈收盘)
    时才采信 —— 说明当前正是次一交易日; 若已漂移到更晚, 昨收会变, 返回None留待日线源。"""
    base = (rec.get("pred") or {}).get("base_close")
    if not base:
        return None
    q = fetch_tencent([code])
    d = parse_row(q.get(code.upper(), []))
    if not d["price"] or not d["prevclose"] or d["prevclose"] <= 0:
        return None
    if abs(d["prevclose"] - base) / base > 0.006:   # 基准对不上 => 已跨多个交易日
        return None
    return {"ret": d["pct"] or 0.0, "price": d["price"],
            "date": rec.get("date"), "src": "live"}



def _live_day_pct(code, rec):
    """实时行情算预测当日涨跌, 仅限预测当日(收盘后pct即当日涨跌幅)。"""
    if rec.get("date") != beijing_now().strftime("%Y-%m-%d"):
        return None
    q = fetch_tencent([code])
    d = parse_row(q.get(code.upper(), []))
    if not d["price"]:
        return None
    return {"ret": d["pct"] or 0.0, "price": d["price"],
            "date": rec.get("date"), "src": "live"}



def _verify_one_pred(rec):
    """抓真实结果回填一条预测。返回 True 表示已回填(无论命中与否)。"""
    module, now = rec.get("module"), beijing_now()
    pred = rec.get("pred") or {}
    today_s = now.strftime("%Y-%m-%d")
    if module == "idx_1h":
        # 1小时后的价格是盘中瞬时值, 只能当日回填; 跨天未回填则记为过期(无法补抓)
        if rec.get("date") != today_s and now.time() > datetime.time(9, 25):
            rec["actual"] = {"expired": True, "hit": None}
            rec["verified"] = True
            rec["verified_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
            return True
        q = fetch_tencent(["sh000001"])
        d = parse_row(q.get("SH000001", []))
        p0, p1 = pred.get("price"), d["price"]
        if not p0 or not p1:
            return False
        ret = (p1 - p0) / p0 * 100
        hit = _verdict_hit(pred.get("verdict"), ret, PRED_MODULES[module]["flat"])
        rec["actual"] = {"price": round(p1, 2), "ret": round(ret, 3), "hit": hit}
    elif module in ("close_market", "close_stock"):
        code = pred.get("qcode")
        if not code:
            return False
        # 三级数据源, 避免单一接口故障导致回测停摆:
        # 1) 本地日线库(每日15:05自采) 2) 腾讯在线日线(按预测日期回溯, 防休眠跨天拿错基准)
        # 3) 实时行情兜底(仅当行情昨收≈预测日基准, 证明今天就是次一交易日时才采信)
        a = _next_day_return_local(code, rec.get("date") or today_s) \
            or _next_day_return(code, rec.get("date") or today_s) \
            or _live_next_day_return(code, rec)
        if not a:
            return False            # 次一交易日K线尚未生成(未收盘), 留待下次
        hit = _verdict_hit(pred.get("verdict"), a["ret"], PRED_MODULES[module]["flat"])
        rec["actual"] = {"price": a["price"], "ret": a["ret"], "hit": bool(hit),
                         "actual_date": a.get("date"), "src": a.get("src", "kline")}
    elif module == "preopen_limitup":
        code = pred.get("qcode")
        if not code:
            return False
        # 同样三级数据源: 本地日线库 -> 在线日线 -> 实时行情(仅限预测当日收盘后)
        a = _day_pct_local(code, rec.get("date") or today_s) \
            or _day_pct(code, rec.get("date") or today_s) \
            or _live_day_pct(code, rec)
        if not a:
            return False            # 当日K线尚未生成(未收盘), 留待下次
        hit = a["ret"] >= LIMITUP_HIT_PCT
        rec["actual"] = {"price": a["price"], "ret": a["ret"], "hit": bool(hit),
                         "actual_date": a.get("date"), "src": a.get("src", "kline")}
    else:
        return False
    rec["verified"] = True
    rec["verified_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    return True



def verify_predictions():
    """扫描待回测记录: 已到期的抓真实结果回填, 然后重算统计。"""
    try:
        now = beijing_now()
        now_s = now.strftime("%Y-%m-%d %H:%M:%S")
        recs = _load_pred_log()
        changed, done = False, 0
        for r in recs:
            if r.get("verified"):
                continue
            va = r.get("verify_at")
            if not va or va > now_s:
                continue
            try:
                if _verify_one_pred(r):
                    changed = True
                    done += 1
            except Exception:
                traceback.print_exc()
        if changed:
            _save_pred_log(recs)
            _recompute_pred_stats()
        if done:
            print(f"[pred-verify] 回填 {done} 条预测结果", flush=True)
            # v3.10.1: 每次回填后, 对各模块重拟合概率校准(达到样本阈值才生效, 否则原样通过)
            for m in PRED_MODULES:
                try:
                    cb = _calib().fit_pred_calib(m)
                    if cb.get("ok"):
                        print(f"[pred-calib] {m} A={cb['A']} B={cb['B']} n={cb['n']} "
                              f"预测均值={cb['pred_mean']}% 实际={cb['actual_rate']}%", flush=True)
                except Exception:
                    traceback.print_exc()
            # v3.11.0: 每次回填后自动调参(阈值+权重), 达到样本阈值才生效, 否则显示待激活
            # 注: 仅对4个 P1 模块; gapup 在 verify_gapup_predictions 中单独触发(避免重复调参)
            for m in PRED_MODULES:
                try:
                    rt = auto_tune_module(m)
                    thr = rt.get("threshold", {})
                    if thr.get("status") != "待激活":
                        print(f"[pred-tune] {m} 阈值={thr.get('threshold')} "
                              f"F1={thr.get('f1')}(默认{thr.get('def_f1')}) n={thr.get('n')}", flush=True)
                    w = rt.get("weights", {})
                    if w.get("status") != "待激活":
                        print(f"[pred-tune] {m} 权重漂移={w.get('drift')} "
                              f"测试F1={w.get('f1_test')}(默认{w.get('f1_test_default')}) n={w.get('n')}", flush=True)
                except Exception:
                    traceback.print_exc()
        return done
    except Exception:
        traceback.print_exc()
        return 0



def _recompute_pred_stats():
    """按模块累计: 样本数/命中率/平均预测概率/校准偏差/分方向明细/最近明细。"""
    stats = {"updated_at": beijing_now().strftime("%Y-%m-%d %H:%M:%S"), "modules": {}}
    buckets = {m: [] for m in PRED_MODULES}
    for r in _load_pred_log():
        if not (r.get("verified") and r.get("actual")):
            continue
        m = r.get("module")
        if m in buckets:
            buckets[m].append(r)
    for m, rows in buckets.items():
        n = len(rows)
        # 观望(弱信号未判方向)为非方向性预测, 方向命中率只统计有明确方向的下注
        n_dir = sum(1 for r in rows if r["actual"].get("hit") is not None)
        ent = {"label": PRED_MODULES[m]["label"], "n": n_dir, "hit": 0,
               "hit_rate": None, "avg_pred": None, "avg_ret": None,
               "bias_pp": None, "by_verdict": {}, "recent": [], "calib": None}
        if n_dir:
            hits = sum(1 for r in rows if r["actual"].get("hit") is True)
            # v3.10.1: 概率校准状态(展示用); 平均预测概率本身已按校准参数对齐
            c = _PRED_CALIB.get(m)
            ent["calib"] = ({"n": c["n"], "B": round(c["B"], 3),
                             "applied": c.get("n", 0) >= PRED_MIN_CALIB_SAMPLES}
                            if c else {"n": 0, "applied": False})
            # v3.10.1: 平均预测概率按已拟合校准参数对齐, 让面板显示真实水平(校准前为原始虚高值)
            preds = [_apply_pred_calib(m, r["pred"].get("prob")) for r in rows
                     if isinstance(r["pred"].get("prob"), (int, float))]
            rets = [r["actual"].get("ret") for r in rows
                    if isinstance(r["actual"].get("ret"), (int, float))]
            ent["hit"] = hits
            ent["hit_rate"] = round(hits / n_dir, 4)
            if preds:
                mp = sum(preds) / len(preds)
                ent["avg_pred"] = round(mp, 2)
                # 校准偏差: 平均宣称概率 - 实际命中率。>0 表示系统性高估(过度自信)
                ent["bias_pp"] = round(mp - hits / n_dir * 100, 2)
            if rets:
                ent["avg_ret"] = round(sum(rets) / len(rets), 3)
            bv = {}
            for r in rows:
                v = r["pred"].get("verdict") or "-"
                b = bv.setdefault(v, {"n": 0, "hit": 0})
                b["n"] += 1
                b["hit"] += 1 if r["actual"].get("hit") else 0
            ent["by_verdict"] = {k: {"n": v["n"], "hit": v["hit"],
                                     "rate": (round(v["hit"] / v["n"], 4)
                                              if (v["n"] and k != "观望") else None)}
                                 for k, v in bv.items()}
            rows_sorted = sorted(rows, key=lambda x: x.get("verified_at", ""), reverse=True)
            ent["recent"] = [{"date": r.get("date"), "key": r.get("key"),
                              "verdict": r["pred"].get("verdict"),
                              "prob": round(_apply_pred_calib(m, r["pred"].get("prob")), 1),
                              "ret": r["actual"].get("ret"),
                              "hit": r["actual"].get("hit")} for r in rows_sorted[:12]]
        stats["modules"][m] = ent
    try:
        tmp = PRED_STATS + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        os.replace(tmp, PRED_STATS)
    except Exception:
        pass
    return stats



def load_pred_stats():
    if os.path.exists(PRED_STATS):
        try:
            return json.load(open(PRED_STATS, encoding="utf-8"))
        except Exception:
            pass
    return _recompute_pred_stats()


# ---------------- v3.10.1: 实时输出的概率校准(展示层, 不污染 STATE/落盘) ----------------
