# -*- coding: utf-8 -*-
"""校准与调参层: 涨停/预测概率校准拟合、阈值与权重自动调参、调参结果落盘。由 app.py 拆分。"""

import json, re, math, time, threading, datetime, os, random, traceback, shutil, copy
import requests
from core import *
from market_data import *
from backtest import *
from config import *

__all__ = [
    '_load_gapup_calib',
    '_apply_gapup_calib',
    'fit_gapup_calib',
    '_load_pred_calib',
    '_apply_pred_calib',
    '_logit_mean_pred',
    'fit_pred_calib',
    '_clamp_threshold',
    '_rescore_idx',
    '_rescore_close_market',
    '_rescore_close_stock',
    '_rescore_preopen',
    '_threshold_metrics',
    '_collect_thr_samples',
    '_collect_w_samples',
    '_current_weights',
    '_default_weights',
    '_tune_threshold',
    '_tune_weights',
    '_tune_gapup',
    'auto_tune_module',
    'auto_tune_all',
    '_save_pred_tune',
    '_load_pred_tune',
    '_apply_pred_tune_one',
    '_apply_pred_tune',
    '__all__',
    'PRED_TUNE',
    'MIN_TUNE_THRESH',
    'MIN_TUNE_WEIGHT',
    '_PRED_TUNE',
    '_TUNE_W_DEFAULT',
    '_SCFG_W_DEFAULT',
    '_TUNE_SPEC',
    '_THR_FLOOR_UP',
    '_THR_CEIL_UP',
    '_RESCORE',
    '_MODULE_THRESHOLDS'
]

def _load_gapup_calib():
    """启动时加载已拟合的校准参数(A/B)。"""
    global _GAPUP_CALIB
    try:
        if os.path.exists(GAPUP_CALIB):
            with open(GAPUP_CALIB, "r", encoding="utf-8") as f:
                d = json.load(f)
            if d.get("A") is not None and d.get("B") is not None:
                _GAPUP_CALIB.update(d)
                print(f"[init] 已加载概率校准: A={d['A']} B={d['B']} n={d.get('n')}", flush=True)
    except Exception:
        pass

def _apply_gapup_calib(score, raw_prob):
    """把原始概率按 Platt scaling 校准; 未拟合/样本不足时原样返回。"""
    A, B = _GAPUP_CALIB.get("A"), _GAPUP_CALIB.get("B")
    if A is None or B is None or _GAPUP_CALIB.get("n", 0) < GAPUP_MIN_CALIB_SAMPLES:
        return raw_prob
    try:
        # 注意符号: sigmoid(z)=1/(1+exp(-z)), 与 gap_up_score 的 exp(-score/gu_sig) 同向,
        # 保证 score 越大概率越高(此前误写成 exp(z) 导致方向反转, 校准结果全错)。
        z = A * score + B
        if z > 60: z = 60.0
        if z < -60: z = -60.0
        return 1 / (1 + math.exp(-z)) * 100
    except Exception:
        return raw_prob

def fit_gapup_calib():
    """v3.10: 概率校准。固定 A=1/gu_sig(保持排序与尺度不变), 只用二分法求 B,
    使"平均预测概率" = "实际高开发生率"(先验平移校准)。

    为何不用完整两参数 Platt: 当前仅 20 样本, 两参数梯度下降会过拟合(A 爆炸到 54,
    预测全压向 0%)。固定 A 后, 校准只做 logit 平移, 不改变排序(不损失 AUC),
    只把概率整体搬到真实水平, 小样本下稳健得多。
    标签按当前阈值 GAPUP_MIN_GAP_PCT 重算, 保证与命中率口径一致。"""
    global _GAPUP_CALIB
    samples = []   # (score, y)
    for rec in _load_gapup_log():
        if not rec.get("verified"):
            continue
        actual = {a.get("code"): a for a in rec.get("actual", [])}
        for s in rec.get("stocks", []):
            a = actual.get(s.get("code"))
            if not a:
                continue
            hit = _actual_hit(a)          # 按当前阈值重算, 兼容旧记录
            if hit is None:
                continue                  # 开盘数据缺失
            sc = s.get("score")
            if sc is None:
                # 兼容旧记录: 从已保存的 prob 反推 score(用当时的 gu_sig)
                p = s.get("prob")
                if not p or not (0 < p < 100):
                    continue
                try:
                    sc = FCONFIG["gu_sig"] * math.log(p / (100.0 - p))
                except Exception:
                    continue
            samples.append((float(sc), 1.0 if hit else 0.0))
    n = len(samples)
    if n < GAPUP_MIN_CALIB_SAMPLES:
        return {"ok": False, "reason": f"样本不足({n}/{GAPUP_MIN_CALIB_SAMPLES}), 保持不校准", "n": n}
    A = 1.0 / max(0.1, FCONFIG.get("gu_sig", 3.0))
    actual_rate = sum(y for _, y in samples) / n
    if actual_rate <= 0.0 or actual_rate >= 1.0:
        return {"ok": False, "reason": f"样本单一(发生率{actual_rate:.2f}), 无法校准", "n": n}

    def _mean_pred(B):
        tot = 0.0
        for sc, _ in samples:
            z = max(-60.0, min(60.0, A * sc + B))
            tot += 1 / (1 + math.exp(-z))   # sigmoid: 与 gap_up_score 同向
        return tot / n

    # B 增大 -> 预测均值增大, 单调, 用二分法求使均值=actual_rate 的 B
    lo, hi = -30.0, 30.0
    if _mean_pred(lo) > actual_rate:
        B = lo
    elif _mean_pred(hi) < actual_rate:
        B = hi
    else:
        for _ in range(80):
            mid = (lo + hi) / 2.0
            if _mean_pred(mid) < actual_rate:
                lo = mid
            else:
                hi = mid
        B = (lo + hi) / 2.0
    _GAPUP_CALIB.update({"A": round(A, 6), "B": round(B, 6), "n": n,
                         "fitted_at": beijing_now().strftime("%Y-%m-%d %H:%M:%S")})
    try:
        tmp = GAPUP_CALIB + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_GAPUP_CALIB, f, ensure_ascii=False, indent=2)
        os.replace(tmp, GAPUP_CALIB)
    except Exception as e:
        print("[calib] 写入失败:", e, flush=True)
    return {"ok": True, "A": round(A, 6), "B": round(B, 6), "n": n,
            "pred_mean": round(_mean_pred(B) * 100, 2),
            "actual_rate": round(actual_rate, 4),
            "note": f"固定A={A:.3f}, 二分法求B={B:.3f}, 使预测均值≈实际发生率"}


# ─────────────────────────────────────────────────────────────────────────────
# v3.10.1: 通用预测概率校准(覆盖 4 个 P1 模块)
# 与 gapup 校准同一套哲学: 固定 A=1(纯 logit 平移), 只用二分法求 B, 使"平均预测概率"
# = "实际命中率"。固定 A 不改变排序(不损失 AUC), 只把概率整体搬到真实水平, 小样本稳健。
# 输入是各模块已输出的 prob(0~100), 经 logit 变换后在 logit 空间做平移校准。
# 关键: 拟合用的是 pred_log 中**原始未校准**的 prob(落盘时不套校准), 因此校准→验证→再拟合
# 不会形成反馈漂移(验证回填读的是原始 prob)。
# ─────────────────────────────────────────────────────────────────────────────
def _load_pred_calib():
    """启动时加载各模块的预测概率校准参数(A/B)。"""
    global _PRED_CALIB
    try:
        if os.path.exists(PRED_CALIB):
            with open(PRED_CALIB, "r", encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict):
                for k, v in d.items():
                    if isinstance(v, dict) and v.get("A") is not None and v.get("B") is not None:
                        _PRED_CALIB[k] = v
                if _PRED_CALIB:
                    _summary = ", ".join("{}(n={})".format(k, v.get("n")) for k, v in _PRED_CALIB.items())
                    print(f"[init] 已加载预测校准: {_summary}", flush=True)
    except Exception:
        pass


def _apply_pred_calib(module, prob):
    """把模块的预测概率按 Platt scaling 校准; 未拟合/样本不足时原样返回。"""
    c = _PRED_CALIB.get(module)
    if not c or c.get("n", 0) < PRED_MIN_CALIB_SAMPLES:
        return prob
    try:
        p = max(1e-6, min(0.999999, float(prob) / 100.0))
        lo = math.log(p / (1.0 - p))            # logit 变换
        A, B = c["A"], c["B"]
        # v3.11.7: 截距/斜率护栏 —— 防止少样本拟合出极端校准参数把概率整体压垮
        # (详见 _CALIB_MAX_ABS_B 处注释)。校准失败风险远小于"概率失真"的风险。
        B = max(-_CALIB_MAX_ABS_B, min(_CALIB_MAX_ABS_B, float(B)))
        A = max(_CALIB_A_RANGE[0], min(_CALIB_A_RANGE[1], float(A)))
        z = max(-60.0, min(60.0, A * lo + B))
        return 1.0 / (1.0 + math.exp(-z)) * 100
    except Exception:
        return prob


def _logit_mean_pred(samples, B, A=1.0):
    """samples: [(prob, _), ...]; 返回经 (A*logit+B) 校准后的平均概率。"""
    tot = 0.0
    for pr, _ in samples:
        p = max(1e-6, min(0.999999, float(pr) / 100.0))
        lo = math.log(p / (1.0 - p))
        z = max(-60.0, min(60.0, A * lo + B))
        tot += 1.0 / (1.0 + math.exp(-z))
    return tot / len(samples) if samples else 0.0


def fit_pred_calib(module):
    """对某模块拟合概率校准: 固定 A=1, 二分 B 使平均校准概率 = 实际命中率。"""
    global _PRED_CALIB
    if module not in PRED_MODULES:
        return {"ok": False, "reason": "未知模块", "module": module}
    samples = []   # (prob, y)
    for rec in _load_pred_log():
        if rec.get("module") != module or not rec.get("verified"):
            continue
        act = rec.get("actual") or {}
        hit = act.get("hit")
        if hit is None:
            continue                              # 过期/无实际结果, 跳过
        pr = (rec.get("pred") or {}).get("prob")
        if not isinstance(pr, (int, float)) or not (0 < pr < 100):
            continue
        samples.append((float(pr), 1.0 if hit else 0.0))
    n = len(samples)
    if n < PRED_MIN_CALIB_SAMPLES:
        return {"ok": False, "reason": f"样本不足({n}/{PRED_MIN_CALIB_SAMPLES}), 暂不校准",
                "module": module, "n": n}
    actual_rate = sum(y for _, y in samples) / n
    if actual_rate <= 0.0 or actual_rate >= 1.0:
        return {"ok": False, "reason": f"样本单一(发生率{actual_rate:.2f}), 无法校准",
                "module": module, "n": n}
    A = 1.0
    lo, hi = -30.0, 30.0
    if _logit_mean_pred(samples, lo, A) > actual_rate:
        B = lo
    elif _logit_mean_pred(samples, hi, A) < actual_rate:
        B = hi
    else:
        for _ in range(80):
            mid = (lo + hi) / 2.0
            if _logit_mean_pred(samples, mid, A) < actual_rate:
                lo = mid
            else:
                hi = mid
        B = (lo + hi) / 2.0
    _PRED_CALIB[module] = {"A": round(A, 6), "B": round(B, 6), "n": n,
                           "fitted_at": beijing_now().strftime("%Y-%m-%d %H:%M:%S")}
    try:
        tmp = PRED_CALIB + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_PRED_CALIB, f, ensure_ascii=False, indent=2)
        os.replace(tmp, PRED_CALIB)
    except Exception as e:
        print("[pred-calib] 写入失败:", e, flush=True)
    return {"ok": True, "module": module, "A": round(A, 6), "B": round(B, 6), "n": n,
            "pred_mean": round(_logit_mean_pred(samples, B, A) * 100, 2),
            "actual_rate": round(actual_rate * 100, 2),
            "note": f"固定A={A:.3f}, 二分法求B={B:.3f}, 使预测均值≈实际发生率"}


# ═══════════════════════════════════════════════════════════════════════════
# v3.11.0 自动调参引擎(按实盘表现调权重/阈值, 真正提升命中率)
# ─────────────────────────────────────────────────────────────────────────────
# 与 v3.10 校准的本质区别:
#   校准(Platt)只让"概率数字"变诚实 —— 固定 A=1 纯平移, 不改排序、不提升准确度。
#   本引擎改"打分权重"与"决策阈值" —— 改变排序与报警边界, 从而提升真实命中率。
# 防过拟合三件套: ① 时间切分留出验证 ② L2 正则回拉默认权重 ③ 最小样本门控。
# 权重自调依赖预测时落盘的特征(feats); 阈值自调仅用已存的 prob+ret, 无需特征。
# ═══════════════════════════════════════════════════════════════════════════
PRED_TUNE = f"{BASE}/pred_tune.json"
MIN_TUNE_THRESH = 15      # 阈值自调最小样本(单参数, 门槛较低)
MIN_TUNE_WEIGHT = 50      # 权重自调最小样本(参数多, 需更多样本防过拟合)
_PRED_TUNE = {}           # 运行时调参结果(启动加载)

# 各模块权重默认值(与 _FORECAST_DEFAULTS / SCFG 默认严格一致, 供 L2 正则回拉)
_TUNE_W_DEFAULT = {
    "idx_pct_w": 2.2, "idx_late_w": 1.8, "idx_pos_w": 2.0, "idx_vr_w": 1.0,
    "idx_wb_w": 1.5, "idx_breadth_w": 3.0, "idx_retail_w": 0.8, "idx_sig": 6.0,
    "cl_sh_w": 1.8, "cl_cyb_w": 1.0, "cl_sec_w": 1.2, "cl_breadth_w": 6.0,
    "cl_retail_w": 1.0, "cl_late_w": 2.5, "cl_sig": 6.0,
    "stk_sh_w": 1.5, "stk_cyb_w": 1.0, "stk_sec_w": 1.2, "stk_yin_w": 1.5,
    "stk_amt_w": 0.5, "stk_posmag": 1.5, "stk_pnl_pos": 0.8, "stk_pnl_neg": 0.8,
    "stk_breadth_w": 3.0, "stk_retail_w": 0.6, "stk_late_w": 1.2, "stk_sig": 5.0,
}
_SCFG_W_DEFAULT = {
    "limitup_weight": 0.6, "vr_weight": 1.5, "pct_weight": 0.8, "weibi_weight": 2.0,
    "fv_optimal_bonus": 5.0, "fv_large_thresh": 100, "fv_large_penalty": -30.0,
    "fv_mid_penalty": -5.0, "yao_consec_bonus": 15, "yao_consec_cap": 45,
    "yao_smallcap_thresh": 30, "yao_smallcap_bonus": 5.0, "resonance_scale": 0.1,
    "sig_scale": 6.0,
}
_TUNE_SPEC = {
    "idx_1h": {"kind": "fcfg", "wkeys": [k for k in _TUNE_W_DEFAULT if k.startswith("idx_")],
               "feats": ["pct", "late", "pos", "vr", "wb", "breadth", "retail"],
               "def_thr": 58, "pos": "up"},
    "close_market": {"kind": "fcfg", "wkeys": [k for k in _TUNE_W_DEFAULT if k.startswith("cl_")],
               "feats": ["sh_pct", "cyb_pct", "sector_avg", "breadth", "retail", "late"],
               "def_thr": 58, "pos": "up"},
    "close_stock": {"kind": "fcfg", "wkeys": [k for k in _TUNE_W_DEFAULT if k.startswith("stk_")],
               "feats": ["sh_pct", "cyb_pct", "sec_pct", "pct_pos", "lower", "upper",
                         "turnover", "pnl_pct", "breadth", "retail_pct", "late"],
               "def_thr": 60, "pos": "up"},
    "preopen_limitup": {"kind": "scfg", "wkeys": list(_SCFG_W_DEFAULT.keys()),
               "feats": ["dist_limit_up", "vr", "pct", "wb", "fmv", "yao", "yao_days", "resonance"],
               "def_thr": 50, "pos": "limitup"},
    # v3.11.1: 尾盘高开潜力(gapup) 接入自动权重调参。评分=排名问题(以 AUC 衡量判别力),
    # 无二元决策阈值; 调权沿用 optimize_gapup_weights(坐标上升 + AUC目标 + L2正则 + 校准惩罚), 落盘 GAPUP_TUNED。
    "gapup": {"kind": "gapup", "wkeys": ["gu_pos_w", "gu_parab_w", "gu_wb_w", "gu_vr_w", "gu_to_w",
              "gu_latepull_w", "gu_breadth_w", "gu_retail_w", "gu_idxlate_w", "gu_parab_peak", "gu_sig"],
              "pos": "gapup", "def_thr": None, "feats": None},
}

# v3.11.7: 方向型预测(pos="up")的阈值语义护栏。
# prob 的语义是"上涨概率", 判定看涨的阈值必须显著高于 50 —— 否则会出现
# 「上涨概率47%却判定看涨」这种概率与方向自相矛盾的结果。
# (实测: 自动调参仅用15条样本就把 idx_1h 阈值从默认58拉到45, 触发了该问题。)
# 涨停预测(pos="limitup")属稀有事件, 阈值低于50是合理的, 不受此约束。
_THR_FLOOR_UP = 52      # 方向型阈值下限(须>50, 留2pp噪声余量)
_THR_CEIL_UP = 85       # 方向型阈值上限(过高则几乎永不触发, 同样无意义)


def _clamp_threshold(module, thr):
    """把阈值收敛到语义合法区间(仅方向型 pos="up" 受限, 其余模块原样返回)。"""
    try:
        t = int(round(float(thr)))
    except (TypeError, ValueError):
        return thr
    spec = _TUNE_SPEC.get(module) or {}
    if spec.get("pos") != "up":
        return t
    return max(_THR_FLOOR_UP, min(_THR_CEIL_UP, t))


def _rescore_idx(f, w):
    s = (f["pct"] * w["idx_pct_w"] + f["late"] * w["idx_late_w"]
         + (f["pos"] - 0.5) * w["idx_pos_w"] + (f["vr"] - 1) * w["idx_vr_w"]
         + (f["wb"] / 100.0) * w["idx_wb_w"] + (f["breadth"] - 0.5) * w["idx_breadth_w"]
         + f["retail"] * w["idx_retail_w"])
    return 1 / (1 + math.exp(-s / w["idx_sig"])) * 100


def _rescore_close_market(f, w):
    s = (f["sh_pct"] * w["cl_sh_w"] + f["cyb_pct"] * w["cl_cyb_w"]
         + f["sector_avg"] * w["cl_sec_w"] + (f["breadth"] - 0.5) * w["cl_breadth_w"]
         + f["retail"] * w["cl_retail_w"] + f["late"] * w["cl_late_w"])
    return 1 / (1 + math.exp(-s / w["cl_sig"])) * 100


def _rescore_close_stock(f, w):
    c1 = f["sh_pct"] * w["stk_sh_w"] + f["cyb_pct"] * w["stk_cyb_w"]
    c2 = f["sec_pct"] * w["stk_sec_w"]
    c3 = w["stk_posmag"] if f["pct_pos"] >= 0 else -w["stk_posmag"]
    c4 = (f["lower"] - f["upper"]) * w["stk_yin_w"]
    c5 = w["stk_amt_w"] if f["turnover"] >= 10 else 0
    c6 = -w["stk_pnl_neg"] if f["pnl_pct"] > 8 else (w["stk_pnl_pos"] if f["pnl_pct"] < -8 else 0)
    c7 = (f["breadth"] - 0.5) * w["stk_breadth_w"]
    c8 = f["retail_pct"] * w["stk_retail_w"]
    c9 = f["late"] * w["stk_late_w"]
    return 1 / (1 + math.exp(-(c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9) / w["stk_sig"])) * 100


def _rescore_preopen(f, w):
    s = 0.0
    s += (15 - f["dist_limit_up"]) * w["limitup_weight"]
    vr = f["vr"] if 0 < f["vr"] <= 10 else 1
    s += (vr - 1) * w["vr_weight"]
    s += min(f["pct"], 9) * w["pct_weight"]
    wb = f["wb"] if 0 <= f["wb"] <= 100 else 0
    s += (wb / 100.0) * w["weibi_weight"]
    fmv = f["fmv"] or 50
    if 30 <= fmv <= 80:
        s += w["fv_optimal_bonus"]
    elif fmv > w.get("fv_large_thresh", 100):
        if not f["yao"]:
            s += w["fv_large_penalty"]
    elif fmv > 80:
        s += w["fv_mid_penalty"]
    if f["yao"]:
        yd = f["yao_days"] or 0
        s += min(yd * w["yao_consec_bonus"], w["yao_consec_cap"])
        if fmv < w.get("yao_smallcap_thresh", 30):
            s += w["yao_smallcap_bonus"]
    s += (f.get("resonance") or 0) * w["resonance_scale"]
    return 1 / (1 + math.exp(-s / w["sig_scale"])) * 100


_RESCORE = {"idx_1h": _rescore_idx, "close_market": _rescore_close_market,
            "close_stock": _rescore_close_stock, "preopen_limitup": _rescore_preopen}

# 决策阈值(verdict 边界), 默认与线上硬编码一致; 自动调参后覆盖
_MODULE_THRESHOLDS = {m: _TUNE_SPEC[m]["def_thr"] for m in _TUNE_SPEC}


def _threshold_metrics(probs, rets, T, pos_kind):
    """阈值 T 下的精确率/召回率/F1。pos_kind='up'->实际涨(ret>0)为正; 'limitup'->涨停为正。"""
    act_pos = (lambda r: r > 0) if pos_kind == "up" else (lambda r: r >= LIMITUP_HIT_PCT)
    ys, ps = [], []
    for p, r in zip(probs, rets):
        ys.append(1 if act_pos(r) else 0)
        ps.append(1 if p >= T else 0)
    if not ys:
        return (0.0, 0.0, 0.0, 0)
    tp = sum(1 for y, p in zip(ys, ps) if y == 1 and p == 1)
    fp = sum(1 for y, p in zip(ys, ps) if y == 0 and p == 1)
    fn = sum(1 for y, p in zip(ys, ps) if y == 1 and p == 0)
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    return (prec, rec, f1, tp + fp)


def _collect_thr_samples(module):
    out = []
    for r in _load_pred_log():
        if r.get("module") != module or not r.get("verified"):
            continue
        a = r.get("actual") or {}
        ret = a.get("ret")
        if ret is None:
            continue
        p = (r.get("pred") or {}).get("prob")
        if not isinstance(p, (int, float)) or not (0 < p < 100):
            continue
        out.append((float(p), float(ret)))
    return out


def _collect_w_samples(module):
    out = []
    for r in _load_pred_log():
        if r.get("module") != module or not r.get("verified"):
            continue
        a = r.get("actual") or {}
        ret = a.get("ret")
        if ret is None:
            continue
        f = (r.get("pred") or {}).get("feats")
        if not isinstance(f, dict):
            continue
        out.append((f, float(ret)))
    return out


def _current_weights(module, spec):
    src = FCONFIG if spec["kind"] == "fcfg" else SCFG
    default = _TUNE_W_DEFAULT if spec["kind"] == "fcfg" else _SCFG_W_DEFAULT
    return {k: float(src.get(k, default[k])) for k in spec["wkeys"]}


def _default_weights(module, spec):
    """v3.11.4: 出厂默认权重。与 _current_weights 的区别: 后者读 FCONFIG/SCFG,
    会被历次调参结果写回覆盖; 本函数始终返回硬编码出厂值, 不受调参历史影响。"""
    default = _TUNE_W_DEFAULT if spec["kind"] == "fcfg" else _SCFG_W_DEFAULT
    return {k: float(default[k]) for k in spec["wkeys"]}


def _tune_threshold(module, samples, spec):
    """网格搜最优报警阈值, 目标 F1; 预测为正太少则惩罚防退化解; L2 回拉默认阈值。"""
    probs = [p for p, _ in samples]
    rets = [r for _, r in samples]
    def_thr, pos = spec["def_thr"], spec["pos"]
    lo, hi = (_THR_FLOOR_UP, _THR_CEIL_UP) if pos == "up" else (30, 90)
    best = None
    for T in range(lo, hi + 1, 5):
        prec, rec, f1, np_ = _threshold_metrics(probs, rets, T, pos)
        score = f1 - (0.5 if np_ < 3 else 0.0) - 0.03 * abs(T - def_thr) / def_thr
        if best is None or score > best[0]:
            best = (score, T, prec, rec, f1, np_)
    _, T, prec, rec, f1, np_ = best
    dp, dr, df1, _ = _threshold_metrics(probs, rets, def_thr, pos)
    return {"threshold": T, "f1": round(f1, 4), "prec": round(prec, 4),
            "rec": round(rec, 4), "def_f1": round(df1, 4), "def_thr": def_thr,
            "n_pos_pred": np_,
            "lift": round(f1 - df1, 4)}


def _tune_weights(module, samples, spec):
    """正则化坐标上升调权重, 目标=留出验证集 F1(每层配最优阈值); L2 回拉默认防过拟合。

    v3.11.3 修复「迭代累积漂移」: 搜索起点与 L2 锚点由"上次调优结果"改为"出厂默认权重"。
    原实现 W0 = _current_weights() 读的是 FCONFIG/SCFG, 而调参结果会写回 FCONFIG/SCFG,
    于是每次都在上次结果上继续迭代 —— 权重逐次累积漂移, 且 L2 的锚点本身就是上次值,
    正则只惩罚"相对上次的抖动", 不再把权重拉回出厂默认, 早期小样本的噪声会被锁进基线。
    改用 _default_weights() 后, 每次都是「出厂默认 + 当前全量样本」独立重搜(与 gapup 同款),
    保留持续优化能力的同时杜绝漂移累积。
    """
    rescore = _RESCORE[module]
    W0 = _default_weights(module, spec)     # v3.11.4: 出厂默认(搜索起点 + L2 锚点)
    k = max(1, int(len(samples) * 0.2))
    train = samples[:-k] if len(samples) > 5 else samples
    test = samples[-k:] if len(samples) > 5 else samples

    def f1_at(w, split):
        probs = [rescore(f, w) for f, _ in split]
        rets = [r for _, r in split]
        lo, hi = (45, 85) if spec["pos"] == "up" else (30, 90)
        best = 0.0
        for TT in range(lo, hi + 1, 5):       # 内层: 该权重集下的最优阈值
            _, _, ff, _ = _threshold_metrics(probs, rets, TT, spec["pos"])
            best = max(best, ff)
        return best

    w = dict(W0)
    lam = 0.03
    facs = [0.6, 0.8, 1.0, 1.25, 1.6, 2.0]
    sig_facs = [0.7, 0.85, 1.0, 1.18, 1.4]
    for _ in range(4):
        improved = False
        for key in spec["wkeys"]:
            is_sig = key.endswith("_sig") or key == "sig_scale"
            cand_facs = sig_facs if is_sig else facs
            base = w[key]
            reg0 = lam * ((base - W0[key]) / max(0.1, abs(W0[key]))) ** 2
            best_score = f1_at(w, train) - reg0
            best_val = base
            for fac in cand_facs:
                cand = base * fac
                if is_sig and cand <= 0.5:
                    continue
                old = w[key]
                w[key] = cand
                reg = lam * ((w[key] - W0[key]) / max(0.1, abs(W0[key]))) ** 2
                sc = f1_at(w, train) - reg
                if sc > best_score + 1e-9:
                    best_score = sc
                    best_val = cand
                    improved = True
                w[key] = old
            w[key] = best_val
        if not improved:
            break
    w_now = _current_weights(module, spec)
    diff = {k: round(v, 4) for k, v in w.items() if abs(v - w_now[k]) > 1e-6}
    return (w, round(f1_at(w, train), 4), round(f1_at(w, test), 4),
            round(f1_at(W0, test), 4), round(sum(abs(w[k] - w_now[k]) for k in w) / len(w), 4), diff)


def _tune_gapup():
    """v3.11.1: 包装 optimize_gapup_weights, 把结果映射进统一自动调参结构 _PRED_TUNE['gapup']。
    gapup 是排名问题(下个交易日高开概率), 以 AUC 衡量判别力, 无二元决策阈值。
    复用既有 optimize_gapup_weights(坐标上升 + AUC目标 + L2正则 + 校准惩罚), 落盘 GAPUP_TUNED。"""
    res = optimize_gapup_weights()
    out = {"module": "gapup",
           "fitted_at": beijing_now().strftime("%Y-%m-%d %H:%M:%S"),
           "threshold": {"status": "排名推荐(无二元阈值)"}}
    if res.get("ok"):
        before, after = res.get("before") or {}, res.get("after") or {}
        diff = {k: round(after.get(k, before.get(k, 0)) - before.get(k, 0), 4)
                for k in after if abs(after.get(k, 0) - before.get(k, 0)) > 1e-6}
        out["weights"] = {"n": res.get("samples"),
                          "auc_before": res.get("auc_before"), "auc_after": res.get("auc_after"),
                          "drift": round(sum(abs(after.get(k, 0) - before.get(k, 0)) for k in after)
                                         / max(1, len(after)), 4),
                          "values": diff}
    else:
        out["weights"] = {"n": res.get("samples", 0), "status": "待激活",
                          "need": GAPUP_MIN_OPT_SAMPLES}
    return out


def auto_tune_module(module):
    """对单模块做阈值+权重自调, 落盘并立即应用。样本不足则返回'待激活'。"""
    if module not in _TUNE_SPEC:
        return {"ok": False, "reason": "未纳入自动调参"}
    spec = _TUNE_SPEC[module]
    # v3.11.1: gapup 是排名问题(高开概率), 用 AUC 衡量判别力、无阈值, 走专用调权分支
    if spec.get("kind") == "gapup":
        res = _tune_gapup()
        _PRED_TUNE[module] = res
        _save_pred_tune()
        _apply_pred_tune_one(module)   # 重载 GAPUP_TUNED 到实时覆盖权重, 立即可见
        return res
    res = {"module": module, "fitted_at": beijing_now().strftime("%Y-%m-%d %H:%M:%S")}
    # —— 阈值自调 ——
    ts = _collect_thr_samples(module)
    n_thr = len(ts)
    if n_thr >= MIN_TUNE_THRESH:
        try:
            t = _tune_threshold(module, ts, spec)
            t["n"] = n_thr
            res["threshold"] = t
        except Exception:
            traceback.print_exc()
    else:
        res["threshold"] = {"n": n_thr, "status": "待激活", "need": MIN_TUNE_THRESH}
    # —— 权重自调(需特征) ——
    ws = _collect_w_samples(module)
    n_w = len(ws)
    if n_w >= MIN_TUNE_WEIGHT:
        try:
            w, tf, tef, df, drift, diff = _tune_weights(module, ws, spec)
            res["weights"] = {"n": n_w, "f1_train": tf, "f1_test": tef,
                              "f1_test_default": df, "drift": drift, "values": diff}
        except Exception:
            traceback.print_exc()
    else:
        res["weights"] = {"n": n_w, "status": "待激活", "need": MIN_TUNE_WEIGHT}
    _PRED_TUNE[module] = res
    _save_pred_tune()
    _apply_pred_tune_one(module)
    return res


def auto_tune_all():
    out = {}
    for m in _TUNE_SPEC:
        try:
            out[m] = auto_tune_module(m)
        except Exception:
            traceback.print_exc()
    return out


def _save_pred_tune():
    try:
        tmp = PRED_TUNE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_PRED_TUNE, f, ensure_ascii=False, indent=2)
        os.replace(tmp, PRED_TUNE)
    except Exception:
        pass


def _load_pred_tune():
    global _PRED_TUNE
    try:
        if os.path.exists(PRED_TUNE):
            d = json.load(open(PRED_TUNE, encoding="utf-8"))
            if isinstance(d, dict):
                _PRED_TUNE = d
    except Exception:
        pass


def _apply_pred_tune_one(module):
    """把单模块已保存的调参结果覆盖到 FCONFIG/SCFG/决策阈值。"""
    res = _PRED_TUNE.get(module)
    if not res:
        return
    spec = _TUNE_SPEC[module]
    # v3.11.1: gapup 调权结果落在 GAPUP_TUNED, 通过实时覆盖权重 GAPUP_WEIGHT_OVERRIDE 生效(无需重启)
    if spec.get("kind") == "gapup":
        global GAPUP_WEIGHT_OVERRIDE
        if os.path.exists(GAPUP_TUNED):
            try:
                GAPUP_WEIGHT_OVERRIDE = {k: float(v)
                                         for k, v in json.load(open(GAPUP_TUNED, encoding="utf-8")).items()}
            except Exception:
                GAPUP_WEIGHT_OVERRIDE = None
        else:
            GAPUP_WEIGHT_OVERRIDE = None
        return
    thr = (res.get("threshold") or {}).get("threshold")
    if isinstance(thr, (int, float)):
        # v3.11.7: 应用前先做语义钳制, 修正历史已落盘的不合法阈值(如 idx_1h 的 45)
        _MODULE_THRESHOLDS[module] = _clamp_threshold(module, thr)
    wv = (res.get("weights") or {}).get("values")
    if isinstance(wv, dict) and wv:
        dst = FCONFIG if spec["kind"] == "fcfg" else SCFG
        for k, v in wv.items():
            dst[k] = float(v)


def _apply_pred_tune():
    """启动加载并应用全部已保存调参结果(权重/阈值)。"""
    _load_pred_tune()
    applied = []
    for m in _TUNE_SPEC:
        before = _MODULE_THRESHOLDS.get(m)
        _apply_pred_tune_one(m)
        if _MODULE_THRESHOLDS.get(m) != before:
            applied.append(f"{m}:阈值->{_MODULE_THRESHOLDS[m]}")
    if applied:
        print("[init] 已应用自动调参(阈值):", applied, flush=True)


