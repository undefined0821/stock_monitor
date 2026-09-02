# -*- coding: utf-8 -*-
"""行情数据层: 腾讯实时行情拉取/解析、分时、K线。由 app.py 拆分。"""

import json, re, math, time, threading, datetime, os, random, copy
import requests
from concurrent.futures import ThreadPoolExecutor
from core import *

__all__ = ['UNIVERSE', '_fetch_chunk', '_fetch_kline', '_fetch_pool', '_tencent_session', 'fetch_minute', 'fetch_tencent', 'load_universe', 'parse_row']
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
        "vol": num(g(F["vol"])),             # 成交量(手), v3.10 供本地日线库落库
    }



def fetch_minute(code, ttl=None):
    """腾讯分时数据, 返回 [{'t':'HHMM','p':price},...]
    v3.0: 流式读取+整体超时, 防止分时接口挂起阻塞。
    v3.10: 加 TTL 缓存(默认 MINUTE_CACHE_TTL 秒), 供高频刷新复用, 降低接口压力。
           抓取失败时回退到上一次的缓存结果(若有), 避免瞬时故障导致图表/特征丢失。"""
    ttl = MINUTE_CACHE_TTL if ttl is None else ttl
    now_ts = time.time()
    cached = _MINUTE_CACHE.get(code)
    if ttl > 0 and cached and (now_ts - cached[0]) < ttl:
        return cached[1]
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
        if out:
            _MINUTE_CACHE[code] = (now_ts, out)
            return out
    except Exception:
        pass
    # 抓取失败: 回退旧缓存
    return cached[1] if cached else []



def _fetch_kline(code, days=12, include_today=False):
    """返回最近 days 个交易日的日K列表(前复权), 每项 {date,open,close,high,low}; 失败返回 []。

    用于妖股检测: 数截至昨日的连续涨停天数(连板基因)。
    include_today=True 时**保留当日(盘中未收盘)那根K线**, 供选股池当日择时参考——
    盘中该根的 close 即最新价, 属于实时未完成K线, 仅供当日择时参考。
    默认 False(排除当日)以保持妖股检测"截至昨日"的原有语义。
    """
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
                if b[0] == end and not include_today:   # 默认排除当日(盘中不完整)
                    continue
                out.append({"date": b[0], "open": float(b[1]), "close": float(b[2]),
                            "high": float(b[3]), "low": float(b[4])})
            except (ValueError, IndexError, TypeError):
                continue
        return out
    except Exception:
        return []


