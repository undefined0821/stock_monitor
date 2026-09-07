# -*- coding: utf-8 -*-
"""SQLite 存储层: 平台运行时数据(预测日志/扫描日志/回测统计/校准与调参参数/日线库等)
统一落 SQLite 单文件库, 替代原先分散的 JSON/JSONL 文件读写。

设计要点:
- 库文件 store.sqlite3 与代码同目录(部署环境属数据文件, 不随代码发布覆盖线上)。
  WAL 模式 + 单连接 + RLock 串行化 —— 本平台写频率低, 串行无瓶颈, 却彻底消除
  "读全量→改→tmp+os.replace 全量重写"在多线程下的交错覆盖风险。
- 三类数据三种表:
    kv          JSON 快照(统计/校准/调参/分类缓存)   -> get_json / set_json / del_json
    logs        逐条记录(预测日志/扫描日志)          -> get_log / save_log
    daily_bars  按日期一行的本地日线库               -> get_dates / save_dates
- 首次访问自动从遗留 JSON/JSONL 导入(每个键只导一次, meta.migrated 记标记),
  遗留文件保留不删, 作为回滚旧版代码时的数据快照。
- 小文件写库后同步镜像回原 JSON/JSONL(环境变量 STORE_MIRROR_JSON=0 可关),
  保证万一回退旧代码时数据无损; 日线库体积大(可达百MB)默认不镜像,
  需要时可用 export_dates_jsonl() 从库导出重建。
- db_version(): 任一写操作自增的数据版本号, 供内存缓存做失效判断
  (替代原先以"文件大小变化"为键的缓存机制)。
- 本模块只依赖标准库, 不 import 任何项目内模块, 任意层级引用均无循环导入风险。
"""

import os
import json
import sqlite3
import threading

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("STORE_DB") or os.path.join(BASE, "store.sqlite3")
MIRROR = os.environ.get("STORE_MIRROR_JSON", "1") not in ("0", "false", "False")

# 遗留文件 <-> kv 键映射(仅用于首次导入与镜像回写)
_LEGACY_KV = {
    "pred_stats": "pred_stats.json",
    "gapup_stats": "gapup_stats.json",
    "pred_calib": "pred_calib.json",
    "gapup_calib": "gapup_calib.json",
    "pred_tune": "pred_tune.json",
    "gapup_tuned": "gapup_weights_tuned.json",
    "classify_cache": "stock_classify.json",
}
_LEGACY_LOG = {
    "pred_log": "pred_log.jsonl",
    "gapup_log": "gapup_log.jsonl",
}
_LEGACY_DATES = ("daily_bars", "daily_bars.jsonl")

_lock = threading.RLock()
_conn = None
_dver = 0            # 数据版本号, 任一写操作 +1
_dates_ver = 0       # 日线库专用版本号, 仅 save_dates 时 +1(供选股池缓存作失效键)


def _connect():
    global _conn, _dver, _dates_ver
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.execute("CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS logs("
              "name TEXT NOT NULL, seq INTEGER NOT NULL, rec TEXT NOT NULL,"
              " PRIMARY KEY(name, seq))")
    c.execute("CREATE TABLE IF NOT EXISTS daily_bars("
              "date TEXT PRIMARY KEY, ts TEXT, bars TEXT NOT NULL)")
    c.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
    c.commit()
    row = c.execute("SELECT v FROM meta WHERE k='dver'").fetchone()
    _dver = int(row[0]) if row else 0
    row = c.execute("SELECT v FROM meta WHERE k='dates_ver'").fetchone()
    _dates_ver = int(row[0]) if row else 0
    return c


def conn():
    global _conn
    if _conn is None:
        with _lock:
            if _conn is None:
                _conn = _connect()
    return _conn


def _bump(c):
    """写操作后数据版本号 +1(同一事务内调用)。"""
    global _dver
    _dver += 1
    c.execute("INSERT INTO meta(k,v) VALUES('dver',?) "
              "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(_dver),))


def db_version():
    with _lock:
        conn()
        return _dver


def _bump_dates(c):
    """日线库写操作后专用版本号 +1(同一事务内调用)。"""
    global _dates_ver
    _dates_ver += 1
    c.execute("INSERT INTO meta(k,v) VALUES('dates_ver',?) "
              "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(_dates_ver),))


def dates_version():
    """日线库数据版本号: 仅 save_dates 使其自增。供内存缓存判断日线库是否变化,
    替代原先以 daily_bars.jsonl 文件大小为键的缓存机制。"""
    with _lock:
        conn()
        return _dates_ver


# ---------------- 遗留文件导入(每键一次) ----------------

def _migrated_keys(c):
    row = c.execute("SELECT v FROM meta WHERE k='migrated'").fetchone()
    try:
        return set(json.loads(row[0])) if row else set()
    except Exception:
        return set()


def _mark_migrated(c, key):
    done = _migrated_keys(c)
    done.add(key)
    c.execute("INSERT INTO meta(k,v) VALUES('migrated',?) "
              "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
              (json.dumps(sorted(done)),))


def _read_legacy_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _read_legacy_jsonl(path):
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                out.append(json.loads(ln))
            except Exception:
                continue
    return out


def _auto_migrate_kv(c, key):
    if key not in _LEGACY_KV:
        return
    if key in _migrated_keys(c):
        return
    path = os.path.join(BASE, _LEGACY_KV[key])
    if os.path.exists(path):
        try:
            d = _read_legacy_json(path)
            c.execute("INSERT OR IGNORE INTO kv(k,v) VALUES(?,?)",
                      (key, json.dumps(d, ensure_ascii=False)))
            print(f"[store] 已导入遗留文件 {_LEGACY_KV[key]} -> kv:{key}", flush=True)
        except Exception as e:
            print(f"[store] 导入 {_LEGACY_KV[key]} 失败(跳过): {e}", flush=True)
    with c:
        _mark_migrated(c, key)


def _auto_migrate_log(c, name):
    if name not in _LEGACY_LOG:
        return
    if name in _migrated_keys(c):
        return
    path = os.path.join(BASE, _LEGACY_LOG[name])
    n = 0
    if os.path.exists(path):
        try:
            recs = _read_legacy_jsonl(path)
            c.executemany("INSERT OR IGNORE INTO logs(name,seq,rec) VALUES(?,?,?)",
                          [(name, i, json.dumps(r, ensure_ascii=False))
                           for i, r in enumerate(recs)])
            n = len(recs)
            print(f"[store] 已导入遗留文件 {_LEGACY_LOG[name]} -> logs:{name}({n}条)", flush=True)
        except Exception as e:
            print(f"[store] 导入 {_LEGACY_LOG[name]} 失败(跳过): {e}", flush=True)
    with c:
        _mark_migrated(c, name)


def _auto_migrate_dates(c):
    key = _LEGACY_DATES[0]
    if key in _migrated_keys(c):
        return
    path = os.path.join(BASE, _LEGACY_DATES[1])
    n = 0
    if os.path.exists(path):
        try:
            recs = _read_legacy_jsonl(path)
            c.executemany("INSERT OR IGNORE INTO daily_bars(date,ts,bars) VALUES(?,?,?)",
                          [(r.get("date"), r.get("ts"),
                            json.dumps(r.get("bars") or {}, ensure_ascii=False))
                           for r in recs if r.get("date")])
            n = len(recs)
            print(f"[store] 已导入遗留文件 {_LEGACY_DATES[1]} -> daily_bars({n}天)", flush=True)
        except Exception as e:
            print(f"[store] 导入 {_LEGACY_DATES[1]} 失败(跳过): {e}", flush=True)
    with c:
        _mark_migrated(c, key)


def migrate_all():
    """启动时全量迁移(幂等): 把所有仍存在的遗留文件导入库。"""
    with _lock:
        c = conn()
        for k in _LEGACY_KV:
            _auto_migrate_kv(c, k)
        for k in _LEGACY_LOG:
            _auto_migrate_log(c, k)
        _auto_migrate_dates(c)


# ---------------- kv: JSON 快照 ----------------

def get_json(key, default=None):
    with _lock:
        c = conn()
        _auto_migrate_kv(c, key)
        row = c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
    if row is None:
        return default
    try:
        return json.loads(row[0])
    except Exception:
        return default


def set_json(key, obj, mirror=None):
    with _lock:
        c = conn()
        with c:
            c.execute("INSERT INTO kv(k,v) VALUES(?,?) "
                      "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                      (key, json.dumps(obj, ensure_ascii=False)))
            _bump(c)
    if mirror and MIRROR:
        _write_json_file(mirror, obj)


def del_json(key, mirror=None):
    with _lock:
        c = conn()
        with c:
            c.execute("DELETE FROM kv WHERE k=?", (key,))
            _bump(c)
    if mirror and os.path.exists(mirror):
        try:
            os.remove(mirror)
        except Exception:
            pass


# ---------------- logs: 逐条记录 ----------------

def get_log(name):
    with _lock:
        c = conn()
        _auto_migrate_log(c, name)
        rows = c.execute("SELECT rec FROM logs WHERE name=? ORDER BY seq",
                         (name,)).fetchall()
    out = []
    for (rec,) in rows:
        try:
            out.append(json.loads(rec))
        except Exception:
            continue
    return out


def save_log(name, recs, mirror=None):
    """整表替换式保存(与旧版 JSONL 全量重写语义一致, 但为单事务原子操作)。"""
    with _lock:
        c = conn()
        with c:
            c.execute("DELETE FROM logs WHERE name=?", (name,))
            c.executemany("INSERT INTO logs(name,seq,rec) VALUES(?,?,?)",
                          [(name, i, json.dumps(r, ensure_ascii=False, default=str))
                           for i, r in enumerate(recs)])
            _bump(c)
    if mirror and MIRROR:
        _write_jsonl_file(mirror, recs)


# ---------------- daily_bars: 按日期一行 ----------------

def get_dates():
    """返回按日期升序的日线库记录 [{'date','ts','bars':{mktcode:{o,h,l,c,v,amt}}}]。"""
    with _lock:
        c = conn()
        _auto_migrate_dates(c)
        rows = c.execute("SELECT date, ts, bars FROM daily_bars ORDER BY date").fetchall()
    out = []
    for d, ts, bars in rows:
        try:
            out.append({"date": d, "ts": ts, "bars": json.loads(bars)})
        except Exception:
            continue
    return out


def save_dates(recs):
    """整表替换式保存日线库(单事务原子)。recs: [{'date','ts','bars':{...}}]"""
    with _lock:
        c = conn()
        with c:
            c.execute("DELETE FROM daily_bars")
            c.executemany("INSERT OR REPLACE INTO daily_bars(date,ts,bars) VALUES(?,?,?)",
                          [(r.get("date"), r.get("ts"),
                            json.dumps(r.get("bars") or {}, ensure_ascii=False))
                           for r in recs if r.get("date")])
            _bump(c)
            _bump_dates(c)


def export_dates_jsonl(path=None):
    """从库导出日线库为旧版 JSONL(重建遗留文件用, 供回滚旧代码前的数据准备)。"""
    path = path or os.path.join(BASE, _LEGACY_DATES[1])
    recs = get_dates()
    _write_jsonl_file(path, recs)
    return path, len(recs)


# ---------------- 镜像回写(遗留文件保真, 供回滚) ----------------

def _write_json_file(path, obj):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except Exception:
        pass


def _write_jsonl_file(path, recs):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
        os.replace(tmp, path)
    except Exception:
        pass
