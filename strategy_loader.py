# -*- coding: utf-8 -*-
"""选股策略加载器。

- 策略源以受保护形式存放于数据文件, 运行时载入并导出 ma / skdj_series / pool_eval 三个纯函数;
- 主密钥不以明文存放, 需逆向本模块才能还原;
- 仅依赖 Python 标准库, 跨平台可移植。
"""
import os, zlib as _c, hashlib as _h, hmac as _m

HERE = os.path.dirname(os.path.abspath(__file__))
BLOB = os.path.join(HERE, "strategy_pool.blob")

# ---- 受保护的主密钥数据(勿外泄)。 ----
_ENC_KEY_HEX = ("4764324277304264394f68384463385864294063205960254274234a642a553c"
                "16131211641717091b5f07524b485a4e4245411e585a594715525954484e135d37")
_SALT = b"strategy-pool::loader-v1"

_cache = None


def _recover_key():
    enc = bytes.fromhex(_ENC_KEY_HEX)
    rev = bytes(b ^ (i % 251) for i, b in enumerate(enc))
    return rev[::-1]


def _derive():
    return _m.new(_recover_key(), _SALT, _h.sha256).digest()


def _stream(key, length):
    out = b""
    ctr = 0
    while len(out) < length:
        out += _h.sha256(key + ctr.to_bytes(8, "big")).digest()
        ctr += 1
    return out[:length]


def _unpack():
    if not os.path.exists(BLOB):
        return None
    blob = open(BLOB, "rb").read()
    key = _derive()
    ks = _stream(key, len(blob))
    comp = bytes(a ^ b for a, b in zip(blob, ks))
    try:
        return _c.decompress(comp)
    except Exception:
        return None


def load():
    """返回 (ma, skdj_series, pool_eval); 载入失败回退 None。"""
    global _cache
    if _cache is not None:
        return _cache
    src = _unpack()
    if not src:
        _cache = None
        return None
    ns = {}
    try:
        code = compile(src, "<strategy_pool>", "exec")
        exec(code, ns)
        _cache = (ns["ma"], ns["skdj_series"], ns["pool_eval"])
    except Exception:
        _cache = None
    return _cache
