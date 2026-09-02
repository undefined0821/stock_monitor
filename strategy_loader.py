# -*- coding: utf-8 -*-
"""选股策略加载器: 运行时从加密载荷 strategy_pool.blob 内存解密出策略源码并动态加载。

- 策略明文(strategy_pool.py)不随服务部署, 只部署本 loader + 加密 blob;
- 主密钥不以明文存放, 以"反转+XOR"混淆字节存储, 需逆向 loader 才能还原;
- 解密后在进程内存里 exec, 暴露 ma / skdj_series / pool_eval 三个纯函数。

依赖: 仅标准库(zlib/hashlib/hmac), 跨平台可移植, 无第三方依赖。
"""
import os, zlib, hashlib, hmac

HERE = os.path.dirname(os.path.abspath(__file__))
BLOB = os.path.join(HERE, "strategy_pool.blob")

# ---- 混淆后的主密钥字节(反转 + 与位置计数器XOR)。勿明文外泄。 ----
_ENC_KEY_HEX = ("4764324277304264394f68384463385864294063205960254274234a642a553c"
                "16131211641717091b5f07524b485a4e4245411e585a594715525954484e135d37")
_SALT = b"strategy-pool::loader-v1"

_cache = None


def _recover_key():
    enc = bytes.fromhex(_ENC_KEY_HEX)
    rev = bytes(b ^ (i % 251) for i, b in enumerate(enc))
    return rev[::-1]


def _derive_key():
    return hmac.new(_recover_key(), _SALT, hashlib.sha256).digest()


def _keystream(key, length):
    out = b""
    ctr = 0
    while len(out) < length:
        out += hashlib.sha256(key + ctr.to_bytes(8, "big")).digest()
        ctr += 1
    return out[:length]


def _decrypt_blob():
    if not os.path.exists(BLOB):
        return None
    blob = open(BLOB, "rb").read()
    key = _derive_key()
    ks = _keystream(key, len(blob))
    comp = bytes(a ^ b for a, b in zip(blob, ks))
    try:
        return zlib.decompress(comp)
    except Exception:
        return None


def load():
    """返回 (ma, skdj_series, pool_eval); 解密失败时回退 None 表示策略不可用。"""
    global _cache
    if _cache is not None:
        return _cache
    src = _decrypt_blob()
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
