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
import json, re, math, time, threading, datetime, os, random, traceback, shutil, copy
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, Response, jsonify, request

import requests

# BASE: 跨平台——默认取脚本所在目录; 沙箱/旧部署兜底到 /workspace/stock_monitor
_HERE = os.path.dirname(os.path.abspath(__file__))
BASE = _HERE if os.path.isdir(_HERE) else "/workspace/stock_monitor"
PORT = int(os.environ.get("PORT", 8800))
VERSION = "v3.11.1"
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
DAILY_MAX_MB = 120            # 日线库文件大小上限(MB), 超限则清理最旧数据
DAILY_UNIVERSE_LIMIT = 0      # 0=全市场; >0 则只存前N只(按代码序), 用于限制体积

# v3.10: 通用预测回测闭环(上证1小时/尾盘大盘/尾盘个股/开盘前涨停)
PRED_LOG = f"{BASE}/pred_log.jsonl"
PRED_STATS = f"{BASE}/pred_stats.json"
# v3.10.1: 预测概率自动校准(Platt scaling, 与 gapup 同款): 4 个 P1 模块(上证1小时/尾盘大盘/尾盘个股/盘前涨停)
# 的概率输出随验证样本积累自动收敛到真实命中率。校准参数按模块分别持久化, 避免互相干扰。
PRED_CALIB = f"{BASE}/pred_calib.json"
PRED_MIN_CALIB_SAMPLES = 12

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

# 细分题材(主题)注册表: 每个题材由若干代表性成分股组成。
# 题材涨跌 = 成分股当日平均涨跌幅(实时, 来自腾讯行情)。
# 用途: ①持仓卡片角落"所属题材涨跌"芯片(任一持仓命中题材即自动显示, 无需手动填板块);
#       ②"题材拉/踩指数"(题材相对大盘偏离, 识别拉动/压制大盘的细分主线)。
# 相比原 12 个中证行业指数, 题材更贴近个股关联度(如 CPO/PCB/纸业/白酒 等)。
# 新增持仓若命中任一题材, 即自动显示芯片。腾讯个股行情不含行业字段, 故通过东方财富
# F10 三级行业 + 股票名称自动归类(见 THEME_KEYWORDS / auto_classify), 前端新增持仓无需手动纠错。
THEMES = [
    ("白酒",       ["600519","000858","000568","600809","002304","000596","603369","000860"]),
    ("造纸",       ["000488","002078","600966","600567","600963","002511"]),
    ("CPO光模块",   ["300308","300502","300394","002281","603083","000988","301165","300570","300620"]),
    ("PCB",        ["002463","002916","300476","600183","603228","002384","300903","002938"]),
    ("光纤光缆",    ["600522","600487","601869","600105","002491"]),
    ("数字货币",    ["002657","603123","300468","002104","300579","003029","300348","000555"]),
    ("消费电子",    ["000049","002241","300136","002475","300433","601231","300115"]),
    ("智慧交通",    ["002401","002373","300212","300020","002869"]),
    ("环保",       ["600475","603903","300190","002573","000820","601330"]),
    ("氢能源",     ["600475","002733","300228","002639","300435","002274"]),
    ("中药",       ["600613","000538","600085","000999","600976","002603","600329","000423"]),
    ("创新药",     ["600276","300760","300347","002821","600196","688180"]),
    ("原料药/CDMO", ["603538","600521","002399","300702","605507"]),
    ("半导体",     ["688981","603986","002049","603501","300661","688041","688256","002371"]),
    ("光伏",       ["601012","300274","002129","600438","688599","002459","300316"]),
    ("锂电池",     ["002594","300750","300014","002460","300207","002340"]),
    ("新能源车",   ["601127","600104","000625","601633","300750"]),
    ("军工",       ["600893","000768","002179","600760","000661","601989"]),
    ("AI算力",     ["000977","002415","600588","300454","603019","300308","603220"]),
    ("机器人",     ["300124","002472","688017","300276","002031"]),
    ("食品饮料",   ["600887","603288","000895","605499","300999"]),
    ("银行",       ["601398","600036","601166","600000","601328"]),
    ("证券",       ["600030","600837","601211","000776","600999"]),
    ("房地产",     ["000002","600048","001979","600606"]),
    ("化工",       ["600309","002493","600426","002648","600989"]),
    ("有色",       ["601600","603993","600362","600219","000630"]),
    ("煤炭",       ["601088","600188","601225","600348"]),
    ("电力",       ["600900","600025","601985","600011","600674"]),
    ("家电",       ["000333","000651","600690","002032","603515"]),
    ("通信",       ["600050","601728","600941","000063","603220"]),
    ("软件",       ["300339","300598","002410","300674","002230","000555"]),
    ("传媒",       ["300413","002555","300418","603444"]),
    ("农业",       ["002714","300498","600598","002311"]),
    ("钢铁",       ["600019","000709","600808"]),
    ("航运港口",   ["601919","601866","600428","601872"]),
    ("工程机械",   ["000157","600031","000528"]),
]
# 股票代码 -> 题材列表(可多归属), 用于持仓卡片芯片自动识别
STOCK_THEMES = {}
for _tn, _ms in THEMES:
    for _c in _ms:
        STOCK_THEMES.setdefault(_c, []).append(_tn)

# ---------- 题材自动归类(前端新增持仓无需手动纠错) ----------
# 腾讯个股行情不含行业字段, 故通过东方财富 F10「公司概况」接口(EM2016 三级行业, 如
# "医药生物-化学制药-化学原料药") + 股票名称, 按下方关键词规则自动匹配到细分题材。
# 命中即自动显示卡片芯片。分类结果缓存到 stock_classify.json, 避免重复请求。
# 规则按"细分优先于宽泛"排序(如 原料药/CDMO 先于 创新药, CPO/光纤 先于 通信)。
CLASSIFY_CACHE_FILE = f"{BASE}/stock_classify.json"

THEME_KEYWORDS = [
    ("白酒", ["白酒"]),
    ("原料药/CDMO", ["化学原料药", "CDMO", "原料药", "化学制药", "美诺华", "华海药业", "九洲药业",
                   "普洛药业", "博腾股份", "凯莱英", "药明康德", "司太立", "奥翔药业", "天宇股份"]),
    ("创新药", ["创新药", "生物制品", "恒瑞医药", "百济", "信达生物", "君实",
                "荣昌", "康方"]),
    ("中药", ["中药"]),
    ("CPO光模块", ["光模块", "CPO", "中际旭创", "新易盛", "天孚通信", "源杰科技",
                 "太辰光", "铭普光磁", "光迅科技"]),
    ("光纤光缆", ["光纤", "光缆", "亨通光电", "中天科技", "长飞光纤", "通鼎互联", "永鼎股份"]),
    ("PCB", ["PCB", "印刷电路", "沪电股份", "深南电路", "景旺电子", "胜宏科技",
             "崇达技术", "兴森科技", "东山精密"]),
    ("半导体", ["半导体", "芯片", "中芯国际", "北方华创", "韦尔股份", "兆易创新",
                "卓胜微", "紫光国微", "长电科技", "通富微电", "寒武纪", "海光信息",
                "士兰微", "华润微", "复旦微电"]),
    ("消费电子", ["消费电子", "歌尔股份", "立讯精密", "蓝思科技", "领益制造",
                 "环旭电子", "信维通信", "欧菲光", "水晶光电", "电连技术"]),
    ("光伏", ["光伏", "太阳能", "阳光电源", "隆基绿能", "通威股份", "晶澳科技", "天合光能",
              "晶科能源", "福莱特", "迈为股份", "爱旭股份"]),
    ("锂电池", ["电池", "锂", "宁德时代", "亿纬锂能", "国轩高科", "欣旺达",
                "璞泰来", "恩捷股份", "天赐材料", "先导智能", "当升科技"]),
    ("新能源车", ["乘用车", "汽车", "长安汽车", "长城汽车", "上汽集团", "广汽集团",
                  "赛力斯", "比亚迪", "理想", "小鹏", "蔚来"]),
    ("氢能源", ["氢能源", "氢能", "美锦能源", "潍柴动力"]),
    ("军工", ["军工", "国防", "中航", "中船", "航发", "洪都航空", "高德红外",
              "内蒙一机", "航天", "船舶"]),
    ("AI算力", ["算力", "浪潮信息", "中科曙光", "海康威视", "深信服", "用友网络",
                "中贝通信", "工业富联", "服务器"]),
    ("机器人", ["机器人", "汇川技术", "双环传动", "绿的谐波", "三丰智能", "巨轮智能",
                "埃斯顿", "拓斯达", "减速器"]),
    ("智慧交通", ["智慧交通", "车联网", "中远海科", "千方科技", "易华录",
                  "银江技术", "金溢科技"]),
    ("数字货币", ["数字货币", "数字人民币", "中科金财", "翠微股份", "四方精创",
                  "恒宝股份", "数字认证", "吉大正元", "长亮科技", "神州信息"]),
    ("环保", ["环保", "环境治理", "水务", "碧水源", "伟明环保", "瀚蓝环境",
              "高能环境", "绿茵生态"]),
    ("食品饮料", ["食品饮料", "食品加工", "伊利股份", "海天味业", "双汇发展",
                  "农夫山泉", "东鹏饮料", "养元饮品", "安井食品", "绝味食品"]),
    ("银行", ["银行-", "银行业"]),
    ("证券", ["证券"]),
    ("房地产", ["房地产", "地产"]),
    ("化工", ["基础化工", "化学制品", "化工", "万华化学", "华鲁恒升", "宝丰能源",
              "扬农化工", "鲁西化工"]),
    ("有色", ["有色金属", "铝", "铜", "黄金", "中国铝业", "云铝股份", "江西铜业",
              "紫金矿业", "洛阳钼业", "天齐锂业", "赣锋锂业"]),
    ("煤炭", ["煤炭", "中国神华", "陕西煤业", "兖矿能源"]),
    ("电力", ["电力", "长江电力", "华能国际", "三峡能源", "国电电力", "大唐发电"]),
    ("家电", ["家用电器", "家电", "美的集团", "格力电器", "海尔智家", "海信视像",
              "老板电器", "苏泊尔", "科沃斯"]),
    ("通信", ["通信"]),
    ("软件", ["计算机", "软件开发", "软件", "金山办公", "恒生电子", "广联达",
              "卫宁健康", "科大讯飞"]),
    ("传媒", ["传媒", "分众传媒", "芒果超媒", "三七互娱", "完美世界", "恺英网络"]),
    ("农业", ["农林牧渔", "牧原股份", "温氏股份", "新希望", "养殖", "大北农"]),
    ("钢铁", ["钢铁", "宝钢股份", "鞍钢股份", "华菱钢铁"]),
    ("航运港口", ["航运", "港口", "中远海控", "招商轮船", "上港集团", "宁波港"]),
    ("工程机械", ["工程机械", "三一重工", "中联重科", "徐工机械"]),
]

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
        "vol": num(g(F["vol"])),             # 成交量(手), v3.10 供本地日线库落库
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
# v3.10: 分时数据缓存。指数预测改为5秒刷新后, 一次预测会多次用到同一份分时数据
# (_market_context 算"尾盘动向" + chart 画图), 缓存可把实际请求降到 TTL 一次。
_MINUTE_CACHE = {}

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
                    log_prediction(
                        "idx_1h",
                        {"prob": base["prob"], "verdict": base["verdict"],
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
            "last_buy_date": str(h.get("last_buy_date", "")).strip(),
            # 今日开盘前持有股数(加仓前基准): 用于当日盈亏分段计算,
            # 由系统在每日开盘重置 + 盘中加仓时自动记录, 无需手动维护。
            "open_shares": float(h.get("open_shares", 0) or 0),
            "open_date": str(h.get("open_date", "")).strip(),
            "name": str(h.get("name", "")).strip(),
            # 题材自动归类结果(前端新增持仓保存时写入, 启动后台线程补齐存量); 为空则回退 STOCK_THEMES/STOCK_SECTOR
            "theme": str(h.get("theme", "")).strip(),
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
    仅处理既无 theme 字段、又不在手工 STOCK_THEMES 表的持仓。"""
    import time
    # 阶段一: 启动补齐(重试)
    for attempt in range(3):
        try:
            with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            hs = cfg.get("holdings", [])
            changed = False
            pending = False
            for h in hs:
                code = str(h.get("code", "")).strip()
                if not code or h.get("theme") or STOCK_THEMES.get(code):
                    continue
                th = auto_classify(code, h.get("market"), h.get("name", ""),
                                   use_cache=(attempt > 0))
                if th:
                    h["theme"] = th
                    changed = True
                else:
                    pending = True
            if changed:
                tmp = PORTFOLIO_PATH + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(cfg, f, ensure_ascii=False, indent=2)
                os.replace(tmp, PORTFOLIO_PATH)
                reload_holdings()
            if not pending:
                break
            time.sleep(60)
        except Exception as e:
            print("[backfill] 题材补齐异常:", e)
            time.sleep(30)
    # 阶段二: 周期性保活(应对 POST 时网络抖动未归类的持仓)
    while True:
        time.sleep(300)
        try:
            with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            hs = cfg.get("holdings", [])
            changed = False
            for h in hs:
                code = str(h.get("code", "")).strip()
                if not code or h.get("theme") or STOCK_THEMES.get(code):
                    continue
                th = auto_classify(code, h.get("market"), h.get("name", ""),
                                   use_cache=False)
                if th:
                    h["theme"] = th
                    changed = True
            if changed:
                tmp = PORTFOLIO_PATH + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(cfg, f, ensure_ascii=False, indent=2)
                os.replace(tmp, PORTFOLIO_PATH)
                reload_holdings()
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
        try:
            _now2 = beijing_now()
            nd = _next_trading_day(_now2)
            vat = nd.strftime("%Y-%m-%d") + " 15:05:00"
            sh_px = next((i.get("price") for i in snap.get("indices", [])
                          if i.get("code") == "sh000001"), None)
            log_prediction("close_market",
                           {"prob": m["prob"], "verdict": m["verdict"],
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
                log_prediction("close_stock",
                               {"prob": s.get("prob"), "verdict": s.get("verdict"),
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
        tlist = h.get("theme") or STOCK_THEMES.get(h["code"])
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
_GAPUP_CALIB = {"A": None, "B": None, "n": 0, "fitted_at": None}
# v3.10.1: 各 P1 模块的概率校准参数(按模块独立), 形如 {"idx_1h": {"A":1.0,"B":..,"n":..}, ...}
_PRED_CALIB = {}

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


def _tune_threshold(module, samples, spec):
    """网格搜最优报警阈值, 目标 F1; 预测为正太少则惩罚防退化解; L2 回拉默认阈值。"""
    probs = [p for p, _ in samples]
    rets = [r for _, r in samples]
    def_thr, pos = spec["def_thr"], spec["pos"]
    lo, hi = (45, 85) if pos == "up" else (30, 90)
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
    """正则化坐标上升调权重, 目标=留出验证集 F1(每层配最优阈值); L2 回拉默认防过拟合。"""
    rescore = _RESCORE[module]
    W0 = _current_weights(module, spec)
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
        _MODULE_THRESHOLDS[module] = int(round(thr))
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
    """方向命中判定: 多/空看符号, 震荡看是否落在阈值带内。"""
    if verdict in ("看涨", "偏多"):
        return ret > 0
    if verdict in ("看跌", "偏空"):
        return ret < 0
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
        rec["actual"] = {"price": round(p1, 2), "ret": round(ret, 3), "hit": bool(hit)}
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
                    cb = fit_pred_calib(m)
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
        ent = {"label": PRED_MODULES[m]["label"], "n": n, "hit": 0,
               "hit_rate": None, "avg_pred": None, "avg_ret": None,
               "bias_pp": None, "by_verdict": {}, "recent": [], "calib": None}
        if n:
            hits = sum(1 for r in rows if r["actual"].get("hit"))
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
            ent["hit_rate"] = round(hits / n, 4)
            if preds:
                mp = sum(preds) / len(preds)
                ent["avg_pred"] = round(mp, 2)
                # 校准偏差: 平均宣称概率 - 实际命中率。>0 表示系统性高估(过度自信)
                ent["bias_pp"] = round(mp - hits / n * 100, 2)
            if rets:
                ent["avg_ret"] = round(sum(rets) / len(rets), 3)
            bv = {}
            for r in rows:
                v = r["pred"].get("verdict") or "-"
                b = bv.setdefault(v, {"n": 0, "hit": 0})
                b["n"] += 1
                b["hit"] += 1 if r["actual"].get("hit") else 0
            ent["by_verdict"] = {k: {"n": v["n"], "hit": v["hit"],
                                     "rate": round(v["hit"] / v["n"], 4) if v["n"] else None}
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
def _calib_idx_view(payload):
    """上证1小时预测的实时概率校准(复制后改, 不动 STATE)。"""
    if not isinstance(payload, dict) or "prob" not in payload:
        return payload
    p = dict(payload)
    p["prob"] = round(_apply_pred_calib("idx_1h", p.get("prob", 0)), 1)
    return p


def _calib_close_view(payload):
    """尾盘预测(大盘+个股)的实时概率校准(复制后改, 不动 STATE)。"""
    if not isinstance(payload, dict):
        return payload
    p = copy.deepcopy(payload)
    if isinstance(p.get("market"), dict):
        p["market"]["prob"] = round(_apply_pred_calib("close_market", p["market"].get("prob", 0)), 1)
    for s in (p.get("stocks") or []):
        if isinstance(s, dict):
            s["prob"] = round(_apply_pred_calib("close_stock", s.get("prob", 0)), 1)
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


def _load_daily():
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

                # 指数1小时预测: v3.10 改为每 IDX_FORECAST_SEC(5秒) 刷新
                # (异步 worker, 不阻塞调度循环; AI融合在worker内按 IDX_AI_FUSE_SEC 降频)
                if now.time() >= datetime.time(9, 15):
                    last = STATE["idx_forecast_time"]
                    if not last or (now - last).total_seconds() >= IDX_FORECAST_SEC:
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

                # v3.10 P1: 通用预测回测 —— 到期的预测抓真实结果回填(每10分钟一次)
                last_pv = STATE.get("pred_verify_time")
                if last_pv is None or (now - last_pv).total_seconds() >= 600:
                    STATE["pred_verify_time"] = now
                    threading.Thread(target=verify_predictions, daemon=True).start()

                # v3.10 P2: 收盘后(15:05)抓取当日完整日线落库 + 定期清理(每天一次)
                if now.time() >= datetime.time(15, 5) and STATE.get("daily_bars_date") != today:
                    STATE["daily_bars_date"] = today
                    threading.Thread(target=capture_daily_bars, daemon=True).start()

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
    STATE["idx_forecast_time"] = beijing_now().strftime("%Y-%m-%d")
    _start_idx_build()
    with LOCK:
        cur = STATE["idx_forecast"]
    return jsonify(_calib_idx_view(cur) if cur else {"building": True,
                                    "note": "上证预测计算中(含AI模型融合), 请稍候自动更新…"})


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
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(268px,1fr));gap:12px}
.card{position:relative;overflow:hidden;background:var(--card);border:1px solid var(--line);border-radius:12px;
  padding:12px 12px 11px;transition:transform .15s ease,box-shadow .15s ease,border-color .15s ease;box-shadow:0 1px 2px rgba(0,0,0,.18)}
.card:hover{transform:translateY(-2px);box-shadow:0 8px 22px rgba(0,0,0,.30);border-color:#3d4756}
.card::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--line)}
.card.up::before{background:var(--up)}.card.down::before{background:var(--down)}
.card h3{margin:0 0 2px;font-size:14.5px;font-weight:600;display:flex;justify-content:space-between;align-items:flex-start;gap:8px}
.code{color:var(--mut);font-size:12px;font-weight:normal;letter-spacing:.3px}
.px{font-size:25px;font-weight:700;line-height:1.05;font-variant-numeric:tabular-nums}
.pct{font-size:13.5px;font-weight:600;margin-left:7px;font-variant-numeric:tabular-nums}
.up{color:var(--up)}.down{color:var(--down)}.flat{color:var(--mut)}
.sector-chip{display:inline-flex;align-items:center;gap:4px;flex-shrink:0;font-size:10.5px;line-height:1;
  padding:3px 8px;border-radius:999px;border:1px solid var(--line);background:var(--row-line);color:var(--mut);white-space:nowrap}
.sector-chip .sp{font-weight:700;font-variant-numeric:tabular-nums}
.sector-chip .rk{opacity:.65;font-size:9.5px}
.grp{margin:8px 0 2px;font-size:10.5px;letter-spacing:1.2px;color:var(--mut);text-transform:uppercase;border-top:1px solid var(--row-line);padding-top:7px}
.row{display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-bottom:1px solid var(--row-line);font-size:12.5px}
.row .lbl{color:var(--mut)}
.row:last-of-type{border-bottom:none}
.row span:last-child{font-variant-numeric:tabular-nums;font-weight:500}
.kpi{display:flex;gap:10px;flex-wrap:wrap;margin:4px 0 18px}
.kpi div{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 12px;min-width:0;flex:1 1 104px;box-shadow:0 1px 2px rgba(0,0,0,.15);overflow:hidden}
.kpi div{color:var(--mut);font-size:12px}
.kpi b{display:block;font-size:clamp(15px,4.4vw,22px);font-weight:700;color:var(--txt);font-variant-numeric:tabular-nums;margin-top:2px;line-height:1.15;overflow-wrap:anywhere;word-break:break-word}
@media(max-width:600px){.kpi{gap:8px}.kpi div{flex:1 1 42%;padding:8px 10px}.kpi b{font-size:clamp(14px,4.8vw,19px)}}
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
/* 高开回测: 紧凑小卡(非核心内容, 仅展示昨日5只实测) */
.gv-card{padding:10px 12px !important}
.gv-head{display:flex;justify-content:space-between;align-items:center;font-size:12px;color:var(--mut);margin-bottom:6px}
.gv-head b{color:var(--txt);font-weight:600}
.gv-hit{font-size:12px;color:var(--mut)}
.gv-hit b{font-size:14px;color:var(--blue)}
.gv-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:5px 12px}
.gv-item{display:flex;align-items:center;gap:6px;font-size:12px;font-variant-numeric:tabular-nums;padding:2px 0;border-bottom:1px solid var(--row-line)}
.gv-item:last-child{border-bottom:none}
.gv-name{flex:1;color:var(--txt);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.gv-prob{color:var(--mut);font-size:11px}
.gv-prob.up{color:var(--up)}.gv-prob.down{color:var(--down)}
.gv-gap{font-weight:600;min-width:52px;text-align:right}
.gv-res{font-size:12px;width:16px;text-align:center}
.gv-res.up{color:var(--up)}.gv-res.down{color:var(--down)}
/* 近5次自动调参变化(紧凑) */
.gv-opt{margin-top:8px;padding-top:7px;border-top:1px solid var(--row-line)}
.gv-opt-h{font-size:11px;color:var(--mut);letter-spacing:.5px;margin-bottom:3px}
.gv-opt-row{display:flex;align-items:baseline;gap:8px;font-size:11px;padding:1px 0;flex-wrap:wrap}
.gv-opt-date{color:var(--mut);font-variant-numeric:tabular-nums}
.gv-opt-auc{font-weight:600;font-variant-numeric:tabular-nums}
.gv-opt-auc.up{color:var(--up)}.gv-opt-auc.down{color:var(--down)}
.gv-opt-delta{color:var(--mut);font-variant-numeric:tabular-nums;opacity:.9}
.prob{font-size:24px;font-weight:700}
.gauge{display:inline-block;font-size:22px;font-weight:700}
.eye{cursor:pointer;background:var(--card);color:var(--txt);border:1px solid var(--line);border-radius:8px;font-size:16px;padding:4px 10px;line-height:1}
.gv-toolbar .gv-btn{background:var(--blue,#2f80ed);color:#06121f;border:none;padding:5px 12px;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600}
.gv-toolbar .gv-btn:hover{opacity:.88}
.eye:hover{border-color:var(--blue)}
/* v3.11.1: 高开回测 / 预测回测总览 并排; 窄屏自动堆叠为单列 */
.gv-twocol{} /* 容器样式内联(两列 grid), 此处仅占位 */
@media(max-width:900px){.gv-twocol{grid-template-columns:1fr !important}}
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
    <h3>✏️ 编辑持仓 <span class="note" style="font-weight:400">仅可改 股票代码 / 成本 / 股数；每次保存视为"今日有买入动作"，当日盈亏即按成本计算（之前买入且未改动的按昨收）</span></h3>
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
    <div class="card"><h3>题材涨跌（细分主线） <span class="code" id="sb2"></span></h3>
      <div id="sectors" style="max-height:300px;overflow:auto"></div>
      <div class="note" id="bias"></div>
    </div>
    <div class="card" id="retailCard" style="border-color:var(--gold)"><h3>国证2000 小盘涨跌（散户代理）</h3><div id="retail"><div class="note">加载中…</div></div></div>
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
  <!-- 7. 回测面板(v3.11.1): 高开回测 与 预测回测总览 并排 -->
  <div class="gv-twocol" style="display:grid;grid-template-columns:1fr 1fr;gap:16px;align-items:start">
    <div class="gv-col">
  <!-- 7.1 高开回测(v3.4) -->
  <div class="section gv-section" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:18px 0 8px;font-size:14px">
    <span>📊 高开回测（推荐→开盘实测）</span>
  </div>
  <div class="card gv-card" id="gapverify"><div class="note">加载中…</div></div>
    </div>
    <div class="gv-col">
  <!-- 7.2 预测回测总览(v3.10): 上证1小时/尾盘大盘/尾盘个股/盘前涨停/尾盘高开潜力 的命中率与校准偏差 -->
  <div class="section gv-section" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:18px 0 8px;font-size:14px">
    <span>🎯 预测回测总览（各模块命中率 & 概率校准）</span>
  </div>
  <div class="card gv-card" id="predstats"><div class="note">加载中…</div></div>
    </div>
  </div>
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
    const pc=cls(h.pnl);      // 卡片主色: 以成本价 vs 实时价(盈亏)为基准, 不基于昨日价
    const pcd=cls(h.pct);     // 当日涨跌%: 以昨收为基准(独立于卡片主色)
    // 角落题材芯片: 所属细分题材名 + 题材平均涨跌幅 + 题材强弱排名(命中题材即自动显示)
    const secChip = h.theme_name ? `<span class="sector-chip" title="所属题材涨跌(成分股平均)">${h.theme_name} <span class="sp ${cls(h.theme_pct)}">${sign(h.theme_pct)}%</span>${h.theme_rank?'<span class="rk">'+h.theme_rank+'/'+(h.theme_total||'')+'</span>':''}</span>` : '';
    let badges=(h.anomalies||[]).map(a=>'<span class="badge b-'+a.level+'">'+a.text+'</span>').join('');
    H.innerHTML+=`<div class="card ${pc}">
      <h3><span>${h.name} <span class="code">${h.market.toUpperCase()}${h.code}</span></span>${secChip}</h3>
      <div style="margin:2px 0 4px"><span class="px ${pc}">${fmt(h.price)}</span><span class="pct ${pcd}">${sign(h.pct)}%</span></div>
      <div class="grp">盈亏概览</div>
      <div class="row"><span class="lbl">持仓市值</span><span class="${showMoney?'':'masked'}">${mval(h.value)}</span></div>
      <div class="row"><span class="lbl">浮动盈亏</span><span class="${showMoney?'':'masked'} ${cls(h.pnl)}">${showMoney?sign(h.pnl)+'元':mask(0)} (${showMoney?sign(h.pnl_pct)+'%':mask(0)})</span></div>
      <div class="row"><span class="lbl">当日盈亏</span><span class="${showMoney?'':'masked'} ${cls(h.day_pnl)}">${showMoney?sign(h.day_pnl)+'元':mask(0)} (${showMoney?sign(h.day_pnl_pct)+'%':mask(0)})<span class="note" style="margin-left:6px">·基${h.day_basis}</span></span></div>
      <div class="row"><span class="lbl">成本 / 股数</span><span class="${showMoney?'':'masked'}">${showMoney?fmt(h.cost,3):'***'} / ${h.shares}股</span></div>
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
  // 题材(细分)涨跌: 取代宽泛的 12 个中证行业, 更贴近个股关联主线
  const ths=d.themes||[];
  const tavg=ths.length?ths.reduce((a,b)=>a+b.pct,0)/ths.length:0;
  document.getElementById('sb2').textContent='↑'+ths.filter(x=>x.pct>0).length+' ↓'+ths.filter(x=>x.pct<0).length+' 均值'+sign(Math.round(tavg*100)/100)+'%';
  let s='';
  ths.forEach(x=>{const w=Math.min(Math.abs(x.pct)*6,100);s+=`<div style="margin:5px 0"><span style="display:inline-block;width:72px">${x.name}</span><span class="bar ${x.pct>=0?'up':''}" style="width:${w}px"></span> <span class="${cls(x.pct)}">${sign(x.pct)}%</span><span class="note" style="margin-left:4px">${x.n}股</span></div>`;});
  document.getElementById('sectors').innerHTML=s;
  document.getElementById('bias').textContent=d.sector_bias;
  // 散户今日平均盈亏(国证2000近似) —— 紧凑版
  const rt=document.getElementById('retail');
  if(d.retail_pnl){const r=d.retail_pnl;const rc=cls(r.pct);
    rt.innerHTML=`<div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">
      <span class="pct ${rc}" style="font-size:20px;font-weight:700">${sign(r.pct)}%</span>
      <span class="badge b-${r.pct>=0?'up':'down'}">小盘(国证2000) ${r.pct>=0?'涨':'跌'} ≈ ${sign(r.pct)}%</span>
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

// 高开回测(v3.4): 仅展示最近一次(昨天)推荐的 5 只开盘实测结果, 版面紧凑(非核心内容)
function renderGapVerify(d){
  const box=document.getElementById('gapverify');
  if(!box) return;
  const recent=(d.stats&&d.stats.recent)||[];
  const day=recent[0];  // recent[0] 即最近一次已验证(昨天推荐→今开实测)
  if(!day||!day.stocks||!day.stocks.length){
    box.innerHTML='<div class="note">暂无昨日的回测结果。下一交易日 09:30 后自动验证当日推荐的 5 只是否高开。</div>';
    return;
  }
  const stocks=day.stocks;
  const hits=stocks.filter(x=>x.is_gap_up).length;
  const rdate=day.date?day.date.slice(5):'--';          // 推荐日(昨天)
  const vdate=day.verified_at?day.verified_at.slice(5,10):rdate;  // 实测日
  let r=`<div class="gv-head"><span>推荐 <b>${rdate}</b> · 今开实测 <b>${vdate}</b></span>`
       +`<span class="gv-hit">命中 <b>${hits}</b>/${stocks.length}</span></div>`;
  r+='<div class="gv-list">';
  stocks.forEach(x=>{
    const gp=x.gap_pct;
    const hit=x.is_gap_up;
    r+=`<div class="gv-item">`
      +`<span class="gv-name">${x.code} ${x.name}</span>`
      +`<span class="gv-prob ${probcls(x.prob)}">预${fmt(x.prob,0)}%</span>`
      +`<span class="gv-gap ${cls(gp)}">${gp==null?'--':(gp>=0?'+':'')+fmt(gp,2)+'%'}</span>`
      +`<span class="gv-res ${hit?'up':'down'}">${hit?'✅':'❌'}</span>`
      +`</div>`;
  });
  r+='</div>';
  // 近5次自动调参的变化值(紧凑, 非核心): 展示 AUC 趋势 + 变动较大的权重
  const opts=(d.stats&&d.stats.optimizations)||[];
  if(opts.length){
    r+='<div class="gv-opt"><div class="gv-opt-h">近5次自动调参变化</div>';
    opts.slice(-5).reverse().forEach(o=>{
      const before=o.before||{}, after=o.after||{};
      const deltas=[];
      for(const k in after){
        const dv=after[k]-before[k];
        if(Math.abs(dv)>=0.1) deltas.push(`${k.replace('gu_','')} ${before[k].toFixed(1)}→${after[k].toFixed(1)}`);
      }
      const aucUp=(o.auc_after||0)>=(o.auc_before||0);
      r+=`<div class="gv-opt-row">`
        +`<span class="gv-opt-date">${o.at?o.at.slice(5,10):''}</span>`
        +`<span class="gv-opt-auc ${aucUp?'up':'down'}">AUC ${(o.auc_before||0).toFixed(3)}→${(o.auc_after||0).toFixed(3)}</span>`
        +`<span class="gv-opt-delta">${deltas.length?deltas.join(' · '):'微调'}</span>`
        +`</div>`;
    });
    r+='</div>';
  }
  box.innerHTML=r;
}

// 预测回测总览(v3.10): 四个预测模块的命中率/平均预测概率/校准偏差 + 日线库状态
const PRED_LABELS={idx_1h:'上证1小时方向',close_market:'尾盘大盘次日',close_stock:'尾盘个股次日',preopen_limitup:'盘前涨停预测',gapup:'尾盘高开潜力'};
const PRED_MIN_CALIB=12;   // 概率校准自动启用的样本阈值(与后端 PRED_MIN_CALIB_SAMPLES 同步)
function renderPredStats(s){
  const box=document.getElementById('predstats');
  if(!box) return;
  const mods=(s&&s.modules)||{};
  const tune=(s&&s.tune)||{};
  const tmeta=(s&&s.tune_meta)||{};
  const keys=Object.keys(PRED_LABELS).filter(k=>mods[k]);
  // v3.11.0: 顶部工具条(自动调参操作 + 门控说明)
  const head='<div class="gv-toolbar" style="display:flex;gap:8px;align-items:center;margin-bottom:6px;flex-wrap:wrap">'
    +'<button class="gv-btn" onclick="tuneNow(this)">🔧 重新调参</button>'
    +'<button class="gv-btn" onclick="tuneReset()">↺ 恢复默认</button>'
    +`<span class="note" style="margin:0">自动调参随样本累积生效：阈值≥${tmeta.min_thr||15}，权重≥${tmeta.min_w||50}（权重多、需更多样本防过拟合）</span>`
    +'</div>';
  if(!keys.length){
    box.innerHTML=head+'<div class="note">暂无回测样本。预测落盘后按到期时刻自动回填真实结果（上证1小时当日验证，尾盘/涨停次日或当日收盘验证），样本会随交易日积累。</div>';
    return;
  }
  let r='<div class="gv-list">';
  keys.forEach(k=>{
    const e=mods[k];
    const rate=e.hit_rate==null?null:e.hit_rate*100;
    const bias=e.bias_pp;
    const biasTxt=bias==null?'--':(bias>1?'高估'+fmt(bias,1)+'pp':bias<-1?'低估'+fmt(-bias,1)+'pp':'贴合±1pp');
    const biasCls=bias==null?'flat':(Math.abs(bias)<=1?'flat':(bias>0?'down':'up')); // 高估=橙红提示, 低估=偏绿
    r+=`<div class="gv-item">`
      +`<span class="gv-name">${PRED_LABELS[k]}</span>`
      +`<span class="gv-prob flat">样本${e.n}</span>`
      +`<span class="gv-gap ${rate==null?'flat':(rate>=50?'up':'down')}">命中率 ${rate==null?'--':fmt(rate,0)+'%'}</span>`
      +`<span class="gv-res ${biasCls}" style="min-width:96px;text-align:right;font-size:12px">偏差 ${biasTxt}</span>`
      +`</div>`;
    // v3.10.1: 概率自动校准徽章(样本≥12 自动拟合, 实时输出与面板概率同步对齐真实命中率)
    const cb=e.calib||{};
    const calibBadge = cb.applied
      ? `<span class="gv-prob" style="color:var(--green,#2ecc71)">✓已校准(n=${cb.n})</span>`
      : `<span class="gv-prob flat">未校准(${cb.n||0}/${PRED_MIN_CALIB||12})</span>`;
    r+=`<div class="gv-opt-row" style="padding-left:10px">${calibBadge}</div>`;
    // v3.11.0: 自动调参(阈值+权重)徽章
    const tn=tune[k]||{};
    const thr=tn.threshold||{}; const wt=tn.weights||{};
    let tuneBadge;
    if(k==='gapup'){   // v3.11.1: gapup 是排名问题, 以 AUC 衡量判别力, 无二元阈值
      if(wt.auc_after!=null){
        const up=wt.auc_after>=wt.auc_before;
        tuneBadge='<span class="gv-prob" style="color:var(--green,#2ecc71)">✓已调参 AUC '
          +fmt(wt.auc_before,3)+'→'+fmt(wt.auc_after,3)+(up?' ▲':' ▼')+' (n='+wt.n+')</span>';
      } else {
        tuneBadge='<span class="gv-prob flat">待激活('+(wt.n||0)+'/'+(wt.need||(tmeta.gapup_min||10))+')</span>';
      }
    } else if(thr.threshold!=null){
      tuneBadge='<span class="gv-prob" style="color:var(--green,#2ecc71)">✓已调参 阈值'+thr.threshold
        +' F1 '+thr.def_f1+'→'+thr.f1;
      if(wt.values&&Object.keys(wt.values).length) tuneBadge+=' 权重漂移'+wt.drift;
      tuneBadge+=' (n='+thr.n+')</span>';
    } else {
      const need=Math.max(thr.need||0, wt.need||0);
      const have=Math.max(thr.n||0, wt.n||0);
      tuneBadge='<span class="gv-prob flat">待激活('+have+'/'+need+')</span>';
    }
    r+=`<div class="gv-opt-row" style="padding-left:10px">${tuneBadge}</div>`;
    // 分方向明细(紧凑一行)
    const bv=e.by_verdict||{};
    const detail=Object.keys(bv).map(v=>{
      const b=bv[v];
      return `${v} ${b.hit}/${b.n}`;
    }).join(' · ');
    if(detail) r+=`<div class="gv-opt-row" style="padding-left:10px"><span class="gv-opt-date">${e.avg_pred!=null?'均预测 '+fmt(e.avg_pred,1)+'%':''}</span><span class="gv-opt-delta">${detail}</span></div>`;
  });
  r+='</div>';
  // 校准说明: 偏差>0 表示模型宣称的概率系统性高于实际发生率(过度自信)
  const anyBias=keys.some(k=>mods[k].bias_pp!=null&&Math.abs(mods[k].bias_pp)>5);
  if(anyBias) r+='<div class="note" style="margin-top:6px">提示: 偏差为"平均预测概率 − 实际命中率"。偏差>5pp 说明该模块概率过度自信，样本≥12 后可启用概率校准自动修正。</div>';
  box.innerHTML=head+r;
}
function tuneNow(b){if(b){b.disabled=true;b.textContent='⏳ 调参中…';} fetch('/api/tune',{method:'POST'}).then(()=>loadPredStats()).finally(()=>{if(b){b.disabled=false;b.textContent='🔧 重新调参';}});}
function tuneReset(){fetch('/api/tune_reset',{method:'POST'}).then(()=>loadPredStats()).catch(()=>{});}
function loadPredStats(){fetch('/api/pred_stats').then(r=>r.json()).then(renderPredStats).catch(()=>{});}
function load(){fetch('/api/snapshot').then(r=>r.json()).then(render).catch(()=>{});fetch('/api/gapup/log').then(r=>r.json()).then(renderGapVerify).catch(()=>{});loadPredStats();}
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
    threading.Thread(target=_backfill_themes, daemon=True).start()  # 后台补齐存量持仓题材
    print(f"监控平台 v2 启动: http://localhost:{PORT}")
    app.run(host="0.0.0.0", port=PORT, threaded=True)
