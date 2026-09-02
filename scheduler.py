# -*- coding: utf-8 -*-
"""调度循环: 定时触发各预测构建/校准/落盘/验证/日线采集。
import app 延迟引用(函数体调用 app.xxx), 避免与 app 的循环导入。由 app.py 拆分。"""

import time, threading, datetime, os, traceback
import app
from core import *

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
            snap = app.build_snapshot()
            with LOCK:
                STATE["trading"] = trading
                STATE["is_weekday"] = True
                for a in app.detect_alerts(snap):
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
                        app._start_idx_build()

                # 主题材拉/踩指数: 每10分钟检测
                if now.time() >= datetime.time(9, 15):
                    last = STATE["sector_drivers_time"]
                    if not last or (now - last).total_seconds() >= 600:
                        try:
                            STATE["sector_drivers"] = app.detect_sector_drivers(snap)
                            STATE["sector_drivers_time"] = now
                        except Exception:
                            pass

                today = now.strftime("%Y-%m-%d")
                # 9:25:02触发候选池首扫(后台构建); 之后由独立快扫线程_preopen_fast_loop
                # 每PREOPEN_FAST_SEC秒刷新Top30盘口+重新计算AI权重概率(不拖主循环)
                if now.time() >= datetime.time(9, 25, 2) and now.time() < datetime.time(9, 30):
                    if STATE["preopen_date"] != today:
                        STATE["preopen_date"] = today
                        app._start_pool_build()
                # 9:30-9:35 开盘后动态炸板校验: 每 reeval_interval 秒重判炸板/红开/继位(可重复, 去重告警)
                rf = _parse_hhmm(SCFG["reeval_from"]); rt = _parse_hhmm(SCFG["reeval_to"])
                if rf <= now.time() < rt:
                    last = STATE.get("preopen_reeval_last")
                    if last is None or (now - last).total_seconds() >= SCFG["reeval_interval"]:
                        STATE["preopen_reeval_last"] = now
                        try:
                            app._post_open_filter()
                        except Exception:
                            traceback.print_exc()
                # 选股池(v3.11.13): 每日定时(默认14:25)全市场扫描主板, 取TopN推荐。
                #   v3.12: 依赖本地日线库预热(见下方 warmup 触发), 扫描纯读本地零网络,
                #   数秒~数十秒出结果, 确保收盘前(留足交易时间)拿到推荐。
                try:
                    sp_from = _parse_hhmm(PCFG["scan_hhmm"])
                except Exception:
                    sp_from = datetime.time(14, 25)
                if now.time() >= sp_from and STATE.get("stock_pool_date") != today:
                    STATE["stock_pool_date"] = today
                    threading.Thread(target=app._build_stock_pool, daemon=True).start()

                # 14:50 尾盘预测(异步 worker + AI 融合, 不阻塞调度循环)
                if now.time() >= datetime.time(14, 50) and STATE["close_date"] != today:
                    STATE["close_date"] = today
                    app._start_close_build()

                # 尾盘高开潜力: 14:52 起自动扫描主板(每次交易日一次, 收盘前8分钟)
                if now.time() >= datetime.time(14, 52):
                    if STATE["gapup_date"] != today:
                        STATE["gapup_date"] = today
                        app._start_gapup_build()

                # v3.4: 开盘后回测上一交易日推荐是否高开(每天09:30后跑一次, 后台线程不阻塞)
                if now.time() >= datetime.time(9, 30) and STATE.get("gapup_verify_date") != today:
                    STATE["gapup_verify_date"] = today
                    threading.Thread(target=app._verify_gapup_open, daemon=True).start()

                # v3.10 P1: 通用预测回测 —— 到期的预测抓真实结果回填(每10分钟一次)
                last_pv = STATE.get("pred_verify_time")
                if last_pv is None or (now - last_pv).total_seconds() >= 600:
                    STATE["pred_verify_time"] = now
                    threading.Thread(target=app.verify_predictions, daemon=True).start()

                # v3.11.11: 预测校准自动调参(无需前端手动按钮): 每30分钟一次,
                # 达到样本阈值即生效, 否则保持待激活; 与 verify_predictions 同节拍, 降频避免抖动
                last_at = STATE.get("auto_tune_time")
                if last_at is None or (now - last_at).total_seconds() >= 1800:
                    STATE["auto_tune_time"] = now
                    try:
                        app.auto_tune_all()
                    except Exception:
                        traceback.print_exc()

                # v3.10 P2: 收盘后(15:05)抓取当日完整日线落库 + 定期清理(每天一次)
                if now.time() >= datetime.time(15, 5) and STATE.get("daily_bars_date") != today:
                    STATE["daily_bars_date"] = today
                    threading.Thread(target=app.capture_daily_bars, daemon=True).start()

                # v3.12 P2: 收盘后(15:12)全主板日线库预热(选股池零网络扫描的前提)。
                # 首次预热抓全主板 ~3151 只各35天日K(约1-2分钟, 后台线程);
                # 之后每日增量: 预热只补当日缺口, 由 capture_daily_bars 已覆盖当日则跳过。
                if now.time() >= datetime.time(15, 12) and STATE.get("daily_warmup_date") != today:
                    STATE["daily_warmup_date"] = today
                    threading.Thread(target=app.warmup_daily_library, daemon=True).start()

            if trading:
                time.sleep(POLL)
            else:
                time.sleep(30)
        except Exception:
            traceback.print_exc()
            time.sleep(15)

