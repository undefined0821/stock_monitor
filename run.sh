#!/bin/bash
# 实时股票监控平台 - 启动脚本
# 用法: bash run.sh   (后台常驻)  或  nohup bash run.sh > monitor.log 2>&1 &
cd "$(dirname "$0")"
export TZ="Asia/Shanghai"
python3.11 app.py
