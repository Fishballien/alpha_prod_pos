# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 20:27:54 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %%
import re
from datetime import datetime


# %%
def parse_time_string(time_string):
    """
    解析格式为 "xxdxxhxxminxxs" 的时间字符串并转换为总秒数。

    参数:
    time_string (str): 表示时间间隔的字符串，如 "1d2h30min45s"。

    返回:
    int: 转换后的总秒数。

    异常:
    ValueError: 如果时间字符串格式无效。
    """
    # 正则模式支持 d（天），h（小时），min（分钟），s（秒）
    pattern = re.compile(r'(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)min)?(?:(\d+)s)?')
    match = pattern.fullmatch(time_string)
    
    if not match:
        raise ValueError("Invalid time string format")
    
    # 将天、小时、分钟、秒提取并转换为整数
    days = int(match.group(1)) if match.group(1) else 0
    hours = int(match.group(2)) if match.group(2) else 0
    mins = int(match.group(3)) if match.group(3) else 0
    secs = int(match.group(4)) if match.group(4) else 0
    
    # 转换为总秒数
    total_seconds = days * 24 * 60 * 60 + hours * 60 * 60 + mins * 60 + secs
    return total_seconds


def get_num_of_bars(period, org_bar):
    """
    计算 period 中包含多少个 org_bar 的整除部分。

    参数:
    period (str): 时间跨度字符串，如 "2h"。
    org_bar (str): 时间周期字符串，如 "30min"。

    返回:
    int: period 中包含 org_bar 的整除倍数。
    """
    # 将 period 和 org_bar 转换为秒数
    period_seconds = parse_time_string(period)
    org_bar_seconds = parse_time_string(org_bar)
    
    # 计算整除部分
    if org_bar_seconds == 0:
        raise ValueError("The org_bar string represents zero duration.")
    
    return period_seconds // org_bar_seconds


# %%
def get_date_based_on_timestamp(ts):
    """
    根据时间戳返回日期。
    
    参数:
    ts (datetime): 要检查的时间戳。
    
    返回:
    str: yyyymmdd 格式的日期。
    """
    return ts.strftime('%Y%m%d')
    
    
def get_curr_utc_date():
    """
    获取当前 UTC 日期，并根据指定的逻辑返回日期。

    此函数首先获取当前的 UTC 时间（datetime.utcnow()），
    然后调用 get_date_based_on_timestamp 函数来根据时间戳确定
    返回的具体日期格式（如 yyyymmdd）。
    
    返回:
    str: 当前 UTC 时间对应的日期，格式为 yyyymmdd。
    """
    now = datetime.utcnow()
    return get_date_based_on_timestamp(now)