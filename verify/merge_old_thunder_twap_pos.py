# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 10:53:04 2025

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %%
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


# %%
def generate_time_ranges(start_date_str, end_date_str, interval_hours=6):
    # 将日期字符串转换为 datetime 对象
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # 创建一个列表来存储时间段
    time_ranges = []
    
    # 循环从 start_date 到 end_date
    current_date = start_date
    while current_date <= end_date:
        # 每指定小时的起始时间
        start_of_period = current_date.strftime("%Y-%m-%d %H:%M:%S")
        # 每指定小时的结束时间
        end_of_period = (current_date + timedelta(hours=interval_hours)).strftime("%Y-%m-%d %H:%M:%S")
        
        # 添加时间段到列表
        time_ranges.append((start_of_period, end_of_period))
        
        # 增加指定的小时数
        current_date += timedelta(hours=interval_hours)
    
    return time_ranges


# %%
start_date = '2025-02-01'
end_date = '2025-02-25'

dir_to_save = Path('D:/crypto/prod/alpha/portfolio_management/verify/old_thunder_pos_250225')

twap_pos_list = []
time_ranges = generate_time_ranges(start_date, end_date)
for start_time, end_time in time_ranges:
    filename = f'{start_time}_{end_time}.parquet'
    filename = filename.replace(' ', '_').replace(':', '')
    twap_pos_path = dir_to_save / filename
    if os.path.exists(twap_pos_path):
        twap_pos_pivot = pd.read_parquet(dir_to_save / filename)
        resampled_twap = twap_pos_pivot.resample('3s', label='right', closed='right').last()
        resampled_twap = resampled_twap.ffill()
        twap_pos_list.append(resampled_twap)
        
# 将所有的 resampled_twap 拼接起来，保持所有列
final_twap_pos = pd.concat(twap_pos_list, axis=0, join='outer')

# 对拼接后的结果进行一次 ffill 确保所有数据都填充完整
final_twap_pos = final_twap_pos.ffill()

final_twap_pos.to_parquet(dir_to_save / 'final_old_thunder_twap_to_3s.parquet')
