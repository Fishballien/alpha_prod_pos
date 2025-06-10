# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 10:45:00 2025

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %%
import pandas as pd
from datetime import timedelta


# %%
new_thunder_pnl_path = 'D:/crypto/prod/alpha/portfolio_management/verify/new_thunder_250225.csv'
theo_pnl_path = 'D:/crypto/prod/alpha/portfolio_management/verify/profit_twd30_sp30_with_fund_rate_v3_online.parquet'


# %%
new_thunder_pnl = pd.read_csv(new_thunder_pnl_path)
new_thunder_pnl['date'] = pd.to_datetime(new_thunder_pnl['日期'])
new_thunder_pnl.set_index('date', inplace=True)
new_thunder_pnl['capital'] = 200000
new_thunder_pnl['thunder_daily_rtn'] = new_thunder_pnl['盈亏'] / new_thunder_pnl['capital']


# %%
theo_pnl = pd.read_parquet(theo_pnl_path)
theo_pnl.index += timedelta(hours=8)
theo_daily_pnl = theo_pnl.resample('1d', label='left').sum()
theo_daily_pnl['theo_daily_rtn'] = theo_daily_pnl['raw_rtn_twd30_sp30'] + theo_daily_pnl['fee'] + theo_daily_pnl['fund_rate_fee']


# %%
merged_pnl = theo_daily_pnl.join(new_thunder_pnl, how='outer')


# %%
import matplotlib.pyplot as plt

# 过滤数据，从 2025-02-01 开始
start_date = '2025-02-01'
end_date = '2025-02-25'
cumsum_thunder = merged_pnl.loc[start_date:end_date, 'thunder_daily_rtn'].cumsum()
cumsum_theo = merged_pnl.loc[start_date:end_date, 'theo_daily_rtn'].cumsum()

daily_thunder = merged_pnl.loc[start_date:end_date, 'thunder_daily_rtn']
daily_theo = merged_pnl.loc[start_date:end_date, 'theo_daily_rtn']

# 创建子图
fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1]})

# 绘制累积收益曲线
axes[0].plot(cumsum_thunder, label='Thunder Cumulative Return', linewidth=2)
axes[0].plot(cumsum_theo, label='Theo Cumulative Return', linewidth=2, linestyle='dashed')

axes[0].set_title('Cumulative Returns from 2025-02-01', fontsize=14)
axes[0].set_ylabel('Cumulative Return', fontsize=12)
axes[0].grid(True, linestyle='--', alpha=0.6)
axes[0].legend(fontsize=12)

# 绘制每日收益柱状图
width = 0.4  # 控制柱子的宽度
dates = daily_thunder.index

axes[1].bar(dates, daily_thunder, width=width, label='Thunder Daily Return', align='edge', alpha=0.7)
axes[1].bar(dates, daily_theo, width=-width, label='Theo Daily Return', align='edge', alpha=0.7)

axes[1].set_title('Daily Returns from 2025-02-01', fontsize=14)
axes[1].set_xlabel('Date', fontsize=12)
axes[1].set_ylabel('Daily Return', fontsize=12)
axes[1].grid(True, linestyle='--', alpha=0.6)
axes[1].legend(fontsize=12)

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()