# -*- coding: utf-8 -*-
"""
Created on Fri Nov  8 10:36:41 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %%
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[1]
sys.path.append(str(project_dir))


# %%
from utility.dirutils import load_path_config


# %%
pos_name = 'agg_241114_to_00125_v0'
model_name = 'merge_agg_241227_cgy_zxt_double3m_15d_73'
backtest_name = 'to_00125_maxmulti_2_mm_03_pf_001'
# start_from = '2025-01-14 16:00:00'
start_from = '2025-01-01'


# %%
path_config = load_path_config(project_dir)
persist_dir = Path(path_config['persist'])
period_pnl_dir = persist_dir / pos_name / 'period_pnl'
backtest_dir = Path(rf'D:\crypto\multi_factor\factor_test_by_alpha\results\model\{model_name}\backtest\{backtest_name}')
pft_name = f'profit_{model_name}__{backtest_name}'


# %%
# 遍历读取所有 parquet 文件并按日期排序
data_frames = []
for file_path in sorted(period_pnl_dir.glob('*.parquet')):  # 使用 pathlib 的 glob 方法
    df = pd.read_parquet(file_path)
    data_frames.append(df)

# 拼接所有数据并按时间排序
all_data = pd.concat(data_frames)
all_data.sort_index(inplace=True)  # 直接按时间索引排序
all_data = all_data.loc[start_from:]

# 计算累计的 net_pnl 和 fee
all_data['cumulative_net_pnl'] = all_data['net_pnl'].cumsum()
all_data['cumulative_fee'] = all_data['fee'].cumsum()

# 计算滚动最大回撤
roll_max = all_data['cumulative_net_pnl'].cummax()
drawdown = roll_max - all_data['cumulative_net_pnl']
drawdown_percentage = drawdown / roll_max
all_data['drawdown'] = -drawdown

# backtest
pft = pd.read_parquet(backtest_dir / f'{pft_name}.parquet')
pft['net'] = pft['raw_rtn_twd30_sp30'] + pft['fee']
pft = pft.loc[start_from:]

# 绘图
FONT_1 = 40
FONT_2 = 36
FONT_3 = 30
PAD_1 = 30
PAD_2 = 20
fig, ax1 = plt.subplots(figsize=(36, 27))  # 设置更大图像尺寸

# 累计 net_pnl 和 fee 折线图
ax1.plot(all_data.index, all_data['cumulative_net_pnl'], label='Cumulative Net PnL', color='red', linewidth=5)
ax1.plot(all_data.index, all_data['cumulative_fee'], label='Cumulative Fee', color='orange')
ax1.plot(pft.index, pft['net'].cumsum(), label='Backtest Net Pnl', color='k', linewidth=5, linestyle='--')
ax1.set_xlabel('Date', fontsize=FONT_2, labelpad=PAD_2)
ax1.set_ylabel('Cumulative Value', fontsize=FONT_2, labelpad=PAD_2)
ax1.legend(loc='upper left', fontsize=FONT_2)
ax1.tick_params(axis='both', which='major', labelsize=FONT_3)  # 调整刻度字体大小
ax1.tick_params(axis='x', rotation=45)
ax1.grid(True)  # 添加网格

# 绘制最大回撤的瀑布式柱状图
ax2 = ax1.twinx()
ax2.bar(all_data.index, all_data['drawdown'], color='skyblue', alpha=0.3, label='Drawdown')
ax2.set_ylim(-0.5, 0)  # 设置 y 轴范围为 -0.5 到 0
ax2.set_ylabel('Drawdown', fontsize=FONT_2, labelpad=PAD_2)
ax2.tick_params(axis='y', labelsize=FONT_3)
ax2.legend(loc='upper right', fontsize=FONT_2)

# 设置标题
plt.title('Cumulative Net PnL, Fee, and Rolling Maximum Drawdown', fontsize=FONT_1, pad=PAD_1)  # 标题字体更大
plt.show()


# %%
from datetime import timedelta

# all_data.index = all_data.index + timedelta(hours=8)
# daily_pnl = all_data.resample('1d', label='right').sum()
# daily_pnl.loc[:, 'net_pnl'].cumsum().plot(grid=':')


# %%
# =============================================================================
# # 先调整时区为16:00
# all_data.index = all_data.index + timedelta(hours=8)  # 将时间调整为北京时间
# all_data.index = all_data.index + timedelta(hours=16)  # 调整为16:00
# 
# # 重新按天进行汇总
# daily_pnl = all_data.resample('1d', label='right', closed='right').sum()
# 
# # 绘制累计净利润
# daily_pnl.loc[:, 'net_pnl'].cumsum().plot(grid=':')
# =============================================================================
