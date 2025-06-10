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
def align_columns(main_col, sub_df):
    sub_aligned = sub_df.reindex(columns=main_col)
    return sub_aligned


def align_index(df_1, df_2):
    inner_index = df_1.index.intersection(df_2.index)
    return df_1.loc[inner_index, :], df_2.loc[inner_index, :]


def align_index_with_main(main_index, sub_df):
    # 使用reindex直接对齐索引
    sub_aligned = sub_df.reindex(index=main_index)
    return sub_aligned


# %%
fee_rate = 0.00075


# %%
dir_to_save = Path('D:/crypto/prod/alpha/portfolio_management/verify/old_thunder_pos_250225')
price_dir = Path('D:/crypto/prod/alpha/portfolio_management/verify/price')


# %%
final_twap_pos = pd.read_parquet(dir_to_save / 'final_old_thunder_twap_to_3s.parquet')
bid1 = pd.read_parquet(price_dir / 'bid1.parquet')
ask1 = pd.read_parquet(price_dir / 'ask1.parquet')
midprice_raw = (bid1 + ask1) / 2
# del bid1, ask1


# %%
final_twap_pos.index -= timedelta(hours=8)


# %%
main_index = final_twap_pos.index
main_col = final_twap_pos.columns
midprice_raw.columns = midprice_raw.columns.str.upper()
midprice = align_columns(main_col, midprice_raw)
midprice, final_twap_pos = align_index(midprice, final_twap_pos)


# %%
import numpy as np

# 这里计算仓位差异时，我们假设当前时间的仓位会延续到下一个 3 秒时间点
pos_shifted = final_twap_pos.shift(1)  # 将当前时间的仓位传递到下一个时间点

# 计算价格差异
price_diff = midprice.replace(0, np.nan).diff().fillna(0)

# 计算 PNL: PNL = (价格差异) * (当前时间点仓位)
# 这里使用 shifted 仓位（目标仓位传递到下一个时间段）
pnl = price_diff * pos_shifted

# 汇总所有币种的 PNL
total_pnl = pnl.sum(axis=1)


# %%
# 计算仓位变化 (仓位的diff)
pos_diff = final_twap_pos.diff().fillna(0)

# 计算换仓的手续费
# 手续费 = abs(仓位变化) * 当前时间的价格 * 手续费率
# 当前时间的价格是 midprice 中相应时间点的价格
fee = abs(pos_diff) * midprice * fee_rate

# 汇总所有币种的手续费
total_fee = fee.sum(axis=1)


# %%
# 先将 total_pnl 和 total_fee 转换为 DataFrame
aggregated_data = pd.DataFrame({'PNL': total_pnl, 'Fee': total_fee})

aggregated_data_resampled = aggregated_data.resample('30min', label='right', closed='right').sum()

aggregated_data_resampled.cumsum().plot()


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


# %%
old_thunder_pnl, theo_pnl = align_index(aggregated_data_resampled, all_data)
old_thunder_pnl['net_pnl'] = (aggregated_data_resampled['PNL'] - aggregated_data_resampled['Fee']) / 10000


# %%
import matplotlib.pyplot as plt

# 计算累计 PNL
old_thunder_pnl['cumulative_net_pnl'] = old_thunder_pnl['net_pnl'].cumsum()
theo_pnl['cumulative_net_pnl'] = theo_pnl['net_pnl'].cumsum()

# 创建图表
plt.figure(figsize=(10, 6))

# 绘制两个 PNL 曲线
plt.plot(old_thunder_pnl.index, old_thunder_pnl['cumulative_net_pnl'], label='Old Thunder PNL', linestyle='-', linewidth=2)
plt.plot(theo_pnl.index, theo_pnl['cumulative_net_pnl'], label='Theo PNL', linestyle='--', linewidth=2)

# 添加标题
plt.title('Cumulative Net PNL Comparison', fontsize=16)

# 添加X轴和Y轴标签
plt.xlabel('Time', fontsize=12)
plt.ylabel('Cumulative Net PNL', fontsize=12)

# 添加网格
plt.grid(True, linestyle='--', alpha=0.6)

# 添加图例
plt.legend()

# 显示图表
plt.tight_layout()
plt.show()


# %%
pnl_cut = pnl.loc['2025-02-06 23:30:00': '2025-02-07 00:30:00']
