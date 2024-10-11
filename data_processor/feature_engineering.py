# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 11:31:43 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import numpy as np


# %%
def calculate_rank(series):
    """
    对 series 进行百分比排名，并进行调整。
    """
    # 对 series 进行百分比排名
    pct_rank = series.rank(pct=True)
    
    # 调整百分比排名，减去 0.5 / 非 NaN 元素的个数
    adjustment = 0.5 / series.count()
    pct_rank_adjusted = pct_rank.sub(adjustment)
    
    # 替换无穷大和负无穷大的值为 NaN
    pct_rank_adjusted = pct_rank_adjusted.replace([np.inf, -np.inf], np.nan)
    
    return pct_rank_adjusted


def calculate_weight_from_rank(adjusted_rank):
    """
    计算从百分比排名中得到的权重，并进行归一化。
    """
    # 计算百分比排名的双倍差值
    adjusted_pct_diff = 2 * (adjusted_rank - 0.5)
    
    # 归一化，使权重的绝对值和为 1
    total_abs_sum = adjusted_pct_diff.abs().sum()

    if total_abs_sum != 0:
        normalized_weight = adjusted_pct_diff / total_abs_sum
    else:
        normalized_weight = adjusted_pct_diff
    
    # 填充 NaN 值为 0
    normalized_weight = normalized_weight.fillna(0)
    
    return normalized_weight