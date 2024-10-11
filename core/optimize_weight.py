# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 13:24:16 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import cvxpy as cp
import numpy as np
import pandas as pd


# %% simple
def future_optimal_weight_lp_cvxpy(alpha, w0, mm_t, his_pft_t, to_rate_thresh, 
                                   max_multi=1, max_wgt=None, momentum_limits={}, pf_limits={}):
# =============================================================================
#     # 过滤和调整输入数据
#     w0 = w0 - np.mean(w0) # ???
#     w0 = w0 / np.sum(np.abs(w0)) if np.sum(np.abs(w0)) != 0 else w0
# =============================================================================
    
    # 定义变量
    n = alpha.size
    w = cp.Variable(n)
    
    # 目标函数
    objective = cp.Maximize(cp.sum(cp.multiply(alpha, w)))

    # 约束列表
    constraints = []
    
    # 总权重为0
    constraints.append(cp.sum(w) == 0)
    
    # 权重绝对值和为1
    constraints.append(cp.norm(w, 1) <= 1)
    
    # 换手率控制
    constraints.append(cp.norm(w - w0, 1) <= to_rate_thresh * 2)
    
    # 单个资产权重控制
    if max_wgt is None:
        alpha_r = pd.Series(alpha)
        alpha_r = alpha_r.rank(pct=True).sub(0.5 / alpha_r.count()).replace([np.inf, -np.inf], np.nan) - 0.5
        max_wgt = (alpha_r / np.abs(alpha_r).sum()).max() * max_multi # org
    constraints += [w <= max_wgt, w >= -max_wgt]

    # 动量约束
    for mm_wd in momentum_limits:
        if mm_wd not in mm_t:
            continue
        try:
            mm_sum = cp.sum(mm_t[mm_wd] @ w)
        except:
            breakpoint()
        mm_thres = momentum_limits[mm_wd]
        constraints += [
            mm_sum >= - mm_thres,
            mm_sum <= mm_thres
        ]
        
    # 盈亏约束
    for pf_wd in pf_limits:
        if pf_wd not in his_pft_t:
            continue
        pf_sum = cp.sum(his_pft_t[pf_wd] @ w)
        pf_thres = pf_limits[pf_wd]
        constraints += [
            pf_sum >= - pf_thres,
            pf_sum <= pf_thres
        ]
        
    problem = cp.Problem(objective, constraints)
    try:
        problem.solve(solver=cp.ECOS, verbose=False)
    except:
        return w0, 'error', None
    w1 = w.value

    # 定义和求解问题
    try:
        w1 = w1 / pd.Series(w1).abs().sum()
    except:
        w1 = w0

    return pd.Series(w1, index=w0.index), problem.status
