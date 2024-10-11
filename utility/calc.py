# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 22:34:27 2024

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
def calc_profit_before_next_t(w0, w1, rtn_c2c, rtn_cw0, rtn_cw1):
    n_assets = w0.shape[0]
    
    # Initialize profit arrays
    hold_pft1 = np.zeros(n_assets)
    hold_pft2 = np.zeros(n_assets)
    hold_pft3 = np.zeros(n_assets)
    hold_pft4 = np.zeros(n_assets)

    # Case 1: w0 >= 0 & w1 >= 0
    w01 = np.where((w0 >= 0) & (w1 >= 0), w0, 0)
    w11 = np.where((w0 >= 0) & (w1 >= 0), w1, 0)
    cw = w01 - w11
    cw0 = np.where(cw > 0, cw, 0)
    cw1 = np.where(cw < 0, -cw, 0)
    hold_pft1 = rtn_c2c.values * (w01 - cw0) + rtn_cw0.values * cw0 + rtn_cw1.values * cw1

    # Case 2: w0 < 0 & w1 < 0
    w02 = np.where((w0 < 0) & (w1 < 0), w0, 0)
    w12 = np.where((w0 < 0) & (w1 < 0), w1, 0)
    cw = w02 - w12
    cw0 = np.where(cw > 0, -cw, 0)
    cw1 = np.where(cw < 0, cw, 0)
    hold_pft2 = rtn_c2c.values * (w02 - cw1) + rtn_cw1.values * cw0 + rtn_cw0.values * cw1

    # Case 3: w0 < 0 & w1 >= 0
    w03 = np.where((w0 < 0) & (w1 >= 0), w0, 0)
    w13 = np.where((w0 < 0) & (w1 >= 0), w1, 0)
    hold_pft3 = rtn_cw0.values * w03 + rtn_cw1.values * w13

    # Case 4: w0 >= 0 & w1 < 0
    w04 = np.where((w0 >= 0) & (w1 < 0), w0, 0)
    w14 = np.where((w0 >= 0) & (w1 < 0), w1, 0)
    hold_pft4 = rtn_cw0.values * w04 + rtn_cw1.values * w14

    # Sum up all holding profits
    hold_pft = hold_pft1 + hold_pft2 + hold_pft3 + hold_pft4
    
    return hold_pft