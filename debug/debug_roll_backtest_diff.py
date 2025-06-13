# -*- coding: utf-8 -*-
"""
Created on Fri Jun 13 16:53:32 2025

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import pickle


# %%
rt_path = 'D:/crypto/prod/alpha/portfolio_management/verify/roll_from_his_backtest/rt/debug_data_20250605_003000.pkl'


# %%
with open(rt_path, 'rb') as f:
    rt_data = pickle.load(f)
    
    
    
