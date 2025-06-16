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
import pandas as pd
import numpy as np


# %%
date_ts = '20250605_003000'
rt_path = f'D:/crypto/prod/alpha/portfolio_management/verify/roll_from_his_backtest/rt/debug_data/debug_data_{date_ts}.pkl'
bt_path = f'D:/crypto/prod/alpha/portfolio_management/verify/roll_from_his_backtest/bt/debug_data/debug_data_{date_ts}.pkl'

# %%
with open(rt_path, 'rb') as f:
    rt_data = pickle.load(f)
    
with open(bt_path, 'rb') as f:
    bt_data = pickle.load(f)
        
    
# %% Get common symbols and use bt_available_symbols for reindexing
bt_symbols = bt_data['available_symbols']
rt_symbols = rt_data['available_symbols']

print(f"BT symbols count: {len(bt_symbols)}")
print(f"RT symbols count: {len(rt_symbols)}")
print(f"Common symbols count: {len(set(bt_symbols) & set(rt_symbols))}")

# %% Helper function to convert array to series and reindex
def array_to_series_reindex(array_data, original_symbols, target_symbols):
    if isinstance(array_data, dict):
        return {k: pd.Series(v, index=original_symbols).reindex(target_symbols).fillna(0) 
                for k, v in array_data.items()}
    else:
        return pd.Series(array_data, index=original_symbols).reindex(target_symbols).fillna(0)

# %% Create mappings
# mm_t mapping: bt key * 4 = hours -> rt key
mm_mapping = {11: '44h', 22: '88h', 33: '132h', 44: '176h'}
# his_pft_t mapping: bt key * 4 hours = rt key
pft_mapping = {1: '4h', 6: '1d', 18: '3d', 30: '5d'}

# %% Process and compare mm_t
rt_mm_t = array_to_series_reindex(rt_data['mm_t'], rt_symbols, bt_symbols)
bt_mm_t = bt_data['mm_t']

print("\n=== mm_t comparison ===")
for bt_key, rt_key in mm_mapping.items():
    if bt_key in bt_mm_t and rt_key in rt_mm_t:
        rt_series = rt_mm_t[rt_key]
        bt_series = pd.Series(bt_mm_t[bt_key], index=bt_symbols)
        diff = (rt_series - bt_series).abs()
        print(f"mm_t[{bt_key}→{rt_key}] max diff: {diff.max():.10f}, mean diff: {diff.mean():.10f}")

# %% Process and compare his_pft_t
rt_his_pft_t = array_to_series_reindex(rt_data['his_pft_t'], rt_symbols, bt_symbols)
bt_his_pft_t = bt_data['his_pft_t']

print("\n=== his_pft_t comparison ===")
for bt_key, rt_key in pft_mapping.items():
    if bt_key in bt_his_pft_t and rt_key in rt_his_pft_t:
        rt_series = rt_his_pft_t[rt_key]
        bt_series = pd.Series(bt_his_pft_t[bt_key], index=bt_symbols)
        diff = (rt_series - bt_series).abs()
        # breakpoint()
        print(f"his_pft_t[{bt_key}→{rt_key}] max diff: {diff.max():.10f}, mean diff: {diff.mean():.10f}")

# %% Process and compare current_pos
# if hasattr(rt_data['current_pos'], 'index'):
#     rt_current_pos = rt_data['current_pos'].reindex(bt_symbols).fillna(0)
# else:
#     rt_current_pos = pd.Series(rt_data['current_pos'], index=rt_symbols).reindex(bt_symbols).fillna(0)

# if hasattr(bt_data['current_pos'], 'index'):
#     bt_current_pos = bt_data['current_pos'].reindex(bt_symbols).fillna(0)
# else:
#     bt_current_pos = pd.Series(bt_data['current_pos'], index=bt_symbols)

# print("\n=== current_pos comparison ===")
# diff = (rt_current_pos - bt_current_pos).abs()
# print(f"current_pos max diff: {diff.max():.6f}, mean diff: {diff.mean():.6f}")

# %% Process and compare alpha
if hasattr(rt_data['alpha'], 'index'):
    rt_alpha = rt_data['alpha'].reindex(bt_symbols).fillna(0)
else:
    rt_alpha = pd.Series(rt_data['alpha'], index=rt_symbols).reindex(bt_symbols).fillna(0)

if hasattr(bt_data['alpha'], 'index'):
    bt_alpha = bt_data['alpha'].reindex(bt_symbols).fillna(0)
else:
    bt_alpha = pd.Series(bt_data['alpha'], index=bt_symbols)

print("\n=== alpha comparison ===")
diff = (rt_alpha - bt_alpha).abs()
print(f"alpha max diff: {diff.max():.10f}, mean diff: {diff.mean():.10f}")

# %% Print top differences for each comparison
print("\n=== Top 5 differences ===")
# print("current_pos top diffs:")
# print((rt_current_pos - bt_current_pos).abs().nlargest(5))

print("\nalpha top diffs:")
print((rt_alpha - bt_alpha).abs().nlargest(5))

# %% Check symbols differences
print("\n=== Symbol differences ===")
rt_only = set(rt_symbols) - set(bt_symbols)
bt_only = set(bt_symbols) - set(rt_symbols)
print(f"RT only symbols ({len(rt_only)}): {list(rt_only)[:10]}")
print(f"BT only symbols ({len(bt_only)}): {list(bt_only)[:10]}")