# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 12:58:52 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import sys
from pathlib import Path
import argparse


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[1]
sys.path.append(str(project_dir))


# %%
from core.pos_updater import PosUpdaterWithBacktest


# %% 
def main(stg_name=None):
    '''read args'''
    if stg_name is None:
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', '--stg_name', type=str, help='stg_name')
    
        args = parser.parse_args()
        stg_name = args.stg_name
    
    pos_updater = PosUpdaterWithBacktest(stg_name)
    pos_updater.run()
    

# %% main
if __name__ == "__main__":
    # stg_name = 'agg_240902_to_00125_v0_test'
    # main(stg_name)
    main()
