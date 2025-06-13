# -*- coding: utf-8 -*-
"""
Created on 2025-06-13

验证启动脚本

用于运行回测验证，对比历史回放与回测仓位的一致性

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
from core.pos_updater_backtest_validator import PosUpdaterBacktestValidator


# %% 
def main(stg_name=None):
    '''run validation'''
    if stg_name is None:
        parser = argparse.ArgumentParser(description='运行回测验证，对比历史回放与回测仓位的一致性')
        parser.add_argument('-s', '--stg_name', type=str, required=True, help='策略名称')
    
        args = parser.parse_args()
        stg_name = args.stg_name
    
    print(f"Starting validation for strategy: {stg_name}")
    
    try:
        # 创建验证器实例（会自动开始验证流程）
        validator = PosUpdaterBacktestValidator(stg_name)
        
        print("Validation completed. Check logs and reports for details.")
        
    except KeyboardInterrupt:
        print("Validation interrupted by user")
    except Exception as e:
        print(f"Validation failed with error: {e}")
        raise


# %% main
if __name__ == "__main__":
    # 可以直接在这里指定策略名称进行测试
    main()
    # 或者使用命令行参数
    # main()