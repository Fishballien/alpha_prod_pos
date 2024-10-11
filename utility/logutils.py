# -*- coding: utf-8 -*-
"""
Created on Sat Jul 13 15:32:48 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import sys
from loguru import logger


# %% logger
class FishStyleLogger:
    
    def __init__(self):
        self._configure_logger()
    
    def _configure_logger(self):
        logger.level("DEBUG", color="<blue>")
        logger.level("INFO", color="")
        logger.level("SUCCESS", color="<green>")
        logger.level("WARNING", color="<yellow>")
        logger.level("ERROR", color="<red>")
        logger.level("CRITICAL", color="<bold><red>")
        
        logger.configure(
            handlers=[
                {
                    "sink": sys.stdout,
                    "colorize": True,
                    "format": "{time:YYYY-MM-DD HH:mm:ss} <level>[{level}]</level> <level>{message}</level>",
                    "backtrace": False  # 设置 backtrace=False 为默认值
                }
            ]
        )
        self._logger = logger

    def __getattr__(self, name):
        return getattr(self._logger, name)


# %%
if __name__=='__main__':
    # 使用示例
    my_log = FishStyleLogger()
    my_log.info("This is an info message")
    my_log.error("This is an error message")

