# -*- coding: utf-8 -*-
"""
Created on Sat Oct 12 11:21:52 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import zmq


from utility.decorator_utils import run_by_thread


# %% 
class StrategySignalSender:
    
    def __init__(self, zmq_address: str, version: str = "4.0"):
        """
        初始化ZMQ套接字，用于发送策略信号消息，并设置版本号。
        
        参数:
        zmq_address (str): ZMQ服务端的连接地址。
        version (str): 消息版本号，默认为 "3.0"。
        """
        self.zmq_address = zmq_address
        self.version = version
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(self.zmq_address)
        self.socket.setsockopt(zmq.SNDHWM, 0)  # 设置高水位线为0，防止消息丢失
    
    @run_by_thread(daemon=False)
    def send_message(self, strategy_name: str, exchange: str, symbol_type: str, symbol: str, position: str):
        """
        发送策略信号消息。
        
        参数:
        strategy_name (str): 策略名称。
        exchange (str): 交易所名称，例如 "BN", "OK"。
        symbol_type (str): 交易品种类型，例如 "PERPETUAL", "SPOT"。
        symbol (str): 交易品种，例如 "BTCUSDT"。
        position (str): 仓位，使用字符串表示，不能使用科学计数法。
        """
        message = f"{self.version},{strategy_name},{exchange},{symbol_type},{symbol},{position}"
        self.socket.send_string(message)

    def close(self):
        """关闭ZMQ套接字和上下文。"""
        self.socket.close()
        self.context.term()


# %% 使用示例
if __name__ == "__main__":
    sender = StrategySignalSender("tcp://172.16.20.247:10086", version="3.0")
    
    # 发送示例消息
    sender.send_message("TMP_BN_STRATEGY", "BN", "PERPETUAL", "BTCUSDT", "0.01")
    # sender.send_message("TMP_OK_STRATEGY", "OK", "SPOT", "BTC-USDT", "0.01")
    
    sender.close()

