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
import time
import zmq


from utility.decorator_utils import run_by_thread


# %% 
class StrategySignalSender:
    
    def __init__(self, zmq_address: str, version: str = "3.0"):
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
        

# %%
class TwapSignalSender:
    
    def __init__(self, zmq_address: str, version: str = "3.0"):
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

    
    def send_message(self, strategy_name: str, exchange: str, symbol_type: str, symbol: str, position: str, split_mode: str):
        """
        发送策略信号消息。
        
        参数:
        strategy_name (str): 策略名称。
        exchange (str): 交易所名称，例如 "BN", "OK"。
        symbol_type (str): 交易品种类型，例如 "PERPETUAL", "SPOT"。
        symbol (str): 交易品种，例如 "BTCUSDT"。
        position (str): 仓位，使用字符串表示，不能使用科学计数法。
        split_mode: 拆单模式(大写)，例如 "TWAP"。目前只有"TWAP"。
        """
        time_start = str(int(time.time() * 1e6)) # 微秒级时间戳
        split_mode = split_mode.upper()
        message = f"{time_start},{self.version},{strategy_name},{exchange},{symbol_type},{symbol},{position},{split_mode}"
        self.socket.send_string(message)

    def close(self):
        """关闭ZMQ套接字和上下文。"""
        self.socket.close()
        self.context.term()


# %% 使用示例
if __name__ == "__main__":
    sender = TwapSignalSender("tcp://172.16.30.192:10086")
    sender.send_message("ZXT_TEST", "BN", "FUP", "BTCUSDT", "0.02", "TWAP")    

    # {time_start},{self.version},{strategy_name},{exchange},{symbol_type},{symbol},{position},{split_mode},{is_init}
    # {time_start} 不用输入
    # """
    # 1739434857544433,3.0,TMP_BN_STRATEGY000,BN,SPOT,BTCUSDT,0.02,TWAP,True
    # 1739434858545633,3.0,TMP_BN_STRATEGY000,OK,PERPETUAL,BTCUSDT,0.02,TWAP,True
    # 1739434859546678,3.0,TMP_BN_STRATEGY000,OK,SPOT,BTCUSDT,0.02,TWAP,True
    # 1739434860547661,3.0,TMP_BN_STRATEGY000,BN,PERPETUAL,BTCUSDT,0.05,TWAP,False
    # """
    sender.close()



