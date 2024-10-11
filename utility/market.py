# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 14:33:01 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import json
import requests
from collections import namedtuple
from pathlib import Path


# %%
MINIMUM_SIZE_FILTER = 1e-8


# %% struct
Exchange = namedtuple('Exchange', ['name', 'tardis_name', 'api_name'])


usd = Exchange('usd', 'binance-futures', 'fapi')
coin = Exchange('coin', 'binance-delivery', 'dapi')
spot = Exchange('spot', 'binance', 'api') # !!!: not sure if the api_name is correct


# %%
def load_binance_data(exchange, file_dir):
    try:
        file_path = file_dir / f'exchange_info_{exchange.name}.json'
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"读取数据时出错: {e}")
        return None


def get_binance_tick_size(data):
    tick_sizes = {}
    
    for symbol_info in data['symbols']:
        if symbol_info['quoteAsset'] == 'USDT': #  and symbol_info.get('contractType') == 'PERPETUAL'
            symbol = symbol_info['symbol'].lower()
            for filter in symbol_info['filters']:
                if filter['filterType'] == 'PRICE_FILTER':
                    tick_size = float(filter['tickSize'])
                    tick_sizes[symbol] = tick_size
                    break
    
    return tick_sizes


def fetch_and_save_binance_data(exchange, file_dir):
    url = f'https://{exchange.api_name}.binance.com/{exchange.api_name}/v1/exchangeInfo'
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # 保存数据到文件
        file_path = file_dir / f'.exchange_info_{exchange.name}.json'
        print(file_path)
        with open(file_path, 'w') as f:
            json.dump(data, f)
        
        print("数据已保存")
    except Exception as e:
        print(f"获取或保存数据时出错: {e}")
        
        
# %%
if __name__=='__main__':
    exchange = usd
    file_dir = Path(r'D:\mnt\Data\xintang\pool\binance_usd_multi')
    fetch_and_save_binance_data(exchange, file_dir)