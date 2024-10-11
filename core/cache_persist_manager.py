# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 10:33:12 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import pandas as pd
from abc import ABC, abstractmethod
import threading
from collections import defaultdict


from utility.timeutils import get_curr_utc_date, get_date_based_on_timestamp
from utility.datautils import add_row_to_dataframe_reindex


# %%
class ParquetManager(ABC):
    
    def __init__(self, log=None):
        self.log = log
        self.locks = defaultdict(threading.Lock)  # 初始化锁
        
    def _add_row(self, dataframe, new_row, index):
        dataframe = add_row_to_dataframe_reindex(dataframe, new_row, index)
        return dataframe

    def _save_to_parquet(self, path, dataframe):
        dataframe.to_parquet(path)
        if self.log:
            self.log.success(f'Successfully saved data to {path}')

    def _load_from_parquet(self, path):
        if path.exists():
            try:
                dataframe = pd.read_parquet(path)
                if self.log:
                    self.log.success(f'Successfully loaded Parquet from {path}')
                return dataframe
            except Exception as e:
                if self.log:
                    self.log.warning(f'Parquet file unreadable: {path}. Error: {e}')
        return pd.DataFrame()  # Return an empty DataFrame if loading fails
    
    
class CacheManager(ParquetManager):
    
    def __init__(self, cache_dir, cache_lookback, cache_list=[], log=None):
        super().__init__(log=log)
        self.cache_dir = cache_dir  # 确保 cache_dir 是 Path 对象
        self.cache_lookback = cache_lookback
        self.cache_list = cache_list
        
        if self.cache_list:
            self.log.success(f'Cache List Registered: {self.cache_list}')

        self._init_containers()
        self._load_or_init_cache()
        
    def _init_containers(self):
        self.cache = defaultdict(pd.DataFrame)

    def _load_or_init_cache(self):
        for cache_name in self.cache_list:
            cache_path = self.cache_dir / f'{cache_name}.parquet'
            self.cache[cache_name] = self._load_from_parquet(cache_path)

    def _cut_cache(self, cache_name, curr_ts):
        cut_time = pd.Timestamp(curr_ts - self.cache_lookback)
        self.cache[cache_name] = self.cache[cache_name].loc[cut_time:]
            
    def save(self, ts):
        for cache_name in self.cache_list:
            with self.locks[cache_name]:
                self._cut_cache(cache_name, ts)  # 保留 cut_cache 步骤
            
            cache_path = self.cache_dir / f'{cache_name}.parquet'
            self._save_to_parquet(cache_path, self.cache[cache_name])
            
    def add_row(self, cache_name, new_row, index):
        with self.locks[cache_name]:
            self.cache[cache_name] = self._add_row(self.cache[cache_name], new_row, index)
        
    def __getitem__(self, cache_name):
        return self.cache[cache_name]
    
    def __setitem__(self, cache_name, data):
        self.cache[cache_name] = data


class PersistManager(ParquetManager):
    
    def __init__(self, persist_dir, persist_list=[], log=None):
        super().__init__(log=log)
        self.persist_dir = persist_dir  # 确保 persist_dir 是 Path 对象
        
        self._init_containers()
        self._init_persist_list(persist_list)
        self._load_or_init_persist()
        
    def _init_containers(self):
        self.factor_persist = defaultdict(pd.DataFrame)
        
    def _init_persist_list(self, persist_list):
        self.persist_list = persist_list
        
        if self.persist_list:
            self.log.success(f'Persist List Registered: {self.persist_list}')
            
        for persist_name in persist_list:
            persist_name_dir = self.persist_dir / persist_name
            persist_name_dir.mkdir(parents=True, exist_ok=True)
        
    def _load_or_init_persist(self):
        date_to_load = get_curr_utc_date()  # 根据当前日期生成文件名
        for key in self.persist_list:
            path = self.persist_dir / key / f'{date_to_load}.parquet'
            self.factor_persist[key] = self._load_from_parquet(path)
    
    def save(self, ts):
        date_to_save = get_date_based_on_timestamp(ts)  # 根据传入时间戳生成日期
        for key in self.persist_list:
            with self.locks[key]:
                self.factor_persist[key] = self.factor_persist[key].loc[date_to_save:]  # 直接裁剪当天的数据
                
                # 保存到 Parquet 文件
                path = self.persist_dir / key / f'{date_to_save}.parquet'
                self._save_to_parquet(path, self.factor_persist[key])
        
    def add_row(self, key, new_row, index):
        with self.locks[key]:
            self.factor_persist[key] = self._add_row(self.factor_persist[key], new_row, index)