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
from pathlib import Path
from datetime import datetime, timedelta


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
        
        if self.cache_list and self.log:
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
        # 新增：缓存已加载的历史数据，避免重复读取文件
        self._historical_data_cache = defaultdict(dict)  # {data_name: {date: DataFrame}}
        
    def _init_persist_list(self, persist_list):
        self.persist_list = persist_list
        
        if self.persist_list and self.log:
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
    
    def has_data(self, data_name, timestamp):
        """检查特定时间戳的数据是否存在"""
        try:
            # 首先检查当前内存中的数据
            current_data = self.factor_persist.get(data_name)
            if current_data is not None and timestamp in current_data.index:
                return True
            
            # 如果内存中没有，检查历史文件
            target_date = get_date_based_on_timestamp(timestamp)
            
            # 检查是否已缓存该日期的数据
            if target_date in self._historical_data_cache[data_name]:
                cached_data = self._historical_data_cache[data_name][target_date]
                return timestamp in cached_data.index
            
            # 尝试加载该日期的文件
            historical_data = self._load_historical_data(data_name, target_date)
            if historical_data is not None:
                return timestamp in historical_data.index
            
            return False
            
        except Exception as e:
            if self.log:
                self.log.warning(f"Error checking data existence for {data_name} at {timestamp}: {e}")
            return False

    def get_row(self, data_name, timestamp):
        """获取特定时间戳的数据行"""
        try:
            # 首先检查当前内存中的数据
            current_data = self.factor_persist.get(data_name)
            if current_data is not None and timestamp in current_data.index:
                return current_data.loc[timestamp]
            
            # 如果内存中没有，从历史文件中获取
            target_date = get_date_based_on_timestamp(timestamp)
            
            # 检查是否已缓存该日期的数据
            if target_date in self._historical_data_cache[data_name]:
                cached_data = self._historical_data_cache[data_name][target_date]
                if timestamp in cached_data.index:
                    return cached_data.loc[timestamp]
            
            # 尝试加载该日期的文件
            historical_data = self._load_historical_data(data_name, target_date)
            if historical_data is not None and timestamp in historical_data.index:
                return historical_data.loc[timestamp]
            
            return None
            
        except Exception as e:
            if self.log:
                self.log.warning(f"Error getting data for {data_name} at {timestamp}: {e}")
            return None

    def _load_historical_data(self, data_name, target_date):
        """加载指定日期的历史数据并缓存"""
        try:
            # 如果已经缓存，直接返回
            if target_date in self._historical_data_cache[data_name]:
                return self._historical_data_cache[data_name][target_date]
            
            # 构建文件路径
            historical_path = self.persist_dir / data_name / f'{target_date}.parquet'
            
            # 加载数据
            historical_data = self._load_from_parquet(historical_path)
            
            # 缓存数据（即使是空DataFrame也缓存，避免重复尝试读取不存在的文件）
            self._historical_data_cache[data_name][target_date] = historical_data
            
            if self.log and not historical_data.empty:
                self.log.info(f"Loaded historical data for {data_name} on {target_date}: {len(historical_data)} rows")
            
            return historical_data if not historical_data.empty else None
            
        except Exception as e:
            if self.log:
                self.log.warning(f"Error loading historical data for {data_name} on {target_date}: {e}")
            # 缓存None以避免重复尝试
            self._historical_data_cache[data_name][target_date] = pd.DataFrame()
            return None

    def get_data_range(self, data_name, start_timestamp, end_timestamp):
        """获取指定时间范围内的数据"""
        try:
            all_data = []
            
            # 计算需要的日期范围
            start_date = get_date_based_on_timestamp(start_timestamp)
            end_date = get_date_based_on_timestamp(end_timestamp)
            
            current_date = datetime.strptime(start_date, '%Y%m%d')
            end_date_dt = datetime.strptime(end_date, '%Y%m%d')
            
            # 遍历每一天
            while current_date <= end_date_dt:
                date_str = current_date.strftime('%Y%m%d')
                
                # 如果是当前日期，使用内存数据
                if date_str == get_curr_utc_date():
                    current_data = self.factor_persist.get(data_name)
                    if current_data is not None and not current_data.empty:
                        all_data.append(current_data)
                else:
                    # 否则加载历史数据
                    historical_data = self._load_historical_data(data_name, date_str)
                    if historical_data is not None:
                        all_data.append(historical_data)
                
                current_date += timedelta(days=1)
            
            # 合并所有数据
            if all_data:
                combined_data = pd.concat(all_data).sort_index()
                # 过滤到指定时间范围
                return combined_data.loc[start_timestamp:end_timestamp]
            
            return pd.DataFrame()
            
        except Exception as e:
            if self.log:
                self.log.warning(f"Error getting data range for {data_name}: {e}")
            return pd.DataFrame()

    def clear_historical_cache(self):
        """清理历史数据缓存（在需要时释放内存）"""
        self._historical_data_cache.clear()
        if self.log:
            self.log.info("Historical data cache cleared")

    def get_cache_info(self):
        """获取缓存信息（用于调试）"""
        cache_info = {}
        for data_name, date_cache in self._historical_data_cache.items():
            cache_info[data_name] = {
                'cached_dates': list(date_cache.keys()),
                'total_rows': sum(len(df) for df in date_cache.values() if not df.empty)
            }
        return cache_info