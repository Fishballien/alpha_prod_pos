# -*- coding: utf-8 -*-
"""
价格数据预加载器 - 用于PosUpdaterWithBacktest回滚优化
基于PersistenceManager的读取方法实现批量预加载
"""

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
import h5py
from datetime import datetime, timedelta

from utils.timeutils import get_date_based_on_timestamp


class PriceDataPreloader:
    """
    价格数据预加载器
    在初始化时批量读取指定时间范围内的currprice和twapprice数据
    """
    
    def __init__(self, persist_dir, start_time, lookback_days=30, log=None):
        """
        初始化价格数据预加载器
        
        Args:
            persist_dir: 持久化数据目录路径 (Path对象)
            start_time: 回测开始时间戳
            lookback_days: 向前加载的天数，默认30天
            log: 日志对象
        """
        self.persist_dir = Path(persist_dir)
        self.start_time = start_time
        self.lookback_days = lookback_days
        self.log = log
        
        # 预加载的价格数据容器
        self.currprice_data = pd.DataFrame()
        self.twapprice_data = pd.DataFrame()
        
        # 执行预加载
        self._preload_price_data()
    
    def _get_date_range(self):
        """
        根据start_time和lookback_days计算需要加载的日期范围
        """
        start_date = get_date_based_on_timestamp(self.start_time)
        start_dt = pd.Timestamp(start_date)
        
        # 向前推lookback_days天
        begin_dt = start_dt - pd.Timedelta(days=self.lookback_days)
        
        # 向后推一些天以确保有足够数据（可根据实际需要调整）
        end_dt = start_dt + pd.Timedelta(days=10)
        
        # 生成日期列表
        date_range = pd.date_range(begin_dt, end_dt, freq='D')
        return [dt.strftime('%Y%m%d') for dt in date_range]
    
    def _load_from_h5_file(self, file_path, target_keys=['currprice', 'twapprice']):
        """
        从单个h5文件中加载指定的价格数据
        参考PersistenceManager的_load_from_h5_batch方法
        """
        if not file_path.exists():
            return {}
        
        loaded_data = {}
        
        try:
            # 首先尝试用pandas HDFStore读取
            with pd.HDFStore(file_path, 'r') as store:
                for key in target_keys:
                    if key in store:
                        data = store[key]
                        loaded_data[key] = data
                        if self.log:
                            self.log.info(f'Loaded {key} from {file_path.name} with shape {data.shape}')
        except Exception as e:
            # 如果HDFStore失败，尝试用h5py读取
            try:
                with h5py.File(file_path, 'r') as f:
                    for key in target_keys:
                        if key in f:
                            data = np.array(f[key])
                            # 重建DataFrame的index和columns
                            index = [
                                pd.Timestamp(i) if isinstance(i, str) and "T" in i else i.decode('utf-8') 
                                if isinstance(i, bytes) else i
                                for i in f[key].attrs['index']
                            ]
                            columns = [
                                col.decode('utf-8') if isinstance(col, bytes) else col 
                                for col in f[key].attrs['columns']
                            ]
                            loaded_data[key] = pd.DataFrame(data, index=index, columns=columns)
                            if self.log:
                                self.log.info(f'Loaded {key} from {file_path.name} with shape {data.shape}')
            except Exception as e2:
                if self.log:
                    self.log.warning(f'Failed to load from {file_path}: {e}, {e2}')
        
        return loaded_data
    
    def _preload_price_data(self):
        """
        批量预加载价格数据
        """
        date_list = self._get_date_range()
        
        currprice_frames = []
        twapprice_frames = []
        
        for date_str in date_list:
            file_path = self.persist_dir / f'{date_str}.h5'
            loaded_data = self._load_from_h5_file(file_path)
            
            if 'currprice' in loaded_data:
                currprice_frames.append(loaded_data['currprice'])
            
            if 'twapprice' in loaded_data:
                twapprice_frames.append(loaded_data['twapprice'])
        
        # 合并所有数据
        if currprice_frames:
            self.currprice_data = pd.concat(currprice_frames, axis=0).sort_index()
            # 去重，保留最后一个值
            self.currprice_data = self.currprice_data[~self.currprice_data.index.duplicated(keep='last')]
            if self.log:
                self.log.success(f'Preloaded currprice data: {self.currprice_data.shape}')
        
        if twapprice_frames:
            self.twapprice_data = pd.concat(twapprice_frames, axis=0).sort_index()
            # 去重，保留最后一个值
            self.twapprice_data = self.twapprice_data[~self.twapprice_data.index.duplicated(keep='last')]
            if self.log:
                self.log.success(f'Preloaded twapprice data: {self.twapprice_data.shape}')
    
    def get_currprice_at_time(self, timestamp, symbols=None):
        """
        获取指定时间点的currprice数据
        
        Args:
            timestamp: 时间戳
            symbols: 股票代码列表，如果为None则返回所有
        
        Returns:
            pd.Series or pd.DataFrame
        """
        if self.currprice_data.empty:
            return pd.Series(dtype=float) if symbols is None else pd.Series(index=symbols, dtype=float)
        
        # 找到小于等于timestamp的最近数据
        valid_data = self.currprice_data[self.currprice_data.index <= timestamp]
        if valid_data.empty:
            return pd.Series(dtype=float) if symbols is None else pd.Series(index=symbols, dtype=float)
        
        latest_data = valid_data.iloc[-1]
        
        if symbols is None:
            return latest_data
        else:
            # 只返回指定symbols的数据
            return latest_data.reindex(symbols)
    
    def get_twapprice_at_time(self, timestamp, symbols=None):
        """
        获取指定时间点的twapprice数据
        
        Args:
            timestamp: 时间戳
            symbols: 股票代码列表，如果为None则返回所有
        
        Returns:
            pd.Series or pd.DataFrame
        """
        if self.twapprice_data.empty:
            return pd.Series(dtype=float) if symbols is None else pd.Series(index=symbols, dtype=float)
        
        # 找到小于等于timestamp的最近数据
        valid_data = self.twapprice_data[self.twapprice_data.index <= timestamp]
        if valid_data.empty:
            return pd.Series(dtype=float) if symbols is None else pd.Series(index=symbols, dtype=float)
        
        latest_data = valid_data.iloc[-1]
        
        if symbols is None:
            return latest_data
        else:
            # 只返回指定symbols的数据
            return latest_data.reindex(symbols)