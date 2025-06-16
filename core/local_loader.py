# -*- coding: utf-8 -*-
"""
价格数据预加载器 - 用于PosUpdaterWithBacktest回滚优化
基于PersistenceManager的读取方法实现批量预加载
"""
# %% imports
import sys
import os
import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
import h5py
from datetime import datetime, timedelta


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[1]
sys.path.append(str(project_dir))


# %%
from utility.timeutils import get_date_based_on_timestamp


# %%
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
        

if __name__ == '__main__':
    # 测试参数
    price_persist_dir = '/mnt/Data/xintang/prod/alpha/factors_update/persist/factors_for_portfolio_management_v0'
    price_lookback_days = 20
    start_time = "2025-06-07 00:00:00"
    
    print("=" * 60)
    print("PriceDataPreloader 测试")
    print("=" * 60)
    
    # 打印测试参数
    print("测试参数:")
    print(f"  price_persist_dir: {price_persist_dir}")
    print(f"  price_lookback_days: {price_lookback_days}")
    print(f"  start_time: {start_time}")
    print()
    
    # 检查目录是否存在
    print("检查数据目录:")
    if os.path.exists(price_persist_dir):
        print(f"✓ 目录存在: {price_persist_dir}")
        files = os.listdir(price_persist_dir)
        print(f"  文件数量: {len(files)}")
        if files:
            print(f"  前几个文件: {files[:3]}")
    else:
        print(f"✗ 目录不存在: {price_persist_dir}")
    print()
    
    # 初始化 PriceDataPreloader
    print("初始化 PriceDataPreloader...")
    try:
        preloader = PriceDataPreloader(
            price_persist_dir=price_persist_dir,
            price_lookback_days=price_lookback_days,
            start_time=start_time
        )
        print("✓ 初始化成功")
        
        # 打印一些基本信息
        print(f"  类型: {type(preloader)}")
        print(f"  属性: {dir(preloader)}")
        
    except Exception as e:
        print(f"✗ 初始化失败: {e}")
        preloader = None
    print()
    
    # 如果初始化成功，尝试加载数据
    if preloader is not None:
        print("尝试加载数据...")
        try:
            data = preloader.load_data()  # 假设有这个方法
            print("✓ 数据加载成功")
            
            if hasattr(data, 'shape'):
                print(f"  数据形状: {data.shape}")
            if hasattr(data, 'columns'):
                print(f"  列名: {list(data.columns)[:5]}...")  # 只显示前5列
            if hasattr(data, 'index'):
                print(f"  索引类型: {type(data.index)}")
                if len(data.index) > 0:
                    print(f"  时间范围: {data.index[0]} 到 {data.index[-1]}")
            
            # 显示前几行数据
            if hasattr(data, 'head'):
                print("  前几行数据:")
                print(data.head(3))
                
        except Exception as e:
            print(f"✗ 数据加载失败: {e}")
            data = None
        print()
    
    # 计算预期的时间范围
    print("时间范围计算:")
    start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    lookback_dt = start_dt - pd.Timedelta(days=price_lookback_days)
    print(f"  开始时间: {start_dt}")
    print(f"  回看开始: {lookback_dt}")
    print(f"  总天数: {price_lookback_days}")
    print()
    
    print("=" * 60)
    print("测试完成，设置断点查看变量:")
    print("  preloader - PriceDataPreloader 实例")
    print("  data - 加载的数据")
    print("  start_dt - 开始时间")
    print("  lookback_dt - 回看开始时间")
    print("=" * 60)
    
    # 断点 - 在这里设置断点来检查变量
    breakpoint()  # 或者用 import pdb; pdb.set_trace()