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
    价格数据预加载器 - 参数化版本
    支持基于factors配置灵活读取目标数据，而不是硬编码currprice和twapprice
    """
    
    def __init__(self, persist_dir, start_time, factors_config, trading_symbols, lookback_days=30, log=None):
        """
        初始化价格数据预加载器
        
        Args:
            persist_dir: 持久化数据目录路径 (Path对象)
            start_time: 回测开始时间戳
            factors_config: 因子配置列表，格式与PosUpdater中assist_factors_params['factors']相同
            trading_symbols: 交易标的列表
            lookback_days: 向前加载的天数，默认30天
            log: 日志对象
        """
        self.persist_dir = Path(persist_dir)
        self.start_time = start_time
        self.factors_config = factors_config
        self.trading_symbols = trading_symbols
        self.lookback_days = lookback_days
        self.log = log
        
        # 预加载的数据容器 - 按因子名称动态创建
        self.loaded_data = {}
        
        # 构建因子到元组的映射
        self._build_factor_mappings()
        
        # 执行预加载
        self._preload_data()
    
    def _build_factor_mappings(self):
        """构建因子配置映射"""
        self.factor_tuples = {}
        self.factor_cache_names = {}
        
        for factor_config in self.factors_config:
            factor_name = factor_config['factor']
            table_name = factor_config['table']
            column_name = factor_config['column']
            
            # 构建因子元组 (table, column)
            factor_tuple = (table_name, column_name)
            self.factor_tuples[factor_name] = factor_tuple
            
            # 确定缓存名称 (与PosUpdater逻辑一致)
            if factor_name.startswith('curr_price'):
                cache_name = 'curr_price'
            elif factor_name.startswith('twap') or 'twap' in factor_name.lower():
                cache_name = 'twap_price'
            else:
                # 对于其他因子类型，使用因子名称作为缓存名
                cache_name = factor_name
            
            self.factor_cache_names[factor_name] = cache_name
            
        if self.log:
            self.log.info(f"Built factor mappings: {len(self.factor_tuples)} factors")
    
    def _get_date_range(self):
        """
        根据start_time和lookback_days计算需要加载的日期范围
        """
        # 导入timeutils中的函数
        from utility.timeutils import get_date_based_on_timestamp
        
        start_date = get_date_based_on_timestamp(self.start_time)
        start_dt = pd.Timestamp(start_date)
        
        # 向前推lookback_days天
        begin_dt = start_dt - pd.Timedelta(days=self.lookback_days)
        
        # 向后推一些天以确保有足够数据（可根据实际需要调整）
        end_dt = start_dt + pd.Timedelta(days=10)
        
        # 生成日期列表
        date_range = pd.date_range(begin_dt, end_dt, freq='D')
        return [dt.strftime('%Y%m%d') for dt in date_range]
    
    def _load_from_h5_file(self, file_path, target_keys):
        """
        从单个h5文件中加载指定的数据
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
                            self.log.debug(f'Loaded {key} from {file_path.name} with shape {data.shape}')
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
                                self.log.debug(f'Loaded {key} from {file_path.name} with shape {data.shape}')
            except Exception as e2:
                if self.log:
                    self.log.warning(f'Failed to load from {file_path}: {e}, {e2}')
        
        return loaded_data
    
    def _preload_data(self):
        """
        批量预加载数据
        """
        date_list = self._get_date_range()
        
        # 为每个因子准备数据框列表
        factor_frames = {factor_name: [] for factor_name in self.factor_tuples.keys()}
        
        # 构建需要从h5文件中读取的key列表
        target_keys = []
        for factor_tuple in self.factor_tuples.values():
            # 将因子元组转换为h5文件中的key格式
            # 假设存储格式为 (table, column, symbol) 的组合
            for symbol in self.trading_symbols:
                key = (*factor_tuple, symbol)
                target_keys.append(key)
        
        # 去重
        target_keys = list(set(target_keys))
        
        for date_str in date_list:
            file_path = self.persist_dir / f'{date_str}.h5'
            loaded_data = self._load_from_h5_file(file_path, target_keys)
            
            # 按因子组织数据
            for factor_name, factor_tuple in self.factor_tuples.items():
                # 收集该因子所有symbol的数据
                factor_data_dict = {}
                for symbol in self.trading_symbols:
                    key = (*factor_tuple, symbol)
                    if key in loaded_data:
                        factor_data_dict[symbol] = loaded_data[key]
                
                # 如果有数据，构建DataFrame
                if factor_data_dict:
                    # 假设每个key对应的是时间序列数据，需要重新组织成 (timestamp, symbol) 的DataFrame
                    # 这里可能需要根据实际的数据存储格式调整
                    factor_df = pd.DataFrame(factor_data_dict)
                    factor_frames[factor_name].append(factor_df)
        
        # 合并所有数据
        for factor_name, frames in factor_frames.items():
            if frames:
                combined_data = pd.concat(frames, axis=0).sort_index()
                # 去重，保留最后一个值
                combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
                
                cache_name = self.factor_cache_names[factor_name]
                self.loaded_data[cache_name] = combined_data
                
                if self.log:
                    self.log.success(f'Preloaded {factor_name} -> {cache_name}: {combined_data.shape}')
    
    def get_data_at_time(self, cache_name, timestamp, symbols=None):
        """
        获取指定缓存名称和时间点的数据
        
        Args:
            cache_name: 缓存名称 ('curr_price', 'twap_price', 等)
            timestamp: 时间戳
            symbols: 股票代码列表，如果为None则返回所有
        
        Returns:
            pd.Series or pd.DataFrame
        """
        if cache_name not in self.loaded_data or self.loaded_data[cache_name].empty:
            return pd.Series(dtype=float) if symbols is None else pd.Series(index=symbols, dtype=float)
        
        data = self.loaded_data[cache_name]
        
        # 找到小于等于timestamp的最近数据
        valid_data = data[data.index <= timestamp]
        if valid_data.empty:
            return pd.Series(dtype=float) if symbols is None else pd.Series(index=symbols, dtype=float)
        
        latest_data = valid_data.iloc[-1]
        
        if symbols is None:
            return latest_data
        else:
            # 只返回指定symbols的数据
            return latest_data.reindex(symbols)
    
    def get_currprice_at_time(self, timestamp, symbols=None):
        """保持向后兼容的方法"""
        return self.get_data_at_time('curr_price', timestamp, symbols)
    
    def get_twapprice_at_time(self, timestamp, symbols=None):
        """保持向后兼容的方法"""
        return self.get_data_at_time('twap_price', timestamp, symbols)
        

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