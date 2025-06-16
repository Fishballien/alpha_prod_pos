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
    价格数据预加载器 - 简化版本
    支持基于factors配置灵活读取目标数据
    """
    
    def __init__(self, persist_dir, start_time, factors, trading_symbols, lookback_days=30, log=None):
        """
        初始化价格数据预加载器
        
        Args:
            persist_dir: 持久化数据目录路径 (Path对象)
            start_time: 回测开始时间戳
            factors: 因子配置列表，格式: [{"category": "general", "group": "portfolio_management_v0", "factor": "curr_price"}, ...]
            trading_symbols: 交易标的列表
            lookback_days: 向前加载的天数，默认30天
            log: 日志对象
        """
        self.persist_dir = Path(persist_dir)
        self.start_time = start_time
        self.factors = factors
        self.trading_symbols = trading_symbols
        self.lookback_days = lookback_days
        self.log = log
        
        # 预加载的数据容器 - 按因子名称动态创建
        self.loaded_data = {}
        
        # 构建因子映射
        self._build_factor_mappings()
        
        # 执行预加载
        self._preload_data()
    
    def _build_factor_mappings(self):
        """构建因子配置映射"""
        self.factor_names = []
        self.factor_cache_names = {}
        
        for factor_config in self.factors:
            factor_name = factor_config['factor']
            self.factor_names.append(factor_name)
            
            # 确定缓存名称 (根据因子名称规则)
            if factor_name.startswith('curr_price') or factor_name == 'curr_price':
                cache_name = 'curr_price'
            elif factor_name.startswith('twap') or 'twap' in factor_name.lower():
                cache_name = 'twap_price'
            else:
                # 对于其他因子类型，使用因子名称作为缓存名
                cache_name = factor_name
            
            self.factor_cache_names[factor_name] = cache_name
            
        if self.log:
            self.log.info(f"Built factor mappings: {len(self.factor_names)} factors")
            self.log.info(f"Factor cache names: {self.factor_cache_names}")
    
    def _get_date_range(self):
        """
        根据start_time和lookback_days计算需要加载的日期范围
        """
        # 解析start_time
        if isinstance(self.start_time, str):
            start_dt = pd.Timestamp(self.start_time)
        else:
            start_dt = pd.Timestamp(self.start_time)
        
        # 向前推lookback_days天
        begin_dt = start_dt - pd.Timedelta(days=self.lookback_days)
        
        # 向后推一些天以确保有足够数据
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
        factor_frames = {factor_name: [] for factor_name in self.factor_names}
        
        # 构建需要从h5文件中读取的key列表 - 直接使用因子名称作为key
        target_keys = self.factor_names
        
        for date_str in date_list:
            file_path = self.persist_dir / f'{date_str}.h5'
            loaded_data = self._load_from_h5_file(file_path, target_keys)
            
            # 按因子组织数据
            for factor_name in self.factor_names:
                if factor_name in loaded_data:
                    # 直接添加DataFrame到列表中，因为读出来就是行为时间戳、列为symbol的格式
                    factor_df = loaded_data[factor_name]
                    factor_frames[factor_name].append(factor_df)
        
        # 合并所有数据 - 简单concat不同日期的数据
        for factor_name, frames in factor_frames.items():
            if frames:
                combined_data = pd.concat(frames, axis=0).sort_index()
                # 去重，保留最后一个值
                combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
                
                cache_name = self.factor_cache_names[factor_name]
                self.loaded_data[cache_name] = combined_data
                
                if self.log:
                    self.log.info(f'Preloaded {factor_name} -> {cache_name}: {combined_data.shape}')
    
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
    persist_dir = '/mnt/Data/xintang/prod/alpha/factors_update/persist/factors_for_portfolio_management_v0'
    lookback_days = 20
    start_time = "2025-06-07 00:00:00"
    
    # 因子配置
    factors = [
        {"category": "general", "group": "portfolio_management_v0", "factor": "curr_price"},
        {"category": "general", "group": "portfolio_management_v0", "factor": "twap_30min"}
    ]
    
    # 交易标的（示例）
    trading_symbols = ['btcusdt', 'ethusdt']
    
    print("=" * 60)
    print("PriceDataPreloader 测试")
    print("=" * 60)
    
    # 打印测试参数
    print("测试参数:")
    print(f"  persist_dir: {persist_dir}")
    print(f"  lookback_days: {lookback_days}")
    print(f"  start_time: {start_time}")
    print(f"  factors: {factors}")
    print(f"  trading_symbols: {trading_symbols}")
    print()
    
    # 检查目录是否存在
    print("检查数据目录:")
    if os.path.exists(persist_dir):
        print(f"✓ 目录存在: {persist_dir}")
        files = os.listdir(persist_dir)
        h5_files = [f for f in files if f.endswith('.h5')]
        print(f"  总文件数量: {len(files)}")
        print(f"  H5文件数量: {len(h5_files)}")
        if h5_files:
            print(f"  前几个H5文件: {h5_files[:3]}")
    else:
        print(f"✗ 目录不存在: {persist_dir}")
    print()
    
    # 初始化 PriceDataPreloader
    print("初始化 PriceDataPreloader...")
    try:
        preloader = PriceDataPreloader(
            persist_dir=persist_dir,
            start_time=start_time,
            factors=factors,
            trading_symbols=trading_symbols,
            lookback_days=lookback_days
        )
        print("✓ 初始化成功")
        
        # 打印基本信息
        print(f"  预加载的数据缓存: {list(preloader.loaded_data.keys())}")
        for cache_name, data in preloader.loaded_data.items():
            print(f"    {cache_name}: {data.shape if hasattr(data, 'shape') else type(data)}")
        
    except Exception as e:
        print(f"✗ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        preloader = None
    print()
    
    # 计算预期的时间范围
    print("时间范围计算:")
    start_dt = pd.Timestamp(start_time)
    lookback_dt = start_dt - pd.Timedelta(days=lookback_days)
    end_dt = start_dt + pd.Timedelta(days=10)
    print(f"  开始时间: {start_dt}")
    print(f"  回看开始: {lookback_dt}")
    print(f"  结束时间: {end_dt}")
    print(f"  总天数: {(end_dt - lookback_dt).days}")
    print()
    
    # 测试数据获取
    if preloader is not None:
        print("测试数据获取:")
        try:
            # 测试获取当前价格
            curr_price_data = preloader.get_currprice_at_time(start_dt, trading_symbols)
            print(f"  curr_price数据: {type(curr_price_data)}, shape: {curr_price_data.shape if hasattr(curr_price_data, 'shape') else 'N/A'}")
            print(f"  curr_price样本: {curr_price_data.head() if hasattr(curr_price_data, 'head') else curr_price_data}")
            
            # 测试获取TWAP价格
            twap_price_data = preloader.get_twapprice_at_time(start_dt, trading_symbols)
            print(f"  twap_price数据: {type(twap_price_data)}, shape: {twap_price_data.shape if hasattr(twap_price_data, 'shape') else 'N/A'}")
            print(f"  twap_price样本: {twap_price_data.head() if hasattr(twap_price_data, 'head') else twap_price_data}")
            
        except Exception as e:
            print(f"  ✗ 数据获取测试失败: {e}")
            import traceback
            traceback.print_exc()
    print()
    
    print("=" * 60)
    print("测试完成，设置断点查看变量:")
    print("  preloader - PriceDataPreloader 实例")
    print("  preloader.loaded_data - 预加载的数据")
    print("  curr_price_data - 当前价格数据")
    print("  twap_price_data - TWAP价格数据")
    print("  start_dt - 开始时间")
    print("  lookback_dt - 回看开始时间")
    print("=" * 60)
    
    # 断点 - 在这里设置断点来检查变量
    breakpoint()  # 或者用 import pdb; pdb.set_trace()