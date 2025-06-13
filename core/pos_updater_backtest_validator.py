# -*- coding: utf-8 -*-
"""
PosUpdater Backtest Validator

用于验证历史回放与实际仓位的一致性
模拟历史时间点，使用persist中的历史数据进行回放验证
"""

# %% imports
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import toml
import signal
import traceback
from functools import partial
import threading


from core.database_handler import (FactorReader, ModelPredictReader, PositionSender, PositionReader, StrategyUpdater,
                                   PremiumIndexReader, FundingRateFetcher)
from core.task_scheduler import TaskScheduler
from core.cache_persist_manager import CacheManager, PersistManager
from core.optimize_weight import future_optimal_weight_lp_cvxpy
from core.signal_sender import StrategySignalSender, TwapSignalSender
from utility.dirutils import load_path_config
from utility.logutils import FishStyleLogger
from utility.market import usd, load_binance_data
from utility.ding import DingReporter, df_to_markdown
from utility.timeutils import parse_time_string, get_date_based_on_timestamp
from utility.calc import calc_profit_before_next_t
from utility.datautils import filter_series
from data_processor.data_checker import (FactorDataChecker, ModelPredictionChecker, 
                                         fetch_n_check, fetch_n_check_once, fetch_n_return_once)
from data_processor.feature_engineering import calculate_rank, calculate_weight_from_rank
from core.pos_updater import PosUpdaterWithBacktest


# %%
class PosUpdaterBacktestValidator(PosUpdaterWithBacktest):
    """
    回测验证器
    继承PosUpdaterWithBacktest，添加历史回放验证功能
    """
    
    def __init__(self, stg_name):
        # 设置验证模式标记
        self.validation_mode = True
        self.validation_results = []
        self.validation_positions = {}
        
        # 先初始化基础参数，避免在验证模式下初始化不必要的模块
        self.stg_name = stg_name
        self._load_path_config()
        self._init_dir()
        self._init_log()
        self._load_params()
        self._format_params() 
        self._init_window_mapping()
        self._init_opt_func()
        self._init_ding_reporter()
        
        # 验证模式下的初始化流程
        self._init_backtest_params()
        self._repo_important_params()
        self._load_exchange_info_detail()
        self._load_exchange_info_lock()
        
        # 验证模式下只初始化数据读取相关的模块，跳过发送模块
        self._init_db_module_for_validation()
        self._init_cache()
        self._init_persist()
        
        # 加载交易币种信息
        self.reload_exchange_info(0)
        
        # 直接运行验证，不添加定时任务
        if self.enable_validation:
            self._run_validation()
        else:
            self.log.info("Validation disabled in config")
    
    def _init_db_module_for_validation(self):
        """验证模式下的数据库模块初始化，只初始化读取相关的模块"""
        mysql_name = self.params['mysql_name']
        model_name = self.params['model_name']

        # 只初始化数据读取模块
        self.factor_reader = FactorReader(mysql_name, log=self.log)
        self.predict_reader = ModelPredictReader(mysql_name, model_name, log=self.log)
        self.pos_reader = PositionReader(mysql_name, self.stg_name, log=self.log)
        
        # 验证模式下不初始化发送模块
        self.pos_sender = None
        self.thunder_sender = None
        
        # 初始化funding fetcher（验证需要）
        self._init_funding_fetcher()
    
    def _repo_important_params(self):
        """重写重要参数报告，验证模式下的报告"""
        allow_init_pre_pos = self.params['allow_init_pre_pos']
        funding_limit_pr = self.params.get('funding_limit')
        
        # 发送 Markdown 消息
        title = f"验证器 {self.stg_name} 已启动"
        markdown_text = (
            "验证模式参数：  \n"
            f"1. 允许在读不到旧仓位时初始化持仓为0的参数 `allow_init_pre_pos` 的设定值为 `{allow_init_pre_pos}`。  \n"
            f"2. 过滤异常Funding Rate参数为：{funding_limit_pr}。  \n"
            "3. 验证模式下不会发送实际交易信号。  \n"
        )
        
        self.ding.send_markdown(title=title, markdown_text=markdown_text, msg_type='info')
    
    def _init_backtest_params(self):
        """重写回测参数初始化，添加验证相关参数"""
        super()._init_backtest_params()
        
        # 验证相关参数
        self.validation_params = self.params.get('validation', {})
        self.enable_validation = self.validation_params.get('enable', False)
        
        if self.enable_validation:
            self.log.info("Validation mode enabled")
            
            # 验证时间范围
            self.validation_start_time = self.validation_params.get('start_time')  # "2025-06-05 00:00:00"
            self.validation_end_time = self.validation_params.get('end_time')      # "2025-06-08 00:00:00"
            
            # 转换为datetime对象
            if self.validation_start_time:
                self.validation_start_time = pd.to_datetime(self.validation_start_time)
            if self.validation_end_time:
                self.validation_end_time = pd.to_datetime(self.validation_end_time)
                
            # Alpha数据路径（可选）
            self.alpha_path = self.validation_params.get('alpha_path')
            if self.alpha_path:
                self.log.info(f"Using alpha data from file: {self.alpha_path}")
                self._preload_alpha_data()
            else:
                self.log.info("Using alpha data from persist")
                
            self.log.info(f"Validation period: {self.validation_start_time} to {self.validation_end_time}")
        else:
            self.log.info("Validation mode disabled")
    
    def _preload_alpha_data(self):
        """预加载alpha数据（如果指定了alpha_path）"""
        try:
            if not self.alpha_path:
                return
                
            self.log.info(f"Loading alpha data from: {self.alpha_path}")
            self.alpha_data = pd.read_parquet(self.alpha_path)
            
            # 过滤到验证时间范围
            self.alpha_data = self.alpha_data.loc[self.validation_start_time:self.validation_end_time]
            
            self.log.success(f"Loaded alpha data: {len(self.alpha_data)} rows, {len(self.alpha_data.columns)} symbols")
            
        except Exception as e:
            self.log.exception(f"Error loading alpha data from {self.alpha_path}: {e}")
            self.alpha_data = None
    
    def _run_validation(self):
        """运行验证流程"""
        try:
            self.log.info("Starting validation process...")
            
            # 1. 加载回测仓位作为对比基准
            comparison_positions = self._load_comparison_positions()
            if comparison_positions is None:
                self.log.error("Cannot load backtest positions for comparison, validation aborted")
                return
            
            # 2. 找到validation_start_time的对应回测仓位作为初始仓位
            start_position = self._get_start_position(comparison_positions)
            if start_position is None:
                self.log.error("Cannot find start position for validation, validation aborted")
                return
            
            # 3. 预加载历史数据
            self.log.info("Preloading historical data...")
            self._preload_historical_price_data()
            self._preload_backtest_profits()
            self._preload_other_cache_data()
            
            # 4. 生成验证时间戳序列
            validation_timestamps = self._generate_validation_timestamps()
            self.log.info(f"Generated {len(validation_timestamps)} timestamps for validation")
            
            # 5. 执行历史回放（从start_position开始）
            self._execute_validation_rollforward(start_position, validation_timestamps)
            
            # 6. 对比结果
            self._compare_results(comparison_positions)
            
            # 7. 生成报告
            self._generate_validation_report()
            
            self.log.success("Validation completed successfully")
            
            # 输出调试数据保存总结
            self._summarize_debug_data_saving()
            
        except Exception as e:
            self.log.exception(f"Error in validation: {e}")
            self.ding.send_text(f"Validation failed: {str(e)}", msg_type='error')
    
    def _summarize_debug_data_saving(self):
        """总结调试数据保存情况"""
        try:
            if hasattr(self, '_debug_save_count'):
                debug_dir = self.persist_dir / 'debug_data'
                self.log.success(f"Total debug data files saved: {self._debug_save_count}")
                self.log.info(f"Debug data location: {debug_dir}")
                
                # 发送钉钉通知
                self.ding.send_text(f"验证完成，已保存 {self._debug_save_count} 个调试数据文件", msg_type='info')
        except Exception as e:
            self.log.warning(f"Error summarizing debug data: {e}")
    
    def _load_comparison_positions(self):
        """加载对比用的回测仓位数据"""
        try:
            # 直接使用回测仓位作为对比基准
            backtest_result_path = Path(self.backtest_init_params['backtest_result_path'])
            test_name = self.backtest_init_params['test_name']
            backtest_name = self.backtest_init_params['backtest_name']
            
            pos_file = backtest_result_path / test_name / 'backtest' / backtest_name / f"pos_{test_name}__{backtest_name}.parquet"
            
            if not pos_file.exists():
                self.log.error(f"Backtest position file not found: {pos_file}")
                return None
            
            comparison_positions = pd.read_parquet(pos_file)
            self.log.info(f"Loaded backtest positions for comparison: {len(comparison_positions)} rows")
            
            # 过滤到验证时间范围
            comparison_positions = comparison_positions.loc[self.validation_start_time:self.validation_end_time]
            self.log.info(f"Filtered backtest positions to validation period: {len(comparison_positions)} rows")
            
            return comparison_positions
            
        except Exception as e:
            self.log.exception(f"Error loading backtest positions for comparison: {e}")
            return None
    
    def _get_start_position(self, comparison_positions):
        """从回测仓位中获取validation_start_time对应的仓位作为起始仓位"""
        try:
            if self.validation_start_time not in comparison_positions.index:
                self.log.error(f"Start time {self.validation_start_time} not found in backtest positions")
                return None
            
            start_position = comparison_positions.loc[self.validation_start_time]
            self.log.info(f"Found start position at {self.validation_start_time}")
            return start_position
            
        except Exception as e:
            self.log.exception(f"Error getting start position: {e}")
            return None
    
    def _generate_validation_timestamps(self):
        """生成验证时间戳序列（不包括start_time，从下一个时间点开始）"""
        sp = self.params['sp']
        interval = timedelta(seconds=parse_time_string(sp))
        
        timestamps = []
        current = self.validation_start_time + interval  # 从下一个时间戳开始
        
        while current <= self.validation_end_time:
            timestamps.append(current)
            current += interval
        
        return timestamps
    
    def _execute_validation_rollforward(self, start_position, validation_timestamps):
        """执行验证回放"""
        try:
            # 将起始仓位添加到cache
            self.cache_mgr.add_row('pos_his', start_position, self.validation_start_time)
            self.log.info(f"Initialized cache with start position at {self.validation_start_time}")
            
            # 初始化当前仓位
            current_pos = start_position
            
            # 逐个时间戳进行回放
            successful_updates = 0
            
            for i, ts in enumerate(validation_timestamps):
                try:
                    if (i + 1) % 100 == 0 or i == len(validation_timestamps) - 1:
                        self.log.info(f"Validating {i+1}/{len(validation_timestamps)}: {ts}")
                    
                    # 执行单次更新
                    new_pos = self._validation_update_once(ts, current_pos)
                    if new_pos is not None:
                        current_pos = new_pos
                        successful_updates += 1
                        # 保存仓位到验证结果中
                        self.validation_positions[ts] = new_pos.copy()
                    
                except Exception as e:
                    self.log.exception(f"Error during validation at {ts}: {e}")
                    # 继续使用上一个仓位
                    continue
            
            self.log.success(f"Validation rollforward completed: {successful_updates}/{len(validation_timestamps)} successful updates")
            
        except Exception as e:
            self.log.exception(f"Error in validation rollforward: {e}")
    
    def _validation_update_once(self, ts, current_pos):
        """验证模式的单次更新（基于历史数据的纯模拟）"""
        try:
            # 1. 从persist读取历史alpha
            alpha = self._fetch_historical_alpha(ts)
            if alpha is None:
                self.log.warning(f"No alpha found for {ts}, keeping current position")
                return current_pos
            
            # 2. 重建必要的cache数据（从persist）
            self._rebuild_cache_for_timestamp(ts)
            
            # 3. 确定当时可交易的symbols（基于当时的数据可用性）
            available_symbols = self._get_available_symbols_at_timestamp(ts, alpha)
            
            # 4. 获取funding过滤
            try:
                allow_to_trade_by_funding = self._filter_by_funding_historical(ts, available_symbols)
            except Exception as e:
                self.log.warning(f"Error in funding filter at {ts}: {e}, using available symbols")
                allow_to_trade_by_funding = available_symbols
            
            # 5. 计算momentum和历史收益
            try:
                mm_t = self._get_momentum_at_t(ts, allow_to_trade_by_funding)
                his_pft_t = self._get_his_profit_at_t(ts, allow_to_trade_by_funding)
            except Exception as e:
                self.log.warning(f"Error calculating momentum/profit at {ts}: {e}, using empty dict")
                mm_t = {}
                his_pft_t = {}
            
            # 6. 保存调试数据到pickle
            self._save_debug_data(ts, {
                'available_symbols': available_symbols,
                'mm_t': mm_t,
                'his_pft_t': his_pft_t,
                'current_pos': current_pos.copy() if hasattr(current_pos, 'copy') else current_pos,
                'alpha': alpha.copy() if hasattr(alpha, 'copy') else alpha,
                'allow_to_trade_by_funding': allow_to_trade_by_funding
            })
            
            # 7. 组合优化
            w1 = self._po_on_tradable(current_pos, alpha, mm_t, his_pft_t, allow_to_trade_by_funding)
            
            # 8. 更新到全集
            new_pos = self._update_positions_on_universal_set(current_pos, w1)
            
            # 9. 添加新仓位到cache（用于后续计算momentum和pft）
            self.cache_mgr.add_row('pos_his', new_pos, ts)
            
            return new_pos
            
        except Exception as e:
            self.log.warning(f"Error in validation_update_once at {ts}: {e}")
            return current_pos
    
    def _save_debug_data(self, ts, debug_data):
        """保存调试数据到pickle文件"""
        try:
            import pickle
            
            # 创建调试数据目录
            debug_dir = self.persist_dir / 'debug_data'
            debug_dir.mkdir(parents=True, exist_ok=True)
            
            # 生成文件名（基于时间戳）
            ts_str = ts.strftime('%Y%m%d_%H%M%S')
            debug_file = debug_dir / f"debug_data_{ts_str}.pkl"
            
            # 添加时间戳到调试数据中
            debug_data['timestamp'] = ts
            
            # 保存到pickle文件
            with open(debug_file, 'wb') as f:
                pickle.dump(debug_data, f)
            
            # 每保存50个文件输出一次日志，避免日志过多
            if not hasattr(self, '_debug_save_count'):
                self._debug_save_count = 0
            self._debug_save_count += 1
            
            if self._debug_save_count % 50 == 0:
                self.log.info(f"Saved {self._debug_save_count} debug data files to {debug_dir}")
                
        except Exception as e:
            self.log.warning(f"Error saving debug data for {ts}: {e}")
    
    def _get_available_symbols_at_timestamp(self, ts, alpha):
        """获取在指定时间戳下实际可用的symbols（alpha和curr_price都有数据）"""
        try:
            available_symbols = []
            
            # 检查alpha中有数据的symbols
            alpha_symbols = set(alpha.dropna().index) if alpha is not None else set()
            
            # 检查curr_price中有数据的symbols
            curr_price_symbols = set()
            curr_price = self.cache_mgr['curr_price']
            if ts in curr_price.index:
                curr_price_at_ts = curr_price.loc[ts].dropna()
                curr_price_symbols = set(curr_price_at_ts.index)
            
            # 取交集：既有alpha又有curr_price的symbols
            available_symbols = list(alpha_symbols.intersection(curr_price_symbols))
            
            if len(available_symbols) == 0:
                self.log.warning(f"No available symbols at {ts}, using alpha symbols as fallback")
                available_symbols = list(alpha_symbols)
            
            return available_symbols
            
        except Exception as e:
            self.log.warning(f"Error getting available symbols at {ts}: {e}")
            # 如果出错，返回alpha中有数据的symbols作为fallback
            if alpha is not None:
                return list(alpha.dropna().index)
            return []
    
    def _fetch_historical_alpha(self, ts):
        """从persist或指定文件获取历史alpha"""
        try:
            # 如果指定了alpha_path，从预加载的数据中获取
            if hasattr(self, 'alpha_data') and self.alpha_data is not None:
                if ts in self.alpha_data.index:
                    return self.alpha_data.loc[ts]
                else:
                    return None
            
            # 否则从persist获取
            return self.persist_mgr.get_row('alpha', ts)
            
        except Exception as e:
            return None
    
    def _rebuild_cache_for_timestamp(self, ts):
        """为特定时间戳重建cache数据"""
        # 从persist读取必要的历史数据到cache
        data_to_rebuild = ['curr_price', 'twap_price', 'est_funding_rate', 'twap_profit', 'fee']
        
        for data_name in data_to_rebuild:
            try:
                data = self.persist_mgr.get_row(data_name, ts)
                if data is not None:
                    self.cache_mgr.add_row(data_name, data, ts)
            except Exception as e:
                # 某些数据可能不存在，继续
                continue
    
    def _filter_by_funding_historical(self, ts, available_symbols):
        """历史模式的funding过滤（基于当时可用的symbols）"""
        funding_limit_pr = self.params.get('funding_limit')
        if funding_limit_pr is None:
            return available_symbols
        
        try:
            est_funding_rate = self.cache_mgr['est_funding_rate']
            if ts not in est_funding_rate.index:
                return available_symbols
                
            funding_abs_limit = funding_limit_pr['funding_abs_limit']
            funding_cooldown = funding_limit_pr.get('funding_cooldown')
            
            funding_invalid = est_funding_rate.abs() > funding_abs_limit
            
            if funding_cooldown is not None:
                rolling_below_threshold = funding_invalid.rolling(window=funding_cooldown, min_periods=1).mean()
                final_mask = rolling_below_threshold != 0
            else:
                final_mask = funding_invalid
                
            latest_final_mask = final_mask.loc[ts].reindex(index=available_symbols).fillna(False)
            allow_to_trade_by_funding = latest_final_mask[~latest_final_mask].index.tolist()
            
            return allow_to_trade_by_funding
            
        except Exception as e:
            self.log.warning(f"Error in historical funding filter: {e}")
            return available_symbols
    
    def _compare_results(self, comparison_positions):
        """对比验证结果"""
        try:
            if not self.validation_positions:
                self.log.error("No validation positions to compare")
                return
            
            # 转换验证仓位为DataFrame
            validation_df = pd.DataFrame.from_dict(self.validation_positions, orient='index')
            
            # 保存验证生成的仓位
            validation_save_path = self.persist_dir / f"validation_generated_positions_{self.stg_name}.parquet"
            validation_df.to_parquet(validation_save_path)
            self.log.success(f"Saved validation generated positions to {validation_save_path}")
            
            # 保存从回测中截取用于核对的仓位
            comparison_save_path = self.persist_dir / f"validation_comparison_positions_{self.stg_name}.parquet"
            comparison_positions.to_parquet(comparison_save_path)
            self.log.success(f"Saved comparison positions (from backtest) to {comparison_save_path}")
            
            # 对比每个时间点
            comparison_results = []
            
            for ts in validation_df.index:
                if ts in comparison_positions.index:
                    validation_pos = validation_df.loc[ts]
                    comparison_pos = comparison_positions.loc[ts]
                    
                    # 对齐币种
                    common_symbols = validation_pos.index.intersection(comparison_pos.index)
                    if len(common_symbols) == 0:
                        continue
                    
                    validation_aligned = validation_pos.reindex(common_symbols).fillna(0)
                    comparison_aligned = comparison_pos.reindex(common_symbols).fillna(0)
                    
                    # 计算差异
                    diff = validation_aligned - comparison_aligned
                    max_diff = diff.abs().max()
                    mean_diff = diff.abs().mean()
                    
                    # 记录结果
                    comparison_results.append({
                        'timestamp': ts,
                        'max_diff': max_diff,
                        'mean_diff': mean_diff,
                        'total_symbols': len(common_symbols),
                        'non_zero_diffs': (diff.abs() > 1e-6).sum()
                    })
                    
                    # 如果差异较大，记录详细信息
                    if max_diff > 0.01:  # 1%的差异阈值
                        large_diffs = diff[diff.abs() > 0.01]
                        self.log.warning(f"Large differences at {ts}: {large_diffs.to_dict()}")
            
            # 保存对比结果
            self.validation_results = comparison_results
            
            if comparison_results:
                comparison_df = pd.DataFrame(comparison_results)
                save_path = self.persist_dir / f"validation_comparison_results_{self.stg_name}.parquet"
                comparison_df.to_parquet(save_path)
                self.log.success(f"Saved comparison results to {save_path}")
            
        except Exception as e:
            self.log.exception(f"Error comparing results: {e}")
    
    def _generate_validation_report(self):
        """生成验证报告"""
        try:
            if not self.validation_results:
                self.log.warning("No validation results to report")
                return
            
            # 统计信息
            comparison_df = pd.DataFrame(self.validation_results)
            
            total_comparisons = len(comparison_df)
            avg_max_diff = comparison_df['max_diff'].mean()
            avg_mean_diff = comparison_df['mean_diff'].mean()
            max_max_diff = comparison_df['max_diff'].max()
            
            perfect_matches = (comparison_df['max_diff'] < 1e-6).sum()
            good_matches = (comparison_df['max_diff'] < 0.01).sum()  # 1%以内
            
            # 创建报告
            report = f"""
=== 回测验证报告 ===
策略: {self.stg_name}
验证期间: {self.validation_start_time} 至 {self.validation_end_time}
总对比次数: {total_comparisons}

=== 精度统计 ===
完全匹配 (差异<1e-6): {perfect_matches}/{total_comparisons} ({perfect_matches/total_comparisons:.2%})
良好匹配 (差异<1%): {good_matches}/{total_comparisons} ({good_matches/total_comparisons:.2%})

=== 差异统计 ===
平均最大差异: {avg_max_diff:.6f}
平均均值差异: {avg_mean_diff:.6f}  
最大差异: {max_max_diff:.6f}

=== 结论 ===
"""
            
            if perfect_matches / total_comparisons > 0.9:
                conclusion = "✅ 验证通过！回放结果与实际仓位高度一致"
            elif good_matches / total_comparisons > 0.9:
                conclusion = "⚠️  验证基本通过，存在小幅差异，建议进一步检查"
            else:
                conclusion = "❌ 验证失败！回放结果与实际仓位差异较大，需要排查问题"
            
            report += conclusion
            
            # 记录和发送报告
            self.log.info(report)
            self.ding.send_markdown("回测验证报告", report, msg_type='info')
            
            # 保存详细报告
            report_path = self.persist_dir / f"validation_report_{self.stg_name}.txt"
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report)
                
                # 添加详细的时间序列统计
                f.write("\n\n=== 详细时间序列统计 ===\n")
                f.write(comparison_df.to_string())
            
            self.log.success(f"详细验证报告已保存至: {report_path}")
            
        except Exception as e:
            self.log.exception(f"Error generating validation report: {e}")
    
    def _preload_historical_price_data(self):
        """预加载历史价格数据供mmt计算使用（从配置的文件路径直接读取）"""
        try:
            # 从验证配置中获取价格数据文件路径
            curr_price_path = self.validation_params.get('curr_price_path')
            twap_price_path = self.validation_params.get('twap_price_path')
            
            if not curr_price_path or not twap_price_path:
                self.log.error("curr_price_path and twap_price_path must be specified in validation config")
                return
            
            # 计算需要的时间范围（改为10天回看）
            preload_start = self.validation_start_time - timedelta(days=10)
            preload_end = self.validation_end_time
            
            self.log.info(f"Preloading price data from {preload_start} to {preload_end}")
            
            curr_price_loaded = 0
            twap_price_loaded = 0
            
            # 加载curr_price数据
            try:
                self.log.info(f"Loading curr_price from: {curr_price_path}")
                curr_price_data = pd.read_parquet(curr_price_path)
                
                # 过滤时间范围
                curr_price_filtered = curr_price_data.loc[preload_start:preload_end]
                
                # 写入cache
                for ts in curr_price_filtered.index:
                    try:
                        price_data = curr_price_filtered.loc[ts]
                        self.cache_mgr.add_row('curr_price', price_data, ts)
                        curr_price_loaded += 1
                    except Exception:
                        continue
                        
                self.log.success(f"Loaded {curr_price_loaded} curr_price records")
                
            except Exception as e:
                self.log.error(f"Error loading curr_price data: {e}")
            
            # 加载twap_price数据
            try:
                self.log.info(f"Loading twap_price from: {twap_price_path}")
                twap_price_data = pd.read_parquet(twap_price_path)
                
                # 过滤时间范围
                twap_price_filtered = twap_price_data.loc[preload_start:preload_end]
                
                # 写入cache
                for ts in twap_price_filtered.index:
                    try:
                        price_data = twap_price_filtered.loc[ts]
                        self.cache_mgr.add_row('twap_price', price_data, ts)
                        twap_price_loaded += 1
                    except Exception:
                        continue
                        
                self.log.success(f"Loaded {twap_price_loaded} twap_price records")
                
            except Exception as e:
                self.log.error(f"Error loading twap_price data: {e}")
            
            self.log.success(f"Total preloaded: {curr_price_loaded} curr_price and {twap_price_loaded} twap_price records")
            
        except Exception as e:
            self.log.exception(f"Error preloading historical price data: {e}")
    
    def _preload_backtest_profits(self):
        """预加载回测的历史twap_profit数据供hispft计算使用"""
        try:
            # 从回测结果文件中读取历史twap_profit数据
            backtest_result_path = Path(self.backtest_init_params['backtest_result_path'])
            test_name = self.backtest_init_params['test_name']
            backtest_name = self.backtest_init_params['backtest_name']
            name_to_save = f'{test_name}__{backtest_name}'
            
            backtest_dir = backtest_result_path / test_name / 'backtest' / backtest_name
            
            # 尝试从验证参数中获取twap_list，如果没有则使用默认值
            twap_names = self.validation_params.get('twap_list', ['twd30_sp30'])
            self.log.info(f"Using twap_list for profit preloading: {twap_names}")
            
            # 使用第一个twap作为主要的twap_profit数据源
            main_twap = twap_names[0]
            
            try:
                profit_file = backtest_dir / f"profit_{main_twap}_{name_to_save}.parquet"
                if not profit_file.exists():
                    self.log.error(f"Backtest twap profit file not found: {profit_file}")
                    return
                
                # 读取回测的twap_profit数据
                backtest_profits = pd.read_parquet(profit_file)
                self.log.info(f"Loaded backtest twap_profit data for {main_twap}: {len(backtest_profits)} rows")
                
                # 过滤到validation_start_time之前的数据
                historical_profits = backtest_profits.loc[:self.validation_start_time]
                
                if not historical_profits.empty:
                    # 一次性批量写入cache（直接赋值给cache的DataFrame）
                    if 'twap_profit' not in self.cache_mgr.cache or self.cache_mgr.cache['twap_profit'].empty:
                        self.cache_mgr.cache['twap_profit'] = historical_profits.copy()
                    else:
                        # 如果cache中已有数据，进行合并
                        existing_data = self.cache_mgr.cache['twap_profit']
                        combined_data = pd.concat([existing_data, historical_profits]).sort_index()
                        # 去重，保留最新的数据
                        self.cache_mgr.cache['twap_profit'] = combined_data[~combined_data.index.duplicated(keep='last')]
                    
                    self.log.success(f"Preloaded {len(historical_profits)} twap_profit records from {main_twap}")
                else:
                    self.log.warning(f"No historical profits found before {self.validation_start_time}")
                    
            except Exception as e:
                self.log.error(f"Error loading twap_profit for {main_twap}: {e}")
            
        except Exception as e:
            self.log.exception(f"Error preloading backtest profits: {e}")
    
    def _preload_other_cache_data(self):
        """预加载其他必要的cache数据"""
        try:
            # 预加载validation_start_time前后的fee数据（改为10天回看）
            preload_start = self.validation_start_time - timedelta(days=10)
            preload_end = self.validation_end_time
            
            sp = self.params['sp']
            interval = timedelta(seconds=parse_time_string(sp))
            
            # 生成时间戳
            current_ts = preload_start
            fee_loaded = 0
            
            while current_ts <= preload_end:
                try:
                    fee_data = self.persist_mgr.get_row('fee', current_ts)
                    if fee_data is not None:
                        self.cache_mgr.add_row('fee', fee_data, current_ts)
                        fee_loaded += 1
                except Exception:
                    pass
                current_ts += interval
            
            self.log.success(f"Preloaded {fee_loaded} fee records")
            
        except Exception as e:
            self.log.exception(f"Error preloading other cache data: {e}")
    
    # 重写信号发送方法，验证模式下不发送实际信号
    def _send_pos_to_zmq_and_db(self, new_pos, ts):
        """验证模式下不发送实际交易信号"""
        pass
    
    def _send_pos_to_db(self, new_pos):
        """验证模式下不写入数据库"""
        pass


# 使用示例
if __name__ == "__main__":
    # 配置验证参数示例:
    # 在策略的.toml配置文件中添加以下配置:
    """
    [backtest_init]
    enable = true
    backtest_result_path = "/path/to/backtest/results"
    test_name = "your_test_name"
    backtest_name = "your_backtest_name"
    max_lookback_days = 7

    [validation]
    enable = true
    start_time = "2025-06-05 00:00:00"
    end_time = "2025-06-08 00:00:00"
    twap_list = ["twd30_sp30"]
    """
    
    stg_name = "your_strategy_name"
    validator = PosUpdaterBacktestValidator(stg_name)
    # 会自动开始验证流程