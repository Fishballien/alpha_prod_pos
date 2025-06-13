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
        self._repo_important_params()
        self._load_exchange_info_detail()
        self._load_exchange_info_lock()
        self._init_task_scheduler()
        
        # 验证模式下只初始化数据读取相关的模块，跳过发送模块
        self._init_db_module_for_validation()
        
        self._init_cache()
        self._init_persist()
        
        # 验证模式下不初始化信号发送
        # self._init_signal_sender()
        
        # 新增：初始化回测相关参数
        self._init_backtest_params()
        
        self.reload_exchange_info(0)
        
        # 新增：尝试从回测初始化
        self._attempt_backtest_initialization()
        
        self._add_tasks()
        self._set_up_signal_handler()
    
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
                
            self.log.info(f"Validation period: {self.validation_start_time} to {self.validation_end_time}")
        else:
            self.log.info("Validation mode disabled")
    
    def _add_tasks(self):
        """重写任务添加，如果是验证模式则直接运行验证而不是添加定时任务"""
        if self.enable_validation:
            self.log.info("Starting validation process...")
            self._run_validation()
        else:
            # 非验证模式时调用父类方法
            super()._add_tasks()
    
    def _run_validation(self):
        """运行验证流程"""
        try:
            # 1. 加载回测仓位作为对比基准
            comparison_positions = self._load_comparison_positions()
            if comparison_positions is None:
                self.log.error("Cannot load backtest positions for comparison, validation aborted")
                return
            
            # 2. 准备验证回测位置（使用validation_start_time作为回测最后时间）
            self._prepare_validation_backtest()
            
            # 3. 执行历史回放
            self._execute_validation_rollforward()
            
            # 4. 对比结果
            self._compare_results(comparison_positions)
            
            # 5. 生成报告
            self._generate_validation_report()
            
        except Exception as e:
            self.log.exception(f"Error in validation: {e}")
            self.ding.send_text(f"Validation failed: {str(e)}", msg_type='error')
    
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
    
    def _prepare_validation_backtest(self):
        """准备验证用的回测数据"""
        # 创建模拟的回测仓位，假设在validation_start_time有一个初始仓位
        try:
            # 从persist获取validation_start_time的历史仓位作为起始点
            start_pos = self.persist_mgr.get_row('pos_his', self.validation_start_time)
            if start_pos is None:
                # 如果没有历史仓位，创建零仓位
                start_pos = pd.Series(data=[0]*len(self.trading_symbols), index=self.trading_symbols)
                self.log.warning(f"No historical position found at {self.validation_start_time}, using zero position")
            
            # 创建模拟的回测仓位DataFrame
            self.mock_backtest_positions = pd.DataFrame([start_pos], index=[self.validation_start_time])
            self.log.info(f"Prepared validation backtest starting position at {self.validation_start_time}")
            
        except Exception as e:
            self.log.exception(f"Error preparing validation backtest: {e}")
            # 创建零仓位作为fallback
            start_pos = pd.Series(data=[0]*len(self.trading_symbols), index=self.trading_symbols)
            self.mock_backtest_positions = pd.DataFrame([start_pos], index=[self.validation_start_time])
    
    def _execute_validation_rollforward(self):
        """执行验证回放"""
        try:
            # 生成验证时间戳
            validation_timestamps = self._generate_validation_timestamps()
            self.log.info(f"Generated {len(validation_timestamps)} timestamps for validation")
            
            # 使用模拟的回测仓位开始滚动
            self._execute_rollforward(self.mock_backtest_positions, validation_timestamps)
            
            # 保存验证期间的所有仓位
            self._save_validation_positions()
            
        except Exception as e:
            self.log.exception(f"Error in validation rollforward: {e}")
    
    def _generate_validation_timestamps(self):
        """生成验证时间戳序列"""
        sp = self.params['sp']
        interval = timedelta(seconds=parse_time_string(sp))
        
        timestamps = []
        current = self.validation_start_time + interval  # 从下一个时间戳开始
        
        while current <= self.validation_end_time:
            timestamps.append(current)
            current += interval
        
        return timestamps
    
    def _rollforward_update_once(self, ts, current_pos):
        """重写单次更新，保存每次的仓位用于验证"""
        try:
            # 调用父类方法进行更新
            new_pos = super()._rollforward_update_once(ts, current_pos)
            
            # 保存仓位到验证结果中
            if new_pos is not None:
                self.validation_positions[ts] = new_pos.copy()
            
            return new_pos
            
        except Exception as e:
            self.log.warning(f"Error in validation update at {ts}: {e}")
            return current_pos
    
    def _save_validation_positions(self):
        """保存验证期间的仓位"""
        try:
            if not self.validation_positions:
                self.log.warning("No validation positions to save")
                return
            
            # 转换为DataFrame
            validation_df = pd.DataFrame.from_dict(self.validation_positions, orient='index')
            
            # 保存到文件
            save_path = self.persist_dir / f"validation_positions_{self.stg_name}.parquet"
            validation_df.to_parquet(save_path)
            self.log.success(f"Saved validation positions to {save_path}")
            
        except Exception as e:
            self.log.exception(f"Error saving validation positions: {e}")
    
    def _compare_results(self, comparison_positions):
        """对比验证结果"""
        try:
            if not self.validation_positions:
                self.log.error("No validation positions to compare")
                return
            
            # 转换验证仓位为DataFrame
            validation_df = pd.DataFrame.from_dict(self.validation_positions, orient='index')
            
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
            comparison_df = pd.DataFrame(comparison_results)
            
            if not comparison_df.empty:
                save_path = self.persist_dir / f"validation_comparison_{self.stg_name}.parquet"
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
    
    # 重写信号发送方法，验证模式下不发送实际信号
    def _send_pos_to_zmq_and_db(self, new_pos, ts):
        """验证模式下不发送实际交易信号"""
        self.log.info(f"Validation mode: skipping signal sending for {ts}")
        return
    
    def _send_pos_to_db(self, new_pos):
        """验证模式下不写入数据库"""
        self.log.info("Validation mode: skipping database write")
        return


# 使用示例
if __name__ == "__main__":
    stg_name = "your_strategy_name"
    validator = PosUpdaterBacktestValidator(stg_name)
    # 会自动开始验证流程