# -*- coding: utf-8 -*-
"""
Created on Sun Sep 29 10:03:06 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

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


# %%
class PosUpdater:
    
    def __init__(self, stg_name):
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
        self._init_db_module()
        self._init_cache()
        self._init_persist()
        self._init_signal_sender()
        self._add_tasks()
        self._set_up_signal_handler()
        
        self.reload_exchange_info(0)
        
    def _load_path_config(self):
        file_path = Path(__file__).resolve()
        project_dir = file_path.parents[1]
        self.path_config = load_path_config(project_dir)
        
    def _init_dir(self):
        path_config = self.path_config
        
        cache_dir = Path(path_config['cache'])
        self.cache_dir = cache_dir / self.stg_name
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        persist_dir = Path(path_config['persist'])
        self.persist_dir = persist_dir / self.stg_name
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        
    def _init_log(self):
        self.log = FishStyleLogger() 
        
    def _load_params(self):
        self.param_dir = Path(self.path_config['param'])
        self.params = toml.load(self.param_dir / f'{self.stg_name}.toml')

    def _format_params(self):
        params = self.params
        
        data_check_params = params['data_check_params']
        data_check_params['time_threshold'] = timedelta(**data_check_params['time_threshold'])
        
        assist_factors_params = params['assist_factors_params']
        assist_factors_params['factors_in_tuple'] = [tuple(factor.values()) for factor in assist_factors_params['factors']]
        
        optimizer_params = self.params['optimizer_params']
        optimizer_params['momentum_limits'] = {content['interval']: content['limit'] 
                                               for content in optimizer_params['momentum_limits']}
        optimizer_params['pf_limits'] = {content['interval']: content['limit'] 
                                         for content in optimizer_params['pf_limits']}
        
    def _init_window_mapping(self):
        assist_factors_params = self.params['assist_factors_params']
        optimizer_params = self.params['optimizer_params']
        delay_fetch = assist_factors_params['delay_fetch']
        momentum_limits = optimizer_params['momentum_limits']
        pf_limits = optimizer_params['pf_limits']
        cache_lookback = self.params['cache']['cache_lookback']
        
        self.delay_mapping = {delay: timedelta(seconds=parse_time_string(delay)) for delay in delay_fetch}
        self.mmt_limits_mapping = {mmt_wd: timedelta(seconds=parse_time_string(mmt_wd)) for mmt_wd in momentum_limits}
        self.pf_limits_mapping = {pf_wd: timedelta(seconds=parse_time_string(pf_wd)) for pf_wd in pf_limits}
        self.cache_lookback = timedelta(seconds=parse_time_string(cache_lookback))
        
    def _init_opt_func(self):
        optimizer_params = self.params['optimizer_params']
        max_multi = optimizer_params.get('max_multi')
        max_wgt = optimizer_params.get('max_wgt')
        momentum_limits = optimizer_params['momentum_limits']
        pf_limits = optimizer_params['pf_limits']
        
        self.opt_func = partial(future_optimal_weight_lp_cvxpy, max_multi=max_multi, max_wgt=max_wgt, 
                                momentum_limits=momentum_limits, pf_limits=pf_limits)
    
    def _init_ding_reporter(self):
        ding_config = self.params['ding']
        self.ding = DingReporter(ding_config)
        
    def _repo_important_params(self):
        allow_init_pre_pos = self.params['allow_init_pre_pos']
        funding_limit_pr = self.params.get('funding_limit')
        # 发送 Markdown 消息
        title = f"策略 {self.stg_name} 已启动"
        markdown_text = (
            "请注意：  \n"
            f"1. 允许在读不到旧仓位时初始化持仓为0的参数 `allow_init_pre_pos` 的设定值为 `{allow_init_pre_pos}`。  \n"
            f"2. 过滤异常Funding Rate参数为：{funding_limit_pr}。  \n"
        )

    
        self.ding.send_markdown(title=title, markdown_text=markdown_text, msg_type='info')
        
    def _load_exchange_info_detail(self):
        self.exchange_info_dir = Path(self.path_config['exchange_info'])
        exchange = self.params['exchange']
        self.exchange = globals()[exchange]
        
    def _load_exchange_info_lock(self):
        self.exchange_info_lock = threading.Lock()

    def _init_task_scheduler(self):
        self.task_scheduler = TaskScheduler(log=self.log, repo=self.ding)
        
    def _init_db_module(self):
        mysql_name = self.params['mysql_name']
        model_name = self.params['model_name']
        thunder2_pr = self.params['thunder2']

        self.factor_reader = FactorReader(mysql_name, log=self.log)
        self.predict_reader = ModelPredictReader(mysql_name, model_name, log=self.log)
        self.pos_sender = PositionSender(mysql_name, self.stg_name, log=self.log)
        self.pos_reader = PositionReader(mysql_name, self.stg_name, log=self.log)
        
        thunder2_sql_name = thunder2_pr['sql_name']
        period = thunder2_pr['period']
        self.thunder_sender = StrategyUpdater(thunder2_sql_name, period, self.stg_name, log=self.log)
        
        self._init_funding_fetcher()
        
    def _init_funding_fetcher(self):
        funding_fetcher_pr = self.params['fetch_funding']
        funding_db_name = funding_fetcher_pr['db_name']
        ts_thres_early = funding_fetcher_pr['ts_thres_early']
        ts_thres_late = funding_fetcher_pr['ts_thres_late']
        max_attempts = funding_fetcher_pr['max_attempts']
        retry_interval = funding_fetcher_pr['retry_interval']
        
        # 创建 PremiumIndexReader 实例
        reader = PremiumIndexReader(funding_db_name, log=self.log)
        
        # 创建 FundingRateFetcher 实例
        self.funding_fetcher = FundingRateFetcher(
            reader=reader,
            ts_thres_early=ts_thres_early,
            ts_thres_late=ts_thres_late,
            max_attempts=max_attempts,
            retry_interval=retry_interval,
            log=self.log,
        )
        
    def _init_cache(self):
        cache_list = ['curr_price', 'twap_price', 'pos_his', 'twap_profit', 'fee', 'period_pnl', 'est_funding_rate']
        self.cache_mgr = CacheManager(self.cache_dir, cache_lookback=self.cache_lookback, 
                                      cache_list=cache_list, log=self.log)
        
    def _init_persist(self):
        persist_list = ['alpha', 'pos_his', 'twap_profit', 'fee', 'period_pnl', 'est_funding_rate']
        self.persist_mgr = PersistManager(self.persist_dir, persist_list=persist_list, log=self.log)
        
    def _init_signal_sender(self):
        zmq_address = self.params['zmq']['address']
        twap_zmq_address = self.params['twap_zmq']['address']
        
        self.signal_sender = StrategySignalSender(zmq_address)
        self.twap_signal_sender = TwapSignalSender(twap_zmq_address)
        
    def _add_tasks(self):
        pos_update_interval = self.params['pos_update_interval']
        
        unit = list(pos_update_interval.keys())[0]
        interval = list(pos_update_interval.values())[0]
        
        self.task_scheduler.add_task('30 Minute Pos Update', unit, interval, self.update_once)
        # self.task_scheduler.add_task('Daily Pnl', unit, interval, self.calc_daily_pnl)
        self.task_scheduler.add_task('Daily Pnl', 'specific_time', ['00:00'], self.calc_daily_pnl)
        self.task_scheduler.add_task('Reload Exchange Info', 'specific_time', 
                                     [f"{hour:02d}:08" for hour in range(24)], 
                                     self.reload_exchange_info)
        
    def _set_up_signal_handler(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGSEGV, self._signal_handler)
        signal.signal(signal.SIGILL, self._signal_handler)
        
    def _signal_handler(self, sig, frame):
        signal_name = signal.Signals(sig).name  # 获取信号的名称
        self.log.warning(f"收到终止信号: {signal_name}，正在清理资源...")
        self.ding.send_text(f"策略 {self.stg_name} 收到终止信号： {signal_name}，已停止")
        self.stop()
        time.sleep(15) # HINT: 设置一定的等待时间，等待task schedule执行完毕后终止
        os._exit(0)
        
    def run(self):
        self.task_scheduler.start(use_thread_for_task_runner=False)
        
    def stop(self):
        self.task_scheduler.stop()
        self.signal_sender.close()
        
    def reload_exchange_info(self, ts):
        cooling_off_period = self.params['cooling_off_period']
        
        with self.exchange_info_lock:
            exchange_info = load_binance_data(self.exchange, self.exchange_info_dir)
            today = datetime.now()
            cooling_delta = timedelta(days=cooling_off_period)
            ipo_before_thres = today - cooling_delta
            trading_symbols = [symbol_info['symbol'].lower() for symbol_info in exchange_info['symbols']
                               if (
                                       symbol_info['status'] == 'TRADING' 
                                       and symbol_info['symbol'].endswith('USDT')
                                       and pd.to_datetime(symbol_info['onboardDate'], unit='ms') < ipo_before_thres
                                       and symbol_info['symbol'] != 'USDCUSDT'
                                   )]
            self.trading_symbols = sorted(trading_symbols)
        self._update_factors_to_fetch_mapping()
        
    def _update_factors_to_fetch_mapping(self):
        assist_factors_params = self.params['assist_factors_params']
        factors_in_tuple = assist_factors_params['factors_in_tuple']
        
        self.factors_to_fetch_mapping = {factor: [tuple([*factor, symbol]) for symbol in self.trading_symbols]
                                         for factor in factors_in_tuple}

    def update_once(self, ts):
        try:
            self._fetch_factors(ts)
            pft_till_t, fee_till_t = self._calc_profit_t_1_till_t(ts)
            self._save_pnl_to_cache(ts, pft_till_t, fee_till_t)
            self._fetch_funding(ts)
            predict_value_matrix = self._fetch_predictions(ts)
            w0 = self._fetch_pre_pos()
            if w0 is None:
                return
            if predict_value_matrix is not None:
                # self._log_po_start()
                alpha = self._get_alpha(predict_value_matrix)
                allow_to_trade_by_funding = self._filter_by_funding(ts)
                mm_t = self._get_momentum_at_t(ts, allow_to_trade_by_funding)
                his_pft_t = self._get_his_profit_at_t(ts, allow_to_trade_by_funding)
                w1 = self._po_on_tradable(w0, alpha, mm_t, his_pft_t, allow_to_trade_by_funding)
            else:
                w1 = w0
                alpha = None
                self._repo_model_error()
            new_pos = self._update_positions_on_universal_set(w0, w1)
            self._send_pos_to_zmq_and_db(new_pos, ts)
            self._send_pos_to_db(new_pos)
            # pft_till_t, fee_till_t = self._calc_profit_t_1_till_t(ts)
            # self._report_new_pos(new_pos)
            period_pnl = self._record_n_repo_pos_diff_and_pnl(new_pos, w0, pft_till_t, fee_till_t)
            self._save_to_cache(ts, new_pos, period_pnl)
            self._save_to_persist(ts, alpha, new_pos, pft_till_t, fee_till_t, period_pnl)
        except:
            self.log.exception('update error')
            error_msg = traceback.format_exc()  # 获取完整的异常信息
            self.ding.send_markdown('UPDATE ERROR', error_msg, msg_type='error')
        
    def _fetch_factors(self, ts):
        fetch_params = self.params['fetch_params']
        data_check_params = self.params['data_check_params']
        assist_factors_params = self.params['assist_factors_params']
        factors = assist_factors_params['factors']
        factors_in_tuple = assist_factors_params['factors_in_tuple']
        delay_fetch = assist_factors_params['delay_fetch']
        
        for factor, factor_in_tuple, delay in list(zip(factors, factors_in_tuple, delay_fetch)):
            columns = [factor_in_tuple]
            
            delay_in_dt = self.delay_mapping[delay]
            ts_to_check = ts - delay_in_dt
            
            factors_to_fetch = self.factors_to_fetch_mapping[factor_in_tuple]
            dc = FactorDataChecker(self.trading_symbols, columns, ts_to_check, **data_check_params, log=self.log)
            
            fetch_n_check_once_func = partial(fetch_n_check_once, fetch_func=self.factor_reader.fetch_batch_data,
                                              list_to_fetch=factors_to_fetch, dc=dc)
            factor_value_matrix = fetch_n_check(fetch_n_check_once_func, fetch_target_name=factor['factor'],
                                                log=self.log, repo=self.ding, **fetch_params)
            
            cache_name = 'curr_price' if factor['factor'].startswith('curr_price') else 'twap_price'
            if factor_value_matrix is None:
                msg = f'Failed to fetch {cache_name}!'
                self.log.warning(msg)
                self.ding.send_text(msg, msg_type='error')
                continue
            self.cache_mgr.add_row(cache_name, factor_value_matrix[factor_in_tuple], ts_to_check)
            
    def _fetch_funding(self, ts):
        funding_rates, status = self.funding_fetcher.fetch_funding_rates(
            timestamp=ts,
            symbols=self.trading_symbols
        )
        if not status['success']:
            missing_symbols = status["missing_symbols"]
            len_missing = len(missing_symbols)
            self.ding.send_text(f'Missing Fundings(total {len_missing}): {missing_symbols}', msg_type='warning')
        funding_rates = funding_rates['funding_rate'].reindex(self.trading_symbols).fillna(0)
        print(funding_rates)
        self.cache_mgr.add_row('est_funding_rate', funding_rates, ts)
    
    def _fetch_predictions(self, ts):
        fetch_params = self.params['fetch_params']
        data_check_params = self.params['data_check_params']
        model_name = self.params['model_name']
        
        columns = [model_name]

        dc = ModelPredictionChecker(self.trading_symbols, columns, ts, **data_check_params, log=self.log)
        
        fetch_n_check_once_func = partial(fetch_n_check_once, fetch_func=self.predict_reader.fetch_batch_data,
                                          list_to_fetch=self.trading_symbols, dc=dc)
        return fetch_n_check(fetch_n_check_once_func, fetch_target_name='predictions',
                             log=self.log, repo=self.ding, **fetch_params)
    
    def _fetch_pre_pos(self):
        fetch_params = self.params['fetch_params'].copy()
        allow_init_pre_pos = self.params['allow_init_pre_pos']
        min_pos = self.params['min_pos']

        fetch_n_return_once_func = partial(fetch_n_return_once, fetch_func=self.pos_reader.fetch_batch_data)
        
        if allow_init_pre_pos:
            fetch_params['max_attempts'] = 1
            
        w0 = fetch_n_check(fetch_n_return_once_func, fetch_target_name='pre_pos',
                           log=self.log, repo=self.ding, **fetch_params)
        if w0 is None:
            if allow_init_pre_pos:
                w0 = pd.Series(data=[0]*(len(self.trading_symbols)), index=self.trading_symbols)
            else:
                self.log.warning('Failed to Fetch pre_pos!')
                self.ding.send_text('Failed to Fetch pre_pos!', msg_type='error')
                
# =============================================================================
#         if w0 is not None:
#             w0 = filter_series(w0, min_abs_value=min_pos)
# =============================================================================
        return w0
    
    def _log_po_start(self):
        msg = 'Successfully fetched factors, prediction and pre pos. Start portfolio optimization...'
        self.log.success(msg)
        self.ding.send_text(msg, msg_type='success')
            
    def _get_alpha(self, predict_value_matrix):
        model_name = self.params['model_name']
        
        predict_res = predict_value_matrix[model_name]
        predict_rank = calculate_rank(predict_res)
        alpha = calculate_weight_from_rank(predict_rank)
        return alpha # TODO: to persist
    
    def _get_momentum_at_t(self, ts, allow_to_trade_by_funding):
        optimizer_params = self.params['optimizer_params']
        momentum_limits = optimizer_params['momentum_limits']
        
        curr_price = self.cache_mgr['curr_price']
        
        mm_t = {}
        for mm_wd in momentum_limits:
            mm_wd_real = self.mmt_limits_mapping[mm_wd]
            pre_t = ts - mm_wd_real
            try:
                assert ts in curr_price.index and pre_t in curr_price.index
            except:
                missing_ts = []
                if ts not in curr_price.index:
                    missing_ts.append(ts)
                if pre_t not in curr_price.index:
                    missing_ts.append(pre_t)
                self.log.warning(f'Timestamp: {missing_ts} is not in curr_price. Skip momentum calc for {mm_wd}.')
                continue
            mm_wd_t = (curr_price.loc[ts] / curr_price.loc[pre_t] -1
                       ).replace([np.inf, -np.inf], np.nan).reindex(allow_to_trade_by_funding).fillna(0.0).values
            mm_t[mm_wd] = mm_wd_t
        return mm_t
    
    def _get_his_profit_at_t(self, ts, allow_to_trade_by_funding):
        optimizer_params = self.params['optimizer_params']
        pf_limits = optimizer_params['pf_limits']
        
        twap_profit = self.cache_mgr['twap_profit']
        
        his_pft_t = {}
        for pf_wd in pf_limits:
            pf_wd_real = self.pf_limits_mapping[pf_wd]
            pre_t = ts - pf_wd_real
            twap_profit_since = twap_profit.loc[pre_t:]
            if len(twap_profit_since) == 0:
                self.log.warning(f'Found no profit record since {pre_t}. Skip his profit calc for {pf_wd}.')
                continue
            pf_wd_t = twap_profit_since.sum(axis=0).reindex(allow_to_trade_by_funding).values
            his_pft_t[pf_wd] = pf_wd_t
        return his_pft_t
    
    def _filter_by_funding(self, ts):  
        funding_limit_pr = self.params.get('funding_limit')
        if funding_limit_pr is None:
            return self.trading_symbols
        funding_abs_limit = funding_limit_pr['funding_abs_limit']
        funding_cooldown = funding_limit_pr['funding_cooldown']
        
        est_funding_rate = self.cache_mgr['est_funding_rate']
        funding_invalid = est_funding_rate.abs() > funding_abs_limit
        
        if funding_cooldown is not None:
            rolling_below_threshold = funding_invalid.rolling(window=funding_cooldown, min_periods=1).mean()
            final_mask = rolling_below_threshold != 0
        else:
            final_mask = funding_invalid
        lastest_final_mask = final_mask.loc[ts].reindex(index=self.trading_symbols).fillna(False)
        allow_to_trade_by_funding =  lastest_final_mask[~lastest_final_mask].index.tolist()
        banned_by_funding = lastest_final_mask[lastest_final_mask].index.tolist()
        
        msg = f'Banned From Trading By Funding: {banned_by_funding}'
        self.log.info(msg)
        self.ding.send_text(msg, msg_type='info')
        
        return allow_to_trade_by_funding
    
    def _po_on_tradable(self, w0, alpha, mm_t, his_pft_t, allow_to_trade_by_funding): # 对可交易部分组合优化
        optimizer_params = self.params['optimizer_params']
        to_rate_thresh_L0 = optimizer_params['to_rate_thresh_L0']
        to_rate_thresh_L1 = optimizer_params['to_rate_thresh_L1']
        min_pos = self.params['min_pos']
        
        alpha_filtered = alpha.reindex(allow_to_trade_by_funding)
        reindexed_w0 = w0.reindex(alpha_filtered.index, fill_value=0)
        w1, status = self.opt_func(alpha_filtered, reindexed_w0, mm_t, his_pft_t, to_rate_thresh_L0)
        self.log.warning(status)
        if status == 'mis_optimal':
            self.log.warning(f'w1: {w1}')
            self.log.warning(f'alpha: {alpha}')
            self.log.warning(f'reindexed_w0: {reindexed_w0}')
            self.log.warning(f'mm_t: {mm_t}')
            self.log.warning(f'his_pft_t: {his_pft_t}')
        if status != "optimal":
            msg = f'Could not find optimal result under to_rate: {to_rate_thresh_L0}, try {to_rate_thresh_L1}.'
            self.log.warning(msg)
            self.ding.send_text(msg, msg_type='warning')
            w1, status = self.opt_func(alpha_filtered, reindexed_w0, mm_t, his_pft_t, to_rate_thresh_L1)
            if status != "optimal":
                msg = (f'Still could not find optimal result under to_rate: {to_rate_thresh_L1}, '
                       'remain previous pos.')
                self.log.warning(msg)
                self.log.warning(f'alpha: {alpha}')
                self.log.warning(f'reindexed_w0: {reindexed_w0}')
                self.log.warning(f'mm_t: {mm_t}')
                self.log.warning(f'his_pft_t: {his_pft_t}')
                self.ding.send_text(msg, msg_type='warning')
                w1 = reindexed_w0
        # w1 = filter_series(w1, min_abs_value=min_pos, remove=False)
        return w1
    
    def _repo_model_error(self):
        model_name = self.params['model_name']
        
        msg = f'无法获取最新预测值，沿用上期仓位，请尽快修复模型端 {model_name} 问题！'
        self.log.warning(msg)
        self.ding.send_text(msg, msg_type='warning')
    
    def _update_positions_on_universal_set(self, w0, w1):
        # Step 1: 对于上期存在但当期没有的币种，保留它们并将仓位设为0
        # 获取上期有，但当期没有的币种
        missing_in_current = w0.index.difference(w1.index)

        # 将这些币种的仓位设为0
        missing_position = pd.Series(0, index=missing_in_current)

        # Step 2: 将当期仓位与处理后的上期仓位合并
        # 先使用当期的仓位，然后补充上期存在但当期没有的币种仓位
        new_pos = pd.concat([w1, missing_position]) # TODO: to cache & persist
        
        return new_pos
    
    def _send_pos_to_zmq_and_db(self, new_pos, ts):
        zmq_params = self.params['zmq']
        twap_zmq_params = self.params['twap_zmq']
        capital = self.params['capital']

        ma_price_reindexed = self._get_ma_price(new_pos, ts)
        new_pos_in_coin = (new_pos / ma_price_reindexed * capital).replace([np.nan, np.inf, -np.inf], 0)
        
        for symbol, pos in new_pos_in_coin.items():
            symbol_upper = symbol.upper()  # 转为大写
            self.signal_sender.send_message(
                zmq_params['strategy_name'], 
                zmq_params['exchange'],
                zmq_params['symbol_type'], 
                symbol_upper, str(pos)
                )
            self.twap_signal_sender.send_message(
                twap_zmq_params['strategy_name'], 
                twap_zmq_params['exchange'],
                twap_zmq_params['symbol_type'], 
                symbol_upper, str(pos),
                twap_zmq_params['mode']
                )
        
        with self.exchange_info_lock:
            new_pos_send_to_thunder = new_pos_in_coin.reindex(self.trading_symbols + ['usdcusdt']).fillna(0)
        try:
            self.thunder_sender.update_signals_and_status(ts, new_pos_send_to_thunder)
        except:
            traceback.print_exc()
            
    def _get_ma_price(self, new_pos, ts):
        sp = self.params['sp']
        ma_price_params = self.params['ma_price']
        ma_wd = ma_price_params['ma_wd']
        check_thres = ma_price_params['check_thres']
        
        interval = timedelta(seconds=parse_time_string(sp))
        start_ma_t = ts - interval * ma_wd
        curr_price = self.cache_mgr['curr_price']
        ma_price = curr_price.loc[start_ma_t:].mean(axis=0)
        
        # check
        if ts in curr_price.index:
            ts_price = curr_price.loc[ts]
            px_chg = (ts_price - ma_price) / ma_price
            abnormal = px_chg.abs() > check_thres
            px_chg_abn = px_chg[abnormal]
            if len(px_chg_abn) > 0:
                msg = f'可能异常的价格波动: {px_chg_abn}'
                self.log.warning(msg)
                px_chg_abn_in_markdown = df_to_markdown(px_chg_abn, show_all=True, columns=['symbol', 'price change'])
                self.ding.send_markdown("异常价格波动告警，请及时查看是否为真实价格，否则可能导致下单数量错误", 
                                        px_chg_abn_in_markdown, msg_type='warning')
        
        return ma_price.reindex(new_pos.index)
        
    def _send_pos_to_db(self, new_pos):
        self.pos_sender.insert(new_pos)
        
    def _calc_profit_t_1_till_t(self, ts): # 计算t-1至t的收益
        sp = self.params['sp']
        fee_rate = self.params['fee_rate']

        interval = timedelta(seconds=parse_time_string(sp))
        pre_t_1 = ts - interval
        pre_t_2 = ts - interval * 2
        
        # 创建 dataset，将需要提取的行和相应的键存储到字典中
        dataset = {
            'close_price_t_1': ('curr_price', ts),
            'close_price_t_2': ('curr_price', pre_t_1),
            'twap_price_t_1': ('twap_price', pre_t_1),
            'w_t_1': ('pos_his', pre_t_1),
            'w_t_2': ('pos_his', pre_t_2)
        }
        
        # 用于存储提取后的数据
        extracted_data = {}
    
        # 从 dataset 中调取数据
        for key, (cache_name, index) in dataset.items():
            try:
                extracted_data[key] = self.cache_mgr[cache_name].loc[index]
            except KeyError:
                self.log.warning(f"KeyError: {index} not found in {cache_name}. Skip calc_profit.")
                return None, None  # 如果有 KeyError，直接返回 None 或者抛出异常
            
        for key in extracted_data:
            extracted_data[key] = extracted_data[key].reindex(extracted_data['w_t_1'].index)
    
        # 直接在后续计算中使用 extracted_data 的值
        rtn_c2c = extracted_data['close_price_t_1'] / extracted_data['close_price_t_2'] - 1
        rtn_cw0 = extracted_data['twap_price_t_1'] / extracted_data['close_price_t_2'] - 1
        rtn_cw1 = extracted_data['close_price_t_1'] / extracted_data['twap_price_t_1'] - 1
        
        # 计算最终的收益
        pft_till_t = calc_profit_before_next_t(extracted_data['w_t_2'], 
                                               extracted_data['w_t_1'], 
                                               rtn_c2c, rtn_cw0, rtn_cw1)
        
        # 计算fee
        fee_till_t = np.abs(extracted_data['w_t_1'] - extracted_data['w_t_2']) * fee_rate
        
        return pft_till_t, fee_till_t # TODO: to cache (index: t-1) & to persist (index: t)
        
    def _report_new_pos(self, new_pos):
        pos_to_repo = filter_series(new_pos, min_abs_value=0)
        pos_to_repo = np.round(pos_to_repo, decimals=3)
        pos_in_markdown = df_to_markdown(pos_to_repo, show_all=True, columns=['symbol', 'pos'])
        self.ding.send_markdown('SUCCESSFULLY SEND POSITION', pos_in_markdown, msg_type='success')
        
    def _record_n_repo_pos_diff_and_pnl(self, new_pos, w0, pft_till_t, fee_till_t): 
        # !!!: 算的是上期pnl和当期会发生的fee，为粗略计算，否则会比较复杂
        pos_change_to_repo = self.params['pos_change_to_repo']

        ## pos diff
        reindexed_w0 = w0.reindex(new_pos.index, fill_value=0)
        pos_diff = new_pos - reindexed_w0
        pos_diff_filtered = filter_series(pos_diff, min_abs_value=pos_change_to_repo)
        pos_diff_to_repo = np.round(pos_diff_filtered, decimals=4)
        pos_diff_in_markdown = df_to_markdown(pos_diff_to_repo, show_all=True, columns=['symbol', 'pos_diff'])
        
        ## hsr
        hsr = pos_diff.abs().sum() / 2
        
        ## agg pnl & fee
        pnl_occurred = np.sum(pft_till_t) if pft_till_t is not None else np.nan
        fee_occurred = np.sum(fee_till_t) if fee_till_t is not None else 0
        
        ## period net pnl
        period_pnl = pd.Series({
            'pnl': pnl_occurred, 
            'fee': fee_occurred,
            'net_pnl': pnl_occurred - fee_occurred,
            })
            
        self.ding.send_markdown(f"当期换手率: {hsr:.2%}, 上期净收益率: {period_pnl['net_pnl']:.2%}", 
                                pos_diff_in_markdown, msg_type='info')

        return period_pnl
    
    def _save_pnl_to_cache(self, ts, pft_till_t, fee_till_t):
        sp = self.params['sp']

        interval = timedelta(seconds=parse_time_string(sp))
        pre_t_1 = ts - interval

        if pft_till_t is not None:
            self.cache_mgr.add_row('twap_profit', pft_till_t, pre_t_1)
        else:
            self.log.warning(f'Failed to calc profit during {pre_t_1} - {ts}. Skip saving cache: twap_profit.')
        if fee_till_t is not None:
            self.cache_mgr.add_row('fee', fee_till_t, pre_t_1)
        else:
            self.log.warning(f'Failed to calc fee during {pre_t_1} - {ts}. Skip saving cache: fee.')
        
    def _save_to_cache(self, ts, new_pos, period_pnl):
        sp = self.params['sp']

        interval = timedelta(seconds=parse_time_string(sp))
        pre_t_1 = ts - interval
        
        if new_pos is not None:
            self.cache_mgr.add_row('pos_his', new_pos, ts)
        self.cache_mgr.add_row('period_pnl', period_pnl, pre_t_1)
        self.cache_mgr.save(ts)
        
    def _save_to_persist(self, ts, alpha, new_pos, pft_till_t, fee_till_t, period_pnl):
        if alpha is not None:
            self.persist_mgr.add_row('alpha', alpha, ts)
        if new_pos is not None:
            self.persist_mgr.add_row('pos_his', new_pos, ts)
        if pft_till_t is not None:
            self.persist_mgr.add_row('twap_profit', pft_till_t, ts)
        else:
            self.log.warning(f'Failed to calc profit till {ts}. Skip saving persist: twap_profit.')
        if fee_till_t is not None:
            self.persist_mgr.add_row('fee', fee_till_t, ts)
        else:
            self.log.warning(f'Failed to calc fee till {ts}. Skip saving persist: fee.')
        self.persist_mgr.add_row('period_pnl', period_pnl, ts)
        self.persist_mgr.add_row('est_funding_rate', self.cache_mgr['est_funding_rate'].loc[ts], ts)
        self.persist_mgr.save(ts)
        
    def calc_daily_pnl(self, ts):
        twap_profit = self.cache_mgr['twap_profit']
        fee = self.cache_mgr['fee']
        
        daily_start_t = ts - timedelta(days=1)
        twap_profit_since = twap_profit.loc[daily_start_t:]
        fee_since = fee.loc[daily_start_t:]
        if len(twap_profit_since) == 0 or len(fee_since) == 0:
            return
        pnl_per_symbol = twap_profit_since.sum(axis=0).fillna(0.0)
        fee_per_symbol = fee_since.sum(axis=0).fillna(0.0)
        net_per_symbol = pnl_per_symbol - fee_per_symbol
        total_pnl = np.sum(pnl_per_symbol)
        total_fee = np.sum(fee_per_symbol)
        net_pnl = total_pnl - total_fee

        # 从大到小排序
        sorted_pnl_per_symbol = net_per_symbol.sort_values(ascending=False)
        sorted_pnl_per_symbol = filter_series(sorted_pnl_per_symbol, min_abs_value=1e-5)
        
        # 将 sorted_pnl_per_symbol 的值转换为百分比格式
        pnl_per_symbol_percentage = sorted_pnl_per_symbol.apply(lambda x: f'{x:.3%}')
        
        # 将转换后的结果转换为 DataFrame 并发送
        pnl_markdown = df_to_markdown(pnl_per_symbol_percentage, show_all=True, 
                                      columns=['symbol', 'pnl_per_symbol'])
        
        # 将 total_pnl 转换为百分比格式并发送
        date = get_date_based_on_timestamp(daily_start_t)
        title = f'[{date}] PnL: {total_pnl:.2%}, Fee: {total_fee:.2%}, Net: {net_pnl:.2%}'
        self.log.info(title)
        self.ding.send_markdown(title, pnl_markdown, msg_type='info')
    
    
# %%
# 在pos_updater.py文件中添加import
from core.local_loader import PriceDataPreloader

class PosUpdaterWithBacktest(PosUpdater):
    """
    支持从回测初始化的PosUpdater
    完整版本，包含所有必要的方法，并使用价格数据预加载器优化回滚性能
    """
    
    def __init__(self, stg_name):
        # 先初始化回测相关参数（在父类__init__之前）
        self._pre_init_backtest_params(stg_name)
        
        # 调用父类初始化
        super().__init__(stg_name)
        
        # 初始化价格数据预加载器
        self.price_preloader = None
        
        if self.use_backtest_init:
            self._attempt_backtest_initialization()
    
    def _pre_init_backtest_params(self, stg_name):
        """在父类初始化前预加载回测参数"""
        # 这里需要预先加载配置来确定是否需要回测初始化
        # 简化处理：先设置默认值，在父类初始化后再正式加载
        self.backtest_initialization_completed = False
        self.use_backtest_init = False
    
    def _init_backtest_params(self):
        """初始化回测相关参数"""
        self.backtest_init_params = self.params.get('backtest_init', {})
        self.use_backtest_init = self.backtest_init_params.get('enable', False)
        
        if self.use_backtest_init:
            self.log.info("Backtest initialization enabled")
            # 验证必要参数
            required_params = ['backtest_result_path', 'test_name', 'backtest_name']
            for param in required_params:
                if not self.backtest_init_params.get(param):
                    self.log.error(f"Missing required backtest parameter: {param}")
                    self.use_backtest_init = False
                    break
        else:
            self.log.info("Backtest initialization disabled")
    
    def _load_params(self):
        """重写参数加载，添加回测参数初始化"""
        super()._load_params()
        self._init_backtest_params()
    
    def _attempt_backtest_initialization(self):
        """尝试从回测初始化（使用价格数据预加载器优化）"""
        try:
            self.log.info("Attempting backtest initialization...")
            
            # 1. 初始化价格数据预加载器（新增！）
            self._initialize_price_preloader()
            
            # 2. 预加载历史twap_profit数据到cache
            self._preload_historical_twap_profit()
            
            # 3. 执行rollforward
            success = self._rollforward_from_backtest()
            if success:
                self.backtest_initialization_completed = True
                self.log.success("Successfully initialized from backtest")
                self.ding.send_text("Successfully initialized from backtest", msg_type='success')
            else:
                self.log.warning("Failed to initialize from backtest, will use DB fallback")
                self.ding.send_text("Failed to initialize from backtest, using DB fallback", msg_type='warning')
        except Exception as e:
            self.log.exception("Error during backtest initialization")
            self.ding.send_text(f"Error during backtest initialization: {str(e)}", msg_type='error')
    
    def _initialize_price_preloader(self):
        """初始化价格数据预加载器"""
        try:
            self.log.info("Initializing price data preloader...")
            
            # 获取rollforward起始时间
            start_time = self._get_rollforward_start_time()
            if start_time is None:
                self.log.warning("Cannot determine start time for price data preloader")
                return
            
            # 获取回看天数（可配置，默认30天）
            price_persist_dir = self.backtest_init_params.get('price_persist_dir', '')
            lookback_days = self.backtest_init_params.get('price_lookback_days', 30)
            
            self.log.info(f"Initializing price preloader with start_time: {start_time}, lookback_days: {lookback_days}")
            
            # 创建价格数据预加载器
            self.price_preloader = PriceDataPreloader(
                persist_dir=price_persist_dir,
                start_time=start_time, 
                lookback_days=lookback_days,
                log=self.log
            )
            
            self.log.success("Price data preloader initialized successfully")
            
        except Exception as e:
            self.log.exception(f"Error initializing price preloader: {e}")
            self.price_preloader = None
    
    def _preload_historical_twap_profit(self):
        """预加载历史twap_profit数据到cache"""
        try:
            self.log.info("Preloading historical twap_profit data...")
            
            # 获取起始时间点
            start_time = self._get_rollforward_start_time()
            if start_time is None:
                self.log.warning("Cannot determine start time for twap_profit preloading")
                return
            
            # 计算需要预加载的时间范围（start_time之前的历史数据用于hispft计算）
            max_lookback_days = self.backtest_init_params.get('max_lookback_days', 30)  # 默认30天
            preload_start = start_time - timedelta(days=max_lookback_days)
            
            self.log.info(f"Preloading twap_profit from {preload_start} to {start_time}")
            
            # 从回测结果文件中读取历史twap_profit数据
            backtest_result_path = Path(self.backtest_init_params['backtest_result_path'])
            test_name = self.backtest_init_params['test_name']
            backtest_name = self.backtest_init_params['backtest_name']
            name_to_save = f'{test_name}__{backtest_name}'
            
            backtest_dir = backtest_result_path / test_name / 'backtest' / backtest_name
            
            # 获取twap配置（优先使用参数配置，否则使用默认值）
            twap_names = self.backtest_init_params.get('twap_list', ['twd30_sp30'])
            main_twap = twap_names[0]  # 使用第一个twap作为主要数据源
            
            profit_file = backtest_dir / f"profit_{main_twap}_{name_to_save}.parquet"
            
            if not profit_file.exists():
                self.log.warning(f"Backtest twap profit file not found: {profit_file}")
                return
            
            # 读取回测的twap_profit数据
            backtest_profits = pd.read_parquet(profit_file)
            self.log.info(f"Loaded backtest twap_profit data: {len(backtest_profits)} rows")
            
            # 过滤到指定时间范围：preload_start 到 start_time（不包含start_time）
            historical_profits = backtest_profits.loc[preload_start:start_time]
            if not historical_profits.empty:
                # 排除start_time本身（因为start_time的profit将在rollforward中重新计算）
                historical_profits = historical_profits.loc[historical_profits.index < start_time]
            
            if not historical_profits.empty:
                # 批量写入cache
                loaded_count = 0
                for ts in historical_profits.index:
                    try:
                        profit_data = historical_profits.loc[ts]
                        self.cache_mgr.add_row('twap_profit', profit_data, ts)
                        loaded_count += 1
                    except Exception as e:
                        self.log.debug(f"Error loading profit at {ts}: {e}")
                        continue
                
                self.log.success(f"Preloaded {loaded_count} historical twap_profit records to cache")
            else:
                self.log.warning(f"No historical profits found in range {preload_start} to {start_time}")
                
        except Exception as e:
            self.log.exception(f"Error preloading historical twap_profit: {e}")
            
    def _get_rollforward_start_time(self):
        """获取rollforward的起始时间"""
        try:
            # 优先使用配置中指定的start_time
            custom_start_time = self.backtest_init_params.get('start_time')
            if custom_start_time:
                if isinstance(custom_start_time, str):
                    start_time = pd.to_datetime(custom_start_time)
                else:
                    start_time = custom_start_time
                self.log.info(f"Using custom start time: {start_time}")
                return start_time
            
            # 否则使用回测的最后时间戳（原有逻辑）
            backtest_positions = self._load_backtest_positions()
            if backtest_positions is not None:
                last_time = backtest_positions.index[-1]
                self.log.info(f"Using backtest last time as start: {last_time}")
                return last_time
            
            return None
            
        except Exception as e:
            self.log.exception(f"Error getting rollforward start time: {e}")
            return None
    
    def _rollforward_from_backtest(self):
        """从回测位置滚动到当前，返回是否成功"""
        try:
            # 1. 加载回测仓位
            backtest_positions = self._load_backtest_positions()
            if backtest_positions is None:
                return False
            
            # 2. 获取起始时间戳
            start_ts = self._get_rollforward_start_time()
            if start_ts is None:
                return False
            
            # 3. 验证起始时间戳在回测数据中存在
            if start_ts not in backtest_positions.index:
                self.log.error(f"Start time {start_ts} not found in backtest positions")
                available_times = backtest_positions.index.tolist()
                self.log.info(f"Available times in backtest: {available_times[:5]}...{available_times[-5:]}")
                return False
            
            current_ts = datetime.now().replace(second=0, microsecond=0)
            
            self.log.info(f"Rollforward start timestamp: {start_ts}, Current: {current_ts}")
            
            # 4. 检查时间范围是否合理
            max_lookback = timedelta(days=self.backtest_init_params.get('max_lookback_days', 7))
            if current_ts - start_ts > max_lookback:
                self.log.warning(f"Start time too old: {start_ts}, max lookback: {max_lookback}")
                return False
            
            # 5. 生成需要滚动的时间戳（从start_ts的下一个时间戳开始）
            rollforward_timestamps = self._generate_rollforward_timestamps(start_ts, current_ts)
            self.log.info(f"Generated {len(rollforward_timestamps)} timestamps for rollforward")
            
            if len(rollforward_timestamps) == 0:
                self.log.info("No timestamps to rollforward, using start position")
                # 直接使用起始时间戳的回测仓位
                start_pos = backtest_positions.loc[start_ts]
                self.cache_mgr.add_row('pos_his', start_pos, start_ts)
                return True
            
            # 6. 检查历史数据完整性
            if not self._check_historical_data_completeness(rollforward_timestamps):
                self.log.warning("Historical data incomplete, cannot rollforward")
                return False
            
            # 7. 执行滚动更新（从指定的起始位置开始）
            self._execute_rollforward(backtest_positions, start_ts, rollforward_timestamps)
            
            return True
            
        except Exception as e:
            self.log.exception(f"Error in rollforward_from_backtest: {e}")
            return False
    
    def _load_backtest_positions(self):
        """加载回测仓位文件"""
        try:
            backtest_result_path = Path(self.backtest_init_params['backtest_result_path'])
            test_name = self.backtest_init_params['test_name']
            backtest_name = self.backtest_init_params['backtest_name']
            
            pos_file = backtest_result_path / test_name / 'backtest' / backtest_name / f"pos_{test_name}__{backtest_name}.parquet"
            
            if not pos_file.exists():
                self.log.warning(f"Backtest position file not found: {pos_file}")
                return None
                
            positions = pd.read_parquet(pos_file)
            self.log.info(f"Loaded backtest positions: {len(positions)} rows, from {positions.index[0]} to {positions.index[-1]}")
            return positions
            
        except Exception as e:
            self.log.exception(f"Error loading backtest positions: {e}")
            return None
    
    def _generate_rollforward_timestamps(self, start_ts, end_ts):
        """生成滚动时间戳序列"""
        sp = self.params['sp']
        interval = timedelta(seconds=parse_time_string(sp))
        
        timestamps = []
        current = start_ts + interval  # 从下一个时间戳开始
        
        while current <= end_ts:
            timestamps.append(current)
            current += interval
        
        return timestamps
    
    def _check_historical_data_completeness(self, timestamps):
        """检查历史数据完整性"""
        # 检查关键的persist数据是否存在
        required_data = ['alpha']  # 至少需要历史预测数据
        missing_count = 0
        total_count = len(timestamps)
        
        for ts in timestamps:
            for data_name in required_data:
                if not self.persist_mgr.has_data(data_name, ts):
                    missing_count += 1
                    if missing_count <= 5:  # 只记录前5个缺失的
                        self.log.warning(f"Missing {data_name} data for timestamp {ts}")
        
        if total_count == 0:
            return True
            
        missing_ratio = missing_count / (total_count * len(required_data))
        self.log.info(f"Data completeness check: {missing_count}/{total_count * len(required_data)} missing ({missing_ratio:.2%})")
        
        # 如果缺失超过20%，认为数据不完整
        return missing_ratio < 0.2
    
    def _execute_rollforward(self, backtest_positions, start_ts, timestamps):
        """执行滚动更新"""
        # 初始化当前仓位为指定起始时间的回测仓位
        current_pos = backtest_positions.loc[start_ts]
        self.log.info(f"Starting rollforward from {len(timestamps)} timestamps, initial position at {start_ts}")
        
        # 将起始仓位添加到cache作为起始点
        self.cache_mgr.add_row('pos_his', current_pos, start_ts)
        
        successful_updates = 0
        
        for i, ts in enumerate(timestamps):
            try:
                if (i + 1) % 10 == 0 or i == len(timestamps) - 1:
                    self.log.info(f"Rolling forward {i+1}/{len(timestamps)}: {ts}")
                
                # 滚动更新一次（历史模式）
                new_pos = self._rollforward_update_once(ts, current_pos)
                if new_pos is not None:
                    current_pos = new_pos
                    successful_updates += 1
                
            except Exception as e:
                self.log.exception(f"Error during rollforward at {ts}: {e}")
                # 继续使用上一个仓位
                continue
        
        # 将最终仓位保存到cache中，作为下次update的起始点
        latest_ts = timestamps[-1] if timestamps else start_ts
        self.cache_mgr.add_row('pos_his', current_pos, latest_ts)
        
        self.log.success(f"Rollforward completed: {successful_updates}/{len(timestamps)} successful updates, final position at {latest_ts}")
    
    def _rollforward_update_once(self, ts, current_pos):
        """历史模式的单次更新 - 使用预加载的价格数据"""
        try:
            # 1. 获取因子数据（回滚模式：从预加载器获取价格数据）
            self._fetch_factors_for_rollforward(ts)
            
            # 2. 计算t-1至t的收益和费用
            pft_till_t, fee_till_t = self._calc_profit_t_1_till_t(ts)
            
            # 3. 保存pnl到cache
            self._save_pnl_to_cache(ts, pft_till_t, fee_till_t)
            
            # 4. 获取funding数据（回滚模式：从数据库读取）
            self._fetch_funding(ts)
            
            # 5. 获取预测值（回滚模式：从persist读取历史alpha）
            predict_value_matrix = self._fetch_predictions(ts)
            
            # 6. 获取前期仓位（回滚模式：使用传入的current_pos）
            w0 = current_pos
            if w0 is None:
                return None
                
            # 7. 组合优化或保持原仓位
            if predict_value_matrix is not None:
                # 7a. 获取alpha
                alpha = self._get_alpha(predict_value_matrix)
                
                # 7b. 根据funding过滤可交易标的
                allow_to_trade_by_funding = self._filter_by_funding(ts)
                
                # 7c. 计算momentum
                mm_t = self._get_momentum_at_t(ts, allow_to_trade_by_funding)
                
                # 7d. 计算历史收益
                his_pft_t = self._get_his_profit_at_t(ts, allow_to_trade_by_funding)
                
                # 7e. 组合优化
                w1 = self._po_on_tradable(w0, alpha, mm_t, his_pft_t, allow_to_trade_by_funding)
            else:
                w1 = w0
                alpha = None
                self._repo_model_error()
                
            # 8. 更新全集仓位
            new_pos = self._update_positions_on_universal_set(w0, w1)
            
            # 9. 发送仓位（回滚模式：跳过实际发送，只记录日志）
            self._send_pos_for_rollforward(new_pos, ts)
            
            # 10. 发送到数据库（回滚模式：跳过实际写入）
            # self._send_pos_to_db(new_pos)  # 回滚时不写数据库
            
            # 11. 记录仓位变化和pnl
            period_pnl = self._record_n_repo_pos_diff_and_pnl_for_rollforward(new_pos, w0, pft_till_t, fee_till_t)
            
            # 12. 保存到cache
            self._save_to_cache(ts, new_pos, period_pnl)
            
            # 13. 保存到persist
            # self._save_to_persist(ts, alpha, new_pos, pft_till_t, fee_till_t, period_pnl)
            
            return new_pos
            
        except Exception as e:
            self.log.exception(f'rollforward update error at {ts}')
            error_msg = traceback.format_exc()
            self.ding.send_markdown(f'ROLLFORWARD UPDATE ERROR at {ts}', error_msg, msg_type='error')
            return current_pos
    
    def _fetch_factors_for_rollforward(self, ts):
        """回滚模式的因子获取 - 使用预加载的价格数据"""
        assist_factors_params = self.params['assist_factors_params']
        factors = assist_factors_params['factors']
        delay_fetch = assist_factors_params['delay_fetch']
        
        for factor, delay in list(zip(factors, delay_fetch)):
            delay_in_dt = self.delay_mapping[delay]
            ts_to_check = ts - delay_in_dt
            
            # 根据因子类型选择对应的缓存名称和预加载器方法
            if factor['factor'].startswith('curr_price'):
                cache_name = 'curr_price'
                if self.price_preloader is not None:
                    # 使用预加载器获取currprice数据
                    price_data = self.price_preloader.get_currprice_at_time(ts_to_check, self.trading_symbols)
                    if not price_data.empty:
                        self.cache_mgr.add_row(cache_name, price_data, ts_to_check)
                        self.log.debug(f"[ROLLFORWARD] Loaded {cache_name} from preloader at {ts_to_check}")
                    else:
                        self.log.warning(f"[ROLLFORWARD] No {cache_name} data found in preloader for {ts_to_check}")
                else:
                    self.log.warning(f"[ROLLFORWARD] Price preloader not available, falling back to DB")
                    # 回退到原有的数据库获取方法
                    self._fetch_factors(ts)
                    
            elif factor['factor'].startswith('twap_30min'):
                cache_name = 'twap_price'
                if self.price_preloader is not None:
                    # 使用预加载器获取twapprice数据
                    price_data = self.price_preloader.get_twapprice_at_time(ts_to_check, self.trading_symbols)
                    if not price_data.empty:
                        self.cache_mgr.add_row(cache_name, price_data, ts_to_check)
                        self.log.debug(f"[ROLLFORWARD] Loaded {cache_name} from preloader at {ts_to_check}")
                    else:
                        self.log.warning(f"[ROLLFORWARD] No {cache_name} data found in preloader for {ts_to_check}")
                else:
                    self.log.warning(f"[ROLLFORWARD] Price preloader not available, falling back to DB")
                    # 回退到原有的数据库获取方法
                    self._fetch_factors(ts)
            else:
                # 对于非价格因子，仍然使用原有的数据库获取方法
                self.log.debug(f"[ROLLFORWARD] Non-price factor {factor['factor']}, using DB")
                self._fetch_factors(ts)
    
    def _send_pos_for_rollforward(self, new_pos, ts):
        """回滚模式的仓位发送 - 只记录日志，不实际发送"""
        try:
            self.log.debug(f"[ROLLFORWARD] Position updated at {ts}: {len(new_pos)} symbols")
            # 回滚时不实际发送到ZMQ和Thunder，只记录
        except Exception as e:
            self.log.warning(f"Error in rollforward position logging: {e}")
    
    def _record_n_repo_pos_diff_and_pnl_for_rollforward(self, new_pos, w0, pft_till_t, fee_till_t):
        """回滚模式的仓位变化和pnl记录 - 计算但不发送报告"""
        try:
            pos_change_to_repo = self.params['pos_change_to_repo']
    
            ## pos diff
            reindexed_w0 = w0.reindex(new_pos.index, fill_value=0)
            pos_diff = new_pos - reindexed_w0
            pos_diff_filtered = filter_series(pos_diff, min_abs_value=pos_change_to_repo)
            
            ## hsr
            hsr = pos_diff.abs().sum() / 2
            
            ## agg pnl & fee
            pnl_occurred = np.sum(pft_till_t) if pft_till_t is not None else np.nan
            fee_occurred = np.sum(fee_till_t) if fee_till_t is not None else 0
            
            ## period net pnl
            period_pnl = pd.Series({
                'pnl': pnl_occurred, 
                'fee': fee_occurred,
                'net_pnl': pnl_occurred - fee_occurred,
            })
            
            # 回滚时只记录到日志，不发送钉钉
            self.log.debug(f"[ROLLFORWARD] HSR: {hsr:.2%}, Net PnL: {period_pnl['net_pnl']:.2%}")
            
            return period_pnl
            
        except Exception as e:
            self.log.warning(f"Error recording rollforward pnl: {e}")
            return pd.Series({'pnl': np.nan, 'fee': 0, 'net_pnl': np.nan})

    def _fetch_historical_alpha(self, ts):
        """从persist获取历史alpha"""
        try:
            return self.persist_mgr.get_row('alpha', ts)
        except Exception as e:
            return None
    
    def _fetch_pre_pos(self):
        """重写获取前期仓位方法，优先使用回测初始化的结果"""
        # 如果刚完成回测初始化，优先使用cache中的仓位
        if self.backtest_initialization_completed:
            try:
                pos_his_cache = self.cache_mgr['pos_his']
                if len(pos_his_cache) > 0:
                    latest_pos = pos_his_cache.iloc[-1]
                    self.log.info("Using position from cache (from backtest initialization)")
                    return latest_pos
            except Exception as e:
                self.log.warning(f"Failed to get position from cache: {e}")
        
        # 否则调用父类方法
        return super()._fetch_pre_pos()