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
from datetime import timedelta
import time
import toml
import signal
import traceback
from functools import partial


from core.database_handler import FactorReader, ModelPredictReader, PositionSender, PositionReader
from core.task_scheduler import TaskScheduler
from core.cache_persist_manager import CacheManager, PersistManager
from core.optimize_weight import future_optimal_weight_lp_cvxpy
from core.signal_sender import StrategySignalSender
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
        # 发送 Markdown 消息
        title = f"策略 {self.stg_name} 已启动"
        markdown_text = f"1. 请注意：允许在读不到旧仓位时初始化持仓为0的参数`allow_init_pre_pos`的设定值为 `{allow_init_pre_pos}`。"
    
        self.ding.send_markdown(title=title, markdown_text=markdown_text, msg_type='info')
        
    def _load_exchange_info_detail(self):
        self.exchange_info_dir = Path(self.path_config['exchange_info'])
        exchange = self.params['exchange']
        self.exchange = globals()[exchange]

    def _init_task_scheduler(self):
        self.task_scheduler = TaskScheduler(log=self.log, repo=self.ding)
        
    def _init_db_module(self):
        mysql_name = self.params['mysql_name']
        model_name = self.params['model_name']

        self.factor_reader = FactorReader(mysql_name, log=self.log)
        self.predict_reader = ModelPredictReader(mysql_name, model_name, log=self.log)
        self.pos_sender = PositionSender(mysql_name, self.stg_name, log=self.log)
        self.pos_reader = PositionReader(mysql_name, self.stg_name, log=self.log)
        
    def _init_cache(self):
        cache_list = ['curr_price', 'twap_price', 'pos_his', 'twap_profit', 'fee', 'period_pnl']
        self.cache_mgr = CacheManager(self.cache_dir, cache_lookback=self.cache_lookback, 
                                      cache_list=cache_list, log=self.log)
        
    def _init_persist(self):
        persist_list = ['alpha', 'pos_his', 'twap_profit', 'fee', 'period_pnl']
        self.persist_mgr = PersistManager(self.persist_dir, persist_list=persist_list, log=self.log)
        
    def _init_signal_sender(self):
        zmq_address = self.params['zmq']['address']
        
        self.signal_sender = StrategySignalSender(zmq_address)
        
    def _add_tasks(self):
        pos_update_interval = self.params['pos_update_interval']
        
        unit = list(pos_update_interval.keys())[0]
        interval = list(pos_update_interval.values())[0]
        
        self.task_scheduler.add_task('30 Minute Pos Update', unit, interval, self.update_once)
        # self.task_scheduler.add_task('Daily Pnl', unit, interval, self.calc_daily_pnl)
        self.task_scheduler.add_task('Daily Pnl', 'specific_time', ['00:00'], self.calc_daily_pnl)
        self.task_scheduler.add_task('Reload Exchange Info', 'specific_time', ['00:05'], 
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
        exchange_info = load_binance_data(self.exchange, self.exchange_info_dir)
        trading_symbols = [symbol_info['symbol'].lower() for symbol_info in exchange_info['symbols']
                           if symbol_info['status'] == 'TRADING' and symbol_info['symbol'].endswith('USDT')]
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
            predict_value_matrix = self._fetch_predictions(ts)
            w0 = self._fetch_pre_pos()
            if w0 is None:
                return
            if predict_value_matrix is not None:
                # self._log_po_start()
                alpha = self._get_alpha(predict_value_matrix)
                mm_t = self._get_momentum_at_t(ts)
                his_pft_t = self._get_his_profit_at_t(ts)
                w1 = self._po_on_tradable(w0, alpha, mm_t, his_pft_t)
            else:
                w1 = w0
                alpha = None
                self._repo_model_error()
            new_pos = self._update_positions_on_universal_set(w0, w1)
            self._send_pos_to_zmq(new_pos, ts)
            self._send_pos_to_db(new_pos)
            # pft_till_t, fee_till_t = self._calc_profit_t_1_till_t(ts)
            # self._report_new_pos(new_pos)
            period_pnl = self._record_n_repo_pos_diff_and_pnl(new_pos, w0, pft_till_t, fee_till_t)
            self._save_to_cache(ts, new_pos, pft_till_t, fee_till_t, period_pnl)
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
                
        if w0 is not None:
            w0 = filter_series(w0, min_abs_value=min_pos)
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
    
    def _get_momentum_at_t(self, ts):
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
                       ).replace([np.inf, -np.inf], np.nan).reindex(self.trading_symbols).fillna(0.0).values
            mm_t[mm_wd] = mm_wd_t
        return mm_t
    
    def _get_his_profit_at_t(self, ts):
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
            pf_wd_t = twap_profit_since.sum(axis=0).reindex(self.trading_symbols).values
            his_pft_t[pf_wd] = pf_wd_t
        return his_pft_t
    
    def _po_on_tradable(self, w0, alpha, mm_t, his_pft_t): # 对可交易部分组合优化
        optimizer_params = self.params['optimizer_params']
        to_rate_thresh_L0 = optimizer_params['to_rate_thresh_L0']
        to_rate_thresh_L1 = optimizer_params['to_rate_thresh_L1']
        min_pos = self.params['min_pos']
        
        reindexed_w0 = w0.reindex(alpha.index, fill_value=0)
        w1, status = self.opt_func(alpha, reindexed_w0, mm_t, his_pft_t, to_rate_thresh_L0)
        if status != "optimal":
            msg = f'Could not find optimal result under to_rate: {to_rate_thresh_L0}, try {to_rate_thresh_L1}.'
            self.log.warning(msg)
            self.ding.send_text(msg, msg_type='warning')
            w1, status = self.opt_func(alpha, reindexed_w0, mm_t, his_pft_t, to_rate_thresh_L1)
            if status != "optimal":
                msg = (f'Still could not find optimal result under to_rate: {to_rate_thresh_L1}, '
                       'remain previous pos.')
                self.log.warning(msg)
                self.log.warning(f'alpha: {alpha}')
                self.log.warning(f'reindexed_w0: {reindexed_w0}')
                self.log.warning(f'mm_t: {mm_t}')
                self.log.warning(f'his_pft_t: {his_pft_t}')
                self.ding.send_text(msg, msg_type='warning')
        w1 = filter_series(w1, min_abs_value=min_pos, remove=False)
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
    
    def _send_pos_to_zmq(self, new_pos, ts):
        zmq_params = self.params['zmq']
        strategy_name = zmq_params['strategy_name']
        exchange = zmq_params['exchange']
        symbol_type = zmq_params['symbol_type']
        capital = self.params['capital']

        ma_price_reindexed = self._get_ma_price(new_pos, ts)
        new_pos_in_coin = (new_pos / ma_price_reindexed * capital).replace([np.nan, np.inf, -np.inf], 0)
        
        for symbol, pos in new_pos_in_coin.items():
            symbol_upper = symbol.upper()  # 转为大写
            self.signal_sender.send_message(strategy_name, exchange, symbol_type, symbol_upper, str(pos))
            
    def _get_ma_price(self, new_pos, ts):
        sp = self.params['sp']
        ma_price_params = self.params['ma_price']
        ma_wd = ma_price_params['ma_wd']
        check_thres = ma_price_params['check_thres']
        
        interval = timedelta(seconds=parse_time_string(sp))
        start_ma_t = ts - interval * ma_wd
        curr_price = self.cache_mgr['curr_price']
        ts_price = curr_price.loc[ts]
        ma_price = curr_price.loc[start_ma_t:].mean(axis=0)
        
        px_chg = (ts_price - ma_price) / ma_price
        abnormal = px_chg.abs() > check_thres
        px_chg_abn = px_chg[abnormal]
        if len(px_chg_abn) > 0:
            msg = f'可能异常的价格波动: {px_chg_abn}'
            self.log.warning(msg)
            self.ding.send_markdown("异常价格波动告警，请及时查看是否为真实价格，否则可能导致下单数量错误", 
                                    msg, msg_type='warning')
        
        return ma_price.loc[ts].reindex(new_pos.index)
        
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
        reindexed_w0 = w0.reindex(new_pos.index)
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
        
    def _save_to_cache(self, ts, new_pos, pft_till_t, fee_till_t, period_pnl):
        sp = self.params['sp']

        interval = timedelta(seconds=parse_time_string(sp))
        pre_t_1 = ts - interval
        
        if new_pos is not None:
            self.cache_mgr.add_row('pos_his', new_pos, ts)
        if pft_till_t is not None:
            self.cache_mgr.add_row('twap_profit', pft_till_t, pre_t_1)
        else:
            self.log.warning(f'Failed to calc profit during {pre_t_1} - {ts}. Skip saving cache: twap_profit.')
        if fee_till_t is not None:
            self.cache_mgr.add_row('fee', fee_till_t, pre_t_1)
        else:
            self.log.warning(f'Failed to calc fee during {pre_t_1} - {ts}. Skip saving cache: fee.')
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
    
