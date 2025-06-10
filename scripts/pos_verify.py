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
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import timedelta
import time
import toml
import signal
import traceback
from functools import partial
from tqdm import tqdm


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[1]
sys.path.append(str(project_dir))


# %%
from core.database_handler import FactorReader, ModelPredictReader, PositionSender, PositionReader
from core.task_scheduler import TaskScheduler
from core.cache_persist_manager import CacheManager, PersistManager
from core.optimize_weight import future_optimal_weight_lp_cvxpy
from utility.dirutils import load_path_config
from utility.logutils import FishStyleLogger
from utility.market import usd, load_binance_data
from utility.ding import DingReporter, df_to_markdown
from utility.timeutils import parse_time_string
from utility.calc import calc_profit_before_next_t
from utility.datautils import filter_series
from data_processor.data_checker import (FactorDataChecker, ModelPredictionChecker, 
                                         fetch_n_check, fetch_n_check_once, fetch_n_return_once)
from data_processor.feature_engineering import calculate_rank, calculate_weight_from_rank


from utility.datautils import align_columns_with_main


# %%
class PosUpdater:
    
    def __init__(self, stg_name):
        self.stg_name = stg_name
        self._load_path_config()
        self._init_dir()
        self._load_data()
        self._init_log()
        self._load_params()
        self._format_params()
        self._init_window_mapping()
        self._init_opt_func()
        self._load_exchange_info_detail()
        self._init_cache()
        self._init_persist()
        self._set_up_signal_handler()
        
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
        
        self.processed_data_dir = Path(path_config['factor_data'])
        self.result_dir = Path(path_config['result']) / 'model'
        self.twap_data_dir = Path(path_config['twap_price'])
        
    def _load_data(self):
        test_name = 'merge_agg_240919_double3m_15d'
        
        curr_px_path = self.twap_data_dir / 'curr_price_sp30.parquet'
        curr_price = pd.read_parquet(curr_px_path)
        close_price = curr_price.shift(-1)
        main_columns = curr_price.columns
        # to_mask = curr_price.isna()

        twap_path = self.twap_data_dir / 'twd30_sp30.parquet'
        twap_price = pd.read_parquet(twap_path)
        twap_price = align_columns_with_main(main_columns, twap_price)
        
        ## 主要为了对齐mask
        to_mask = None
        rtn_c2c = (close_price / close_price.shift(1) - 1).replace([np.inf, -np.inf], np.nan) #.fillna(0.0)
        rtn_cw0 = (twap_price / close_price.shift(1) - 1).replace([np.inf, -np.inf], np.nan) #.fillna(0.0)
        rtn_cw1 = (close_price / twap_price - 1).replace([np.inf, -np.inf], np.nan) #.fillna(0.0)
        # breakpoint()
        if to_mask is None:
            to_mask = rtn_c2c.isna() | rtn_cw0.isna() | rtn_cw1.isna()
        else:
            to_mask = to_mask | rtn_c2c.isna() | rtn_cw0.isna() | rtn_cw1.isna()

        predict_dir = self.result_dir / test_name / 'predict'
        try:
            predict_res_path = predict_dir / f'predict_{test_name}.parquet'
            predict_res = pd.read_parquet(predict_res_path)
        except:
            predict_res_path = predict_dir / 'predict.parquet'
            predict_res = pd.read_parquet(predict_res_path)
        predict_res = align_columns_with_main(main_columns, predict_res)
        to_mask = to_mask | predict_res.isna()
        predict_res = predict_res.mask(to_mask)
        
        # file_path = 'D:/crypto/multi_factor/factor_test_by_alpha/results/model/merge_agg_240919_double3m_15d/backtest/to_00125_maxmulti_25_mm_03_pf_001/pos_merge_agg_240919_double3m_15d__to_00125_maxmulti_25_mm_03_pf_001.csv'
        backtest_name = 'to_00125_maxmulti_25_mm_03_pf_001_240701'
        file_path = f'D:/crypto/multi_factor/factor_test_by_alpha/results/model/merge_agg_240919_double3m_15d/backtest/{backtest_name}/pos_merge_agg_240919_double3m_15d__{backtest_name}.csv'
        backtest_pos = pd.read_csv(file_path, index_col=0, parse_dates=True)
        
        file_path = f'D:/crypto/multi_factor/factor_test_by_alpha/results/model/merge_agg_240919_double3m_15d/backtest/{backtest_name}/profit_merge_agg_240919_double3m_15d__{backtest_name}.csv'
        profit = pd.read_csv(file_path, index_col=0, parse_dates=True)
        
        import pickle
        name = 'data_230721_1130'
        file_path = f'D:/crypto/multi_factor/factor_test_by_alpha/results/model/merge_agg_240919_double3m_15d/backtest/{backtest_name}/{name}.pkl'
        with open(file_path, 'rb') as f:
            self.idx1 = pickle.load(f)
        
        self.curr_price = curr_price
        self.twap_price = twap_price
        self.predict_res = predict_res
        self.backtest_pos = backtest_pos
        self.profit = profit
        self.if_to_trade = ~to_mask
        
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

    def _load_exchange_info_detail(self):
        self.exchange_info_dir = Path(self.path_config['exchange_info'])
        exchange = self.params['exchange']
        self.exchange = globals()[exchange]

    def _init_cache(self):
        cache_list = ['curr_price', 'twap_price', 'pos_his', 'twap_profit']
        self.cache_mgr = CacheManager(self.cache_dir, cache_lookback=self.cache_lookback, 
                                      cache_list=cache_list)
        
    def _init_persist(self):
        persist_list = ['alpha', 'pos_his', 'twap_profit']
        self.persist_mgr = PersistManager(self.persist_dir, persist_list=persist_list)

    def _set_up_signal_handler(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGSEGV, self._signal_handler)
        signal.signal(signal.SIGILL, self._signal_handler)
        
    def _signal_handler(self, sig, frame):
        signal_name = signal.Signals(sig).name  # 获取信号的名称
        self.log.warning(f"收到终止信号: {signal_name}，正在清理资源...")
        # self.ding.send_text(f"策略 {self.stg_name} 收到终止信号： {signal_name}，已停止")
        # self.stop()
        # time.sleep(15) # HINT: 设置一定的等待时间，等待task schedule执行完毕后终止
        os._exit(0)
        
    def run(self):
        self.record = pd.DataFrame()
        for ts in tqdm(list(self.curr_price.loc['20230620':].index), desc='realtime'):
            self.update_once(ts)
            # realtime_pos, predict_value_matrix, tradable = self.update_once(ts)
            # backtest_pos = self.backtest_pos.loc[ts]
            # ## check
            # realtime_pos = realtime_pos.reindex_like(backtest_pos).fillna(0)
            # diff = realtime_pos - backtest_pos
            # if diff.le(1e-4).mean() != 1:
            #     breakpoint()

    def update_once(self, ts):
        # try:
        tradable = self.if_to_trade.columns[self.if_to_trade.loc[ts]]
        self._fetch_factors(ts)
        predict_value_matrix = self._fetch_predictions(ts, tradable)
        w0 = self._fetch_pre_pos(ts)
        if predict_value_matrix is not None and w0 is not None:
            self._log_po_start()
            alpha = self._get_alpha(predict_value_matrix)
            mm_t = self._get_momentum_at_t(ts, tradable)
            pft_till_t = self._calc_profit_t_1_till_t(ts)
            his_pft_t = self._get_his_profit_at_t(ts, tradable)
            w1 = self._po_on_tradable(w0, alpha, mm_t, his_pft_t)
            new_pos = self._update_positions_on_universal_set(w0, w1)
            # self._send_pos_to_db(new_pos)
            # self._report_new_pos(new_pos)
            self._save_to_cache(ts, new_pos, pft_till_t)
            self._save_to_persist(ts, alpha, new_pos, pft_till_t)
            
            ## check
            backtest_pos = self.backtest_pos.loc[ts]
            ## check
            realtime_pos = new_pos.reindex_like(backtest_pos).fillna(0)
            diff = realtime_pos - backtest_pos
            try:
                self.record.loc[ts, 'pos_diff'] = 1 - diff.abs().sum() / 2
                self.record.loc[ts, 'accuracy_rate'] = diff.le(1e-4).mean()
            except:
                breakpoint()
            if diff.le(1e-4).mean() != 1:
                breakpoint()
                idx1 = self.idx1
                bt_if_trade = idx1['if_to_trade']
                ## check mm
                realtime_mm = mm_t
                backtest_mm = {wd: mmt[bt_if_trade] for wd, mmt in idx1['mm'].items()}
                for bt_wd, rt_wd in zip([11, 22, 33, 44], ['44h', '88h', '132h', '176h']):
                    print('mm', rt_wd, all(backtest_mm[bt_wd] == realtime_mm[rt_wd]))
                ## check pf
                twap_profit = self.cache_mgr['twap_profit']
                rt_pf = his_pft_t
                bt_pf = {wd: pf[bt_if_trade] for wd, pf in idx1['pf'].items()}
                for bt_wd, rt_wd in zip([1, 6, 18, 30], ['4h', '1d', '3d', '5d']):
                    if_same = all(np.isclose(bt_pf[bt_wd].astype(float), rt_pf[rt_wd].astype(float), atol=1e-5))
                    print('pf', rt_wd, if_same)
                    if bt_wd == 1 and not if_same:
                        bt_pf1 = bt_pf[1]
                        rt_pf1 = rt_pf['4h']
                        diff_pf1 = bt_pf1 - rt_pf1
                    if bt_wd == 6 and not if_same:
                        bt_pf6 = bt_pf[6]
                        rt_pf6 = rt_pf['1d']
                        diff_pf6 = bt_pf6 - rt_pf6
                ## check alpha
                bt_alpha = idx1['alpha'][bt_if_trade]
                if_alpha_eq = all(np.isclose(alpha.astype(float), bt_alpha.astype(float), atol=1e-5))
                print('alpha', if_alpha_eq)
                if not if_alpha_eq:
                    diff_alpha = alpha - bt_alpha
                ## check w0
                bt_w0 = pd.Series(idx1['w0'][bt_if_trade], index=tradable)
                reindexed_w0 = w0.reindex(alpha.index, fill_value=0)
                if_eq = all(np.isclose(reindexed_w0.astype(float), bt_w0.astype(float), atol=1e-5))
                print('w0', if_eq)
                if not if_eq:
                    diff_w0 = bt_w0 - reindexed_w0
                    
                ## check replace
                mm_names = dict(zip([11, 22, 33, 44], ['44h', '88h', '132h', '176h']))
                renamed_mm = {mm_names[k]: v.values.astype(np.float64) for k, v in backtest_mm.items()}
                pf_names = dict(zip([1, 6, 18, 30], ['4h', '1d', '3d', '5d']))
                renamed_pf = {pf_names[k]: v.values.astype(np.float64) for k, v in bt_pf.items()}
                try:
                    w1_rp = self._po_on_tradable(bt_w0, bt_alpha, renamed_mm, renamed_pf)
                except:
                    traceback.print_exc()
                new_pos_rp = self._update_positions_on_universal_set(bt_w0, w1_rp)
                realtime_pos_rp = new_pos_rp.reindex_like(backtest_pos).fillna(0)
                diff_rp = realtime_pos_rp - backtest_pos
                print('diff_rp', diff_rp.le(1e-4).mean())
            
        # except:
        #     self.log.exception('update error')
            # error_msg = traceback.format_exc()  # 获取完整的异常信息
            # self.ding.send_markdown('UPDATE ERROR', error_msg, msg_type='error')
        
    def _fetch_factors(self, ts):
        assist_factors_params = self.params['assist_factors_params']
        factors = assist_factors_params['factors']
        factors_in_tuple = assist_factors_params['factors_in_tuple']
        delay_fetch = assist_factors_params['delay_fetch']
        
        for factor, factor_in_tuple, delay in list(zip(factors, factors_in_tuple, delay_fetch)):

            delay_in_dt = self.delay_mapping[delay]
            ts_to_check = ts - delay_in_dt

            cache_name = 'curr_price' if factor['factor'].startswith('curr_price') else 'twap_price'
            
            if cache_name == 'curr_price':
                data = self.curr_price.loc[ts_to_check]
            else:
                data = self.twap_price.loc[ts_to_check]
                
            self.cache_mgr.add_row(cache_name, data, ts_to_check)
    
    def _fetch_predictions(self, ts, tradable):
        try:
            predict = self.predict_res.loc[ts].reindex(tradable)
        except:
            predict = None
        return predict
    
    def _fetch_pre_pos(self, ts):
        allow_init_pre_pos = self.params['allow_init_pre_pos']
        min_pos = self.params['min_pos']
        sp = self.params['sp']

        interval = timedelta(seconds=parse_time_string(sp))
        pre_t_1 = ts - interval
        
        try:
            w0 = self.cache_mgr['pos_his'].loc[pre_t_1]
        except:
            w0 = None
            
        if w0 is None:
            if allow_init_pre_pos:
                try:
                    w0 = self.backtest_pos.loc[pre_t_1]
                    self.cache_mgr.add_row('pos_his', w0, pre_t_1)
                except:
                    # self.log.warning('Failed to Fetch pre_pos!')
                    return None
                
# =============================================================================
#         if w0 is not None:
#             w0_old = w0
#             w0 = filter_series(w0, min_abs_value=min_pos)
#             if w0 != w0_old:
#             # if len(w0) != len(w0_old):
#                 breakpoint()
# =============================================================================
        return w0
    
    def _log_po_start(self):
        msg = 'Successfully fetched factors, prediction and pre pos. Start portfolio optimization...'
        # self.log.success(msg)
        # self.ding.send_text(msg, msg_type='success')
            
    def _get_alpha(self, predict_value_matrix):
        predict_res = predict_value_matrix
        predict_rank = calculate_rank(predict_res)
        alpha = calculate_weight_from_rank(predict_rank)
        return alpha # TODO: to persist
    
    def _get_momentum_at_t(self, ts, tradable):
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
                    missing_ts.append(ts)
                # self.log.warning(f'Timestamp: {missing_ts} is not in curr_price. Skip momentum calc for {mm_wd}.')
                continue
            mm_wd_t = (curr_price.loc[ts] / curr_price.loc[pre_t] -1
                       ).replace([np.inf, -np.inf], np.nan).reindex(tradable).fillna(0.0).values
            mm_t[mm_wd] = mm_wd_t
        return mm_t
    
    def _get_his_profit_at_t(self, ts, tradable):
        optimizer_params = self.params['optimizer_params']
        pf_limits = optimizer_params['pf_limits']
        
        twap_profit = self.cache_mgr['twap_profit']
        
        his_pft_t = {}
        for pf_wd in pf_limits:
            pf_wd_real = self.pf_limits_mapping[pf_wd]
            pre_t = ts - pf_wd_real
            twap_profit_since = twap_profit.loc[pre_t:]
            if len(twap_profit_since) == 0:
                # self.log.warning(f'Found no profit record since {pre_t}. Skip his profit calc for {pf_wd}.')
                his_pft_t[pf_wd] = np.zeros_like(tradable)
                continue
            pf_wd_t = twap_profit_since.sum(axis=0).reindex(tradable).values
            his_pft_t[pf_wd] = pf_wd_t
        return his_pft_t
    
    def _po_on_tradable(self, w0, alpha, mm_t, his_pft_t): # 对可交易部分组合优化
        optimizer_params = self.params['optimizer_params']
        to_rate_thresh_L0 = optimizer_params['to_rate_thresh_L0']
        to_rate_thresh_L1 = optimizer_params['to_rate_thresh_L1']
        min_pos = self.params['min_pos']
        
        reindexed_w0 = w0.reindex(alpha.index, fill_value=0)
        # print(alpha, reindexed_w0, mm_t, his_pft_t)
        w1, status = self.opt_func(alpha, reindexed_w0, mm_t, his_pft_t, to_rate_thresh_L0)
        if status != "optimal":
            msg = f'Could not find optimal result under to_rate: {to_rate_thresh_L0}, try {to_rate_thresh_L1}.'
            self.log.warning(msg)
            w1, status = self.opt_func(alpha, reindexed_w0, mm_t, his_pft_t, to_rate_thresh_L1)
            if status != "optimal":
                msg = (f'Still could not find optimal result under to_rate: {to_rate_thresh_L1}, '
                       'maintain previous pos.')
                self.log.warning(msg)
        # w1 = filter_series(w1, min_abs_value=min_pos, remove=False)
        return w1
    
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

    def _calc_profit_t_1_till_t(self, ts): # 计算t-1至t的收益
        sp = self.params['sp']

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
                # self.log.warning(f"KeyError: {index} not found in {cache_name}. Skip calc_profit.")
                return None  # 如果有 KeyError，直接返回 None 或者抛出异常
            
        for key in extracted_data:
            extracted_data[key] = extracted_data[key].reindex(extracted_data['w_t_1'].index)
    
        # 直接在后续计算中使用 extracted_data 的值
        rtn_c2c = extracted_data['close_price_t_1'] / extracted_data['close_price_t_2'] - 1
        rtn_cw0 = extracted_data['twap_price_t_1'] / extracted_data['close_price_t_2'] - 1
        rtn_cw1 = extracted_data['close_price_t_1'] / extracted_data['twap_price_t_1'] - 1
        
        # 计算最终的收益
        try:
            pft_till_t = calc_profit_before_next_t(extracted_data['w_t_2'], 
                                                   extracted_data['w_t_1'], 
                                                   rtn_c2c, rtn_cw0, rtn_cw1)
        except:
            breakpoint()
        if pft_till_t is not None:
            self.cache_mgr.add_row('twap_profit', pft_till_t, pre_t_1)
        return pft_till_t # TODO: to cache (index: t-1) & to persist (index: t)

    def _save_to_cache(self, ts, new_pos, pft_till_t):
        sp = self.params['sp']

        interval = timedelta(seconds=parse_time_string(sp))
        pre_t_1 = ts - interval
        
        if new_pos is not None:
            self.cache_mgr.add_row('pos_his', new_pos, ts)
# =============================================================================
#         if pft_till_t is not None:
#             self.cache_mgr.add_row('twap_profit', pft_till_t, pre_t_1)
# =============================================================================
        else:
            pass
            # self.log.warning(f'Failed to calc profit during {pre_t_1} - {ts}. Skip saving cache: twap_profit.')
        # self.cache_mgr.save(ts)
        
    def _save_to_persist(self, ts, alpha, new_pos, pft_till_t):
        if alpha is not None:
            self.persist_mgr.add_row('alpha', alpha, ts)
        if new_pos is not None:
            self.persist_mgr.add_row('pos_his', new_pos, ts)
        if pft_till_t is not None:
            self.persist_mgr.add_row('twap_profit', pft_till_t, ts)
        else:
            pass
            # self.log.warning(f'Failed to calc profit till {ts}. Skip saving persist: twap_profit.')
        self.persist_mgr.save(ts)
        
        
# %%
stg_name = 'agg_240919_to_00125_v0_test'
pos_updater = PosUpdater(stg_name)
pos_updater.run()
        