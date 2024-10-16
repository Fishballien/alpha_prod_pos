# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 14:53:12 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %%
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import time


from utility.logutils import FishStyleLogger


# %%
class DataChecker(ABC):
    
    def __init__(self, symbols, columns, ts, time_threshold=timedelta(minutes=30), 
                 symbol_threshold=0.9, factor_threshold=0.9, valid_rate=0.9, 
                 verbose_symbol_thresh=20, verbose_factor_thresh=20, verbose_pair_thresh=20,
                 log=None, repo=None):
        """
        :param symbols: 排序好的 symbol 列表
        :param columns: 包含fetched data中可能出现的多个字段信息组合
        :param ts: 当前时间 (datetime 格式)
        :param time_threshold: 时间差的默认阈值 (timedelta 格式，默认 30 分钟)
        :param symbol_threshold: symbol 完整率的阈值
        :param factor_threshold: factor 完整率的阈值
        :param log: 日志对象，用于打印日志信息
        :param repo: repo 对象，用于发送 markdown 信息
        """
        self.symbols = symbols
        self.columns = [tuple(col) if isinstance(col, (np.record, list)) else col for col in columns]
        self.ts = ts
        self.time_threshold = time_threshold
        self.symbol_threshold = symbol_threshold
        self.factor_threshold = factor_threshold
        self.valid_rate = valid_rate
        self.verbose_symbol_thresh = verbose_symbol_thresh
        self.verbose_factor_thresh = verbose_factor_thresh
        self.verbose_pair_thresh = verbose_pair_thresh
        self.log = log or FishStyleLogger()  # 使用自定义日志类 FishStyleLogger
        self.repo = repo
        
        # 存储完整率结果
        self.symbol_completeness = None
        self.factor_completeness = None
        self.missing_pairs = None

    def check_once(self, fetched_data, verbose=False):
        """
        处理 fetched_data 并执行完整性检查。
        如果所有 factor 的数据完整性高于 factor_threshold，则生成 factor value 矩阵并转化为 np.array。
        :param fetched_data: 查询到的数据列表
        :return: np.array 类型的 factor value 矩阵，如果不满足完整性要求则返回 None。
        """
        # 执行检查并保存完整率结果
        if_valid = self._check(fetched_data, verbose)

        # 如果数据完整性满足要求，生成 factor value 矩阵
        if if_valid:
            factor_value_matrix = self._create_factor_value_matrix(fetched_data)
            return factor_value_matrix
        return None

    def _check(self, fetched_data, verbose):
        """
        处理 fetched_data，并执行完整性检查。
        返回布尔值，表示是否所有 factor 的数据完整性高于 factor_threshold。
        """
        # 创建数据完整率和时间戳检查矩阵
        data_matrix, time_matrix = self._create_matrices(fetched_data)

        # 检查 symbol（行）的完整率
        self.symbol_completeness = self._calculate_symbol_completeness(data_matrix, time_matrix)

        # 检查 factor（列）的完整率
        self.factor_completeness = self._calculate_factor_completeness(data_matrix, time_matrix)

        # 检查单独的 symbol-factor pair 是否有缺失
        self.missing_pairs = self._check_individual_missing(data_matrix, time_matrix)

        # 判断是否所有 factor 的 data 和 time 完整性都高于阈值
        valid_or_not = [self.factor_completeness[factor]['data'] >= self.factor_threshold * 100 and 
                       self.factor_completeness[factor]['time'] >= self.factor_threshold * 100
                       for factor in self.factor_completeness]
        return np.mean(valid_or_not) >= self.valid_rate

    def log_and_report(self, verbose):
        """
        统一控制日志打印和 repo 消息发送的函数
        """
        if self.symbol_completeness is None or self.factor_completeness is None or self.missing_pairs is None:
            self.log.warning("Please run check_once before calling log_and_report.")
            return
        
        # 打印低于阈值的 symbol
        if self.verbose_symbol_thresh:
            self._log_low_completeness('Symbols with low completeness', self.symbol_completeness, 
                                       self.symbol_threshold, verbose, self.verbose_symbol_thresh)
        
        # 打印低于阈值的 factor
        if self.verbose_factor_thresh:
            self._log_low_completeness('Factor combinations with low completeness', self.factor_completeness, 
                                       self.factor_threshold, verbose, self.verbose_factor_thresh)
            
        # 打印单独缺失的 symbol-factor 对
        if self.missing_pairs and self.verbose_pair_thresh:
            title_str = "Individual missing symbol-factor pairs".upper()
            if len(self.missing_pairs) < self.verbose_pair_thresh:
                content_str = "\n".join([f"{symbol} - {factor}" for symbol, factor in self.missing_pairs])
                self._log_and_send(title_str, content_str)

    def _log_low_completeness(self, title, completeness_dict, completeness_threshold, verbose, threshold=20):
        """
        处理低完整率的 symbol/factor 的日志和报警。
        :param title: 标题
        :param completeness_dict: 完整率字典
        :param verbose: 是否发送 repo 报告
        :param threshold: 完整率检查对象的数量阈值，默认为 20
        """
        low_completeness_items = {k: v for k, v in completeness_dict.items() 
                                  if v['data'] < completeness_threshold * 100 
                                  or v['time'] < completeness_threshold * 100}
    
        if low_completeness_items:
            title_str = title.upper()
            if len(low_completeness_items) >= threshold:
                content_str = f"超过 {threshold} 个检查对象低完整率，数量为 {len(low_completeness_items)}"
            else:
                content_str = "\n".join([f"{item}: data={v['data']}%, time={v['time']}%" 
                                         for item, v in low_completeness_items.items()])
            self._log_and_send(title_str, content_str, verbose)

    def _log_and_send(self, title_str, content_str, verbose):
        """
        统一日志打印和 repo 发送逻辑
        """
        if verbose >= 0:
            result_str = f'[{title_str}]\n{content_str}'
            self.log.warning(result_str.strip())
        if verbose >= 1 and self.repo:
            self.repo.send_markdown(title_str, content_str, msg_type='warning')
    
    @abstractmethod
    def _read_one_row(self, row):
        pass
            
    def _create_matrices(self, fetched_data):
        """
        创建两个矩阵：一个表示数据完整率，另一个表示时间戳检查。
        """
        data_matrix = pd.DataFrame(0, index=self.symbols, columns=self.columns)
        time_matrix = pd.DataFrame(0, index=self.symbols, columns=self.columns)

        for row in fetched_data:
            factor_combination, symbol, factor_value, timestamp = self._read_one_row(row)
            if factor_combination in self.columns:
                data_matrix.at[symbol, factor_combination] = 1
                time_diff = self.ts - timestamp
                if time_diff <= self.time_threshold:
                    time_matrix.at[symbol, factor_combination] = 1

        return data_matrix, time_matrix

    def _calculate_symbol_completeness(self, data_matrix, time_matrix):
        """
        计算 symbol 的数据和时间完整率。
        :param data_matrix: 数据完整率矩阵
        :param time_matrix: 时间戳检查矩阵
        :return: 包含完整率的字典
        """
        symbol_completeness = {}
        for symbol in data_matrix.index:
            data_completeness = data_matrix.loc[symbol].mean() * 100
            time_completeness = time_matrix.loc[symbol].mean() * 100
            symbol_completeness[symbol] = {'data': data_completeness, 'time': time_completeness}
        return symbol_completeness

    def _calculate_factor_completeness(self, data_matrix, time_matrix):
        """
        计算每个因子的完整率。
        :param data_matrix: 数据完整率矩阵
        :param time_matrix: 时间戳检查矩阵
        :return: 包含完整率的字典
        """
        factor_completeness = {}
        for factor in data_matrix.columns:
            data_completeness = data_matrix[factor].mean() * 100
            time_completeness = time_matrix[factor].mean() * 100
            factor_completeness[factor] = {'data': data_completeness, 'time': time_completeness}
        return factor_completeness

    def _check_individual_missing(self, data_matrix, time_matrix):
        """
        检查单独的 symbol-factor pair 是否有缺失。
        仅当 symbol 和 factor 的整体完整率均满足阈值时才打印零散缺失。
        :return: 缺失的 symbol-factor 对列表
        """
        missing_pairs = []
    
        for symbol in data_matrix.index:
            symbol_ok = self.symbol_completeness[symbol]['data'] >= self.symbol_threshold * 100 and \
                        self.symbol_completeness[symbol]['time'] >= self.symbol_threshold * 100
    
            if symbol_ok:
                for factor in data_matrix.columns:
                    factor_ok = self.factor_completeness[factor]['data'] >= self.factor_threshold * 100 and \
                                self.factor_completeness[factor]['time'] >= self.factor_threshold * 100
                    
                    if factor_ok and (data_matrix.at[symbol, factor] == 0 or time_matrix.at[symbol, factor] == 0):
                        missing_pairs.append((symbol, factor))
    
        return missing_pairs
    
    def _create_factor_value_matrix(self, fetched_data):
        """
        创建 factor value 的矩阵。
        :param fetched_data: 查询到的数据列表
        :return: 包含 factor value 的 DataFrame 矩阵
        """
        factor_value_matrix = pd.DataFrame(np.nan, index=self.symbols, columns=self.columns)

        for row in fetched_data:
            factor_combination, symbol, factor_value, timestamp = self._read_one_row(row)

            if factor_combination in self.columns:
                factor_value_matrix.at[symbol, factor_combination] = factor_value

        return factor_value_matrix
    

# %%
class FactorDataChecker(DataChecker):
    
    def _read_one_row(self, row):
        author, factor_category, factor_name, symbol, factor_value, timestamp = row
        factor_combination = (author, factor_category, factor_name)
        return factor_combination, symbol, factor_value, timestamp


class ModelPredictionChecker(DataChecker):
    
    def _read_one_row(self, row):
        model_name, symbol, factor_value, timestamp = row
        factor_combination = model_name
        return factor_combination, symbol, factor_value, timestamp
    
    
# %%
def fetch_n_check(fetch_n_return_if_valid_func, fetch_target_name, 
                  log, repo, max_attempts=5, sleep_seconds=5, verbose_interval=60):
    """
    通用的 fetch 并检查函数，支持动态休眠和重试。

    参数:
        fetch_n_return_if_valid_func (Callable): 用于获取并检查数据的函数，使用 partial 绑定所需参数。
        fetch_target_name (str): 获取目标的名称，用于日志记录和通知。
        log (Logger): 日志记录实例，用于输出日志信息。
        repo (Repo): 通知系统实例，用于发送通知消息。
        max_attempts (int): 最大重试次数，默认为 5 次。
        sleep_seconds (int): 每次重试之间的休眠时间，单位为秒，默认为 5 秒。
        verbose_interval (int): 控制 verbose 日志输出的间隔时间，单位为秒，默认为 60 秒。
        
    返回:
        fetch_res: 如果获取并验证成功，返回有效的数据结果；否则，在最大重试次数后返回 None。
    """
    attempts = 0

    while attempts < max_attempts:
        start_t = datetime.now()  # 记录开始时间
        verbose = attempts != 0 and attempts % (verbose_interval // sleep_seconds) == 0
        fetch_res = fetch_n_return_if_valid_func(verbose=verbose)

        if fetch_res is not None:
            return fetch_res  # 成功获取后直接返回

        attempts += 1
        log.info(f"Attempt {attempts} returned None. Retrying...")

        if verbose:
            repo.send_text(f"Fetching {fetch_target_name}: Attempt {attempts} returned None. Retrying...", 
                           msg_type='warning')

        end_t = datetime.now()
        elapsed_time = (end_t - start_t).total_seconds()
        time_to_sleep = max(0, sleep_seconds - elapsed_time)

        if attempts < max_attempts and time_to_sleep > 0:
            log.info(f"Sleeping for {time_to_sleep} seconds before retry...")
            time.sleep(time_to_sleep)

    log.warning("Max retry attempts reached. Returning None.")
    return None


def fetch_n_check_once(verbose, fetch_func, list_to_fetch, dc):
    """
    外置的 fetch 并验证数据的通用函数。

    参数:
        verbose (bool/int): 是否打印详细日志。
        fetch_func (Callable): 获取数据的函数（例如 factor_reader.fetch_batch_data）。
        list_to_fetch (list): 用于获取数据的条件列表。
        dc (DataChecker): 数据检查器实例。
        
    返回:
        fetch_res: 如果数据有效则返回结果，否则返回 None。
    """
    # 获取数据
    fetched_data = fetch_func(list_to_fetch)
    
    # 检查数据
    fetch_res = dc.check_once(fetched_data)
    
    # 如果检查结果有效，将 verbose 设置为 1
    if fetch_res is not None:
        verbose = 1
    
    # 记录日志并报告
    dc.log_and_report(verbose=verbose)
    
    return fetch_res


def fetch_n_return_once(verbose, fetch_func):
    """
    外置的 fetch 并验证数据的通用函数。

    参数:
        verbose (bool/int): 是否打印详细日志。
        fetch_func (Callable): 获取数据的函数（例如 factor_reader.fetch_batch_data）。
        
    返回:
        fetched_data: 如果数据有效则返回结果，否则返回 None。
    """
    # 获取数据
    fetched_data = fetch_func()

    return fetched_data
