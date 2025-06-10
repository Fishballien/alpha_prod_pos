# -*- coding: utf-8 -*-
"""
Created on Tue Oct  8 13:14:41 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %%
import sys
import pymysql
import yaml
import time
from pathlib import Path
import pandas as pd
from datetime import datetime, date, timedelta, timezone
from typing import List, Union, Optional, Dict, Any, Tuple


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[1]
sys.path.append(str(project_dir))


# %%
from utility.logutils import FishStyleLogger
from utility.dirutils import load_path_config


# %%
class DatabaseHandler:
    
    def __init__(self, mysql_name, log=None):
        self.mysql_name = mysql_name
        self.log = log
        
        self._init_logger()
        self._load_path_config()
        self._load_sql_config()
        
    def _init_logger(self):
        """初始化日志"""
        self.log = self.log or FishStyleLogger()
            
    def _load_path_config(self):
        """加载路径配置"""
        file_path = Path(__file__).resolve()
        project_dir = file_path.parents[1]
        path_config = load_path_config(project_dir)
        
        self.sql_config_dir = Path(path_config['sql_config'])
        
    def _load_sql_config(self):
        """加载 SQL 配置"""
        file_path = self.sql_config_dir / f'{self.mysql_name}.yaml'
        with open(file_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        self.mysql_info = config['mysql']
        self.max_retries = config['max_retries']
        self.retry_delay = config['retry_delay']
        
    def connect(self):
        """尝试建立数据库连接，最多重试 max_retries 次"""
        retries = 0
        connection = None
        while retries < self.max_retries:
            try:
                # 尝试建立数据库连接
                connection = pymysql.connect(**self.mysql_info)
                if connection.open:
                    return connection
            except pymysql.MySQLError as e:
                retries += 1
                self.log.warning(f"Connection attempt {retries} failed: {e}")
                if retries < self.max_retries:
                    self.log.warning(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.log.error("Max retries reached, failed to connect to the database")
                    raise e  # 超过重试次数，抛出异常
        return connection
    
    
# %% factor reader
class FactorReader(DatabaseHandler):
    
    def __init__(self, mysql_name, log=None):
        super().__init__(mysql_name, log=log)
    
    def fetch_data(self, author, factor_category, factor_name, symbol):
        """
        根据 (author, factor_category, factor_name, symbol) 查询 factor_value 和 data_ts
        """
        connection = None
        cursor = None
        try:
            # 建立数据库连接
            connection = self.connect()
            if not connection:
                return None  # 如果没有连接上，不继续执行查询

            # 执行查询操作
            cursor = connection.cursor()
            select_query = """
            SELECT factor_value, data_ts 
            FROM factors_update
            WHERE author = %s 
              AND factor_category = %s 
              AND factor_name = %s 
              AND symbol = %s
            """
            cursor.execute("SET time_zone = '+00:00';")  # 设置时区为 UTC，确保时间一致
            cursor.execute(select_query, (author, factor_category, factor_name, symbol))
            
            result = cursor.fetchone()  # 获取单条查询结果
            if result:
                return result  # 返回 (factor_value, data_ts)
            else:
                self.log.warning(f"No data found for ({author}, {factor_category}, {factor_name}, {symbol})")
                return None

        except pymysql.MySQLError as e:
            self.log.error(f"Error while fetching data: {e}")
            return None

        finally:
            # 关闭游标和连接
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def fetch_batch_data(self, criteria_list):
        """
        批量根据 (author, factor_category, factor_name, symbol) 查询 factor_value 和 data_ts
        :param criteria_list: 包含多个查询条件的列表，每个条件为 (author, factor_category, factor_name, symbol) 的元组
        :return: 返回查询结果的列表，每个元素为 (author, factor_category, factor_name, symbol, factor_value, data_ts)
        """
        connection = None
        cursor = None
        try:
            # 建立数据库连接
            connection = self.connect()
            if not connection:
                return None  # 如果没有连接上，不继续执行查询

            # 构建批量查询的 SQL 语句
            select_query = """
            SELECT author, factor_category, factor_name, symbol, factor_value, data_ts
            FROM factors_update
            WHERE (author, factor_category, factor_name, symbol) IN (%s)
            """
            
            # 将条件列表转换为查询参数的格式
            format_strings = ', '.join(['(%s, %s, %s, %s)'] * len(criteria_list))
            select_query = select_query % format_strings
            
            # 展开元组列表中的值并传递给 execute 方法
            flattened_values = [item for sublist in criteria_list for item in sublist]
            cursor = connection.cursor()
            cursor.execute("SET time_zone = '+00:00';")  # 设置时区为 UTC，确保时间一致
            cursor.execute(select_query, flattened_values)
            
            # 获取所有查询结果
            results = cursor.fetchall()
            
            self.log.success(f"Successfully fetched {len(results)} records from factor_update.")

            return results  # 返回一个包含查询结果的列表 (author, factor_category, factor_name, symbol, factor_value, data_ts)

        except pymysql.MySQLError as e:
            self.log.error(f"Error while fetching batch data: {e}")
            return None

        finally:
            # 关闭游标和连接
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    
# %% model predict reader
class ModelPredictReader(DatabaseHandler):
    
    def __init__(self, mysql_name, model_name, log=None):
        """
        初始化 ModelPredictReader，设置数据库连接和模型名称
        :param mysql_name: 数据库连接名称
        :param model_name: 该实例所使用的模型名称
        :param log: 日志对象（可选）
        """
        super().__init__(mysql_name, log=log)
        self.model_name = model_name  # 使用同一个 model_name

    def fetch_batch_data(self, symbol_list):
        """
        批量获取 symbol 列表中对应的 symbol, predict_value 和 data_ts。
        :param symbol_list: 需要查询的 symbol 列表
        :return: 包含 (model_name, symbol, predict_value, data_ts) 的结果列表
        """
        connection = None
        cursor = None
        result = []
        
        try:
            # 建立数据库连接
            connection = self.connect()
            if not connection:
                return result  # 无法连接数据库，停止执行
            
            # 构建 IN 子句，使用多个占位符
            in_clause = ','.join(['%s'] * len(symbol_list))

            # 准备批量查询的 SQL 语句，查询 model_name, symbol, predict_value 和 data_ts
            select_query = f"""
            SELECT model_name, symbol, predict_value, data_ts
            FROM model_predict
            WHERE model_name = %s
              AND symbol IN ({in_clause})
            """
            
            # 执行查询操作
            cursor = connection.cursor()
            cursor.execute(select_query, [self.model_name] + symbol_list)
            result = cursor.fetchall()  # 获取查询结果
            self.log.success(f"Successfully fetched {len(result)} records from model_predict.")
        
        except pymysql.MySQLError as e:
            self.log.error(f"Error while fetching predictions: {e}")
        
        finally:
            # 关闭游标和连接
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        
        return result
    
    
# %% pos sender
class PositionSender(DatabaseHandler):
    
    def __init__(self, mysql_name, stg_name, log=None):
        """
        初始化 PositionSender，设置数据库连接和策略名称
        :param mysql_name: 数据库连接名称
        :param stg_name: 策略名称
        :param log: 日志对象（可选）
        """
        super().__init__(mysql_name, log=log)
        self.stg_name = stg_name  # 初始化 stg_name
    
    def insert(self, pos_series):
        """
        批量插入 stg_name, symbol 和 pos 到 positions 表。
        :param pos_series: 一个 Pandas Series，index 为 symbol，value 为 pos
        """
        connection = None
        cursor = None
        
        try:
            # 建立数据库连接
            connection = self.connect()
            if not connection:
                return  # 无法连接数据库，停止执行
            
            # 准备批量插入的 SQL 语句
            insert_query = """
            INSERT INTO positions (stg_name, symbol, pos)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE pos = VALUES(pos), timestamp = CURRENT_TIMESTAMP;
            """
            
            # 将 stg_name、symbol 和 pos 组合成批量插入的数据
            data_to_insert = [(self.stg_name, symbol, pos) 
                              for symbol, pos in pos_series.items()]
            
            # 执行批量插入操作
            cursor = connection.cursor()
            cursor.executemany(insert_query, data_to_insert)
            connection.commit()  # 提交事务
            self.log.success(f"Successfully inserted {len(pos_series)} records into positions.")
        
        except pymysql.MySQLError as e:
            self.log.error(f"Error while inserting positions: {e}")
        
        finally:
            # 关闭游标和连接
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                
                
# %% pos reader
class PositionReader(DatabaseHandler):
    
    def __init__(self, mysql_name, stg_name, log=None):
        """
        初始化 PositionReader，设置数据库连接和策略名称
        :param mysql_name: 数据库连接名称
        :param stg_name: 策略名称
        :param log: 日志对象（可选）
        """
        super().__init__(mysql_name, log=log)
        self.stg_name = stg_name  # 初始化 stg_name
    
    def fetch_batch_data(self):
        """
        读取对应策略名称 (stg_name) 的仓位数据，并返回一个以 symbol 为索引，pos 为值的 Pandas Series。
        :return: Pandas Series 或 None
        """
        connection = None
        cursor = None
        try:
            # 建立数据库连接
            connection = self.connect()
            if not connection:
                return None  # 无法连接数据库，停止执行
            
            # 查询 SQL 语句
            select_query = """
            SELECT symbol, pos
            FROM positions
            WHERE stg_name = %s;
            """
            
            # 执行查询操作
            cursor = connection.cursor()
            cursor.execute(select_query, (self.stg_name,))
            result = cursor.fetchall()  # 获取所有结果
            
            if not result:
                return None  # 如果没有结果，返回 None
            
            # 将查询结果转换为 Pandas Series
            data = pd.Series({row[0]: row[1] for row in result})
            
            return data
        
        except pymysql.MySQLError as e:
            self.log.error(f"Error while fetching positions: {e}")
            return None
        
        finally:
            # 关闭游标和连接
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                
                
# %% check thunder
class StrategyUpdater(DatabaseHandler):
    """
    继承自 DatabaseHandler，在初始化时设置 period 和 strategy_name，
    并提供同时更新 strategy_bar_signal 和 strategy_csv_end 的方法。
    """

    def __init__(self, mysql_name, period, strategy_name, log=None):
        """
        :param mysql_name: 对应 {mysql_name}.yaml 数据库配置文件
        :param period: 策略周期(如 5, 15, 60 等)
        :param strategy_name: 账户/策略名
        :param log: 日志对象(可选)，不传则使用基类里默认的 FishStyleLogger
        """
        super().__init__(mysql_name, log=log)
        self.period = period
        self.strategy_name = strategy_name  # 相当于原来的 acname

    def update_signals_and_status(self, stockdate, symbol_pos_series):
        """
        同时更新 strategy_bar_signal (写入一组 symbol-pos 对) 和
        strategy_csv_end (更新该 strategy_name 对应的计算状态)。

        :param stockdate: bar 时间或本轮计算对应的数据时间(可以是 str 或 datetime)
        :param symbol_pos_series: pd.Series, index 为 symbol, value 为 pos
                                  例如:
                                     BTCUSDT    0.123
                                     ETHUSDT   -0.456
                                     dtype: float64
        """
        # 如果 stockdate 是 datetime 或 date，需要先统一成字符串
        if isinstance(stockdate, (datetime, date)):
            stockdate = (stockdate + timedelta(hours=8) - timedelta(minutes=self.period)).strftime('%Y-%m-%d %H:%M:%S')
        
        # 如果 symbol_pos_series 不是 Series，可以在此处做一个检查或转换
        if not isinstance(symbol_pos_series, pd.Series):
            raise TypeError("symbol_pos_series 必须是一个 Pandas Series，其index为symbol，value为pos。")

        # 去除空值(以免造成插入错误或插入NaN)
        symbol_pos_series = symbol_pos_series.dropna()
        
        # 数据库连接
        conn = self.connect()
        
        try:
            with conn.cursor() as cursor:
                # ================================
                # 1) 批量插入 strategy_bar_signal
                # ================================
                # cal_stockdate 通常为当前时间
                cal_stockdate = (datetime.now() + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')

                # 构建批量插入所需的数据
                # 格式: (acname, symbol, ps, bar_stockdate, cal_stockdate)
                insert_values = []
                for symbol, pos in symbol_pos_series.items():
                    insert_values.append((self.strategy_name, symbol, pos, stockdate, cal_stockdate))

                if insert_values:
                    sql_bar_signal = """
                    INSERT INTO strategy_bar_signal
                        (acname, symbol, ps, bar_stockdate, cal_stockdate)
                    VALUES
                        (%s, %s, %s, %s, %s)
                    """
                    cursor.executemany(sql_bar_signal, insert_values)
                    self.log.info(
                        f"[StrategyUpdater] 插入 strategy_bar_signal 条数: {len(insert_values)}"
                    )
                else:
                    self.log.warning("[StrategyUpdater] symbol_pos_series 为空，未插入 strategy_bar_signal")

                # ================================
                # 2) 插入/更新 strategy_csv_end
                # ================================
                # status 可以设定为 'end'，如果需要动态传参可以改成参数
                sql_csv_end = """
                INSERT INTO strategy_csv_end(acname, status, stockdate, period)
                VALUES(%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status    = VALUES(status),
                    stockdate = VALUES(stockdate),
                    period    = VALUES(period)
                """
                cursor.execute(sql_csv_end, (self.strategy_name, 'end', stockdate, self.period))
                self.log.info(
                    f"[StrategyUpdater] 更新 strategy_csv_end: "
                    f"acname={self.strategy_name}, stockdate={stockdate}, period={self.period}"
                )

                # 提交事务
                conn.commit()

        except pymysql.MySQLError as e:
            self.log.error(f"[StrategyUpdater] 数据库操作失败: {e}")
            raise
        finally:
            conn.close()
            self.log.info("[StrategyUpdater] 数据库连接已关闭")
            
            
# %%
class PremiumIndexReader(DatabaseHandler):
    """
    数据库读取类，专门用于从 premium_index 表读取数据
    """
    
    def __init__(self, mysql_name, log=None):
        """
        初始化 PremiumIndexReader 类
        
        Args:
            mysql_name: MySQL 配置文件名称
            log: 日志对象，如果未提供则创建新的日志对象
        """
        super().__init__(mysql_name, log=log)
    
    def get_funding_rates(self, symbols, timestamp, ts_thres_early=60000, ts_thres_late=0):
        """
        获取指定符号列表在特定时间戳附近的资金费率
        
        Args:
            symbols: 符号列表，例如 ['BTCUSDT', 'ethusdt']，不区分大小写
            timestamp: 目标时间戳（毫秒）或 datetime.datetime 对象
            ts_thres_early: 早于目标时间戳的阈值（毫秒），默认为 1 分钟
            ts_thres_late: 晚于目标时间戳的阈值（毫秒），默认为 0（不允许晚于目标时间戳）
            
        Returns:
            pandas.DataFrame: 包含符号、资金费率和时间戳的 DataFrame，索引为原始输入的 symbols
        """
        # 保存原始输入的符号映射关系
        original_symbols = {symbol.upper(): symbol for symbol in symbols}
        
        # 将所有符号转换为大写用于数据库查询
        upper_symbols = [symbol.upper() for symbol in symbols]
        
        # 将 datetime 对象转换为毫秒级时间戳
        if isinstance(timestamp, datetime):
            # 确保 datetime 对象带有时区信息，如果没有则假设为 UTC
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            timestamp = int(timestamp.timestamp() * 1000)
            self.log.info(f"Converted datetime to timestamp: {timestamp}")
        
        connection = None
        cursor = None
        try:
            # 建立数据库连接
            connection = self.connect()
            if not connection:
                self.log.error("Failed to establish database connection")
                return pd.Series()
    
            # 将符号列表格式化为 SQL IN 子句
            symbols_str = ', '.join(['%s'] * len(upper_symbols))
            
            # 准备查询
            # 查询条件：symbol 在列表中且时间戳在指定窗口内 [timestamp - ts_thres_early, timestamp + ts_thres_late]
            # 在这个窗口内寻找每个 symbol 的最新记录
            query = f"""
            SELECT t1.symbol, t1.last_funding_rate, t1.timestamp
            FROM premium_index t1
            INNER JOIN (
                SELECT symbol, MAX(timestamp) as max_ts
                FROM premium_index
                WHERE symbol IN ({symbols_str})
                  AND timestamp >= %s - {ts_thres_early}
                  AND timestamp <= %s + {ts_thres_late}
                GROUP BY symbol
            ) t2 ON t1.symbol = t2.symbol AND t1.timestamp = t2.max_ts
            """
            
            # 执行查询
            cursor = connection.cursor()
            params = upper_symbols + [timestamp, timestamp]
            cursor.execute(query, params)
            
            # 获取结果
            results = cursor.fetchall()
            
            # 构建返回的 DataFrame
            data = []
            for symbol, rate, ts in results:
                try:
                    # 尝试将字符串转换为浮点数
                    funding_rate = float(rate)
                except (ValueError, TypeError):
                    # 如果转换失败，记录警告并设置为 None
                    self.log.warning(f"Invalid funding rate value for {symbol}: {rate}")
                    funding_rate = None
                
                data.append({
                    'symbol': symbol,  # 这里是大写的符号
                    'funding_rate': funding_rate,
                    'timestamp': ts
                })
            
            # 创建 DataFrame
            df = pd.DataFrame(data)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # 检查是否所有请求的符号都有返回结果
            returned_symbols = set(df['symbol']) if not df.empty else set()
            missing_symbols = set(upper_symbols) - returned_symbols
            if missing_symbols:
                self.log.warning(f"No funding rate data found for symbols: {missing_symbols}")
            
            self.log.success(f"Successfully fetched funding rates for {len(df)} symbols")
            
            # 将 DataFrame 的索引转换为原始输入的符号大小写
            if not df.empty:
                # 创建映射以将大写符号转回原始大小写
                df['original_symbol'] = df['symbol'].map(original_symbols)
                df.set_index('original_symbol', inplace=True)
                # 可以选择删除原始的大写符号列，取决于你是否需要它
                # df.drop(columns=['symbol'], inplace=True)
            
            # 返回 DataFrame
            return df
            
        except pymysql.MySQLError as e:
            self.log.error(f"Error while fetching funding rates: {e}")
            return pd.Series()
            
        finally:
            # 关闭游标和连接
            if cursor:
                cursor.close()
            if connection:
                connection.close()


class FundingRateFetcher:
    """
    用于获取资金费率的类，支持多次尝试直到获取所有请求的符号数据
    """
    
    def __init__(self, 
                 reader: 'PremiumIndexReader', 
                 ts_thres_late: int = 15000, 
                 ts_thres_early: int = 60000,
                 max_attempts: int = 5, 
                 retry_interval: float = 1.0,
                 log=None):
        """
        初始化 FundingRateFetcher 类
        
        Args:
            reader: PremiumIndexReader 实例，用于从数据库获取资金费率
            ts_thres_late: 晚于目标时间戳的阈值（毫秒），默认为 15000
            ts_thres_early: 早于目标时间戳的阈值（毫秒），默认为 60000
            max_attempts: 最大尝试次数，默认为 5
            retry_interval: 每次尝试之间的间隔时间（秒），默认为 1.0
            log: 日志对象，如果未提供则使用传入的 reader 中的日志对象
        """
        self.reader = reader
        self.ts_thres_late = ts_thres_late
        self.ts_thres_early = ts_thres_early
        self.max_attempts = max_attempts
        self.retry_interval = retry_interval
        self.log = log if log is not None else reader.log
    
    def fetch_funding_rates(self, 
                           timestamp: Union[int, datetime], 
                           symbols: List[str]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        获取指定符号列表在特定时间戳的资金费率，尝试多次直到所有符号都有返回结果
        
        Args:
            timestamp: 目标时间戳（毫秒）或 datetime.datetime 对象
            symbols: 符号列表，例如 ['BTCUSDT', 'ETHUSDT']
            
        Returns:
            Tuple[pandas.DataFrame, Dict]: 包含符号、资金费率和时间戳的 DataFrame，以及状态信息
        """
        if not symbols:
            self.log.warning("Empty symbols list provided")
            return pd.DataFrame(), {"success": False, "message": "Empty symbols list", "attempts": 0}
        
        attempts = 0
        collected_data = pd.DataFrame()
        missing_symbols = set(symbols)
        
        while attempts < self.max_attempts and missing_symbols:
            attempts += 1
            
            # 获取数据
            self.log.info(f"Attempt {attempts}/{self.max_attempts} to fetch funding rates for {len(missing_symbols)} symbols")
            df = self.reader.get_funding_rates(
                symbols=list(missing_symbols),
                timestamp=timestamp,
                ts_thres_early=self.ts_thres_early,
                ts_thres_late=self.ts_thres_late
            )
            
            if df.empty:
                self.log.warning(f"Attempt {attempts}: No data returned")
            else:
                # 更新已收集的数据
                if collected_data.empty:
                    collected_data = df.copy()
                else:
                    # 合并新数据
                    collected_data = pd.concat([collected_data, df])
                
                # 更新缺失的符号
                returned_symbols = set(df.index) if not df.empty else set()
                missing_symbols = missing_symbols - returned_symbols
                
                self.log.info(f"Attempt {attempts}: Got {len(returned_symbols)} symbols, still missing {len(missing_symbols)}")
                
                # 如果所有符号都已获取，则退出循环
                if not missing_symbols:
                    break
            
            # 如果还有缺失的符号并且未达到最大尝试次数，则等待一段时间后重试
            if missing_symbols and attempts < self.max_attempts:
                self.log.info(f"Waiting {self.retry_interval}s before next attempt...")
                time.sleep(self.retry_interval)
        
        # 构建返回状态
        status = {
            "success": len(missing_symbols) == 0,
            "attempts": attempts,
            "total_symbols": len(symbols),
            "returned_symbols": len(collected_data),
            "missing_symbols": list(missing_symbols) if missing_symbols else []
        }
        
        # 如果仍有缺失的符号，记录警告
        if missing_symbols:
            warning_msg = f"Failed to fetch all funding rates after {attempts} attempts. Missing symbols: {missing_symbols}"
            self.log.warning(warning_msg)
            status["message"] = warning_msg
        else:
            success_msg = f"Successfully fetched all funding rates in {attempts} attempts"
            self.log.success(success_msg)
            status["message"] = success_msg
        
        return collected_data, status

    def fetch_until_complete(self, 
                            timestamp: Union[int, datetime], 
                            symbols: List[str],
                            custom_max_attempts: Optional[int] = None,
                            custom_retry_interval: Optional[float] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        获取指定符号列表在特定时间戳的资金费率，可以自定义最大尝试次数和重试间隔
        
        Args:
            timestamp: 目标时间戳（毫秒）或 datetime.datetime 对象
            symbols: 符号列表，例如 ['BTCUSDT', 'ETHUSDT']
            custom_max_attempts: 自定义的最大尝试次数，如果为 None 则使用初始化时设置的值
            custom_retry_interval: 自定义的重试间隔（秒），如果为 None 则使用初始化时设置的值
            
        Returns:
            Tuple[pandas.DataFrame, Dict]: 包含符号、资金费率和时间戳的 DataFrame，以及状态信息
        """
        # 临时覆盖配置
        original_max_attempts = self.max_attempts
        original_retry_interval = self.retry_interval
        
        if custom_max_attempts is not None:
            self.max_attempts = custom_max_attempts
        if custom_retry_interval is not None:
            self.retry_interval = custom_retry_interval
        
        try:
            return self.fetch_funding_rates(timestamp, symbols)
        finally:
            # 恢复原始配置
            self.max_attempts = original_max_attempts
            self.retry_interval = original_retry_interval


# %%
if __name__ == "__main__":
    
    pass
    
    ## StrategyUpdater
# =============================================================================
#     # 1. 创建一个 StrategyUpdater 实例
#     updater = StrategyUpdater(mysql_name="yun138", period=30, strategy_name='testtest')
# 
#     # 2. 准备一个 pd.Series，其 index 为 symbol，value 为 pos
#     symbols = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
#     poses = [0.123, -0.456, 0.789]
#     symbol_pos_series = pd.Series(data=poses, index=symbols)
# 
#     # 3. stockdate 可以是字符串或 datetime
#     #    这里示例用 datetime
#     dt_stock = datetime(2024, 12, 25, 10, 0, 0)
# 
#     # 4. 更新数据库
#     updater.update_signals_and_status(
#         stockdate=dt_stock,
#         symbol_pos_series=symbol_pos_series
#     )
# =============================================================================

    ## PremiumIndexReader
# =============================================================================
#     reader = PremiumIndexReader('idc_1021')
#     ts = datetime(2025, 3, 19, 6, 0, 0)
#     trading_symbols = ['BTCUSDT', 'ETHUSDT']
#     funding_rates = reader.get_funding_rates(symbols=trading_symbols, timestamp=ts,
#                                              ts_thres_late=15000)
# =============================================================================
    
    ## FundingRateFetcher
    # 创建 PremiumIndexReader 实例
    reader = PremiumIndexReader('idc_1021')
    
    # 创建 FundingRateFetcher 实例
    fetcher = FundingRateFetcher(
        reader=reader,
        ts_thres_late=15000,
        max_attempts=3,
        retry_interval=2.0
    )
    
    # 设置时间戳和符号列表
    ts = datetime(2025, 3, 27, 5, 27, 0)
    trading_symbols = ['btcusdt', 'ETHUSDT', 'SOLUSDT', 'DOGEUSDT']
    
    # 获取资金费率
    funding_rates, status = fetcher.fetch_funding_rates(
        timestamp=ts,
        symbols=trading_symbols
    )
    
    # 打印结果
    print(f"Status: {status['message']}")
    print(f"Attempts: {status['attempts']}")
    print(f"Data shape: {funding_rates.shape}")
    print(funding_rates)
