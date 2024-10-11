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
import pymysql
import yaml
import time
from pathlib import Path
import pandas as pd


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


