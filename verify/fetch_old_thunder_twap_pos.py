# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 10:53:04 2025

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %%
import pymysql
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


from core.database_handler import DatabaseHandler
from utility.logutils import FishStyleLogger


# %%
class AcnameDataFetcher(DatabaseHandler):
    
    def __init__(self, mysql_name, log=None):
        """
        初始化 AcnameDataFetcher，设置数据库连接
        :param mysql_name: 数据库连接名称
        :param log: 日志对象（可选）
        """
        super().__init__(mysql_name, log=log)
    
    def fetch_by_acname(self, acname, start_time=None, end_time=None):
        """
        根据 acname 获取指定时间范围内的 position 和 updatetime 数据，并返回一个 DataFrame。
        :param acname: 需要筛选的账户名称
        :param start_time: 开始时间（字符串格式，如 '2025-02-24 00:00:00'）
        :param end_time: 结束时间（字符串格式，如 '2025-02-25 00:00:00'）
        :return: Pandas DataFrame，包含 symbol, position 和 updatetime 列
        """
        connection = None
        cursor = None
        result = pd.DataFrame(columns=["symbol", "position", "updatetime"])
        
        try:
            # 建立数据库连接
            connection = self.connect()
            if not connection:
                return result  # 无法连接数据库，返回空 DataFrame
            
            # 构建 SQL 查询
            query = """
            SELECT symbol, position, updatetime 
            FROM update_python_position 
            WHERE acname = %s
            """
            params = [acname]
            
            # 如果传入了时间区间，则增加筛选条件
            if start_time and end_time:
                query += " AND updatetime BETWEEN %s AND %s"
                params.extend([start_time, end_time])
            elif start_time:
                query += " AND updatetime >= %s"
                params.append(start_time)
            elif end_time:
                query += " AND updatetime < %s"
                params.append(end_time)
            
            cursor = connection.cursor()
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            
            # 如果查询到数据，将其转换为 DataFrame 格式
            if rows:
                result = pd.DataFrame(rows, columns=["symbol", "position", "updatetime"])
            
            self.log.success(f"Successfully fetched {len(result)} records for acname: {acname} within time range {start_time} - {end_time}")
        
        except pymysql.MySQLError as e:
            self.log.error(f"Error while fetching data by acname: {e}")
        
        finally:
            # 关闭游标和连接
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        
        return result


def generate_time_ranges(start_date_str, end_date_str, interval_hours=6):
    # 将日期字符串转换为 datetime 对象
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # 创建一个列表来存储时间段
    time_ranges = []
    
    # 循环从 start_date 到 end_date
    current_date = start_date
    while current_date <= end_date:
        # 每指定小时的起始时间
        start_of_period = current_date.strftime("%Y-%m-%d %H:%M:%S")
        # 每指定小时的结束时间
        end_of_period = (current_date + timedelta(hours=interval_hours)).strftime("%Y-%m-%d %H:%M:%S")
        
        # 添加时间段到列表
        time_ranges.append((start_of_period, end_of_period))
        
        # 增加指定的小时数
        current_date += timedelta(hours=interval_hours)
    
    return time_ranges


# %%
if __name__=='__main__':
    mysql_name = 'yun138'
    acname = 'agg_241114_to_00125_v0'
    start_date = '2025-02-01'
    end_date = '2025-02-25'
    
    dir_to_save = Path('D:/crypto/prod/alpha/portfolio_management/verify/old_thunder_pos_250225')
    
    
    log = FishStyleLogger()
    fetcher = AcnameDataFetcher(mysql_name, log=log)
    
    time_ranges = generate_time_ranges(start_date, end_date)
    for start_time, end_time in time_ranges:
        twap_pos = fetcher.fetch_by_acname(acname, start_time=start_time, end_time=end_time)
        twap_pos_pivot = twap_pos.pivot(index='updatetime', columns='symbol', values='position')
        if len(twap_pos) > 0:
        #     breakpoint()
            filename = f'{start_time}_{end_time}.parquet'
            filename = filename.replace(' ', '_').replace(':', '')
            twap_pos_pivot.to_parquet(dir_to_save / filename)