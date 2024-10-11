# -*- coding: utf-8 -*-
"""
Created on Thu Feb  1 13:27:54 2024

@author: Xintang Zheng

"""
# %% imports
import time
import threading
from decorator import decorator
import gc
from functools import wraps
from datetime import datetime


# %% decorator
@decorator
def run_every(func, interval=1, *args, **kwargs):
    time.sleep(interval)
    while True:
        start = time.time()
        func(*args, **kwargs)
        duration = (time.time() - start)
        sleep_t = max(interval - duration, 0)
        time.sleep(sleep_t)
        
        
@decorator
def run_by_thread(func, daemon=True, *args, **kwargs):
    thread = threading.Thread(target=func, args=args, kwargs=kwargs)
    thread.daemon = daemon
    thread.start()
    return thread


# %%
def timeit(func):
    """装饰器函数，用于测量函数执行时间"""
    def wrapper(*args, **kwargs):
        start_time = time.time()  # 记录函数开始时间
        result = func(*args, **kwargs)  # 调用函数
        end_time = time.time()  # 记录函数结束时间
        print(f"{func.__name__} ran in {end_time - start_time:.4f} seconds")
        return result
    return wrapper


# %%
def sleep_adjusted(sleep_seconds):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_t = datetime.now()
            result = func(*args, **kwargs)
            end_t = datetime.now()
            elapsed_time = (end_t - start_t).total_seconds()
            time_to_sleep = sleep_seconds - elapsed_time
            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
            return result
        return wrapper
    return decorator


def retry_with_sleep(max_attempts, sleep_seconds, delay=1):
    """
    一个装饰器，用于在函数返回 None 时自动重试，并动态调整每次重试之间的休眠时间。
    如果函数返回非 None，直接返回结果，不再执行 sleep。

    :param max_attempts: 最大重试次数
    :param sleep_seconds: 指定的最小总执行时间（包括运行时间和休眠时间）
    :param delay: 每次重试之间的最短延迟时间（秒）
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                start_t = datetime.now()  # 记录开始时间
                result = func(*args, **kwargs)  # 执行函数

                # 如果函数的返回值不是 None，则成功返回，不需要 sleep
                if result is not None:
                    return result

                # 如果函数返回 None，则继续重试
                attempts += 1
                print(f"Attempt {attempts} returned None. Retrying...")

                # 记录结束时间并计算运行时长
                end_t = datetime.now()
                elapsed_time = (end_t - start_t).total_seconds()

                # 在失败时，考虑休眠时间的动态调整
                time_to_sleep = max(delay, sleep_seconds - elapsed_time)
                if time_to_sleep > 0:
                    print(f"Sleeping for {time_to_sleep} seconds before retry...")
                    time.sleep(time_to_sleep)
            
            # 如果超过最大重试次数，返回 None
            print("Max retry attempts reached. Returning None.")
            return None
        return wrapper
    return decorator


# %%
def gc_collect_after(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)  # 调用原始函数
        gc.collect()  # 在函数执行后调用垃圾回收
        return result
    return wrapper


# %%
if __name__=='__main__':
    @run_by_thread()
    @run_every(interval=1)
    def func():
        print(time.time())
        time.sleep(0.5)
    func()
        
