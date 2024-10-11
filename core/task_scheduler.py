# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 15:30:36 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import threading
import time
from datetime import datetime, timedelta
from queue import Queue


from utility.decorator_utils import run_by_thread
from utility.logutils import FishStyleLogger


# %%
class TaskScheduler:
    
    def __init__(self, log, repo=None):
        self.log = log
        self.repo = repo
        self.tasks = []  # 存储任务列表
        self.task_queue = Queue()  # 任务队列
        self.task_event = threading.Event()  # 控制任务触发的事件
        self._init_logger()
        self._running = True  # 控制调度器的运行状态
        
    def _init_logger(self):
        self.log = self.log or FishStyleLogger()

    def add_task(self, name, frequency_type, frequency_value, task_fn):
        """
        添加任务到调度器中
        :param name: 任务名称
        :param frequency_type: 'second', 'minute', 'hour', 或 'specific_time'
        :param frequency_value: 对于 'second', 'minute', 'hour' 为间隔秒、分钟或小时数，对于 'specific_time' 是时间列表 ['HH:MM']
        :param task_fn: 要执行的任务函数
        """
        next_run_time = self._calculate_next_run(frequency_type, frequency_value)
        self.tasks.append({
            'name': name,
            'frequency_type': frequency_type,
            'frequency_value': frequency_value,
            'task_fn': task_fn,
            'next_run_time': next_run_time
        })

    def _calculate_next_run(self, frequency_type, frequency_value):
        """
        计算下次运行时间
        :param frequency_type: 任务类型：'second', 'minute', 'hour', 'specific_time'
        :param frequency_value: 对应的时间间隔或具体时间列表
        :return: 下次运行的具体时间
        """
        now = datetime.utcnow()

        if frequency_type == 'second':
            next_second = ((now.second // frequency_value) + 1) * frequency_value
            if next_second >= 60:
                next_run_time = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
            else:
                next_run_time = now.replace(second=next_second, microsecond=0)
            return next_run_time
        
        elif frequency_type == 'minute':
            next_minute = (now.minute // frequency_value + 1) * frequency_value
            if next_minute == 60:
                next_run_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            else:
                next_run_time = now.replace(minute=next_minute, second=0, microsecond=0)
            return next_run_time
        
        elif frequency_type == 'hour':
            next_hour = (now.hour // frequency_value + 1) * frequency_value
            next_run_time = now.replace(hour=next_hour, minute=0, second=0, microsecond=0)
            if next_hour == 24:
                next_run_time += timedelta(days=1)
            return next_run_time
        
        elif frequency_type == 'specific_time':
            next_run_time = None
            for time_str in frequency_value:
                run_time = datetime.strptime(time_str, '%H:%M').replace(year=now.year, month=now.month, day=now.day)
                if run_time > now:
                    next_run_time = run_time
                    break
            if next_run_time is None:
                # 如果今天的时间点都已过去，取明天的第一个时间点
                first_run_time = datetime.strptime(frequency_value[0], '%H:%M').replace(year=now.year, month=now.month, day=now.day)
                next_run_time = first_run_time + timedelta(days=1)
            return next_run_time
        
    @run_by_thread
    def _trigger_events(self):
        """
        检查时间并触发任务
        """
        tasks_to_execute = False  # 用于标记是否有任务需要执行
        
        while self._running:
            now = datetime.utcnow()

            for task in self.tasks:
                if now >= task['next_run_time']:
                    # 保存当前任务的 next_run_time（更新前的时间戳）
                    current_run_time = task['next_run_time']
                    
                    # 将任务和对应的时间戳放入队列
                    self.task_queue.put((task, current_run_time))
                    
                    # 计算下次运行时间
                    task['next_run_time'] = self._calculate_next_run(task['frequency_type'], task['frequency_value'])
                    tasks_to_execute = True

            if tasks_to_execute:
                self.task_event.set()
                tasks_to_execute = False
        
    def _task_runner(self):
        """
        任务执行线程，从队列中执行任务，保证按顺序执行
        """
        while self._running:
            self.task_event.wait()  # 等待任务触发

            while not self.task_queue.empty():
                task, current_run_time = self.task_queue.get()
                info_text = f"Executing task: {task['name']} scheduled at {current_run_time}"
                self.log.info(info_text)
                if self.repo is not None:
                    self.repo.send_text(info_text)
                
                # 调用任务函数时，将 next_run_time 作为参数传递
                task['task_fn'](current_run_time)

            self.task_event.clear()
    
    @run_by_thread(daemon=False)
    def _task_runner_by_thread(self):
        self._task_runner()

    def start(self, use_thread_for_task_runner=True):
        """
        启动调度器
        :param use_thread_for_task_runner: 是否在单独线程中运行 task_runner
        """
        self._trigger_events()
        
        # 根据参数决定是否在单独线程中运行 task_runner
        if use_thread_for_task_runner:
            self._task_runner_by_thread()
        else:
            self._task_runner()  # 同步运行，不使用线程

    def stop(self):
        """
        停止调度器
        """
        self._running = False
        self.task_event.set()


# %%
if __name__=='__main__':
    # 示例任务函数
    def every_3s_task():
        time.sleep(3)
        print(f"[{datetime.utcnow()}] Every 3 seconds task is running...")
        
    def every_6s_task():
        time.sleep(3)
        print(f"[{datetime.utcnow()}] Every 6 seconds task is running...")
    
    def minute_task():
        print(f"[{datetime.utcnow()}] Minute task is running...")
    
    def five_minute_task():
        print(f"[{datetime.utcnow()}] Five-minute task is running...")
    
    def specific_time_task():
        print(f"[{datetime.utcnow()}] Specific time task is running...")
    
    # 初始化调度器
    scheduler = TaskScheduler()
    
    # 注册任务
    scheduler.add_task("Every 3 Seconds Task", 'second', 3, every_3s_task)  # 每3秒运行一次
    scheduler.add_task("Every 6 Seconds Task", 'second', 6, every_6s_task)  # 每3秒运行一次
    scheduler.add_task("Minute Task", 'minute', 1, minute_task)  # 每分钟运行一次
    scheduler.add_task("Five-Minute Task", 'minute', 5, five_minute_task)  # 每5分钟运行一次
    scheduler.add_task("Specific Time Task", 'specific_time', ['09:00', '15:00'], specific_time_task)  # 在特定时间运行
    
    # 启动调度器
    scheduler.start()
    
    # 停止调度器
    # scheduler.stop()
    
