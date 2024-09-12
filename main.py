# -*- coding: utf-8 -*-
"""
Project Name: auto_upload_handle
File Created: 2024.09.11
Author: ZhangYuetao
File Name: main.py
last update： 2024.09.12
"""

import time
import queue
import threading

from watchdog.observers import Observer

from pre_queues import PreQueueProcess
from ready_queues import ReadyQueueProcess


class Watcher:
    def __init__(self, directory_to_watch, ready_thread_nums=10):
        self.directory_to_watch = directory_to_watch
        self.observer = Observer()
        self.pre_queue = queue.Queue()  # 传入队列
        self.ready_queue = queue.Queue()  # 就绪队列
        self.ready_thread_nums = ready_thread_nums  # 就绪队列处理线程的数量
        self.threads = []  # 存储线程的列表
        self.stop_event = threading.Event()  # 用于通知线程停止

    def run(self):
        # 创建并启动文件夹监听器
        event_handler = PreQueueProcess(self.pre_queue)
        self.observer.schedule(event_handler, self.directory_to_watch, recursive=True)
        self.observer.start()

        # 启动多个就绪队列处理线程
        for _ in range(self.ready_thread_nums):
            thread = threading.Thread(target=self.ready_processor_thread)
            thread.start()
            self.threads.append(thread)

        try:
            # 持续运行，保持监听
            print("Press Ctrl+C to stop the observer...")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Stopping observer...")
        finally:
            self.stop()

    def ready_processor_thread(self):
        """就绪队列处理线程逻辑"""
        ready_processor = ReadyQueueProcess(self.pre_queue, self.ready_queue, self.stop_event)
        ready_processor.start()

    def stop(self):
        # 发出停止信号给所有线程
        self.stop_event.set()

        # 停止 Observer 和所有线程
        self.observer.stop()
        self.observer.join()

        # 停止所有处理线程
        for thread in self.threads:
            thread.join()

        self.print_queues()  # 输出队列中的所有数据

    def print_queues(self):
        # 输出传入队列和就绪队列中的全部数据
        print("Input Queue contents:")
        while not self.pre_queue.empty():
            print(self.pre_queue.get())

        print("Ready Queue contents:")
        while not self.ready_queue.empty():
            print(self.ready_queue.get())


if __name__ == "__main__":
    # 设置要监控的文件夹路径
    folder_to_watch = '/home/zyt/桌面/listen'
    watcher = Watcher(folder_to_watch)
    watcher.run()
