# -*- coding: utf-8 -*-
"""
Project Name: auto_upload_training
File Created: 2024.09.11
Author: ZhangYuetao
File Name: ready_queues.py
last update： 2024.09.11
"""

import os
import time


class ReadyQueueProcess:
    def __init__(self, pre_queue, ready_queue, stop_event):
        self.pre_queue = pre_queue
        self.ready_queue = ready_queue
        self.stop_event = stop_event  # 用于控制线程停止
        self.running = True

    def start(self):
        # 启动就绪队列处理
        while not self.stop_event.is_set():
            if not self.pre_queue.empty():
                file_path = self.pre_queue.get()
                if self._is_file_complete(file_path):
                    print(f"File ready: {file_path}")
                    self.ready_queue.put(file_path)  # 将文件路径放入就绪队列
            time.sleep(1)  # 避免过度占用CPU

    @staticmethod
    def _is_file_complete(file_path, timeout=60, interval=1):
        """检测文件是否上传完成"""
        start_time = time.time()
        last_size = -1

        while time.time() - start_time < timeout:
            try:
                current_size = os.path.getsize(file_path)
            except FileNotFoundError:
                return False  # 文件被删除了

            if current_size == last_size:
                return True  # 文件大小没有变化，认为上传完成

            last_size = current_size
            time.sleep(interval)

        return False  # 超时，认为上传未完成
