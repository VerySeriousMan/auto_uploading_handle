# -*- coding: utf-8 -*-
"""
Project Name: auto_upload_handle
File Created: 2024.09.11
Author: ZhangYuetao
File Name: ready_queues.py
last update： 2024.09.11
"""

import os
import time


class ReadyQueueProcess:
    def __init__(self, pre_queue, ready_queue, stop_event, max_retries=3):
        self.pre_queue = pre_queue
        self.ready_queue = ready_queue
        self.stop_event = stop_event  # 用于控制线程停止
        self.max_retries = max_retries  # 最大重试次数

    def start(self):
        # 启动就绪队列处理
        while not self.stop_event.is_set():
            if not self.pre_queue.empty():
                file_info = self.pre_queue.get()  # 获取队列中的文件信息，(file_path, retry_count)
                file_path, retry_count = file_info if isinstance(file_info, tuple) else (file_info, 0)

                if self._is_file_complete(file_path):
                    print(f"File ready: {file_path}")
                    self.ready_queue.put(file_path)  # 将文件路径放入就绪队列
                else:
                    if retry_count < self.max_retries:
                        retry_count += 1
                        print(f"File incomplete: {file_path}, retrying ({retry_count}/{self.max_retries})")
                        # 将文件重新放回队列，并增加重试次数
                        self.pre_queue.put((file_path, retry_count))
                    else:
                        print(f"File incomplete after {self.max_retries} retries: {file_path}, skipping...")
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
                return current_size != 0  # 返回非空文件

            last_size = current_size
            time.sleep(interval)

        return False  # 超时，认为上传未完成
