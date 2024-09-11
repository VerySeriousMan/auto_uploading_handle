# -*- coding: utf-8 -*-
"""
Project Name: auto_upload_training
File Created: 2024.09.11
Author: ZhangYuetao
File Name: pre_queues.py
last update： 2024.09.11
"""

import os
from watchdog.events import FileSystemEventHandler


class PreQueueProcess(FileSystemEventHandler):
    def __init__(self, pre_queue):
        self.pre_queue = pre_queue

    def on_created(self, event):
        # 当新文件或文件夹创建时触发
        if not os.path.basename(event.src_path).startswith('.goutputstream-'):  # 排除替换时产生的临时文件
            if event.is_directory:
                print(f"New directory detected: {event.src_path}")
                self._dir_put_queue(event.src_path)
            else:
                print(f"New file loading: {event.src_path}")
                self.pre_queue.put(event.src_path)  # 将新文件路径放入队列

    def on_modified(self, event):
        # 当文件或文件夹被修改时触发
        if event.is_directory:
            print(f"Directory modified: {event.src_path}")
        else:
            print(f"File modified: {event.src_path}")

    def on_deleted(self, event):
        # 当文件或文件夹被删除时触发
        if event.is_directory:
            print(f"Directory deleted: {event.src_path}")
        else:
            print(f"File deleted: {event.src_path}")

    def _dir_put_queue(self, dir_path):
        # 遍历文件夹中的所有文件并放入队列
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                print(f"File in new directory loading: {file_path}")
                self.pre_queue.put(file_path)  # 将文件路径放入队列
