# -*- coding: utf-8 -*-
"""
Project Name: auto_upload_handle
File Created: 2024.09.11
Author: ZhangYuetao
File Name: pre_queues.py
last update： 2024.09.13
"""

import os
import time

from watchdog.events import FileSystemEventHandler


class PreQueueProcess(FileSystemEventHandler):
    def __init__(self, pre_queue, logger, cleanup_interval=10, event_ignore_interval=2):
        self.pre_queue = pre_queue
        self.logger = logger
        self.last_event_times = {}  # 记录每个文件的上次事件时间
        self.cleanup_interval = cleanup_interval  # 清理过期记录的时间间隔（秒）
        self.event_ignore_interval = event_ignore_interval  # 创建/修改后忽略事件的时间间隔
        self.last_cleanup_time = time.time()  # 上次清理的时间

    def on_created(self, event):
        # 当新文件或文件夹创建时触发
        if not os.path.basename(event.src_path).startswith('.goutputstream-'):  # 排除替换时产生的临时文件
            if event.is_directory:
                self.logger.info(f"New directory detected: {event.src_path}")
                self._dir_put_queue(event.src_path)
            else:
                # 调用防重复机制，避免短时间内重复处理
                if not self._should_process_file(event.src_path):
                    self.logger.debug(f"Ignoring create file: {event.src_path}")
                    return

                self.logger.info(f"New file loading: {event.src_path}")
                self.pre_queue.put((event.src_path, 0))  # 将新文件路径放入队列
                self.last_event_times[event.src_path] = time.time()  # 记录文件创建时间

    def on_modified(self, event):
        # 当文件或文件夹被修改时触发
        if event.is_directory:
            self.logger.info(f"Directory modified: {event.src_path}")
        else:
            # 调用防重复机制，避免短时间内重复处理
            if not self._should_process_file(event.src_path):
                self.logger.debug(f"Ignoring modification right after creation: {event.src_path}")
                return

            self.logger.info(f"File modified: {event.src_path}")
            self.pre_queue.put((event.src_path, 0))  # 将修改后的文件放入队列重新处理

            # 定期清理过期的记录
            self._cleanup_old_entries()

    def on_moved(self, event):
        # 当文件或文件夹被重命名或移动时触发
        if event.is_directory:
            self.logger.info(f"Directory moved or renamed from {event.src_path} to {event.dest_path}")
        else:
            # # 防止相同文件被重复处理，但允许不同路径的修改
            # if event.src_path in self.last_event_times and event.dest_path in self.last_event_times:
            #     if self.last_event_times[event.src_path] < self.last_event_times[event.dest_path]:
            #         # 如果短时间内发生了两次相同的移动操作，忽略第二次
            #         print(f"Ignoring move: {event.src_path} to {event.dest_path}")
            #         return

            self.logger.info(f"File moved or renamed from {event.src_path} to {event.dest_path}")
            self.pre_queue.put((event.dest_path, 0))
            self.last_event_times[event.dest_path] = time.time()
            self.logger.info(f"File moved or renamed,redo: {event.dest_path}")

            # 定期清理过期的记录
            self._cleanup_old_entries()

    def on_deleted(self, event):
        # 当文件或文件夹被删除时触发
        if event.is_directory:
            self.logger.info(f"Directory deleted: {event.src_path}")
        else:
            self.logger.info(f"File deleted: {event.src_path}")

    def _should_process_file(self, file_path):
        """
        检查文件是否在短时间内被多次修改，防止重复处理。
        interval: 限定重复修改的时间间隔，默认 2 秒。
        """
        current_time = time.time()

        # 检查文件是否在 last_event_times 中记录过
        if file_path in self.last_event_times:
            last_time = self.last_event_times[file_path]
            # 如果上次修改时间与当前时间间隔小于设定的 event_ignore_interval，则跳过处理
            if current_time - last_time < self.event_ignore_interval:
                return False

        # 更新文件的事件时间记录
        self.last_event_times[file_path] = current_time
        return True

    def _cleanup_old_entries(self):
        """
        定期清理 last_event_times 中超过 cleanup_interval 的记录，
        以避免字典无限增长。
        """
        current_time = time.time()

        # 如果距离上次清理时间小于 cleanup_interval，则不执行清理
        if current_time - self.last_cleanup_time < self.cleanup_interval:
            return

        # 初始化一个列表，用于保存需要删除的文件记录
        keys_to_delete = []

        # 遍历 last_event_times 字典中的所有条目
        for file_path, last_time in self.last_event_times.items():
            # 检查每个文件记录的最后一次事件时间是否超过 cleanup_interval
            if current_time - last_time > self.cleanup_interval:
                # 如果超过了 cleanup_interval，则将该文件路径添加到待删除列表中
                keys_to_delete.append(file_path)

        # 遍历待删除的文件路径，并从 last_event_times 字典中删除这些记录
        for key in keys_to_delete:
            del self.last_event_times[key]

        # 更新上次清理时间为当前时间
        self.last_cleanup_time = current_time

    def _dir_put_queue(self, dir_path):
        # 遍历文件夹中的所有文件并放入队列
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)

                # 调用防重复机制，避免短时间内重复处理
                if not self._should_process_file(file_path):
                    self.logger.debug(f"Ignoring create file in directory: {file_path}")
                    return

                self.logger.info(f"File in new directory loading: {file_path}")
                self.pre_queue.put((file_path, 0))  # 将文件路径放入队列
                self.last_event_times[file_path] = time.time()  # 记录文件创建时间
