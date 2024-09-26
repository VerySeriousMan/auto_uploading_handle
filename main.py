import os
import time
import queue
import threading
import subprocess

from watchdog.observers import Observer
from pre_queues import PreQueueProcess
from ready_queues import ReadyQueueProcess
from logger import setup_logger


class Watcher:
    def __init__(self, directory_to_watch, ready_thread_nums=10):
        self.directory_to_watch = directory_to_watch
        self.observer = Observer()
        self.pre_queue = queue.Queue()  # 传入队列
        self.ready_queue = queue.Queue()  # 就绪队列
        self.ready_thread_nums = ready_thread_nums  # 就绪队列处理线程的数量
        self.threads = []  # 存储线程的列表
        self.stop_event = threading.Event()  # 用于通知线程停止
        self.logger = setup_logger()

    def run(self):
        """启动文件监控器和处理线程"""
        event_handler = PreQueueProcess(self.pre_queue, self.logger)
        self.observer.schedule(event_handler, self.directory_to_watch, recursive=True)
        self.observer.start()

        # 启动多个就绪队列处理线程
        for _ in range(self.ready_thread_nums):
            thread = threading.Thread(target=self.ready_processor_thread)
            thread.start()
            self.threads.append(thread)

        try:
            # 持续运行，保持监听
            self.logger.info("Observer started. Press Ctrl+C to stop...")
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Stopping observer...")
        finally:
            self.stop()

    def ready_processor_thread(self):
        """就绪队列处理线程逻辑"""
        ready_processor = ReadyQueueProcess(self.pre_queue, self.ready_queue, self.logger, self.stop_event)
        ready_processor.start()

    def stop(self):
        """停止所有监控器和处理线程"""
        self.logger.info("Stopping watchdog and threads...")

        # 发出停止信号给所有线程
        self.stop_event.set()

        # 停止 Observer 和所有线程
        self.observer.stop()
        self.observer.join()

        # 停止所有处理线程
        for thread in self.threads:
            thread.join()

        self.print_queues()  # 输出队列中的所有数据

        # 确保强制退出
        os._exit(0)

    def print_queues(self):
        """输出传入队列和就绪队列中的全部数据"""
        self.logger.info("Input Queue contents:")
        while not self.pre_queue.empty():
            self.logger.info(self.pre_queue.get())

        self.logger.info("Ready Queue contents:")
        while not self.ready_queue.empty():
            self.logger.info(self.ready_queue.get())

    def pause(self):
        """暂停文件夹监控"""
        self.logger.info("Pausing watchdog...")
        self.observer.stop()

    def resume(self):
        """恢复文件夹监控"""
        self.logger.info("Resuming watchdog...")
        self.observer.start()

    def process_modified_files(self, modified_files):
        """处理rsync同步后的修改文件"""
        for file in modified_files:
            full_path = os.path.join(self.directory_to_watch, file)
            self.logger.info(f"Detected modification for: {full_path}")
            self.pre_queue.put(full_path)


def sync_files(remote_path, local_path):
    """使用 rsync 同步文件，并记录变动的文件"""
    modified_files = []
    rsync_command = [
        "rsync", "-avz", "--delete",
        "--out-format=%f",  # 仅输出文件路径
        remote_path, local_path
    ]
    result = subprocess.run(rsync_command, capture_output=True, text=True)

    # 解析 rsync 输出，获取所有变动文件的列表
    for line in result.stdout.splitlines():
        if line:  # 过滤空行
            modified_files.append(line)

    print("Files synchronized successfully.")
    return modified_files


def auto_sync_and_watch():
    """自动化同步和监控"""
    # 设置远程目录和本地挂载目录
    remote_directory = "zngzhangyuet@10.0.1.206:/data3/数据采集/auto_test/"
    local_directory = "/home/zyt/桌面/data"

    # 初始化 Watcher 监控
    watcher = Watcher(local_directory)

    try:
        # 启动监控
        watcher.run()

        while True:
            # 每隔一段时间进行同步
            time.sleep(30)

            # 在同步之前暂停 watchdog 监控
            watcher.pause()

            # 执行文件同步并获取同步变动的文件列表
            modified_files = sync_files(remote_directory, local_directory)

            # 处理这些修改的文件
            watcher.process_modified_files(modified_files)

            # 同步完成后恢复监控
            watcher.resume()

    except KeyboardInterrupt:
        watcher.logger.info("Stopping observer...")
        watcher.stop()


if __name__ == "__main__":
    auto_sync_and_watch()
