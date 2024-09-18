# -*- coding: utf-8 -*-
"""
Project Name: auto_upload_handle
File Created: 2024.09.13
Author: ZhangYuetao
File Name: logger.py
last update： 2024.09.13
"""

import os
import logging
from datetime import datetime

from colorlog import ColoredFormatter

# 日志颜色配置
log_colors = {
    'DEBUG': 'cyan',
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red',
    'CRITICAL': 'bold_red',
}


def setup_logger():
    # 获取当前日期
    log_folder = "log"
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

    log_date = datetime.now().strftime("%Y_%m_%d")
    log_file = os.path.join(log_folder, f"{log_date}_log.log")

    # 配置日志格式
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    formatter = ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
        log_colors=log_colors,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 配置logger
    logger = logging.getLogger("file_watcher_logger")
    logger.setLevel(logging.DEBUG)

    # 日志文件输出
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(log_format))
    file_handler.setLevel(logging.DEBUG)

    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)

    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
