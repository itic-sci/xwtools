#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved
Author: xuwei
Email: weix@sensedeal.ai
Description:
'''

import configparser
import logging
import sys
import os

__local_config = 'settings.local.ini'
__server_config = 'settings.ini'
session = 'settings'

__root_deep = 8
SETTINGS_PATH = ''

def _config_path(cur_path):
    filename_local = os.path.join(cur_path, __local_config)
    filename = os.path.join(cur_path, __server_config)
    if os.path.isfile(filename_local):
        return filename_local, True
    elif os.path.isfile(filename):
        return filename, True
    else:
        up_path = os.path.dirname(cur_path)
        return up_path, False


def _init_config_path():
    global SETTINGS_PATH
    if SETTINGS_PATH: return
    root_deep = __root_deep
    current_path = sys.path[0]
    while root_deep > 0:
        file_path, state = _config_path(current_path)
        if state:
            SETTINGS_PATH = file_path
            return None
        else:
            current_path = os.path.dirname(current_path)
            root_deep -= 1
    print('not find settings config file')


class Config(object):
    def __init__(self):
        _init_config_path()
        self.cf = configparser.ConfigParser()
        if SETTINGS_PATH:
            self.cf.read(SETTINGS_PATH, encoding='utf8')

    def __call__(self, *args):
        try:
            if len(args) == 1:
                parameter = self.cf.get(session, args[0])
            else:
                parameter = self.cf.get(*args[:2])
            return parameter
        except Exception as e:
            if len(args) > 2:
                return args[2]
            raise e


config = Config()


# 在第一次使用的时候才会初始化 logger，self.__init_logger()
class Log(object):
    def __init__(self, **kwargs):
        self.init_log = False
        self.log_path = './'
        self.project_name = 'log'
        self.format_log = False
        self.graylog_host = ""
        self.graylog_port = 0

    ## 根据 init_log 修改 self, 只修改一次
    def __init_logger(self):
        if self.init_log: return
        self.init_log = True

        self.logger = logging.getLogger(self.project_name)
        self.logger.setLevel(logging.DEBUG)

        # 确定用哪个handler
        if self.graylog_host:
            import graypy
            self.handler = graypy.GELFUDPHandler(self.graylog_host, self.graylog_port)
        else:
            log_name = os.path.join(self.log_path, '%s.log' % self.project_name)
            self.handler = logging.FileHandler(log_name, 'a', encoding='utf-8')  # 这个是python3的

        formatter = logging.Formatter('[%(levelname)s][%(asctime)s]%(message)s')
        self.handler.setLevel(logging.DEBUG)
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)

        # 创建一个StreamHandler,用于输出到控制台
        # handler = logging.StreamHandler()
        # handler.setLevel(logging.DEBUG)
        # handler.setFormatter(formatter)
        # self.logger.addHandler(handler)

    def __console(self, level, message):
        self.__init_logger()

        data = message
        if self.project_name and self.format_log:
            data = {'facility': self.project_name, 'log_level': level, 'msg': message}
        if level == 'info':
            self.logger.info(data)
        elif level == 'debug':
            self.logger.debug(data)
        elif level == 'warning':
            self.logger.warning(data)
        elif level == 'error':
            self.logger.error(data)
        elif level == 'except':
            self.logger.exception(data)

    def debug(self, message):
        self.__console('debug', message)

    def info(self, message):
        self.__console('info', message)

    def warning(self, message):
        self.__console('warning', message)

    def error(self, message):
        self.__console('error', message)

    def exception(self, message):
        self.__console('except', message)

    def raise_exception(self, message):
        self.exception(message)
        raise Exception(message)


log = Log()

from functools import wraps


def catch_error_log(func):
    @wraps(func)
    def try_catch(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print("%s: %s" % (func.__name__, e))
            log.exception("%s: %s" % (func.__name__, e))
            return None

    return try_catch


def init_log(
        file_path='./',
        module='log',
        graylog_udp_host: str = "",
        graylog_udp_port: int = 0,
        log_format=True,
):
    """
    file_path: 存日志文件的文件夹路径
    module: 可以看做是服务的名称，修改project_name参数，日志文件的前缀名称，默认是 log.error.log，log.info.log
    log_format: 日志输出的格式是否格式化，格式化后是个json [ERROR][2024-01-24 14:04:21,693]{'module': 'test.', 'log_level': 'error', 'msg': 'this is a test2'}
    graylog_host, graylog_port: 为真的话，日志不写文件，而是写入graylog中
    """
    if not os.path.exists(file_path):
        os.mkdir(file_path)

    log.log_path = file_path
    log.project_name = module
    log.format_log = log_format
    log.graylog_host = graylog_udp_host
    log.graylog_port = graylog_udp_port


if __name__ == '__main__':
    # host = config('db_stock', 'host', '12')
    # print(host)
    # port = config('log_path')
    # print(port)
    init_log(
        module='udp-test',
        # file_path="./",
        # graylog_udp_host="192.168.0.26", graylog_udp_port=12201,
        log_format=True
    )
    log.info('info zxcv')
    log.error('error asdf')
