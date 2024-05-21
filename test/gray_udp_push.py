#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                          
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved                                                      
Author: xuwei                                        
Email: weix@sensedeal.ai                                 
Description:                                    
'''

import logging
import graypy

# 参考 https://www.cnblogs.com/yyds/p/6897964.html
class MyLoggerAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        if 'extra' not in kwargs:
            kwargs["extra"] = self.extra
        return msg, kwargs

class GrayLogging(object):
    def __init__(self, host='192.168.0.26', port=12201, log_type='error'):
        self.host = host
        self.port = port
        self.log_type = log_type

    def __logs_to_gray(self, handler, log_class_name: str, log_msg: str):
        # LoggerAdapter makes it easy to add static information to your GELF log messages:
        my_logger = logging.getLogger(log_class_name)
        my_logger.setLevel(logging.DEBUG)
        my_logger.addHandler(handler)

        # my_adapter = MyLoggerAdapter(my_logger, {'project': 'udp-test'})
        my_adapter = logging.LoggerAdapter(my_logger, {'project': 'udp-test', "appversion": 'v0.0.1'})

        if self.log_type == 'error':
            my_adapter.error(log_msg)
        elif self.log_type == 'debug':
            my_adapter.debug(log_msg)
        elif self.log_type == 'warning':
            my_adapter.warning(log_msg)
        else:
            my_adapter.info(log_msg)

    def udp_gray(self, log_class_name: str, log_msg: str):
        handler = graypy.GELFUDPHandler(self.host, self.port)
        self.__logs_to_gray(handler, log_class_name, log_msg)


if __name__ == '__main__':
    gray_log = GrayLogging()
    gray_log.udp_gray('udp-xw123', "project 123测试")
