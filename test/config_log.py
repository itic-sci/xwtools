#!/usr/bin/env python 
# encoding: utf-8 


''' 
@author: xuwei 
@contact: awei@sciencecores.com
@file: config_log.py
@time: 2024/1/24 18:29 
'''


import xwtools as xw

if __name__ == '__main__':
    xw.init_log(
        module='udp-test',
        # file_path="./",
        graylog_udp_host="192.168.0.26", graylog_udp_port=12201,
        log_format=True
    )
    xw.log.info('xw info zxcv')
    xw.log.error('xw error asdf')
