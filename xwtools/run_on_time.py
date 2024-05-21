#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                                                                             
Author: xuwei                                        
Email: 18810079020@163.com                                 
File: run_on_time.py
Date: 2020/7/29 10:27 上午
'''

import time
import datetime

class RunOnTime(object):
    def __init__(self, hour=None, minute=None, sleep_minute=40):
        self.__hour = hour
        self.__minute = minute
        self.__sleep_minute = sleep_minute

    def runOnHour(self, fun, *args, **kwargs):
        while True:
            now = datetime.datetime.now()
            if now.hour == self.__hour:
                fun(*args, **kwargs)
            time.sleep(60 * self.__sleep_minute)


if __name__ == '__main__':
    pass
