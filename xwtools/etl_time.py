#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved
File: .py
Author: xuwei
Email: weix@sensedeal.ai
Last modified: date
Description:
'''

import datetime
import time


class TimeEtl(object):

    @classmethod
    def ge_now(cls):
        return datetime.datetime.now()

    @classmethod
    def time_str(cls, _now, format='%Y-%m-%d %H:%M:%S'):
        _str_now = _now.strftime(format)
        return _str_now

    @classmethod
    def time_stamp(cls, _now):
        un_time = time.mktime(_now.timetuple())
        return int(un_time)

    @classmethod
    def str_time(cls, _str, format='%Y-%m-%d %H:%M:%S'):
        _time = datetime.datetime.strptime(_str, format)
        return _time

    @classmethod
    def str_time_stamp(cls, _str):
        _time = cls.str_time(_str)
        _time_stamp = cls.time_stamp(_time)
        return _time_stamp

    @classmethod
    def stamp_time(cls, _stamp, format="%Y-%m-%d %H:%M:%S"):
        _dt = time.strftime(format, time.localtime(_stamp))
        return _dt

    @classmethod
    def before_day(cls, _now, _day, format="%Y-%m-%d %H:%M:%S"):
        _time = (_now - datetime.timedelta(days=_day)).strftime(format)
        return _time

    @classmethod
    def after_day(cls, _now, _day, format="%Y-%m-%d %H:%M:%S"):
        _time = (_now + datetime.timedelta(days=_day)).strftime(format)
        return _time


def format_time_cost(val):
    result = ''
    if val >= 3600:
        hour = int(val / 3600)
        result += str(hour) + '小时'
        val -= hour * 3600
    if val >= 60:
        min = int(val / 60)
        result += str(min) + '分'
        val -= min * 60
    result += str(val) + '秒'
    return result


def now_time(format='%Y-%m-%d %H:%M:%S'):
    # 获取当前时间str
    now = datetime.datetime.now().strftime(format)
    return now


def make_timestamp(time_str=None, format='%Y-%m-%d'):
    if not time_str or time_str == 'NaT':
        time_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        format = '%Y-%m-%d %H:%M:%S'
    d = datetime.datetime.strptime(time_str, format)
    t = d.timetuple()
    _timestamp = int(time.mktime(t))
    return _timestamp


def sleep(seconds):
    time.sleep(seconds)


def get_current_second():
    return int(round(time.time()))


def get_current_millisecond():
    return int(round(time.time() * 1000))


class TimeCost(object):
    __start = get_current_millisecond()

    @classmethod
    def show_time_diff(cls):
        return format_time_cost((get_current_millisecond() - cls.__start) / 1000)

    @classmethod
    def reset_time(cls):
        cls.__start = get_current_millisecond()

    @classmethod
    def time_diff(cls):
        return get_current_millisecond() - cls.__start


if __name__ == '__main__':
    _now = TimeEtl.ge_now()
    # _str_time = TimeEtl.time_stamp(_now)
    # print(_str_time)
    #
    _time = TimeEtl.before_day(_now, 5)
    print(_time)
