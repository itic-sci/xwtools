#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                                                                             
Author: xuwei                                        
Email: 18810079020@163.com                                 
File: test_multi_process_thread.py
Date: 2020/4/28 12:05 下午
'''

import time
from xwtools.multi_process_thread import multi_process_map, multi_thread_map, multi_thread_return, multi_process_by_pool


def fun(a):
    time.sleep(a[-1])
    print(sum(a))
    return sum(a)


if __name__ == '__main__':
    pass
    start = time.time()
    data = multi_thread_return(fun, ([0, 2], [2, 3], [3, 4]), thread_num=3, timeout=1)
    end = time.time()
    print(end - start)
    print(data)


    start = time.time()
    # data = multi_process_map(fun, ([0, 1], [2, 3], [4]), pool_num=3, if_return=True)
    data = multi_process_by_pool(fun, ([0, 2], [2, 3], [3, 4]), pool_num=3, timeout=1)
    # data = multi_thread_map(fun, ([0, 2], [2, 3], [4]), 3, if_return=True, timeout=1)
    # data = multi_thread_return(fun, ([0, 2], [2, 3], [3, 4]), 6, timeout=0.2)
    end = time.time()
    print(end - start)
    print(data)
