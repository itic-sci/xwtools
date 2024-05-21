#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                          
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved                                                      
Author: xuwei                                        
Email: weix@sensedeal.ai                                 
Description:                                    
'''

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED,as_completed
from multiprocessing import Pool
import multiprocessing as mp
from .config_log import log



"""
Python3.2开始，标准库为我们提供了concurrent.futures模块，它提供了ThreadPoolExecutor和ProcessPoolExecutor两个类，
实现了对threading和multiprocessing的进一步抽象，对编写线程池/进程池提供了直接的支持。
"""


def multi_process_map(work_fun, itr_arg, pool_num=3, if_return=False):
    pool = Pool(pool_num)
    res = pool.map(work_fun, itr_arg)
    pool.close()  # 关闭进程池
    pool.join()  # 等待子进程结束
    if if_return:
        return res


# 函数只接受一个参数，然后得到所有函数的返回结果
def multi_process_by_pool(work_fun, itr_arg, pool_num=3, timeout=None):
    res_list = []
    pool = Pool(pool_num)
    all_tasks = [pool.apply_async(work_fun, args=(arg, )) for arg in itr_arg]
    for task in all_tasks:
        try:
            res_list.append(task.get(timeout=timeout))
        except:
            res_list.append(None)
    return res_list


"""
一定要使用with，而不要使用for，如果你一定要用for，那么一定要手动进行executor.shutdown，而你使用了with方法的话，
    在with方法内部已经实现了wait(),在使用完毕之后可以自行关闭线程池，减少资源浪费。

如果不用with：
    executor = ThreadPoolExecutor(max_workers=thread_num)
    all_task = [executor.submit(run_main, (each)) for each in var_list]
    executor.shutdown(wait=True) 等待所有线程执行完毕，相当于进程池的pool.close()+pool.join()操作，wait=True，等待池内所有任务执行完毕回收完资源后才继续，
    wait=False，立即返回，并不会等待池内的任务执行完毕，但不管wait参数为何值，整个程序都会等到所有任务执行完毕，默认True。

    如果不用shutdown，还可以用wait方法让主线程阻塞，直到满足设定的要求。
    wait(all_task, return_when=ALL_COMPLETED) wait方法接收3个参数，等待的任务序列、超时时间以及等待条件。
    等待条件return_when默认为ALL_COMPLETED，表明要等待所有的任务都结束。可以看到运行结果中，确实是所有任务都完成了，
    主线程才打印出main。等待条件还可以设置为FIRST_COMPLETED，表示第一个任务完成就停止等待。
"""


def multi_thread_map(work_fun, itr_arg, thread_num, if_return=False, timeout=None):
    ###多线程
    with ThreadPoolExecutor(max_workers=thread_num) as executor:
        res = executor.map(work_fun, itr_arg, timeout=timeout)
    if if_return:
        return list(res)


# 多线程返回控制，并返回线程值
def multi_thread_return(work_fun, itr_arg, thread_num=3, timeout=None, return_when=FIRST_COMPLETED, if_wait=False):
    res_list = []
    executor = ThreadPoolExecutor(max_workers=thread_num)
    all_tasks = [executor.submit(work_fun, args) for args in itr_arg]
    wait(all_tasks, return_when=return_when)
    executor.shutdown(wait=if_wait)
    for future in all_tasks:
        try:
            res_list.append(future.result(timeout=timeout))
        except:
            res_list.append(None)
    return res_list


# 多线程返回控制，并返回线程值
def multi_thread_return_funs_list(work_funs_list, thread_num=3, timeout=None, return_when=FIRST_COMPLETED, if_wait=False):
    res_list = []
    executor = ThreadPoolExecutor(max_workers=thread_num)
    all_tasks = [executor.submit(work_fun, ) for work_fun in work_funs_list]
    wait(all_tasks, return_when=return_when)
    executor.shutdown(wait=if_wait)
    for future in all_tasks:
        try:
            res_list.append(future.result(timeout=timeout))
        except Exception as e:
            log.exception(e)
            res_list.append(None)
    return res_list


def multi_process(func, args):
    p = mp.Process(target=func, args=args)
    p.start()  # 子进程 开始执行
    p.join()  # 等待子进程结束
    pass


def fun(a):
    print(sum(a))
    return sum(a)


if __name__ == '__main__':
    pass
    current_labeled = multi_process_map(fun, ([0, 1], [2, 3], [4]), 4, if_return=True)
    data = multi_thread_map(fun, ([0, 1], [2, 3], [4]), 2, if_return=True)
    print(data)
