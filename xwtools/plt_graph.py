#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                                                                             
Author: xuwei                                        
Email: 18810079020@163.com                                 
File: plt_graph.py
Date: 2020/7/6 4:10 下午
'''



import matplotlib.pyplot as plt

def series_plot_graph(series, title, file_path='.', xlabel='', ylabel='', width=8, height=7, fontsize=9, **kwargs):
    plt.figure(figsize=(width, height))
    ax = series.plot(kind='bar', fontsize=fontsize, **kwargs)
    plt.title(title, fontsize=fontsize)
    plt.xlabel(xlabel, fontsize=fontsize)
    plt.ylabel(ylabel, fontsize=fontsize)
    times_list = series.to_list()
    for i in range(len(times_list)):
        plt.text(i, times_list[i], times_list[i], ha='center', va='bottom',
                 fontdict={'size': fontsize, 'color': 'black'})
    plt.tight_layout() # 解决ticklabel或者title等过长、或者过大的话，显示不全的问题
    plt.show()
    fig = ax.get_figure()
    fig.savefig('%s/%s.png' % (file_path, title))