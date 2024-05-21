#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                                                                             
Author: xuwei                                        
Email: 18810079020@163.com                                 
File: test_spider.py
Date: 2021/1/7 3:59 下午
'''

from xwtools import SpiderOp

if __name__ == '__main__':
    query = '二叉树'
    url_data = SpiderOp(headless=True, wait_time=2, try_num=2)
    _url = 'https://zhihu.sogou.com/'
    url_data.get_data_by_selenium_xpath(_url,
                                        send_xpath_dict={'//*[@id="query"]': query},
                                        click_xpath_list=['//*[@id="stb"]'],
                                        need_appear_xpath='//*[@id="main"]/div[1]/p',
                                        format='html', raise_error=True
                                        )

    res_list = url_data.get_data_by_selenium_xpath(_url,
                                                   click_xpath_list=[
                                                       '//*[@id="sogou_vr_30010170_0"]',
                                                       '//*[@id="TopicMain"]/div[3]/div/div/div/div[1]/div/div[2]/div[1]',
                                                       '//*[@id="TopicMain"]/div[3]/div/div/div/div[2]/div/div[2]/div[1]',
                                                       '//*[@id="TopicMain"]/div[3]/div/div/div/div[3]/div/div[2]/div[1]'
                                                   ],
                                                   text_xpath_list=[
                                                       '//*[@id="root"]/div/main/div/div[1]/div[1]/div/div',
                                                       '//*[@id="TopicMain"]/div[3]/div/div/div/div[1]/div',
                                                       '//*[@id="TopicMain"]/div[3]/div/div/div/div[2]/div',
                                                       '//*[@id="TopicMain"]/div[3]/div/div/div/div[3]/div',
                                                   ],
                                                   format='html'
                                                   )
    url = url_data.current_url.replace('top-answers', 'hot')
    # print(url)
    discuss_list = url_data.get_data_by_selenium_xpath(url,
                                                       click_xpath_list=[
                                                           '//*[@id="TopicMain"]/div[3]/div/div/div/div[1]/div/div[2]/div[1]',
                                                           '//*[@id="TopicMain"]/div[3]/div/div/div/div[2]/div/div[2]/div[1]',
                                                           '//*[@id="TopicMain"]/div[3]/div/div/div/div[3]/div/div[2]/div[1]'
                                                       ],
                                                       text_xpath_list=[
                                                           '//*[@id="TopicMain"]/div[3]/div/div/div/div[1]/div',
                                                           '//*[@id="TopicMain"]/div[3]/div/div/div/div[2]/div',
                                                           '//*[@id="TopicMain"]/div[3]/div/div/div/div[3]/div'
                                                       ],
                                                       format='html', reload_url=True
                                                       )
    res_list = res_list + discuss_list
    print(len(res_list))
    print(res_list[0])
    url_data.close_selenium()