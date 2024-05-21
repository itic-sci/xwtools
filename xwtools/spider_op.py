#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved
Author: xuwei
Email: weix@sensedeal.ai
Description:
'''

import time
import uuid
import json
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class SpiderOp(object):
    def __init__(self, executable_path='chromedriver', headless=None,
                 ip=None, wait_time=1, try_num=2, time_to_wait_load=None,
                 downLoad=False, downLoadPath='./', js_str=None, options=None):
        """
        :param executable_path:
        :param headless:
        :param ip:
        :param wait_time:
        :param try_num:
        :param time_to_wait_load:
        :param downLoad:
        :param downLoadPath:
        :param js_str: with open('/Users/kingname/test_pyppeteer/stealth.min.js') as f: js = f.read()
        """
        if options:
            self.opt = options
        else:
            self.opt = webdriver.ChromeOptions()  # 创建Chrome参数对象
        # self.opt.headless = headless  # 把True Chrome设置成可视化无界面模式，windows/Linux 皆可
        if headless:
            self.opt.add_argument('--no-sandbox')  # 解决DevToolsActivePort文件不存在的报错
            self.opt.add_argument('window-size=1920x1080')  # 指定浏览器分辨率
            self.opt.add_argument('--disable-gpu')  # 谷歌文档提到需要加上这个属性来规避bug
            self.opt.add_argument('log-level=3')
            self.opt.add_experimental_option('excludeSwitches', ['enable-automation'])
            self.opt.add_argument('--headless')
            self.opt.add_argument(
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36")
            self.opt.add_argument("--disable-dev-shm-usage")
            self.opt.add_argument("lang=zh_CN.UTF-8")

        if downLoad:
            prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': downLoadPath}
            self.opt.add_experimental_option('prefs', prefs)


        if executable_path.startswith('http'):
            # command_executor="http://127.0.0.1:4444/wd/hub" docker selenium环境http地址
            self.driver = webdriver.Remote(
                command_executor=executable_path, desired_capabilities=DesiredCapabilities.CHROME,
                                           options=self.opt)  # 没有把Chromedriver放到python安装路径
        else:
            self.driver = webdriver.Chrome(executable_path=executable_path, options=self.opt)  # 没有把Chromedriver放到python安装路径

        if time_to_wait_load:
            self.driver.set_page_load_timeout(time_to_wait_load)
            self.driver.set_script_timeout(time_to_wait_load)

        if js_str:
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": js_str
            })

        self.driver_url = None
        self.wait_time = wait_time
        self.try_num = try_num
        self.current_url = ''
        self.proxy = {'https': ip}
        self.head = {
            'Host': 'emdata.pd.eastmoney.com',
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15', }

    def get_selenium_test(self, url):
        self.driver.get(url)
        time.sleep(600)

    def __send_by_xpath_list(self, send_xpath_dict, raise_error=None, by=By.XPATH):
        if send_xpath_dict:
            for send_xpath in send_xpath_dict:
                try:
                    self.driver.find_element(by, send_xpath).clear()
                    self.driver.find_element(by, send_xpath).send_keys(send_xpath_dict[send_xpath])
                except Exception as e:
                    if raise_error:
                        raise e
                    # print(send_xpath)
                    # print('__send_by_xpath_list error: %s' % e)

    def __click_by_xpath_list(self, click_xpath_list, raise_error=None, by=By.XPATH):
        if click_xpath_list:
            for click_xpath in click_xpath_list:
                num = 0
                while num < self.try_num:
                    try:
                        self.driver.find_element(by, click_xpath).click()
                        time.sleep(self.wait_time)

                        windows = self.driver.window_handles
                        self.current_url = self.driver.current_url
                        if len(windows) > 1:
                            self.driver.close()
                            self.driver.switch_to.window(windows[-1])
                        break
                    except Exception as e:
                        if raise_error:
                            raise e
                        num += 1
                        # print(self.driver.page_source)
                        # print(click_xpath)
                        # print('__click_by_xpath_list error: %s' % e)
                        time.sleep(self.wait_time)

    def __get_data_list_by_xpath_list(self, text_xpath_list, format='text', raise_error=None, by=By.XPATH):
        xpath_data_list = []
        if text_xpath_list:
            for xpath in text_xpath_list:
                try:
                    div = self.driver.find_element(by, xpath)
                    if format == 'html':  # 用于BeautifulSoup
                        html = div.get_attribute('innerHTML')
                        xpath_data_list.append(html)
                    else:
                        xpath_data_list.append(div.text)
                except Exception as e:
                    if raise_error:
                        raise e
                    # print(xpath)
                    # print('__get_data_list_by_xpath_list error: %a' % e)
        return xpath_data_list

    def __need_appear_xpath(self, need_appear_xpath: str, by=By.XPATH):
        if need_appear_xpath:
            self.driver.find_element(by, need_appear_xpath)

    def get_data_by_selenium_xpath(self, url,
                                   send_xpath_dict=None,
                                   click_xpath_list=None,
                                   need_appear_xpath=None,  # 点击之后必须存在的xpath，否则报错
                                   wait_xpath=None,
                                   text_xpath_list=None,
                                   format='text',  # html or text
                                   reload_url=False,
                                   raise_error=None,
                                   wait_time=2,
                                   mouse_pull=False,
                                   page_height_xpath=None,
                                   by=By.XPATH
                                   ):
        if not self.driver_url or reload_url:
            try:
                self.driver.get(url)
            except Exception as e:
                print(f"加载页面太慢，停止加载，继续下一步操作")
                print(e)
                self.driver.execute_script("window.stop()")
            self.driver_url = True

        self.__send_by_xpath_list(send_xpath_dict, raise_error=raise_error)
        self.__click_by_xpath_list(click_xpath_list, raise_error=raise_error)
        self.__need_appear_xpath(need_appear_xpath, by=by)

        if mouse_pull:
            if page_height_xpath:
                ele = self.driver.find_element(by, page_height_xpath)
                height = ele.size['height']
                loop = int(height / 1000)
            else:
                loop = 0
            # 一共下滑十次，下滑一次停顿0.5s
            for i in range(loop + 1):
                self.driver.execute_script("window.scrollBy(0, 1000)")
                time.sleep(0.5)

        self.__wait_until_xpath_appear(wait_xpath, timeout=wait_time)
        xpath_data_list = self.__get_data_list_by_xpath_list(text_xpath_list, format=format, raise_error=raise_error)
        return xpath_data_list

    def __wait_until_xpath_appear(self, wait_xpath, timeout=10):
        if wait_xpath:
            try:
                WebDriverWait(self.driver, timeout).until(EC.presence_of_element_located((By.XPATH, wait_xpath)))
                return True
            except Exception as e:
                raise e
                # return None

    def downloadBySelenium(self, url, downloadXpath: str, by=By.XPATH):
        try:
            self.driver.get(url)
        except:
            print("加载页面太慢，停止加载，继续下一步操作")
            # driver.execute_script("window.stop()")
        self.driver.find_element(by, downloadXpath).click()

    def close_selenium(self):
        self.driver.close()

    def get_json_data(self, url, cookies):
        try:
            # _data = requests.get(url, headers=self.head, proxies=self.proxy)
            _data = requests.get(url, headers=self.head, cookies=cookies, verify=False)
            # print(_data)
            _data_dict = json.loads(_data.text)
            return _data_dict
        except Exception as e:
            # print(e)
            raise e

    def get_html_data(self, url):
        # _data = requests.get(url, headers=self.head, proxies=self.proxy)
        _data = requests.get(url)
        _data.encoding = 'utf-8'
        _soup = BeautifulSoup(_data.text, 'lxml')
        print(_soup)
        return _soup

    def request_download(self, url, path):
        try:
            r = requests.get(url)
            _uuid = str(uuid.uuid1())
            with open(path + _uuid + '.png', 'wb') as fp:
                fp.write(r.content)
        except Exception as e:
            print(e)

    def chunk_download(self, url, path):
        try:
            r = requests.get(url)
            _uuid = str(uuid.uuid1())
            with open(path + _uuid + '.png', 'wb') as fp:
                for chunk in r.iter_content(chunk_size=32):
                    fp.write(chunk)
        except Exception as e:
            print('chunk_download error %s' % e)


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
