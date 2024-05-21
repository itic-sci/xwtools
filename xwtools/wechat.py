#!/usr/bin/env python
# -*- coding: utf-8 -*-

#                                                           
# Copyright (C)2017 SenseDeal AI, Inc. All Rights Reserved  
#                                                           

"""                                                   
File: wechat.py
Author: lzl
E-mail: zll@sensedeal.ai
Last modified: 2019/9/18
Description:                                              
"""

import json
import requests
from .config_log import config, log


class WechatFactory(object):
    def __init__(self, label):
        self.key = config(label, 'key')
        self.url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key='
        self.headers = {'Content-Type': 'application/json'}

    def send_message(self, content: str, phone: list):
        try:
            message = {
                "msgtype": "text",
                "text": {
                    "mentioned_list": [],
                    "content": content,
                    "mentioned_mobile_list": phone
                }
            }
            _code = 0
            res = requests.post(self.url + self.key, headers=self.headers, data=json.dumps(message), timeout=10)
            if res.status_code == 200:
                return _code
        except Exception as e:
            log.info(e.__str__())
            _code = -1
        return _code

