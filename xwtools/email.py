#!/usr/bin/env python
# -*- coding: utf-8 -*-

#                                                           
# Copyright (C)2017 SenseDeal AI, Inc. All Rights Reserved  
#                                                           

"""                                                   
File: email.py
Author: lzl
E-mail: zll@sensedeal.ai
Last modified: 2019/10/16
Description:                                              
"""

from .config_log import config, log
from email.mime.text import MIMEText
from email.header import Header
import smtplib
from email.utils import formataddr


class EmailFactory(object):
    def __init__(self, label):
        self.sender = config(label, 'sender')
        self.password = config(label, 'password')
        self.receivers = config(label, 'receivers')

    def send_message(self, content: str, title: str):
        """
        写入标题与内容发送邮件，成功时返回0
        :param content:
        :param title:
        :return:
        """
        try:
            _code = 0
            if '@126' in self.sender:
                mail_msg = content
                message = MIMEText(mail_msg, 'html', 'utf-8')
                message['From'] = Header(title, 'utf-8')
                message['To'] = self.receivers
                message['Subject'] = title
                server = smtplib.SMTP("smtp.126.com", 25)
                server.login(self.sender, self.password)
                server.sendmail(self.sender, self.receivers.split(','), message.as_string())
                server.quit()
            else:
                _code = -1
                log.info("不支持此邮件")
                return _code
            return _code
        except Exception as e:
            log.info(e.__str__())
            _code = -1
        return _code


def send_mail(subject, content, receiver):
    my_sender = 'm13911639030@163.com'
    msg = MIMEText(content, 'plain', 'utf-8')
    msg['From'] = formataddr(["DailyCheck", my_sender])
    msg['To'] = formataddr(["xuwei", receiver])
    msg['Subject'] = subject  # 邮件的主题，也可以说是标题
    server = smtplib.SMTP("smtp.163.com", 25)
    server.login(my_sender, "sdai@2018")
    server.sendmail(my_sender, [receiver], msg.as_string())
    server.quit()
