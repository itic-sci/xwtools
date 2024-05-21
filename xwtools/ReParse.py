#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved
Author: xuwei
Email: weix@sensedeal.ai
Description:
'''

import re


class Parse(object):
    def __init__(self):
        pass

    # 经过排除正则后返回为真继续处理
    @classmethod
    def __exclude_parse(cls, txt, exclude):
        if type(txt).__name__ != 'str':
            return False
        # 去除含有某些字段的句子
        if exclude == '':
            return True
        out = re.compile(exclude)
        rs = out.search(txt)
        if rs:
            return False
        else:
            return True

    @classmethod
    def match_label(cls, txt, contain, label='match'):
        # 含有某些字段的句子
        p = re.compile(contain)
        r = p.search(txt)
        if r:
            return label
        else:
            return None

    # 逗号代表and，包含规则和排除规则都可以有
    @classmethod
    def __and_exclude(cls, txt, exclude):
        if ',' in exclude:
            _exclude_list = exclude.split(',')
            _res_list = []
            for _exclude in _exclude_list:
                _res_list.append(cls.__exclude_parse(txt, _exclude))
            if len(set(_res_list)) == 1 and _res_list[0] == False:
                return False
        else:
            _remove_res = cls.__exclude_parse(txt, exclude)
            if _remove_res == False:
                return False
        return True

    # 逗号代表and，包含规则和排除规则都可以有
    @classmethod
    def __and_contain(cls, txt, contain, label):
        if ',' in contain:
            _contain_list = contain.split(',')
            for _contain in _contain_list:
                _res = cls.match_label(txt, _contain, label)
                if _res == None:
                    return None
            return label
        else:
            _res = cls.match_label(txt, contain, label)
            return _res

    @classmethod
    def re_match_label(cls, txt, contain, exclude='', label='match'):
        """
        :param txt:
        :param contain: 包含正则
        :param exclude: 排除正则
        :param label: txt标签
        :return:
        """
        if exclude:
            # 分号代表排除规则有一个没通过则不通过
            _exclude_list = exclude.split(';')
            for _exclude in _exclude_list:
                _exclude_state = cls.__and_exclude(txt, _exclude)
                if not _exclude_state:
                    return None

        _contain_list = contain.split(';')
        for _contain in _contain_list:
            _match_res = cls.__and_contain(txt, _contain, label)
            if _match_res:
                return _match_res
        return None

    @classmethod
    def re_find_all(cls, txt, contain):
        # 含有某些字段的句子
        p = re.compile(contain)
        r = p.findall(txt)
        return r

    # 逗号代表and，包含规则和排除规则都可以有
    @classmethod
    def __round_find_all(cls, txt, contain, exclude='', sep_and=','):
        """
        :param txt:
        :param contain: 包含正则
        :param exclude: 排除正则
        :param label: txt标签
        :return:
        """
        if sep_and in exclude:
            _exclude_list = exclude.split(',')
            _res_list = []
            for _exclude in _exclude_list:
                _res_list.append(cls.__exclude_parse(txt, _exclude))
            if len(set(_res_list)) == 1 and _res_list[0] == False:
                return []
        else:
            _remove_res = cls.__exclude_parse(txt, exclude)
            if _remove_res == False:
                return []

        if sep_and in contain:
            _contain_list = contain.split(',')
            _res_list = []
            for _contain in _contain_list:
                _list = cls.re_find_all(txt, _contain)
                if not _list:
                    return []
                _res_list += _list
            return _res_list
        else:
            _res_list = cls.re_find_all(txt, contain)
            return _res_list

    @classmethod
    def re_replace(cls, txt, replaced_regexp, new_str):
        match_res_list = cls.re_find_all(txt, replaced_regexp)
        for mm in match_res_list:
            txt = txt.replace(mm, mm.replace(mm, new_str))
        return txt

    @classmethod
    def get_start_end_list(cls, word, txt):
        pos_list = [i.start() for i in re.finditer(word, txt)]
        res_list = []
        for start_pos in pos_list:
            end_pos = start_pos + len(word)
            res_list.append({'start': start_pos, 'end': end_pos})
        return res_list



if __name__ == '__main__':
    _txt = "一季度增长6.4%，二季度中国GDP增速或将下滑到6.3%！那美国呢？"
    _contain = '季度|美国'
    _exclude = ''
    r = Parse.re_find_all(_txt, _contain)
    # r = Parse.re_match_label('徐威喜欢数据文件', '徐|喜欢,文件')
    print(r)
