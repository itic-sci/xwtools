#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                                                                             
Author: xuwei                                        
Email: 18810079020@163.com                                 
File: text_process.py
Date: 2021/9/16 8:46 下午
'''

import re
import jieba
import difflib
from collections import deque

stopwords = ['的', '是什么']
re_sub_regexp = '|'.join(stopwords)
re_sub = re.compile(re_sub_regexp)

re_punctuation = re.compile(
    "[！？。｡＂＃＄％＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、〃《》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘’‛“”„‟…‧﹏.!\"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]+")


class TextMain:

    # 移除停用词
    @classmethod
    def remove_stop_words(cls, text):
        return re_sub.sub('', text)

    @classmethod
    def remove_punctuation(cls, text):
        """
            去掉标点符号(非文本部分)
            #中文标点 ！？｡＂＃＄％＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、〃》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘’‛“”„‟…‧﹏.
            #英文标点 !"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~
            """
        text = re_punctuation.sub('', f'{text}')
        return text

    @classmethod
    def process_text(cls, text):
        text = cls.remove_punctuation(text)
        text = cls.remove_stop_words(text)
        return text

    @classmethod
    def max_substring_length(cls, target_text, compare_text):
        """
        求两个字符串的最长公共子串的长度
        """
        seqMatch = difflib.SequenceMatcher(None, target_text, compare_text, autojunk=False)
        match = seqMatch.find_longest_match(0, len(target_text), 0, len(compare_text))
        # print(match)
        return match.size

    @classmethod
    def ratio_substring(cls, target_text, compare_text):
        """
        最长子串占目标文本长度的比率
        """
        substring_length = cls.max_substring_length(target_text, compare_text)
        return substring_length / len(target_text)

    @classmethod
    def jieba_cut(cls, sentence):
        setlast = jieba.lcut(sentence, cut_all=False)
        seg_list = [i.lower() for i in setlast if i not in stopwords]
        return " ".join(seg_list)

    @classmethod
    def public_substrings_list(cls, target_text: str, compare_text: str):
        """
        compare_text文本中含有target_text子串的列表
        :param target_text:
        :param compare_text:
        :return:
        """
        queue = deque(list(target_text))
        max_equal_str_length = 0
        max_equal_str = ''
        substring_list = []
        i = 0
        while queue:
            v = queue.popleft()
            for j in range(len(compare_text)):
                if v != compare_text[j]: continue
                equal_str = v
                for t, c in zip(target_text[i + 1:], compare_text[j + 1:]):
                    if t != c: break
                    equal_str += t
                if len(equal_str) > max_equal_str_length:
                    max_equal_str_length = len(equal_str)
                    max_equal_str = equal_str

            if max_equal_str:
                substring_list.append(max_equal_str)
                max_equal_str = ''

            i += max_equal_str_length if max_equal_str_length else 1
            for n in range(max_equal_str_length - 1):
                if not queue: break
                queue.popleft()
            max_equal_str_length = 0

        return substring_list

    @classmethod
    def similar_ratio_text_way1(cls, target_text: str, compare_text: str):
        """
        一种 compare_text 和 target_text的相似度计算方法，求 target_text 在 compare_text中的公共子串
        :param target_text:
        :param compare_text:
        :return:
        """
        substring_list = cls.public_substrings_list(target_text, compare_text)
        substring_length = sum((len(x) for x in substring_list))

        r1 = substring_length / len(target_text)
        r2 = substring_length / len(compare_text)
        if r2 > 1: r2 = 1
        ratio = (r1 + r2) / 2
        return ratio


if __name__ == '__main__':
    target_text, compare_text = "智谱华章", '北京智谱华章有限公司怎么样'
    r = TextMain.public_substrings_list(target_text, compare_text)
    print(r)

    r = TextMain.similar_ratio_text_way1(target_text, compare_text)
    print(r)
