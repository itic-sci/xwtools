#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                                                                             
Author: xuwei                                        
Email: 18810079020@163.com                                 
File: TextSimilarity.py
Date: 2021/12/1 9:20 上午
'''

import jieba
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer


"""
通过 同义词林、jaccard、余弦相似度、BM25、DSSM 这几个算法计算两句话的语义相似度
"""
class TextSimilarity:

    @classmethod
    def _getCountVectorizer(cls, text_1: str, text_2: str):
        seg_list_1 = jieba.lcut(text_1, cut_all=False)
        sentence_1 = " ".join(seg_list_1)

        seg_list_2 = jieba.lcut(text_2, cut_all=False)
        sentence_2 = " ".join(seg_list_2)

        vocab = {}
        for word in sentence_1.split():
            vocab[word] = 0  # 生成所在句子的单词字典，值为0

        for word in sentence_2.split():
            vocab[word] = 1

        cv = CountVectorizer(vocabulary=vocab.keys())
        text_1_vector = cv.fit_transform([sentence_1])
        text_2_vector = cv.fit_transform([sentence_2])
        return text_1_vector, text_2_vector

    @classmethod
    def _get_vector(cls, text_1: str, text_2: str):
        text_1_vector, text_2_vector = cls._getCountVectorizer(text_1, text_2)
        return text_1_vector.toarray()[0], text_2_vector.toarray()[0]

    # 同义词林
    @classmethod
    def SynonymLin(cls, text_1: str, text_2):
        pass

    # jaccard
    @classmethod
    def Jaccard(cls, text_1: str, text_2):
        terms_reference = jieba.cut(text_1)  # 默认精准模式
        terms_model = jieba.cut(text_2)
        grams_reference = set(terms_reference)  # 去重；如果不需要就改为list
        grams_model = set(terms_model)
        temp = 0
        for i in grams_reference:
            if i in grams_model:
                temp = temp + 1
        fenmu = len(grams_model) + len(grams_reference) - temp  # 并集
        jaccard_coefficient = float(temp / fenmu)  # 交集
        return jaccard_coefficient

    # 余弦相似度
    @classmethod
    def CosineDistance(cls, text_1: str, text_2):
        docVector, sentenceVector = cls._getCountVectorizer(text_1, text_2)
        similarity = cosine_similarity(docVector, sentenceVector)[0][0]
        return similarity

    # BM25
    @classmethod
    def BM25(cls, text_1: str, text_2):
        pass

    # DSSM
    @classmethod
    def DSSM(cls, text_1: str, text_2):
        pass

if __name__ == '__main__':
    r1,r2 = TextSimilarity._get_vector("悲电流放2悲伤13大倍数", "电流放大倍数悲伤")
    print(r1)
    print(r2)

    r = TextSimilarity.CosineDistance("悲电流放2悲伤13大倍数", "电流放大倍数悲伤")
    print(r)