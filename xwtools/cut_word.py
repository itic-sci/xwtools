#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved
File: .py
Author: xuwei
Email: weix@sensedeal.ai
Last modified: date
Description: 对原始文本进行处理，分词后保存txt，供word2vec训练词向量使用。
'''


import jieba
import re

class TextEtl(object):

    def __init__(self, stopWordPath = None):
        self.stopwords = None
        if stopWordPath:
            self.stopwords = [line.strip() for line in open(stopWordPath, 'r', encoding='utf-8').readlines()]

    def numLower(self, text):
        # constants for chinese_to_arabic
        cn_num = {
            '〇': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '零': 0,
            '壹': 1, '贰': 2, '叁': 3, '肆': 4, '伍': 5, '陆': 6, '柒': 7, '捌': 8, '玖': 9, '貮': 2, '两': 2,
            '仟': '000', }
        for _key in cn_num:
            text = text.replace(_key, str(cn_num[_key]))
        return text

    def filter_emoji(self, line):
        """
        过滤表情
        """
        try:
            # python UCS-4 build的处理方式
            highpoints = re.compile(u'[\U00010000-\U0010ffff]')
        except re.error:
            # python UCS-2 build的处理方式
            highpoints = re.compile(u'[\uD800-\uDBFF][\uDC00-\uDFFF]')

        return highpoints.sub(u'', line)

    def remove_other(self, line: str):
        """
        去除txt文本中的空格、数字、特定字母等
        #去掉文本行里面的空格、换行\t、数字（其他有要去除的也可以放到' \t1234567890'里面）
        """
        lines = filter(lambda ch: ch not in ' \t1234567890☝✌ ☺', line)
        return ''.join(lines)

    def remove_punctuation(self, line):
        """
        去掉标点符号(非文本部分)
        #中文标点 ！？｡＂＃＄％＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、〃》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘’‛“”„‟…‧﹏.
        #英文标点 !"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~
        """
        try:
            line = re.sub(
                "[！？。｡＂＃＄％＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、〃《》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘’‛“”„‟…‧﹏.!\"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]+",
                "", line)
        except Exception as e:
            print(e)
        return line

    def jieba_cut_filter(self, text):
        text = text.encode("utf-8")
        word_list = jieba.cut(text)
        word_list = list(filter(lambda x: len(x) > 1, word_list))  # 去掉长度小于1的词
        return word_list

    def remove_stopwords(self, word_list):
        """
        创建停用词list很专业，这里也可以过滤标点符号，替代函数def remove_punctuation(line)，
        参数line_cutted是分词后的列表。
        """
        if self.stopwords:
            word_list = list(filter(lambda x: x not in self.stopwords, word_list))
        return word_list

    def getWordListByCut(self, sentence):
        """
        对一行句子进行分词，去数字、空格、换行和标点符合等，并丢弃停用词，可以实现Series的并行处理
        """
        #jieba添加词典
        #     jieba.add_word('非负面')
        text = sentence.replace("\n", "")
        text = text.replace('\\r', '')
        text = text.replace('\\n', '')
        text = text.replace('\r', '')
        text = text.replace('　　', '。')
        text = text.replace(' ', '')

        text = self.filter_emoji(text)
        text = self.remove_other(text)
        text = self.remove_punctuation(text)
        word_list = self.jieba_cut_filter(text)
        word_list = self.remove_stopwords(word_list)  #分词后的列表去除停用词
        return word_list


if __name__ == '__main__':
    sentence = '　　墨斐、孙晓东加盟后，观致汽车正在迅速组建“通用队”。3月16日，观致员工收到的内部邮件显示：来自上海通用的徐宛将担任观致汽车市场与销售部公关总监。而就在不到一周之前的3月12日，也就是原观致公关总监梁虹离职的同一天，观致的员工收到由孙晓东亲自签发的信函，凌海加盟观致汽车，担任市场与销售部网络总监。\r\n　　徐宛和凌海均来自“通用系”，徐最早曾任职上海通用公关部，加盟观致前任职雪佛兰品牌部；而凌是第一任雪佛兰品牌总监，此后加盟吉利汽车，担任吉利汽车副总经理。\r\n　　从孙晓东加盟观致到墨斐加盟，观致的营销岗位纷纷开始由“通用系”接手，而上海通用是业内公认的“营销之王”，这也被视为墨斐要为观致下一剂营销“猛药”的开端。\r\n　　“通用队”雏形\r\n　　3月12日孙晓东签发的邮件显示：凌海的主要工作是负责中国市场零售网络的扩张和优化。随着凌海和徐宛的到来，观致组建起了CEO墨斐、 市场及销售执行总监孙晓东为首的“通用队”雏形，而这个通用队，是要来解决观致的实际问题的。\r\n　　“观致目前要做的第一件事是要树立品牌定位，提高品牌的知名度。”2月2日墨斐上任观致CEO的七个小时后就曾表示。而根据董事会给他的目标：观致的目标是要让中国的企业，可以在国际市场竞争，可以与其他国际品牌媲美。\r\n　　不过，没有销量作为基础，一切都是空谈。“孙总来了以后，提出第一步今年3月销量破千的目标。”观致知情人士透露，而之前，观致的月销量仅百辆级别。而销售网络的能力，是决定销量很重要的因素，之前，观致也一直受困于网络发展速度太慢。\r\n　　近年来，随着市场进入平稳期，有实力的经销商纷纷转向豪华车，包括一汽丰田、斯柯达等合资企业，都不断有经销商退出，这对于全新的观致，是致命的要害。观致的网络发展一直被业内垢病，至今为止，观致的网络远未达到目标，去年接受记者采访时，时任观致CEO郭谦和市场及销售执行总监卫思梵纷纷表示了对网络发展的不满意。\r\n　　而孙晓东的这份邮件表示：凌海以前的工作经验，将使其很胜任这份工作，帮助观致达到2015年的目标。\r\n　　大众制造、通用营销\r\n　　上海通用的营销能力是有口皆碑的，“通用队”组建后，推动在营销层面尽快建立起通用的流程和体系并不难。虽然处于这样高压的环境，但墨斐并未对观致的设计团队进行调整。以何歌特带领的观致设计团队并无变化，而产品也按照之前的既定计划在推进。在产品上，观致仍然会延续之前的高标准，仍然以国际化标准对标大众。\r\n　　墨斐本身虽然不是中国人，但绝对是中国通，他曾经为了与上汽之间更畅通的合作，而与时任上汽董事长胡茂元坐在一个办公室办公。\r\n　　上任观致第一天，记者连线墨斐，他便在电话中告诉记者，观致面前最缺的是品牌。品牌的打造最关键是要了解消费者需求。\r\n　　郭谦带领的第一代观致管理层，将观致对标于大众，郭谦多次表示，观致是“大众品质，现代价格”。观致的确做到了大众品质。\r\n　　“观致从产品上看已经实现了国际标准，在国际市场也有竞争力，所以我对达成目标非常有信心。”墨斐也承认。不过观致并没有做到现代价格，原因很简单，汽车是一个以规模论成本的产业，同样的品质，规模越大，成本越低。\r\n　　虽然与大众相比，观致不需要向总部缴纳技术转让费，但从无到有打造一个平台，为之付出的成本也非常高昂。此前，有消息称观致的投资已经超过100亿人民币。如果销量能迅速打开，100亿人民币的投资额并不算大，一旦销量不能达到预期目标，“节流”成为第二代管理层不得不面对的现实问题。\r\n　　人才本土化\r\n　　“当时我在大众，本来非常安稳，为了梦想才来到观致。”梁虹在离职后的第一时间这样对记者说。她说她并不后悔当初的选择。与任何一个前期创业者一样，处于起步期的观致，需要高强度的投入，每个人都为它付出了沉重的代价。\r\n　　实际上，在梁之前，质量部的执行总监Ralf已经离开观致，制造部的执行总监和网络部总监也已离职，除了主动离开的梁虹，其中也不乏有“被离职”的。据观致的内部员工透露，墨斐到观致以后，将裁员列入计划。并提出了外籍员工减少50位，中方籍员工减少200位的裁员目标。\r\n　　记者了解到，墨斐的裁员计划已经被分解到各个部门，不过，由于观致主动离职的也不在少数，所以，这一裁员计划给内部员工造成的压力并不大。但是，对于外籍员工还是有一定压力的。\r\n　　“同样的职位，外籍员工的收入远远高于本土员工，这是行业的惯例。”观致员工透露。对于一家实际销量远远低于预期销量，连续处于亏损状态的企业而言，墨斐上任后第一时间裁员，就是想改善观致的盈利状态。而有消息称，之前因为缺乏了解本土市场的人才，观致大量启用高成本的外籍顾问和咨询公司，而大量启用本土人才后，墨斐也可能会减少这部分投入。\r\n　　人才本土化是美系企业区别于欧系、日系乃至韩系企业很重要的一个因素，美系企业一般大胆启用本土人才，以上海通用为例子，除了核心部门以外，基本都是本土化人才。“观致一些部门执行总监离职后，该职位尚在空缺期，很可能未来都会启用本土人才填补空缺。”知情人士透露，启用本土人才不仅仅成本相对更低，且本土人才更了解本土市场，更接地气，这对观致而言，可谓一举两得。\r\n】【、|'
    rs = TextEtl('./stop_word.txt').getWordListByCut(sentence)
    print(rs)
