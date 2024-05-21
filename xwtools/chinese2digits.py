#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Author: xuwei
Email: 18810079020@163.com
File: chinese2digits.py
Date: 2021/4/7 11:54 上午
'''
import re
from typing import Union
import time
import pathlib
import logging

# 把汉语句子中的汉字（大小写）数字转为阿拉伯数字，不能识别“百分之”
common_used_numerals = {'零': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8,
                        '九': 9, '十': 10, u'〇': 0, u'壹': 1, u'贰': 2, u'叁': 3, u'肆': 4, u'伍': 5, u'陆': 6,
                        u'柒': 7, u'捌': 8, u'玖': 9, '拾': 10, '百': 100, '千': 1000, u'貮': 2, u'俩': 2, '佰': 100,
                        '仟': 1000, '萬': 10000, '万': 10000, '亿': 100000000, '億': 100000000, '兆': 1000000000000}


def chinese2digits(uchars_chinese):
    total = 0
    r = 1  # 表示单位：个十百千...
    for i in range(len(uchars_chinese) - 1, -1, -1):
        val = common_used_numerals.get(uchars_chinese[i])
        if val >= 10 and i == 0:  # 应对 十三 十四 十*之类
            if val > r:
                r = val
                total = total + val
            else:
                r = r * val
                # total =total + r * x
        elif val >= 10:
            if val > r:
                r = val
            else:
                r = r * val
        else:
            total = total + r * val
    return total


num_str_start_symbol = ['一', '二', '两', '三', '四', '五', '六', '七', '八', '九', '十',
                        '壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌', '玖', '拾', '貮', '俩', ]
more_num_str_symbol = ['零', '一', '二', '两', '三', '四', '五', '六', '七', '八', '九', '十', '百', '千', '万', '亿',
                       '〇', '壹', '贰', '叁', '肆', '伍', '陆', '柒', '捌', '玖', '拾', '貮', '俩', '佰', '仟', '萬', '億', '兆']


def ChineseNumToArab(oriStr):
    lenStr = len(oriStr)
    aProStr = ''
    if lenStr == 0:
        return aProStr
    hasNumStart = False
    numberStr = ''
    for idx in range(lenStr):
        if oriStr[idx] in num_str_start_symbol:
            if not hasNumStart:
                hasNumStart = True
            numberStr += oriStr[idx]
        else:
            if hasNumStart:
                if oriStr[idx] in more_num_str_symbol:
                    numberStr += oriStr[idx]
                    continue
                else:
                    numResult = str(chinese2digits(numberStr))
                    numberStr = ''
                    hasNumStart = False
                    aProStr += numResult
            aProStr += oriStr[idx]
            pass
    if len(numberStr) > 0:
        resultNum = chinese2digits(numberStr)
        aProStr += str(resultNum)
    return aProStr


def get_default_conf() -> dict:
    config_data = {
        'number_cn2an': {'零': 0, '一': 1, '壹': 1, '幺': 1, '二': 2, '贰': 2, '两': 2, '三': 3, '叁': 3, '四': 4, '肆': 4, '五': 5,
                         '伍': 5, '六': 6, '陆': 6, '七': 7, '柒': 7, '八': 8, '捌': 8, '九': 9, '玖': 9},
        'unit_cn2an': {'十': 10, '拾': 10, '百': 100, '佰': 100, '千': 1000, '仟': 1000, '万': 10000, '亿': 100000000},
        'unit_low_an2cn': {10: '十', 100: '百', 1000: '千', 10000: '万', 100000000: '亿'},
        'number_low_an2cn': {0: '零', 1: '一', 2: '二', 3: '三', 4: '四', 5: '五', 6: '六', 7: '七', 8: '八', 9: '九'},
        'number_up_an2cn': {0: '零', 1: '壹', 2: '贰', 3: '叁', 4: '肆', 5: '伍', 6: '陆', 7: '柒', 8: '捌', 9: '玖'},
        'unit_low_order_an2cn': ['', '十', '百', '千', '万', '十', '百', '千', '亿', '十', '百', '千', '万', '十', '百', '千'],
        'unit_up_order_an2cn': ['', '拾', '佰', '仟', '万', '拾', '佰', '仟', '亿', '拾', '佰', '仟', '万', '拾', '佰', '仟'],
        'strict_cn_number': {'零': '零', '一': '一壹', '二': '二贰', '三': '三叁', '四': '四肆', '五': '五伍', '六': '六陆', '七': '七柒',
                             '八': '八捌', '九': '九玖', '十': '十拾', '百': '百佰', '千': '千仟', '万': '万', '亿': '亿'},
        'normal_cn_number': {'零': '零', '一': '一壹幺', '二': '二贰两', '三': '三叁仨', '四': '四肆', '五': '五伍', '六': '六陆', '七': '七柒',
                             '八': '八捌', '九': '九玖', '十': '十拾', '百': '百佰', '千': '千仟', '万': '万', '亿': '亿'}}
    return config_data


def get_logger(name: str = "cn2an", level: str = "info") -> logging.Logger:
    # set logger
    logger = logging.getLogger(name)

    level_dict = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }
    logger.setLevel(level_dict[level])

    if not logger.handlers:
        # file handler
        log_path = log_path_util()
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.INFO)
        fh_fmt = logging.Formatter("%(asctime)-15s %(filename)s %(levelname)s %(lineno)d: %(message)s")
        fh.setFormatter(fh_fmt)

        # stream handler
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console_fmt = logging.Formatter("%(filename)s %(levelname)s %(lineno)d: %(message)s")
        console.setFormatter(console_fmt)

        logger.addHandler(fh)
        logger.addHandler(console)

    return logger


def log_path_util(name: str = "cn2an") -> str:
    day = time.strftime("%Y-%m-%d", time.localtime())
    log_path = pathlib.Path(f"./log/{day}")
    if not log_path.exists():
        log_path.mkdir(parents=True)
    return f"{str(log_path)}/{name}.log"


class Cn2An(object):
    def __init__(self) -> None:
        self.conf = get_default_conf()
        self.all_num = "".join(list(self.conf["number_cn2an"].keys()))
        self.all_unit = "".join(list(self.conf["unit_cn2an"].keys()))
        self.strict_cn_number = self.conf["strict_cn_number"]
        self.normal_cn_number = self.conf["normal_cn_number"]
        self.check_key_dict = {
            "strict": "".join(self.strict_cn_number.values()) + "点负",
            "normal": "".join(self.normal_cn_number.values()) + "点负",
            "smart": "".join(self.normal_cn_number.values()) + "点负" + "01234567890.-"
        }
        self.pattern_dict = self.__get_pattern()
        self.ac = An2Cn()

    def cn2an(self, inputs: str = None, mode: str = "strict") -> int:
        if inputs is not None or inputs == "":
            # 检查转换模式是否有效
            if mode not in ["strict", "normal", "smart"]:
                raise ValueError("mode 仅支持 strict normal smart 三种！")

            # 特殊转化 廿
            inputs = inputs.replace("廿", "二十")

            # 检查输入数据是否有效
            sign, integer_data, decimal_data, is_all_num = self.__check_input_data_is_valid(inputs, mode)

            # smart 下的特殊情况
            if sign == 0:
                return integer_data
            else:
                if not is_all_num:
                    if decimal_data is None:
                        output = self.__integer_convert(integer_data)
                    else:
                        output = self.__integer_convert(integer_data) + self.__decimal_convert(decimal_data)
                        # fix 1 + 0.57 = 1.5699999999999998
                        output = round(output, len(decimal_data))
                else:
                    if decimal_data is None:
                        output = self.__direct_convert(integer_data)
                    else:
                        output = self.__direct_convert(integer_data) + self.__decimal_convert(decimal_data)
                        # fix 1 + 0.57 = 1.5699999999999998
                        output = round(output, len(decimal_data))
        else:
            raise ValueError("输入数据为空！")

        return sign * output

    def __get_pattern(self) -> dict:
        # 整数严格检查
        _0 = "[零]"
        _1_9 = f"[一二三四五六七八九]"
        _10_99 = f"{_1_9}?[十]{_1_9}?"
        _1_99 = f"({_10_99}|{_1_9})"
        _100_999 = f"({_1_9}[百]([零]{_1_9})?|{_1_9}[百]{_10_99})"
        _1_999 = f"({_100_999}|{_1_99})"
        _1000_9999 = f"({_1_9}[千]([零]{_1_99})?|{_1_9}[千]{_100_999})"
        _1_9999 = f"({_1000_9999}|{_1_999})"
        _10000_99999999 = f"({_1_9999}[万]([零]{_1_999})?|{_1_9999}[万]{_1000_9999})"
        _1_99999999 = f"({_10000_99999999}|{_1_9999})"
        _100000000_9999999999999999 = f"({_1_99999999}[亿]([零]{_1_99999999})?|{_1_99999999}[亿]{_10000_99999999})"
        _1_9999999999999999 = f"({_100000000_9999999999999999}|{_1_99999999})"
        str_int_pattern = f"^({_0}|{_1_9999999999999999})$"
        nor_int_pattern = f"^({_0}|{_1_9999999999999999})$"

        str_dec_pattern = "^[零一二三四五六七八九]{0,15}[一二三四五六七八九]$"
        nor_dec_pattern = "^[零一二三四五六七八九]{0,16}$"

        for str_num in self.strict_cn_number.keys():
            str_int_pattern = str_int_pattern.replace(str_num, self.strict_cn_number[str_num])
            str_dec_pattern = str_dec_pattern.replace(str_num, self.strict_cn_number[str_num])
        for nor_num in self.normal_cn_number.keys():
            nor_int_pattern = nor_int_pattern.replace(nor_num, self.normal_cn_number[nor_num])
            nor_dec_pattern = nor_dec_pattern.replace(nor_num, self.normal_cn_number[nor_num])

        pattern_dict = {
            "strict": {
                "int": str_int_pattern,
                "dec": str_dec_pattern
            },
            "normal": {
                "int": nor_int_pattern,
                "dec": nor_dec_pattern
            }
        }
        return pattern_dict

    def __copy_num(self, num):
        cn_num = ""
        for n in num:
            cn_num += self.conf["number_low_an2cn"][int(n)]
        return cn_num

    def __check_input_data_is_valid(self, check_data: str, mode: str) -> (int, str, str, bool):
        # 去除 元整、圆整
        check_data = check_data.replace("元整", "").replace("圆整", "")

        for data in check_data:
            if data not in self.check_key_dict[mode]:
                raise ValueError(f"当前为{mode}模式，输入的数据不在转化范围内：{data}！")

        # 确定正负号
        if check_data[0] == "负":
            check_data = check_data[1:]
            sign = -1
        else:
            sign = 1

        if "点" in check_data:
            split_data = check_data.split("点")
            if len(split_data) == 2:
                integer_data, decimal_data = split_data
                # 将 smart 模式中的阿拉伯数字转化成中文数字
                if mode == "smart":
                    integer_data = re.sub(r"\d+", lambda x: self.ac.an2cn(x.group()), integer_data)
                    decimal_data = re.sub(r"\d+", lambda x: self.__copy_num(x.group()), decimal_data)
                    mode = "normal"
            else:
                raise ValueError("数据中包含不止一个点！")
        else:
            integer_data = check_data
            decimal_data = None
            # 将 smart 模式中的阿拉伯数字转化成中文数字
            if mode == "smart":
                # 10.1万
                pattern1 = re.compile(fr"^-?\d+(\.\d+)?[{self.all_unit}]$")
                result1 = pattern1.search(integer_data)
                if result1:
                    if result1.group() == integer_data:
                        output = int(float(integer_data[:-1]) * self.conf["unit_cn2an"][integer_data[-1]])
                        return 0, output, None, None

                integer_data = re.sub(r"\d+", lambda x: self.ac.an2cn(x.group()), integer_data)
                mode = "normal"

        result_int = re.compile(self.pattern_dict[mode]["int"]).search(integer_data)

        if result_int:
            if result_int.group() == integer_data:
                if decimal_data is not None:
                    result_dec = re.compile(self.pattern_dict[mode]["dec"]).search(decimal_data)
                    if result_dec:
                        if result_dec.group() == decimal_data:
                            return sign, integer_data, decimal_data, False
                else:
                    return sign, integer_data, decimal_data, False
        else:
            if mode == "strict":
                raise ValueError(f"不符合格式的数据：{integer_data}")
            elif mode == "normal":
                # 纯数模式：一二三
                ptn_all_num = re.compile(f"^[{self.all_num}]+$")
                result_all_num = ptn_all_num.search(integer_data)
                if result_all_num:
                    if result_all_num.group() == integer_data:
                        if decimal_data is not None:
                            result_dec = re.compile(self.pattern_dict[mode]["dec"]).search(decimal_data)
                            if result_dec:
                                if result_dec.group() == decimal_data:
                                    return sign, integer_data, decimal_data, True
                        else:
                            return sign, integer_data, decimal_data, True

                # 口语模式：一万二，两千三，三百四
                ptn_speaking_mode = re.compile(f"^[{self.all_num}][{self.all_unit}][{self.all_num}]$")
                result_speaking_mode = ptn_speaking_mode.search(integer_data)
                if result_speaking_mode:
                    if result_speaking_mode.group() == integer_data:
                        _unit = self.conf["unit_low_an2cn"][self.conf["unit_cn2an"][integer_data[1]] // 10]
                        integer_data = integer_data + _unit
                        if decimal_data is not None:
                            result_dec = re.compile(self.pattern_dict[mode]["dec"]).search(decimal_data)
                            if result_dec:
                                if result_dec.group() == decimal_data:
                                    return sign, integer_data, decimal_data, False
                        else:
                            return sign, integer_data, decimal_data, False

        raise ValueError(f"不符合格式的数据：{check_data}")

    def __integer_convert(self, integer_data: str) -> int:
        # 核心
        output_integer = 0
        unit = 1
        ten_thousand_unit = 1
        for index, cn_num in enumerate(reversed(integer_data)):
            # 数值
            if cn_num in self.conf["number_cn2an"]:
                num = self.conf["number_cn2an"][cn_num]
                output_integer += num * unit
            # 单位
            elif cn_num in self.conf["unit_cn2an"]:
                unit = self.conf["unit_cn2an"][cn_num]
                # 判断出万、亿、万亿
                if unit % 10000 == 0:
                    # 万 亿
                    if unit > ten_thousand_unit:
                        ten_thousand_unit = unit
                    # 万亿
                    else:
                        ten_thousand_unit = unit * ten_thousand_unit
                        unit = ten_thousand_unit

                if unit < ten_thousand_unit:
                    unit = unit * ten_thousand_unit

                if index == len(integer_data) - 1:
                    output_integer += unit
            else:
                raise ValueError(f"{cn_num} 不在转化范围内")

        return int(output_integer)

    def __decimal_convert(self, decimal_data: str) -> float:
        len_decimal_data = len(decimal_data)

        if len_decimal_data > 16:
            print(f"注意：小数部分长度为 {len_decimal_data} ，将自动截取前 16 位有效精度！")
            decimal_data = decimal_data[:16]
            len_decimal_data = 16

        output_decimal = 0
        for index in range(len(decimal_data) - 1, -1, -1):
            unit_key = self.conf["number_cn2an"][decimal_data[index]]
            output_decimal += unit_key * 10 ** -(index + 1)

        # 处理精度溢出问题
        output_decimal = round(output_decimal, len_decimal_data)

        return output_decimal

    def __direct_convert(self, data: str) -> int:
        output_data = 0
        for index in range(len(data) - 1, -1, -1):
            unit_key = self.conf["number_cn2an"][data[index]]
            output_data += unit_key * 10 ** (len(data) - index - 1)

        return output_data


class An2Cn(object):
    def __init__(self) -> None:
        self.conf = get_default_conf()
        self.all_num = "0123456789"
        self.number_low = self.conf["number_low_an2cn"]
        self.number_up = self.conf["number_up_an2cn"]
        self.mode_list = ["low", "up", "rmb", "direct"]

    def an2cn(self, inputs: Union[str, int, float] = None, mode: str = "low") -> str:
        """
        阿拉伯数字转中文数字
        :param inputs: 阿拉伯数字
        :param mode: 小写数字，大写数字，人民币大写，直接转化
        :return: 中文数字
        """
        if inputs is not None and inputs != "":
            if mode not in self.mode_list:
                raise ValueError(f"mode 仅支持 {str(self.mode_list)} ！")

            # 将数字转化为字符串，这里会有Python会自动做转化
            # 1. -> 1.0 1.00 -> 1.0 -0 -> 0
            if not isinstance(inputs, str):
                inputs = self.__number_to_string(inputs)

            # 将全角数字和符号转化为半角
            inputs = self.__full_to_half(inputs)

            # 检查数据是否有效
            self.__check_inputs_is_valid(inputs)

            # 判断正负
            if inputs[0] == "-":
                sign = "负"
                inputs = inputs[1:]
            else:
                sign = ""

            if mode == "direct":
                output = self.__direct_convert(inputs)
            else:
                # 切割整数部分和小数部分
                split_result = inputs.split(".")
                len_split_result = len(split_result)
                if len_split_result == 1:
                    # 不包含小数的输入
                    integer_data = split_result[0]
                    if mode == "rmb":
                        output = self.__integer_convert(integer_data, "up") + "元整"
                    else:
                        output = self.__integer_convert(integer_data, mode)
                elif len_split_result == 2:
                    # 包含小数的输入
                    integer_data, decimal_data = split_result
                    if mode == "rmb":
                        int_data = self.__integer_convert(integer_data, "up")
                        dec_data = self.__decimal_convert(decimal_data, "up")
                        len_dec_data = len(dec_data)

                        if len_dec_data == 0:
                            output = int_data + "元整"
                        elif len_dec_data == 1:
                            raise ValueError(f"异常输出：{dec_data}")
                        elif len_dec_data == 2:
                            if dec_data[1] != "零":
                                if int_data == "零":
                                    output = dec_data[1] + "角"
                                else:
                                    output = int_data + "元" + dec_data[1] + "角"
                            else:
                                output = int_data + "元整"
                        else:
                            if dec_data[1] != "零":
                                if dec_data[2] != "零":
                                    if int_data == "零":
                                        output = dec_data[1] + "角" + dec_data[2] + "分"
                                    else:
                                        output = int_data + "元" + dec_data[1] + "角" + dec_data[2] + "分"
                                else:
                                    if int_data == "零":
                                        output = dec_data[1] + "角"
                                    else:
                                        output = int_data + "元" + dec_data[1] + "角"
                            else:
                                if dec_data[2] != "零":
                                    if int_data == "零":
                                        output = dec_data[2] + "分"
                                    else:
                                        output = int_data + "元" + "零" + dec_data[2] + "分"
                                else:
                                    output = int_data + "元整"
                    else:
                        output = self.__integer_convert(integer_data, mode) + self.__decimal_convert(decimal_data, mode)
                else:
                    raise ValueError(f"输入格式错误：{inputs}！")
        else:
            raise ValueError("输入数据为空！")

        return sign + output

    def __direct_convert(self, inputs: str) -> str:
        _output = ""
        for d in inputs:
            if d == ".":
                _output += "点"
            else:
                _output += self.number_low[int(d)]
        return _output

    @staticmethod
    def __number_to_string(number_data: Union[int, float]) -> str:
        # 小数处理：python 会自动把 0.00005 转化成 5e-05，因此 str(0.00005) != "0.00005"
        string_data = str(number_data)
        if "e" in string_data:
            string_data_list = string_data.split("e")
            string_key = string_data_list[0]
            string_value = string_data_list[1]
            if string_value[0] == "-":
                string_data = "0." + "0" * (int(string_value[1:]) - 1) + string_key
            else:
                string_data = string_key + "0" * int(string_value)
        return string_data

    @staticmethod
    def __full_to_half(inputs: str) -> str:
        # 全角转半角
        full_inputs = ""
        for uchar in inputs:
            inside_code = ord(uchar)
            # 全角空格直接转换
            if inside_code == 12288:
                full_inputs += chr(32)
            # 全角字符（除空格）根据关系转化
            elif 65281 <= inside_code <= 65374:
                full_inputs += chr(inside_code - 65248)
            else:
                full_inputs += uchar
        return full_inputs

    def __check_inputs_is_valid(self, check_data: str) -> None:
        # 检查输入数据是否在规定的字典中
        all_check_keys = self.all_num + ".-"
        for data in check_data:
            if data not in all_check_keys:
                raise ValueError(f"输入的数据不在转化范围内：{data}！")

    def __integer_convert(self, integer_data: str, mode: str) -> str:
        numeral_list = self.conf[f"number_{mode}_an2cn"]
        unit_list = self.conf[f"unit_{mode}_order_an2cn"]

        # 去除前面的 0，比如 007 => 7
        integer_data = str(int(integer_data))

        len_integer_data = len(integer_data)
        if len_integer_data > len(unit_list):
            raise ValueError(f"超出数据范围，最长支持 {len(unit_list)} 位")

        output_an = ""
        for i, d in enumerate(integer_data):
            if int(d):
                output_an += numeral_list[int(d)] + unit_list[len_integer_data - i - 1]
            else:
                if not (len_integer_data - i - 1) % 4:
                    output_an += numeral_list[int(d)] + unit_list[len_integer_data - i - 1]

                if i > 0 and not output_an[-1] == "零":
                    output_an += numeral_list[int(d)]

        output_an = output_an.replace("零零", "零").replace("零万", "万").replace("零亿", "亿").replace("亿万", "亿") \
            .strip("零")

        # 解决「一十几」问题
        if output_an[:2] in ["一十"]:
            output_an = output_an[1:]

        # 0 - 1 之间的小数
        if not output_an:
            output_an = "零"

        return output_an

    def __decimal_convert(self, decimal_data: str, o_mode: str) -> str:
        len_decimal_data = len(decimal_data)

        if len_decimal_data > 16:
            print(f"注意：小数部分长度为 {len_decimal_data} ，将自动截取前 16 位有效精度！")
            decimal_data = decimal_data[:16]

        if len_decimal_data:
            output_an = "点"
        else:
            output_an = ""
        numeral_list = self.conf[f"number_{o_mode}_an2cn"]

        for data in decimal_data:
            output_an += numeral_list[int(data)]
        return output_an


class Transform(object):
    def __init__(self) -> None:
        self.conf = get_default_conf()
        self.all_num = "零一二三四五六七八九"
        self.all_unit = "".join(list(self.conf["unit_cn2an"].keys()))
        self.cn2an = Cn2An().cn2an
        self.an2cn = An2Cn().an2cn
        self.cn_pattern = f"负?([{self.all_num}{self.all_unit}]+点)?[{self.all_num}{self.all_unit}]+"
        self.smart_cn_pattern = f"-?([0-9]+.)?[0-9]+[{self.all_unit}]+"

    def transform(self, inputs: str, method: str = "cn2an") -> str:
        inputs = inputs.replace("廿", "二十")
        if method == "cn2an":
            # date
            inputs = re.sub(
                fr"((({self.smart_cn_pattern})|({self.cn_pattern}))年)?([{self.all_num}十]+月)?([{self.all_num}十]+日)?",
                lambda x: self.__sub_util(x.group(), "cn2an", "date"), inputs)
            # fraction
            inputs = re.sub(fr"{self.cn_pattern}分之{self.cn_pattern}",
                            lambda x: self.__sub_util(x.group(), "cn2an", "fraction"), inputs)
            # percent
            inputs = re.sub(fr"百分之{self.cn_pattern}",
                            lambda x: self.__sub_util(x.group(), "cn2an", "percent"), inputs)
            # celsius
            inputs = re.sub(fr"{self.cn_pattern}摄氏度",
                            lambda x: self.__sub_util(x.group(), "cn2an", "celsius"), inputs)
            # number
            output = re.sub(self.cn_pattern,
                            lambda x: self.__sub_util(x.group(), "cn2an", "number"), inputs)
        elif method == "an2cn":
            # date
            inputs = re.sub(r"(\d{2,4}年)?(\d{1,2}月)?(\d{1,2}日)?",
                            lambda x: self.__sub_util(x.group(), "an2cn", "date"), inputs)
            # fraction
            inputs = re.sub(r"\d+/\d+",
                            lambda x: self.__sub_util(x.group(), "an2cn", "fraction"), inputs)
            # percent
            inputs = re.sub(r"-?(\d+\.)?\d+%",
                            lambda x: self.__sub_util(x.group(), "an2cn", "percent"), inputs)
            # celsius
            inputs = re.sub(r"\d+℃",
                            lambda x: self.__sub_util(x.group(), "an2cn", "celsius"), inputs)
            # number
            output = re.sub(r"-?(\d+\.)?\d+",
                            lambda x: self.__sub_util(x.group(), "an2cn", "number"), inputs)
        else:
            raise ValueError(f"error method: {method}, only support 'cn2an' and 'an2cn'!")

        return output

    def __sub_util(self, inputs, method: str = "cn2an", sub_mode: str = "number") -> str:
        try:
            if inputs:
                if method == "cn2an":
                    if sub_mode == "date":
                        return re.sub(fr"(({self.smart_cn_pattern})|({self.cn_pattern}))",
                                      lambda x: str(self.cn2an(x.group(), "smart")), inputs)
                    elif sub_mode == "fraction":
                        if inputs[0] != "百":
                            frac_result = re.sub(self.cn_pattern,
                                                 lambda x: str(self.cn2an(x.group(), "smart")), inputs)
                            numerator, denominator = frac_result.split("分之")
                            return f"{denominator}/{numerator}"
                        else:
                            return inputs
                    elif sub_mode == "percent":
                        return re.sub(f"(?<=百分之){self.cn_pattern}",
                                      lambda x: str(self.cn2an(x.group(), "smart")), inputs).replace("百分之", "") + "%"
                    elif sub_mode == "celsius":
                        return re.sub(f"{self.cn_pattern}(?=摄氏度)",
                                      lambda x: str(self.cn2an(x.group(), "smart")), inputs).replace("摄氏度", "℃")
                    elif sub_mode == "number":
                        return str(self.cn2an(inputs, "smart"))
                    else:
                        raise Exception(f"error sub_mode: {sub_mode} !")
                else:
                    if sub_mode == "date":
                        inputs = re.sub(r"\d+(?=年)",
                                        lambda x: self.an2cn(x.group(), "direct"), inputs)
                        return re.sub(r"\d+",
                                      lambda x: self.an2cn(x.group(), "low"), inputs)
                    elif sub_mode == "fraction":
                        frac_result = re.sub(r"\d+", lambda x: self.an2cn(x.group(), "low"), inputs)
                        numerator, denominator = frac_result.split("/")
                        return f"{denominator}分之{numerator}"
                    elif sub_mode == "celsius":
                        return self.an2cn(inputs[:-1], "low") + "摄氏度"
                    elif sub_mode == "percent":
                        return "百分之" + self.an2cn(inputs[:-1], "low")
                    elif sub_mode == "number":
                        return self.an2cn(inputs, "low")
                    else:
                        raise Exception(f"error sub_mode: {sub_mode} !")
        except Exception as e:
            print(f"WARN: {e}")
            return inputs


an2Cn = Transform().transform

if __name__ == '__main__':
    pass
    testStr = ['罚款2000元', '罚款两千元', '罚款俩千元整', '罚款人民币两千元',
               '《中华人民共和国水污染防治法》第八十三条第(二)项，并处十万元以上一百万元以下的罚款',
               '十一', '一百二十三', '两百三十二', '一千二百零三', '一万一千一百零一', u'十万零三千六百零九', u'一百二十三万四千五百六十七',
               u'一千一百二十三万四千五百六十七', u'一亿一千一百二十三万四千五百六十七', u'一百零二亿五千零一万零一千零三十八',
               u'一千一百一十一亿一千一百二十三万四千五百六十七', u'一兆一千一百一十一亿一千一百二十三万四千五百六十七',
               '我有百三十二块钱', '十二个套餐', '一亿零八万零三百二十三', '今天天气真不错',
               '百分之八十 discount rate很高了', '千万不要', '我们俩个人', '这个invoice value值一百万',
               '我的一百件商品have quality', '找一找我的收藏夹里，有没有一个眼镜']
    for tstr in testStr:
        print(tstr + ' = ' + ChineseNumToArab(tstr))

    output = an2Cn("消防法第2条", "an2cn")
    print(output)

    output = an2Cn("消防法第十五条", "cn2an")
    print(output)
