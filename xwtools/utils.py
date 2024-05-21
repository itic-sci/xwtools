import json
import re
import shortuuid
import numpy
import datetime
from datetime import date
from urllib.parse import quote

'''
不依赖本项目的其它包，全是线上包
'''


class MyException(Exception):  # 让MyException类继承Exception
    def __init__(self, str_describe):
        self.msg = str_describe


##datetime.datetime is not JSON serializable 报错问题解决
class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if obj != obj:
            return None
        elif isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        elif isinstance(obj, bytes):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


# URL 编码
def get_quote(text):
    return quote(text)


def dump_json(data):
    return json.dumps(data, ensure_ascii=False, cls=CJsonEncoder)


def load_json(data):
    try:
        return json.loads(data)
    except:
        return None


def format_blank(content):
    clear_list = {
        u'\u2002': '', u'\u2003': '', u'\u2009': '', u'\u200c\u200d': '', u'\xa0': '', '&nbsp;': '',
        '&ensp;': '', '&emsp;': '', '&zwj;': '', '&zwnj;': ''
    }
    rep = dict((re.escape(k), v) for k, v in clear_list.items())
    pattern = re.compile('|'.join(rep.keys()))
    content = pattern.sub(lambda x: rep[re.escape(x.group(0))], content)
    return content


def get_url_uuid(url):
    return shortuuid.uuid(name=url)


# ['总论', '自然', '自然辩证法', '自然辩证法总论'],
def drop_element_contain_in_str_list(str_list, reverse=True, case_sensitive=False):
    """
    :param str_list: 列表元素必须全是字符串
    :param reverse: reverse=True代表按字符串长度降序排列
    :param case_sensitive: 大小写是否敏感，默认不区分大小写
    :return:
    """
    remove_elements = set()
    all_elements = set(str_list)

    group_dict = dict()
    for obj in all_elements:
        group_dict[len(obj)] = group_dict.get(len(obj), []) + [obj]

    name_group_list_tuple = sorted(group_dict.items(), key=lambda x: x, reverse=True)

    for i in range(1, len(name_group_list_tuple)):
        layer_elements_list = name_group_list_tuple[i][1]
        for elements_tuple in name_group_list_tuple[:i]:
            current_str = '、'.join(elements_tuple[1])
            for ele in layer_elements_list:
                if not case_sensitive:
                    if ele.lower() in current_str.lower():
                        remove_elements.add(ele)
                else:
                    if ele in current_str:
                        remove_elements.add(ele)

    remain_elements = all_elements - remove_elements
    if reverse:
        remain_elements = sorted(remain_elements, key=lambda x: len(x), reverse=reverse)
    return list(remain_elements)



if __name__ == '__main__':
    str_list = ['总论', '自然xw', '自然XW辩证法', '自然辩证法总论']
    r = drop_element_contain_in_str_list(str_list, case_sensitive=True)
    print(r)
    print(str_list)

