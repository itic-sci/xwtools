#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''                                                                                                             
Author: xuwei                                        
Email: 18810079020@163.com                                 
File: data_to_str.py
Date: 2021/1/6 6:39 下午
'''

import json
import numpy
import datetime
import pandas as pd
from datetime import date
from .utils import CJsonEncoder


class DataTypeToStr(object):
    @classmethod
    def data_type_to_str(cls, obj):
        if obj != obj:
            return None
        elif isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, numpy.datetime64):
            return pd.to_datetime(str(obj)).strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj, numpy.integer) or isinstance(obj, numpy.floating) or isinstance(obj, int) \
                or isinstance(obj, float) or isinstance(obj, bytes):
            return str(obj)
        elif isinstance(obj, numpy.ndarray):
            return json.dumps(obj.tolist(), ensure_ascii=False, cls=CJsonEncoder)
        elif isinstance(obj, list):
            return json.dumps(obj, ensure_ascii=False, cls=CJsonEncoder)
        elif isinstance(obj, dict):
            return json.dumps(obj, ensure_ascii=False, cls=CJsonEncoder)
        else:
            return obj

    @classmethod
    def dict_values_type_etl_str(cls, dict_param):
        for key in dict_param:
            dict_param[key] = cls.data_type_to_str(dict_param[key])
        return dict_param

    @classmethod
    def list_dict_values_type_to_str(cls, list_dict):
        list_dict = list(map(cls.dict_values_type_etl_str, list_dict))
        return list_dict
