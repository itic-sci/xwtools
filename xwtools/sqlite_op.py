#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Author: xuwei
Email: 18810079020@163.com
File: sqlite_op.py
Date: 2021/2/23 11:37 上午
'''

import sqlite3
import json
import numpy
import datetime
import pandas as pd
from datetime import date


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


class SqliteOp(object):
    def __init__(self, db_name):
        self.db = db_name

    # 数据库操作
    def operate_sql(self, sql):
        con = sqlite3.connect(self.db)
        try:
            cursor = con.cursor()
            cursor.execute(sql)
            con.commit()
        except Exception as e:
            con.rollback()
            raise e
        finally:
            con.close()

    # 主要用来insert into 多条数据
    def operate_execute_many(self, sql, data):
        con = sqlite3.connect(self.db)
        try:
            cursor = con.cursor()
            cursor.executemany(sql, data)
            con.commit()
        except Exception as e:
            con.rollback()
            raise e
        finally:
            con.close()

    # 此函数中df的columns或dict的key值必须和数据库中的字段一致
    def execute_many_write_data(self, data, table_name):
        """
        :param data: list or Dataframe
        :param table_name:
        :return:
        """
        if not isinstance(data, list):
            list_dict = data.to_dict('records')
        else:
            list_dict = data
        list_dict = DataTypeToStr.list_dict_values_type_to_str(list_dict)
        _columns_list = list_dict[0].keys()
        _columns_num = len(_columns_list)
        _columns = '`,`'.join(_columns_list)
        _columns = '`' + _columns + '`'
        _placeholder = ','.join(['?'] * _columns_num)
        _sql = 'insert or ignore into `%s` (' % table_name + _columns + ') values (' + _placeholder + ')'
        _list_tuple = [tuple(_dict.values()) for _dict in list_dict]
        self.operate_execute_many(_sql, _list_tuple)

    def sql_query(self, sql):
        con = sqlite3.connect(self.db)
        try:
            cursor = con.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        except Exception as e:
            raise e
        finally:
            con.close()

if __name__ == '__main__':
    sqliteOp = SqliteOp('../goods.db')

    sql = """
    CREATE TABLE if not exists `goods_details` (
  `url` varchar(400) PRIMARY KEY  NOT NULL,
  `title` varchar(600) DEFAULT NULL,
  `pic` varchar(500) DEFAULT NULL,
  `source` varchar(100) DEFAULT NULL,
  `price` varchar(100)  DEFAULT NULL,
  `comment` varchar(100)  DEFAULT NULL,
  `commnet_url` varchar(500) DEFAULT NULL
);
    """
    sqliteOp.operate_sql(sql)