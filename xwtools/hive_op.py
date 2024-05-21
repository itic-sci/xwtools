#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Author: xuwei
Email: 18810079020@163.com
File: test_data.py
Date: 2020/5/14 2:39 下午
'''

import pandas as pd


class HiveOp(object):
    def __init__(self, host=None, port=None, username=None, password=None, database='default', hive_config=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.hive_config = hive_config

    def get_con(self):
        from pyhive import hive
        conn = hive.Connection(host=self.host, database=self.database, port=self.port, username=self.username,
                               password=self.password, configuration=self.hive_config)
        return conn

    def get_dict_sql(self, sql):
        res_list = []
        try:
            conn = self.get_con()
            cursor = conn.cursor()
            cursor.execute(sql)
            query_list = cursor.fetchall()
            columns = cursor.description
            col_names = []
            for column in columns:
                col_names.append(column[0].split('.')[-1])
            # print(col_names)
            for item in query_list:
                res_dict = dict()
                for i in range(len(col_names)):
                    res_dict[col_names[i]] = item[i]
                res_list.append(res_dict)
            return res_list
        except Exception as e:
            print('get_dict_sql error %s' % e)
            raise e
        finally:
            cursor.close()
            conn.close()

    def get_tuple_sql(self, sql):
        try:
            conn = self.get_con()
            cursor = conn.cursor()
            cursor.execute(sql)
            query_list = cursor.fetchall()
            return query_list
        except Exception as e:
            print('get_tuple_sql error %s' % e)
        finally:
            cursor.close()
            conn.close()

    def get_loop_df(self, sql, chunksize=10):
        df_loop = pd.read_sql(sql, self.get_con(), chunksize=chunksize)
        return df_loop


if __name__ == '__main__':
    hive_config = {
        'mapreduce.job.queuename': 'my_hive',
        'hive.exec.compress.output': 'false',
        'hive.exec.compress.intermediate': 'true',
        'mapred.min.split.size.per.node': '1',
        'mapred.min.split.size.per.rack': '1',
        'hive.map.aggr': 'true',
        'hive.groupby.skewindata': 'true'
    }

    hive_op = HiveOp(host='xxxx', database='huanghe_dim', hive_config=hive_config)

    sql = 'SELECT * FROM `huanghe_dim`.`dim_comment_f` LIMIT 100'

    # res = hive_op.get_dict_sql(sql)
    # print(res)
    #
    # print(hive_op.get_tuple_sql(sql))

    df_loop = hive_op.get_loop_df(sql)
    for df in df_loop:
        print(df)
