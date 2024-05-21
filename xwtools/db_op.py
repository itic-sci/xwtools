#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved
File: {name}.py
Author: xuwei
Email: weix@sensedeal.ai
Last modified: 2018.12.23
Description:
'''

import time
import pymysql
import pandas as pd
from .config_log import config
from multiprocessing import Pool
from sqlalchemy import create_engine
from .data_to_str import DataTypeToStr
from urllib.parse import quote_plus

_ProcessNum = 3
_Thread = 4


def engine_con(label, host='', port=0, user='', password='', db='', **kwargs):
    if label:
        _dict = dict(host=config(label, 'host'), port=int(config(label, 'port')), user=config(label, 'user'),
                     passwd=config(label, 'pass'), db=config(label, 'db'))
    else:
        _dict = dict(host=host, port=port, user=user, passwd=password, db=db)
    # print(_dict)
    engine = create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}". \
        format(
        host=_dict['host'],
        port=_dict['port'],
        user=_dict['user'],
        passwd=quote_plus(_dict['passwd']),
        db=_dict['db'],
    ),
        connect_args={'charset': 'utf8'},
        pool_size=5,
        pool_recycle=1800,
    )
    return engine


def get_db_con(label, con_type='get_dict', is_multi_thread=False,
               maxconnections=10, mincached=3, maxcached=5, maxshared=0, maxusage=1000,
               host='', port=0, user='', password='', db=''):
    """
    :param label:
    :param con_type: get_dict, get_tuple, get_con
    :param is_multi_thread:
    :param maxconnections:
    :param mincached:
    :param maxcached:
    :param maxshared:
    :param maxusage:
    :return:
    """
    if label:
        _dict = dict(host=config(label, 'host'), port=int(config(label, 'port')), user=config(label, 'user'),
                     passwd=config(label, 'pass'), db=config(label, 'db'))
    else:
        _dict = dict(host=host, port=port, user=user, passwd=password, db=db)

    db_info = {
        'host': _dict['host'],
        'port': _dict['port'],
        'user': _dict['user'],
        'password': _dict['passwd'],
        'charset': 'utf8mb4',
    }

    if con_type == 'get_con':
        _add_param = {}
    elif con_type == 'get_tuple':
        _add_param = {'database': _dict['db']}
    else:
        _add_param = {'database': _dict['db'], 'cursorclass': pymysql.cursors.DictCursor, }
    db_info.update(_add_param)
    # print(db_info)
    if is_multi_thread:
        from DBUtils.PooledDB import PooledDB, SharedDBConnection
        poolDB = PooledDB(
            creator=pymysql,  # 指定数据库连接驱动
            maxconnections=maxconnections,  # 连接池允许的最大连接数,0和None表示没有限制
            mincached=mincached,  # 初始化时,连接池至少创建的空闲连接,0表示不创建
            maxcached=maxcached,  # 连接池中空闲的最多连接数,0和None表示没有限制
            maxshared=maxshared,  # 连接池中最多共享的连接数量,0和None表示全部共享(其实没什么卵用)
            blocking=True,  # 连接池中如果没有可用共享连接后,是否阻塞等待,True表示等等,False表示不等待然后报错
            setsession=[],  # 开始会话前执行的命令列表
            ping=0,  # ping Mysql服务器检查服务是否可用
            connect_timeout=300,
            **db_info
        )
        return poolDB
    else:
        _state = True
        _max_retries_count = 10  # 设置最大重试次数
        _conn_retries_count = 0  # 初始重试次数
        while _state and _conn_retries_count < _max_retries_count:
            try:
                from DBUtils.PersistentDB import PersistentDB, PersistentDBError, NotSupportedError
                poolDB = PersistentDB(
                    creator=pymysql,  # 指定数据库连接驱动
                    maxusage=maxusage,  # 一个连接最大复用次数,0或者None表示没有限制,默认为0
                    connect_timeout=300,
                    **db_info
                )
                return poolDB
            except Exception as e:
                _conn_retries_count += 1
                time.sleep(1)
                if _conn_retries_count == _max_retries_count:
                    raise e


def _get_update_key(update_df, old_df, unique_key):
    update_list = []
    for i in range(len(update_df)):
        row_dict = update_df.iloc[i].dropna().to_dict()
        key = row_dict[unique_key]
        old_data = old_df[old_df[unique_key] == key].iloc[0].dropna().to_dict()
        tuple_set = set(row_dict.items()) - set(old_data.items())
        # print(tuple_set)
        if tuple_set:
            dict_update = {}
            dict_update[unique_key] = old_data[unique_key]
            for tuple_one in tuple_set:
                dict_update[tuple_one[0]] = tuple_one[1]
            update_list.append(dict_update)
    return update_list


class DB(object):
    def __init__(self, label, **kwargs):
        """
        :param label:
        :param kwargs: con_type='get_tuple'/'get_dict'
        :param is_multi_thread: is_multi_thread default False
        :param maxconnections:
        :param mincached:
        :param maxcached:
        :param maxshared:
        :param maxusage:
        """
        self.is_multi_thread = kwargs.get('is_multi_thread', False)
        self.engine = engine_con(label, **kwargs)
        self.db_pool = get_db_con(label, **kwargs)

    # mysql数据库操作
    def operate_sql(self, sql):
        con = self.db_pool.connection()
        try:
            with con.cursor() as cursor:
                cursor.execute(sql)
            con.commit()
        except Exception as e:
            con.rollback()
            raise e
        finally:
            con.close()

    # 主要用来insert into 多条数据
    def operate_execute_many(self, sql, data):
        con = self.db_pool.connection()
        try:
            with con.cursor() as cursor:
                cursor.executemany(sql, data)
            con.commit()
        except Exception as e:
            con.rollback()
            raise e
        finally:
            con.close()

    # 可根据con_type='get_tuple'/'get_dict'调整获取是列表字典还是元组
    def get_data_sql(self, sql):
        con = self.db_pool.connection()
        try:
            with con.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                return results
        except Exception as e:
            raise e
        finally:
            con.close()

    def get_df_sql(self, sql):
        df = pd.read_sql(sql, self.engine, coerce_float=False)
        return df

    def get_df_sql_loop(self, sql, num):
        df = pd.read_sql(sql, self.engine, coerce_float=False, chunksize=num)
        return df

    # write current_labeled to mysql.
    def to_sql_write_data(self, data, table_name):
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data
        df.to_sql(table_name, self.engine, if_exists='append', index=False, chunksize=5000)

    # 此函数中df的columns或dict的key值必须和数据库中的字段一致
    def execute_many_write_data(self, data, table_name):
        if not isinstance(data, list):
            list_dict = data.to_dict('records')
        else:
            list_dict = data
        list_dict = DataTypeToStr.list_dict_values_type_to_str(list_dict)
        _columns_list = list_dict[0].keys()
        _columns_num = len(_columns_list)
        _columns = '`,`'.join(_columns_list)
        _columns = '`' + _columns + '`'
        _placeholder = ','.join(['%s'] * _columns_num)
        _sql = 'insert ignore into `%s` (' % table_name + _columns + ') values (' + _placeholder + ')'
        _list_tuple = [tuple(_dict.values()) for _dict in list_dict]
        self.operate_execute_many(_sql, _list_tuple)

    def update_table_data(self, list_dict, table_name, unique_key):
        _update_dict = {'insert': [], 'update': []}
        if isinstance(list_dict, list):
            origin_df = pd.DataFrame(list_dict)
        else:
            origin_df = list_dict

        unique_key_df = self.get_df_sql("""select {0} from {1}""".format(unique_key, table_name)).dropna()

        if len(unique_key_df) != 0:
            unique_key_list = unique_key_df[unique_key].tolist()
            append_id_list = list(set(origin_df[unique_key].tolist()) - set(unique_key_list))
            if append_id_list:
                append_df = origin_df[origin_df[unique_key].isin(append_id_list)]
                append_df = append_df.drop_duplicates(unique_key)
                _update_dict['insert'] = append_df.to_dict('records')
                self.to_sql_write_data(append_df, table_name)
                # self.execute_many_write_data(append_df.astype(str), table_name)
                print(table_name + '表中添加了{}条新记录。'.format(len(append_df)))
            else:
                print(table_name + '表中没有新数据添加。')

            update_id_list = list(set(origin_df[unique_key].tolist()) - set(append_id_list))
            if update_id_list:
                columns_str = ','.join(origin_df.columns.tolist())
                if len(update_id_list) != 1:
                    _where = " from {0} where {1} in {2}".format(table_name, unique_key, tuple(update_id_list))
                else:
                    _where = " from {0} where {1}='{2}'""".format(table_name, unique_key, update_id_list[0])
                _sql = "select " + columns_str + _where
                if isinstance(list_dict, list):
                    old_data = self.get_data_sql(_sql)
                    old_df = pd.DataFrame(old_data)
                else:
                    old_df = self.get_df_sql(_sql)

                update_df = origin_df[origin_df[unique_key].isin(update_id_list)]
                update_df = update_df.drop_duplicates(unique_key)
                # print('match_start', datetime.datetime.now())
                loop_num = int(len(update_df) / _ProcessNum) + 1
                pool = Pool(processes=_ProcessNum)
                jobs = []
                for i in range(0, len(update_df), loop_num):
                    p = pool.apply_async(_get_update_key, (update_df.iloc[i:i + loop_num], old_df, unique_key,))
                    jobs.append(p)
                pool.close()  # 关闭进程池，表示不能在往进程池中添加进程
                pool.join()  # 等待进程池中的所有进程执行完毕，必须在close()之后调用

                update_data = []
                for j in jobs:
                    update_data = update_data + j.get()

                # print(update_data)
                # print('match_end', datetime.datetime.now())
                if update_data:
                    _update_dict['update'] = update_data
                    for data_dict in update_data:
                        _sql_update = "update {0} set ".format(table_name, ) + \
                                      ','.join(
                                          ['%s=%r' % (k, str(data_dict[k])) for k in data_dict if k != unique_key]) + \
                                      " where {0}='{1}'".format(unique_key, data_dict[unique_key])
                        # print(_sql_update)
                        self.operate_sql(_sql_update)
                    print(table_name + '更新了{0}条记录。'.format(len(update_data)))
                else:
                    print(table_name + '表中数据无更新。')
        else:
            _update_dict['insert'] = origin_df.to_dict('records')
            write_df = origin_df.drop_duplicates(unique_key)
            self.to_sql_write_data(write_df, table_name)
        return _update_dict

    def dispose(self):
        if self.is_multi_thread:
            self.db_pool.close()
        self.engine.dispose()


if __name__ == '__main__':
    _db = DB('160')
    # res = _db.get_data_sql("select * from risk_push_logs limit 1")
    df = _db.get_df_sql("""select model_class,report_name,row_name from report_model limit 10""")
    _db.operate_sql("truncate table test_table")
    _db.execute_many_write_data(df, 'test_table')
    # _db.dispose()
    # print(res)
    print(df)
