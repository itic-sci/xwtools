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

import pymysql
import pandas as pd
from .config_log import config
from multiprocessing import Pool
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from .data_to_str import DataTypeToStr
from .utils import MyException

# import warnings
# warnings.filterwarnings("ignore")
# from common.config_log import config

_ProcessNum = 3
_Thread = 4



class ConMysql(object):
    def __init__(self, label=None, single_entity=False, host=None, port=None, user=None, password=None, db=None):
        self._single_entity = single_entity
        self.__con_entity = dict()
        if label:
            self._host = config(label, 'host')
            self._port = int(config(label, 'port'))
            self._user = config(label, 'user')
            self._password = config(label, 'pass')
            self._db_name = db if db else config(label, 'db')
        else:
            self._host = host
            self._port = port
            self._user = user
            self._password = password
            self._db_name = db

    def __get_engine_con(self):
        _state = True
        _max_retries_count = 10  # 设置最大重试次数
        _conn_retries_count = 0  # 初始重试次数
        while _state and _conn_retries_count < _max_retries_count:
            try:
                engine = create_engine("mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}".format(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    passwd=quote_plus(self._password),
                    db=self._db_name,
                ), connect_args={'charset': 'utf8'},
                    pool_size=5,
                    pool_recycle=1800,
                )
                return engine
            except:
                _conn_retries_count += 1
        raise MyException('%s-engine_db数据库连接失败' % self._host)

    def __get_con_db_dict(self):
        _state = True
        _max_retries_count = 10  # 设置最大重试次数
        _conn_retries_count = 0  # 初始重试次数
        while _state and _conn_retries_count < _max_retries_count:
            try:
                con = pymysql.connect(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    passwd=self._password,
                    db=self._db_name,
                    charset='utf8',
                    cursorclass=pymysql.cursors.DictCursor,
                )
                return con
            except:
                _conn_retries_count += 1
        raise MyException('%s-con_db_dict数据库连接失败' % self._host)

    def __get_con_db(self):
        _state = True
        _max_retries_count = 10  # 设置最大重试次数
        _conn_retries_count = 0  # 初始重试次数
        while _state and _conn_retries_count < _max_retries_count:
            try:
                con = pymysql.connect(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    passwd=self._password,
                    db=self._db_name,
                    charset='utf8mb4',
                )
                return con
            except:
                _conn_retries_count += 1
        raise MyException('%s-con_db数据库连接失败' % self._host)

    def engine_db(self):
        if self._single_entity:
            if 'engine_db' in self.__con_entity:
                return self.__con_entity['engine_db']
            else:
                self.__con_entity['engine_db'] = self.__get_engine_con()
                return self.__con_entity['engine_db']
        else:
            return self.__get_engine_con()

    # 数据形式为列表字典
    def con_db_dict(self):
        if self._single_entity:
            if 'con_db_dict' in self.__con_entity:
                try:
                    self.__con_entity['con_db_dict'].ping()
                    return self.__con_entity['con_db_dict']
                except:
                    self.__con_entity['con_db_dict'] = self.__get_con_db_dict()
                    return self.__con_entity['con_db_dict']
            else:
                self.__con_entity['con_db_dict'] = self.__get_con_db_dict()
                return self.__con_entity['con_db_dict']
        else:
            return self.__get_con_db_dict()

    # 数据形式为元组
    def con_db(self):
        if self._single_entity:
            if 'con_db' in self.__con_entity:
                try:
                    self.__con_entity['con_db'].ping()
                    return self.__con_entity['con_db']
                except:
                    self.__con_entity['con_db'] = self.__get_con_db()
                    return self.__con_entity['con_db']
            else:
                self.__con_entity['con_db'] = self.__get_con_db()
                return self.__con_entity['con_db']
        else:
            return self.__get_con_db()

    def close_all(self):
        if self._single_entity:
            if self.__con_entity.get('engine_db'):
                self.__con_entity['engine_db'].dispose()
            if self.__con_entity.get('con_db'):
                self.__con_entity['con_db'].close()
            if self.__con_entity.get('con_db_dict'):
                self.__con_entity['con_db_dict'].close()


class MysqlOp(ConMysql):
    # mysql数据库操作
    def operate_sql(self, sql, close_con=True):
        con = self.con_db()
        try:
            with con.cursor() as cursor:
                cursor.execute(sql)
            con.commit()
        except Exception as e:
            con.rollback()
            raise e
        finally:
            if close_con:
                con.close()

    def get_dict_sql(self, sql, close_con=True):
        con = self.con_db_dict()
        try:
            with con.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                return results
        except Exception as e:
            raise e
        finally:
            if close_con:
                con.close()

    def get_tuple_sql(self, sql, close_con=True):
        con = self.con_db()
        try:
            with con.cursor() as cursor:
                cursor.execute(sql)
                results = cursor.fetchall()
                return results
        except Exception as e:
            raise e
        finally:
            if close_con:
                con.close()

    # 主要用来insert into 多条数据
    def operate_execute_many(self, sql, data, close_con=True):
        con = self.con_db()
        try:
            with con.cursor() as cursor:
                cursor.executemany(sql, data)
            con.commit()
        except Exception as e:
            con.rollback()
            raise e
        finally:
            if close_con:
                con.close()

    # # 此函数中df的columns或dict的key值必须和数据库中的字段一致
    # def execute_many_write_data(self, data, table_name):
    #     if isinstance(data, list):
    #         df = pd.DataFrame(data)
    #     else:
    #         df = data
    #     _columns_list = df.columns
    #     _columns_num = len(_columns_list)
    #     _columns = '`,`'.join(_columns_list)
    #     _columns = '`' + _columns + '`'
    #     _placeholder = ','.join(['%s'] * _columns_num)
    #     _sql = 'insert ignore into `%s` (' % table_name + _columns + ') values (' + _placeholder + ')'
    #     _list_tuple = [tuple(obj) for obj in df.to_records(index=False)]
    #     self.operate_execute_many(_sql, _list_tuple)

    # 此函数中df的columns或dict的key值必须和数据库中的字段一致
    def execute_many_write_data(self, data, table_name, close_con=False):
        """
        :param data: df or list_dict
        :param table_name:
        :param close_con:
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
        _placeholder = ','.join(['%s'] * _columns_num)
        _sql = 'insert ignore into `%s` (' % table_name + _columns + ') values (' + _placeholder + ')'
        _list_tuple = [tuple(_dict.values()) for _dict in list_dict]
        self.operate_execute_many(_sql, _list_tuple, close_con=close_con)

    def get_df_sql(self, sql):
        try:
            df = pd.read_sql(sql, self.engine_db(), coerce_float=False)
            return df
        except Exception as e:
            raise e
        finally:
            self.engine_db().dispose()

    def get_df_sql_loop(self, sql, num):
        try:
            df = pd.read_sql(sql, self.engine_db(), coerce_float=False, chunksize=num)
            return df
        except Exception as e:
            raise e
        finally:
            self.engine_db().dispose()

    # write current_labeled to mysql.
    def to_sql_write_data(self, data, table_name, if_exists='append'):
        try:
            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data
            df.to_sql(table_name, self.engine_db(), if_exists=if_exists, index=False, chunksize=10000)
        except Exception as e:
            raise e
        finally:
            self.engine_db().dispose()


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


class UpdateTable(MysqlOp):
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
                self.execute_many_write_data(append_df, table_name)
                # self.execute_many_write_data(append_df.astype(str), table_name)
                print(table_name + '表中添加了{}条新记录。'.format(len(append_df)))
            else:
                print(table_name + '表中没有新数据添加。')

            update_id_list = list(set(origin_df[unique_key].tolist()) - set(append_id_list))
            if update_id_list:
                columns_str = '`' + '`,`'.join(origin_df.columns.tolist()) + '`'
                if len(update_id_list) != 1:
                    _where = " from {0} where {1} in {2}".format(table_name, unique_key, tuple(update_id_list))
                else:
                    _where = " from {0} where `{1}`='{2}'""".format(table_name, unique_key, update_id_list[0])
                _sql = "select " + columns_str + _where
                if isinstance(list_dict, list):
                    old_data = self.get_dict_sql(_sql)
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
                                          ['`%s`=%r' % (k, str(data_dict[k])) for k in data_dict if k != unique_key]) + \
                                      " where `{0}`='{1}'".format(unique_key, data_dict[unique_key])
                        # print(_sql_update)
                        self.operate_sql(_sql_update)
                    print(table_name + '更新了{0}条记录。'.format(len(update_data)))
                else:
                    print(table_name + '表中数据无更新。')
        else:
            _update_dict['insert'] = origin_df.to_dict('records')
            write_df = origin_df.drop_duplicates(unique_key)
            self.execute_many_write_data(write_df, table_name)
        return _update_dict


if __name__ == '__main__':
    _db = MysqlOp(label='160', db='xw_test', single_entity=True)
    # _db_1 = MysqlOp('160', 'xw_test', single_entity=True)
    # print(id(_db))
    # print(id(_db_1))
    df = _db.get_dict_sql("""select * from test_table limit 10""", close_con=False)
    print(df)
    df = _db.get_dict_sql("""select * from test_table limit 10""", close_con=False)
    print(df)
    _db.close_all()
