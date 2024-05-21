#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Author: xuwei
Email: 18810079020@163.com
File: sqlserver_op.py
Date: 2020/8/18 3:36 下午
'''

import pandas as pd
from .config_log import config
from .utils import MyException

'''
## mac 使用sqlServer

- 1、先准备环境

        参考网址： https://www.microsoft.com/en-us/sql-server/developer-get-started/python/mac/?rtc=1
        brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
        brew update
        HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install msodbcsql17 mssql-tools
        sudo ln -s /usr/local/etc/odbcinst.ini /etc/odbcinst.ini
        sudo ln -s /usr/local/etc/odbc.ini /etc/odbc.ini
        
'''

class ConMysql(object):
    def __init__(self, label=None, host=None, port=None, user=None, password=None, db=None):
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

        self.con_text = "DRIVER={driver};SERVER={host};DATABASE={db};UID={user};PWD={password}".format(
                    driver="{ODBC Driver 17 for SQL Server}",
                    host=self._host,
                    db=self._db_name,
                    user=self._user,
                    password=self._password,
                )

    def engine_db(self):
        _state = True
        _max_retries_count = 10  # 设置最大重试次数
        _conn_retries_count = 0  # 初始重试次数
        while _state and _conn_retries_count < _max_retries_count:
            try:
                import pyodbc
                engine = pyodbc.connect(self.con_text)
                return engine
            except:
                _conn_retries_count += 1
        raise MyException('%s-engine_db数据库连接失败' % self._host)

    def con_db(self):
        _state = True
        _max_retries_count = 10  # 设置最大重试次数
        _conn_retries_count = 0  # 初始重试次数
        while _state and _conn_retries_count < _max_retries_count:
            try:
                import pyodbc
                con = pyodbc.connect(self.con_text)
                return con
            except:
                _conn_retries_count += 1
        raise MyException('%s-con_db数据库连接失败' % self._host)


class SqlServerOp(ConMysql):
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

    def get_dict_sql(self, sql, close_con=True):
        con = self.con_db()
        try:
            with con.cursor() as cursor:
                cursor.execute(sql)
                list_tuple = cursor.fetchall()
                columns = [column [0] for column in cursor.description]
                result = []
                for obj_tuple in list_tuple:
                    item = dict()
                    for obj in zip(columns, obj_tuple):
                        item[obj[0]] = obj[1]
                    result.append(item)
                return result
        except Exception as e:
            raise e
        finally:
            if close_con:
                con.close()

    def get_df_sql(self, sql):
        try:
            df = pd.read_sql(sql, self.engine_db(), coerce_float=False)
            return df
        except Exception as e:
            raise e
        finally:
            self.engine_db().close()

    def get_df_sql_loop(self, sql, num):
        try:
            df_loop = pd.read_sql(sql, self.engine_db(), coerce_float=False, chunksize=num)
            return df_loop
        except Exception as e:
            raise e
        finally:
            self.engine_db().close()
