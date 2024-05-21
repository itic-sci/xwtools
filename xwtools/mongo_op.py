#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Author: xuwei
Email: 18810079020@163.com
File: mongo_op.py
Date: 2020/8/19 9:58 上午
'''

import pymongo
from pymongo import UpdateOne, InsertOne
from .config_log import config


def get_client(label='mongo', **kwargs):
    if config(label, 'user', ''):
        my_client = pymongo.MongoClient(
            host=config(label, 'host'),
            port=int(config(label, 'port')),
            username=config(label, 'user'),
            password=config(label, 'pass'),
            authSource=config(label, 'db'),
            unicode_decode_error_handler='ignore',
            **kwargs
        )
    else:
        my_client = pymongo.MongoClient(
            config(label, 'host'), # mongodb://user:password@192.168.6.28:3017/db
            unicode_decode_error_handler='ignore',
            **kwargs
        )
    return my_client


class MongoOp(object):
    def __init__(self, db, collection, label='mongo', limit=10, **kwargs):
        self.db = db
        self.limit = limit
        self.client = get_client(label=label, **kwargs)
        self.col = self.client[db][collection]

    def find_count(self, *args, **kwargs):
        return self.col.find(*args, **kwargs).count()

    def show_db(self):
        dbs_list = self.client.database_names()
        return dbs_list

    def show_col(self):
        db = self.client[self.db]
        cols_list = db.collection_names()
        return cols_list

    # create_index 只会在索引不存在的时候创建一个索引（ensure_index是老版本的）
    def create_index(self, keys, session=None, **kwargs):
        """
        :param keys: [('name', pymongo.ASCENDING), ('create_time', pymongo.ASCENDING)]
        :param session:
        :param kwargs: create_time=1, expireAfterSeconds=3600*20
        :return:
        """
        self.col.create_index(keys, session=session, **kwargs)

    def drop_index(self, index_or_name, session=None, **kwargs):
        self.col.drop_index(index_or_name, session=session, **kwargs)

    def index_info(self):
        return self.col.index_information()

    def view_index(self):
        indexs_list = self.col.list_indexes()
        for index in indexs_list:
            print(index)

    def drop_col(self):
        self.col.drop()

    def batch_write(self, write_list_dict):
        to_be_request = [InsertOne(b) for b in write_list_dict]
        self.col.bulk_write(to_be_request, ordered=False)

    def batch_update(self, write_list_dict):
        to_be_request = [UpdateOne({'_id': b['_id']}, {'$set': b}, upsert=True) for b in write_list_dict]
        self.col.bulk_write(to_be_request, ordered=False)

    def insert_one(self, write_dict):
        """
        :param write_dict:
        :return: 返回col中这条数据对象
        """
        try:
            return self.col.insert_one(write_dict)
        except Exception as e:
            raise e
            pass

    def insert(self, write_dict):
        """
        :param write_dict:
        :return: 返回col中这条数据的id
        """
        try:
            return self.col.insert(write_dict)
        except:
            pass

    def find_one(self, *args, **kwargs):
        res = self.col.find_one(*args, **kwargs)
        return res

    def find(self, *args, **kwargs):
        yield_res = self.col.find(*args, **kwargs).limit(self.limit)
        res_list = [_ for _ in yield_res]
        return res_list

    def find_all_by_iter(self, *args, limit=None, **kwargs):
        if limit:
            yield_res = self.col.find(*args, **kwargs).limit(self.limit)
        else:
            yield_res = self.col.find(*args, **kwargs)
        return yield_res

    def close(self):
        self.client.close()


# 设置过期时间示例
def message_more_cache_expire_index():
    mg_op = MongoOp('xiaomu', 'message_more_cache', label='mongo')
    mg_op.drop_index('time_-1')
    mg_op.create_index([("time", pymongo.DESCENDING)], expireAfterSeconds=3600 * 18)


if __name__ == '__main__':
    mg_op = MongoOp('xiaomu', 'knowledge_map', label='mongo')
    # mg_op.create_index([("other_name", pymongo.ASCENDING), ("stock_code", pymongo.ASCENDING)])
    r = mg_op.show_col()
    print(r)
    mg_op.view_index()
    r = mg_op.find_one({'type': 'teacher'})
    print(r)
    r = mg_op.find({'type': 'teacher'})
    print(r)
    mg_op.close()
