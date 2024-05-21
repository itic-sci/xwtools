#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved
File: .py
Author: xuwei
Email: weix@sensedeal.ai
Last modified: date
Description:
'''

from .config_log import config
import json
import redis
import time

__client_map = {}


def get_redis_client(label='redis', db=None, config_map=None, strict=True):
    global __client_map
    label1 = label + str(db)
    if label1 not in __client_map:
        if config_map:
            redis_host = config_map['host']
            redis_port = int(config_map['port'])
            redis_pass = config_map['pass'] if config_map.get('pass') else config_map['password']
        else:
            redis_host = config(label, 'host')
            redis_port = int(config(label, 'port'))
            redis_pass = config(label, 'pass', '')
            if redis_pass == '':
                redis_pass = config(label, 'password', '')
        if len(redis_pass) == 0:
            redis_pass = None
        if strict:
            redis_pool = redis.ConnectionPool(host=redis_host, port=redis_port, password=redis_pass, db=db,
                                              decode_responses=True)
        else:
            redis_pool = redis.ConnectionPool(host=redis_host, port=redis_port, password=redis_pass, db=db)
        __client_map[label1] = redis_pool

    clients = redis.StrictRedis(connection_pool=__client_map[label1], decode_responses=True)
    return clients


class RedisOp(object):
    def __init__(self, label='redis', db=None, strict=True):
        self.label = label
        self.db = db
        self.strict = strict

    def redis_hkeys(self, name):
        get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
        keys = get_con.hkeys(name)
        return keys

    def get(self, name):
        get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
        keys = get_con.get(name)
        return keys

    def set(self, name, value):
        get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
        keys = get_con.set(name, value)
        return keys

    def delete(self, *names):
        get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
        keys = get_con.delete(*names)
        return keys

    def redis_hget(self, name, key):
        get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
        val = get_con.hget(name, key)
        return val

    def redis_hgetall(self, name):
        get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
        res_dict = get_con.hgetall(name)
        return res_dict

    # 列表字典
    def redis_one_dict_hset(self, name, one_dict, key):
        _num = 0
        while _num < 4:
            try:
                get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
                get_con.hset(name, one_dict[key], json.dumps(one_dict, ensure_ascii=False))
                return
            except:
                _num += 1
                time.sleep(1)

    # 列表字典
    def other_name_to_redis(self, name, list_dict, key):
        try:
            get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
            write_pip = get_con.pipeline()
            for one_dict in list_dict:
                plate_value_dict = json.loads(one_dict['plate_value'])
                one_dict['plate'] = plate_value_dict['plate']
                one_dict['plate_value'] = plate_value_dict['plate_value']
                write_pip.hset(name, one_dict[key], json.dumps(one_dict, ensure_ascii=False))
            write_pip.execute()
        except Exception as e:
            print(e)

    def redis_hset(self, name, key, value):
        get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
        get_con.hset(name, key, value)

    # [(key1,value1),(key2,value2)]
    def redis_hset_tuple(self, name, list_tuple):
        get_con = get_redis_client(label=self.label, db=self.db, strict=self.strict)
        write_pip = get_con.pipeline()
        for one_tuple in list_tuple:
            write_pip.hset(name, one_tuple[0], one_tuple[1])
        write_pip.execute()

    def redis_lpush(self, name, list_data):
        get_redis_client(label=self.label, db=self.db, strict=self.strict).lpush(name, json.dumps(list_data))

    def redis_expire(self, name, time_pram):
        get_redis_client(label=self.label, db=self.db, strict=self.strict).expire(name, time_pram)

    def redis_llen(self, name):
        return get_redis_client(label=self.label, db=self.db, strict=self.strict).llen(name)

    def redis_lrange(self, name, start, end):
        return get_redis_client(label=self.label, db=self.db, strict=self.strict).lrange(name, start, end)

    def redis_hdel(self, name, *keys):
        return get_redis_client(label=self.label, db=self.db, strict=self.strict).hdel(name, *keys)


if __name__ == '__main__':
    xw_redis = RedisOp('redis', 1)
    keys = xw_redis.set('stock_company', 'test')
    val = xw_redis.get('stock_company')

    print(keys)
    print(val)
