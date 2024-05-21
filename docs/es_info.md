
## Python库之Elasticsearch

### 与关系型数据库的对比

|关系型数据库|Elasticsearch|
|---|---|
|Databases 库	|Indices 索引|
|Tables 表	|Types 类型|
|Rows 行（记录）	|Documents 文档|
|Columns 列（字段）	|Fields 域（字段）|

- 6.0 以后官方已经不赞同这种对比了，建议单索引单类型，所以也可以把索引看成关系型数据库的表，而每个集群就当成只支持一个库的数据库好了，或者用前缀来区分，但可能后期会很乱



### 测试群集是否启动
```python
from elasticsearch import Elasticsearch
es = Elasticsearch(['127.0.0.1:9200'], http_auth=('user', 'password'))
print(es.ping())
# 返回为布而值 True
```


### 获取当前群集基本信息
```
print(es.info())
# 返回，没啥用
{
    'cluster_name': 'es-rghboamm',
    'cluster_uuid': 'z9-IH2X3S566P_4H7c0nig',
    'name': '1571644769000123456',
    'tagline': 'You Know, for Search',
    'version': {
        'build_date': '2019-07-01T12:14:28.577924Z',
        'build_flavor': 'default',
        'build_hash': 'fe40335',
        'build_snapshot': False,
        'build_type': 'zip',
        'lucene_version': '7.4.0',
        'minimum_index_compatibility_version': '5.0.0',
        'minimum_wire_compatibility_version': '5.6.0',
        'number': '6.4.3'
    }
}
```

### 列出当前所有索引 Indices（库）
```
# 加上format参数，返回格式为json
print(es.cat.indices(format='json'))
# 返回
[{
    'health': 'green',
    'status': 'open',
    'index': '.monitoring-alerts-6',
    'uuid': 'k-N6e8fEQRCJGCsPXr94fg',
    'pri': '1',
    'rep': '1',
    'docs.count': '1',
    'docs.deleted': '0',
    'store.size': '12.5kb',
    'pri.store.size': '6.2kb'
}, {
    'health': 'green',
    'status': 'open',
    'index': '.monitoring-kibana-6-2019.10.22',
    'uuid': 'rSBw1CKDTR2uT00korJ4vQ',
    'pri': '1',
    'rep': '1',
    'docs.count': '3489',
    'docs.deleted': '0',
    'store.size': '1.8mb',
    'pri.store.size': '941.4kb'
},
……
]
```

### 获取索引 Indices（库）信息
```
# 获取索引全部信息，返回太长了，省略
print(es.indices.get(index='edc'))
# 获取索引mappings信息，返回太长了，省略
print(es.indices.get_mapping(index='edc'))
# 获取某索引某类型某字段信息
print(es.indices.get_field_mapping(index='edc', doc_type='user_info', fields=['sex', 'birthday']))
# 返回
{
    'edc': {
        'mappings': {
            'user_info': {
                'birthday': {
                    'full_name': 'birthday',
                    'mapping': {
                        'birthday': {
                            'type': 'date',
                            'store': True,
                            'format': 'yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis'
                        }
                    }
                },
                'sex': {
                    'full_name': 'sex',
                    'mapping': {
                        'sex': {
                            'type': 'text',
                            'index': False
                        }
                    }
                }
            }
        }
    }
}
```

### 统计所有索引 Indices（库）文档 Documents（记录）计数
```
# 统计所有索引文档计数
print(es.cat.count())
# 统计某一个索引文档计数
print(es.cat.count(index=""edc""))
# 统计某一个索引文档计数，返回格式为json
print(es.cat.count(index=""edc"", format='json'))
# 统计某一个索引文档计数，仅返回某一个值，返回格式为json
print(es.cat.count(index=""edc"", format='json', h=['count']))
# 统计某一个索引文档计数，仅返回某一个值
print(es.cat.count(index=""edc"", h=['count']))
# 返回
1571719854 12:50:54 118921
1571719854 12:50:54 0
[{'epoch': '1571719854', 'timestamp': '12:50:54', 'count': '0'}]
[{'count': '0'}]
0
```

### 检查某个索引 Indices（库）是否存在
```
print(es.indices.exists(index=""edc""))
# 返回为布而值 True
```

### 自动创建一个索引 Indices（库）及类型 Types（表）
```
# ignore=400，跳过服务器400错误
print(es.indices.create(index='my-index', ignore=400))
# 返回
{
    'acknowledged': True,
    'shards_acknowledged': True,
    'index': 'my-index'
}
```

### ★★手动创建一个索引 Indices（库）及类型 Types（表）

- 字段属性中 ==type== 为字段类型，常用字段类型：

|类型	|说明	|备注|
|---|---|---|
|text	|文本	|分词并可检索，文本字段不用于排序，很少用于聚合，主要用于正文|
|keyword	|关键词	|索引结构化内容（如电子邮件地址、主机名、状态码、邮政编码或标记）的字段|
|integer	|整数	|
|float	|浮点	|分词并可检索|
|date	|日期	|分词并可检索|
更多字段类型见：https://www.elastic.co/guide/en/elasticsearch/reference/6.4/mapping-types.html

- 字段属性中还有一些属性，用于配合字段类型

|参数	|默认值	|说明|
|---|---|---|
|index	|true	|该字段是否可以搜索|
|store	|false	|字段值是否应与源字段分开存储和检索，具体见：注1|
|fielddata	|false	|针对text字段加快排序和聚合（doc_values对text无效）建议不开启，耗内存|
|copy_to|	-	|可以将多个字段的值复制到一个字段中，具体见：注2|
|doc_values	|true	|字段是否应该以列跨距方式存储在磁盘上，具体见：注3|
|format	|-	|配合字段类型为date的字段，预先配置格式来识别值中的日期字符串|
|ignore_malformed	|false	|如果为true，则忽略格式错误，反之，则引发异常并拒绝整个文档|

- 注1：如为true且类型为text，则即可分词也可检索，可用于文章标题这种即需要搜索又需要查询的字段

- 注2：可用于组合查询

- 注3：类型为keyword，且只是一些无意义的字符，以后不用排序、聚合的话，可设为false节省空间

- 更多属性见：https://www.elastic.co/guide/en/elasticsearch/reference/6.4/mapping-params.html

```
_mappings = {
    ""mappings"": {
        ""user_info"": {  # 数据库中的表
            ""properties"": {  # 定义表结构属性
                ""user_id"": {  # 数据库中的字段
                    ""type"": ""keyword""
                },
                ""user_name"": {
                    ""type"": ""keyword""
                },
                ""sex"": {
                    ""type"": ""keyword"",
                    ""index"": ""false""  # 该字段是否可以搜索，true（默认）和false
                },
                ""age"": {
                    ""type"": ""integer"",
                    ""ignore_malformed"": ""true""  # 如果为true，则忽略格式错误，如果为false（默认），则引发异常并拒绝整个文档
                },
                ""birthday"": {
                    ""type"": ""date"",
                    ""store"": 'true',  # 字段值是否应与源字段分开存储和检索。接受true或false（默认）
                    ""format"": ""yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis""
                }
            }
        }
    }
}
print(es.indices.create(index=""edc"", body=_mappings, ignore=400))
# 成功返回
{'acknowledged': True, 'shards_acknowledged': True, 'index': 'edc'}
# 失败返回
{
    'error': {
        'root_cause': [{
            'type': 'resource_already_exists_exception',
            'reason': 'index [edc/IeegOo2jTPCNVoL6_trjNA] already exists',
            'index_uuid': 'IeegOo2jTPCNVoL6_trjNA',
            'index': 'edc'
        }],
        'type': 'resource_already_exists_exception',
        'reason': 'index [edc/IeegOo2jTPCNVoL6_trjNA] already exists',
        'index_uuid': 'IeegOo2jTPCNVoL6_trjNA',
        'index': 'edc'
    },
    'status': 400
}
```

### 更新一个索引 Indices（库）的 mapping
```
_mappings = {
    ""user_info"": {  # 数据库中的表
        ""properties"": {  # 定义表结构属性
            ""add_time"": {
                ""type"": ""date"",
                ""store"": 'true',  # 字段值是否应与源字段分开存储和检索。接受true或false（默认）
                ""format"": ""yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis""
            }
        }
    }
}
print(es.indices.put_mapping(index=""edc"", doc_type='user_info', body=_mappings, ignore=400))
# 成功返回
{'acknowledged': True}
```

### 刷新索引 Indices（库）
```
print(es.indices.refresh(index='edc'))
# 返回
{'_shards': {'total': 10, 'successful': 10, 'failed': 0}}
```

### 删除索引 Indices（库）

```
print(es.indices.delete(index='my-index', ignore=404))
# 成功返回
{'acknowledged': True}
# 失败返回
{
    'error': {
        'root_cause': [{
            'type': 'index_not_found_exception',
            'reason': 'no such index',
            'index_uuid': '_na_',
            'resource.type': 'index_or_alias',
            'resource.id': 'test',
            'index': 'test'
        }],
        'type': 'index_not_found_exception',
        'reason': 'no such index',
        'index_uuid': '_na_',
        'resource.type': 'index_or_alias',
        'resource.id': 'test',
        'index': 'test'
    },
    'status': 404
}
```

### 检查某个类型 Types（表）是否存在

```
# index为指定的索引，也可以index='_all'为从所有索引中查询
print(es.indices.exists_type(index='edc', doc_type='user_info'))
# 返回为布而值
True
```


### 添加文档
```
new_id = md5_id_16()
_body = {
    'user_id': new_id,
    'user_name': 'Jam',
    'sex': '男',
    'age': 20,
    'birthday': '1977-11-25',
    'add_time': time.time()
}
# refresh=True, 刷新受影响的碎片以使此操作对搜索可见
# id 参数为可选，如果需要指定ID则加上
print(es.index(index=""edc"", doc_type=""user_info"", id=new_id, body=_body, refresh=True))
# 返回，注意'result': 'created'
{
    '_index': 'edc',
    '_type': 'user_info',
    '_id': 'PEKl8m0Bz-_cj1xDN_iz',
    '_version': 1,
    'result': 'created',
    'forced_refresh': True,
    '_shards': {
        'total': 2,
        'successful': 2,
        'failed': 0
    },
    '_seq_no': 0,
    '_primary_term': 1
}
```

### 按ID更新文档
```
# 文档结构必须全，不然只保留更新的内容
_body = {
    'user_id': new_id,
    'user_name': 'Tom',
    'sex': '男',
    'age': 43,
    'birthday': '1977-11-25',
    'add_time': time.time()
}
print(es.index(index=""edc"", doc_type=""user_info"", id='PEKl8m0Bz-_cj1xDN_iz', body=_body, refresh=True))
# 返回，注意'result': 'updated'
{
    '_index': 'edc',
    '_type': 'user_info',
    '_id': 'PEKl8m0Bz-_cj1xDN_iz',
    '_version': 2,
    'result': 'updated',
    'forced_refresh': True,
    '_shards': {
        'total': 2,
        'successful': 2,
        'failed': 0
    },
    '_seq_no': 1,
    '_primary_term': 1
}
```

### 按ID查询文档
```
print(es.get(index=""edc"", doc_type=""user_info"", id='d71c9b9b2a1dfd41'))
# 返回
{
    '_index': 'edc',
    '_type': 'user_info',
    '_id': 'd71c9b9b2a1dfd41',
    '_version': 4,
    'found': True,
    '_source': {
        'user_id': '9f9c27322eb33fb5',
        'user_name': 'Jam',
        'sex': '男',
        'age': 20,
        'birthday': '1977-11-25',
        'add_time': '2019-10-1'
    }
}
```

### 查询所有文档
```
# 6.0 以后已经没有doc_type参数指定类型查询了
print(es.search(index=""edc""))
# 返回
{
    'took': 4,
    'timed_out': False,
    '_shards': {
        'total': 5,
        'successful': 5,
        'skipped': 0,
        'failed': 0
    },
    'hits': {
        'total': 7,
        'max_score': 1.0,
        'hits': [{
            '_index': 'edc',
            '_type': 'user_info',
            '_id': 'd71c9b9b2a1dfd41',
            '_score': 1.0,
            '_source': {
                'user_id': 'd71c9b9b2a1dfd41',
                'user_name': 'Jam',
                'sex': '男',
                'age': 20,
                'birthday': '1977-11-25',
                'add_time': 1571734285.7937682
            }
        }, {
            '_index': 'edc',
            '_type': 'user_info',
            '_id': 'PEKl8m0Bz-_cj1xDN_iz',  # 这是不指定ID添加的一条文档,系统自动生成的ID
            '_score': 1.0,
            '_source': {
                'user_id': 'b3dd98cacd461060', 
                'user_name': 'Tom',
                'sex': '男',
                'age': 43,
                'birthday': '1977-11-25',
                'add_time': 1571734460.6932411
            }
        },……
        ]
    }
}
```

### 条件查询
```
_query = {
    'query': {
        'match_phrase': {   # 完全匹配搜索关键词，模糊匹配用match
            'user_co': '耀华'   # 搜索的字段和关键词
        }
    },
    ""size"": 10,     # 每页返回结果数量
    ""from"": 0,      # 偏移量，如果用下面深分页，这里必须为0
    ""search_after"": [   # 深分页，排序字段当前页最后一条的数据
        '23PN4WWEXD'
    ],
    ""sort"": [   # 排序字段和方式
        {""_id"": {""order"": ""desc""}}
    ],
    ""highlight"": {  # 结果高亮
        ""require_field_match"": False,   # 多字段高亮
        ""fields"": { # 需要高亮的字段
            ""body"": {},
            ""title"": {}
        }
    }
}
print(es.search(index=""hpjy_web"", body=_query))
# 返回
{
    '_shards': {
        'failed': 0,
        'skipped': 0,
        'successful': 5,
        'total': 5
    },
    'hits': {
        'hits': [{
            '_id': 'zAZM9m0B_4DPsZqSiAIJ',
            '_ignored': ['ip'],
            '_index': 'hpjy_web',
            '_score': None,
            '_source': {
                'body': '太长，省略',
                'ck_num': 57,
                'id': '',
                'ip': '',
                'post_time': '2019-03-25 21:58:56',
                'title': '水润教育展风采 中美交流建友谊——耀华小学接待美国基础教育专家来校访问',
                'true_name': '耀华小学管理员',
                'user_co': '耀华小学管理员',
                'user_id': '',
                'user_name': 'yhxx'
            },
            '_type': 'hpjy_web',
            'highlight': {
                'body': ['<p>3月25日早上九点半，<em>耀</em><em>华</em>小学迎来了美国洛杉矶吉诺港学区基础教育校长一行参观访问，校长张喜英携师生代表热情接待。',
                    '</p>\n'
                    '<p>学访活动开始了，张喜英校长首先对<em>耀</em><em>华</em>小学水润教育的办学特色进行了介绍。随后，小主持人用流利的英语向学访团介绍了学校丰富多彩的素拓课程。',
                    '为了让专家们深入了解<em>耀</em><em>华</em>小学的课程特色，学生们先后进行了合唱、啦啦操队展示及古筝演奏和钢琴表演，国画、书法、创意缠绕展示也得到美国同行的频频称赞。',
                    '</p>\n'
                    '<p>这次学访活动不仅展示了<em>耀</em><em>华</em>小学的水润教育特色，对中美的文化交流也具有积极的意义。'
                ],
                'title': ['水润教育展风采 '
                    '中美交流建友谊——<em>耀</em><em>华</em>小学接待美国基础教育专家来校访问'
                ]
            },
            'sort': ['zAZM9m0B_4DPsZqSiAIJ']
        }, {
            '_id': 'ywZO9m0B_4DPsZqSyxM5',
            '_ignored': ['ip'],
            '_index': 'hpjy_web',
            '_score': None,
            '_source': {
                'body': '太长，省略',
                'ck_num': 302,
                'id': '',
                'ip': '',
                'post_time': '2018-08-20 15:09:59',
                'title': '局领导视察耀华小学开学准备工作',
                'true_name': '耀华小学',
                'user_co': '耀华小学',
                'user_id': '219RTZMVMM',
                'user_name': 'x20yhxx'
            },
            '_type': 'hpjy_web',
            'highlight': {
                'body': ['<p> '
                    '8月20日上午，政协副主席、和平区教育局张素华局长和陈志红副局长来到<em>耀</em><em>华</em>小学众诚里校区视察开学准备工作。',
                    '两位领导对<em>耀</em><em>华</em>小学暑期工作给予了肯定，同时也针对今后的工作提出了要求。<em>耀</em><em>华</em>小学将以此次视察为契机，继续积极做好新学期的各项工作准备，保证新学期开学平安、有序。'
                ],
                'title': ['局领导视察<em>耀</em><em>华</em>小学开学准备工作']
            },
            'sort': ['ywZO9m0B_4DPsZqSyxM5']
        }, {
            '_id': 'ywZM9m0B_4DPsZqShwLy',
            '_ignored': ['ip'],
            '_index': 'hpjy_web',
            '_score': None,
            '_source': {
                'body': '太长，省略',
                'ck_num': 14,
                'id': '',
                'ip': '',
                'post_time': '2019-03-25 22:12:31',
                'title': '耀华小学学生在和平区中小学生读书系列活动中取得佳绩 ',
                'true_name': '耀华小学管理员',
                'user_co': '耀华小学管理员',
                'user_id': '',
                'user_name': 'yhxx'
            },
            '_type': 'hpjy_web',
            'highlight': {
                'title': ['<em>耀</em><em>华</em>小学学生在和平区中小学生读书系列活动中取得佳绩']
            },
            'sort': ['ywZM9m0B_4DPsZqShwLy']
        }, {
            '_id': 'ywZM9m0B_4DPsZqSRABh',
            '_ignored': ['ip'],
            '_index': 'hpjy_web',
            '_score': None,
            '_source': {
                'body': '太长，省略',
                'ck_num': 2,
                'id': '',
                'ip': '',
                'post_time': '2019-04-15 16:50:14',
                'title': '国家安全  '
                '关系你我——耀华小学开展师生“4·15”全民国家安全教育日活动',
                'true_name': '耀华小学管理员',
                'user_co': '耀华小学管理员',
                'user_id': '',
                'user_name': 'yhxx'
            },
            '_type': 'hpjy_web',
            'highlight': {
                'body': ['为深入学习贯彻新时代中国特色社会主义思想、坚持总体国家安全观、着力防范化解重大风险、喜迎中华人民共和国成立70周年，根据上级文件要求，<em>耀</em><em>华</em>小学在师生中开展了“4·15”全民国家安全教育日活动。'],
                'title': ['国家安全  '
                    '关系你我——<em>耀</em><em>华</em>小学开展师生“4·15”全民国家安全教育日活动'
                ]
            },
            'sort': ['ywZM9m0B_4DPsZqSRABh']
        }, {
            '_id': 'ygZN9m0B_4DPsZqSEgZM',
            '_ignored': ['ip'],
            '_index': 'hpjy_web',
            '_score': None,
            '_source': {
                'body': '太长，省略',
                'ck_num': 22,
                'id': '',
                'ip': '',
                'post_time': '2019-01-22 19:04:49',
                'title': '游出精彩 泳往直前——耀华小学“润之队”游泳队集训成果展示',
                'true_name': '耀华小学管理员',
                'user_co': '耀华小学管理员',
                'user_id': '',
                'user_name': 'yhxx'
            },
            '_type': 'hpjy_web',
            'highlight': {
                'body': ['<p><em>耀</em><em>华</em>小学“润之队”游泳队本学期的集训即将结束,1月22日下午,学校开展了题为“游出精彩 '
                    '泳往直前”集训成果展示并邀请家长进场观看。'
                ],
                'title': ['游出精彩 '
                    '泳往直前——<em>耀</em><em>华</em>小学“润之队”游泳队集训成果展示'
                ]
            },
            'sort': ['ygZN9m0B_4DPsZqSEgZM']
        },……],
        'max_score': None,
        'total': 1290
    },
    'timed_out': False,
    'took': 173
}
```
