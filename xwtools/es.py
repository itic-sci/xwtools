from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk
from elasticsearch import NotFoundError, Elasticsearch
from elasticsearch_dsl import Document, Integer, Keyword
from .config_log import config
from .config_log import log
import json
from .utils import drop_element_contain_in_str_list
from .etl_time import make_timestamp
from elasticsearch_dsl.query import Q, Match

__es_clients = {}
__es_entitys = {}


def create_connection(label='es'):
    try:
        host = config(label, 'host')
        user = config(label, 'user', '')
        password = config(label, 'pass', '')
        port = config(label, 'port', '')
        timeout = int(config(label, 'timeout', 10))
        if user and password:
            return connections.create_connection(alias=label, hosts=[host], port=port, http_auth=(user, password),
                                                 timeout=timeout)
        return connections.create_connection(alias=label, hosts=[host], port=port, timeout=timeout)
    except Exception as e:
        log.exception(e)


def create_es_entity(label='es'):
    try:
        host = config(label, 'host')
        user = config(label, 'user', '')
        port = config(label, 'port', 9200)
        password = config(label, 'pass', '')
        timeout = int(config(label, 'timeout', 10))
        if user and password:
            return Elasticsearch(hosts=[host], http_auth=(user, password), port=port, timeout=timeout)
        return Elasticsearch(hosts=[host], timeout=timeout)
    except Exception as e:
        log.exception(e)
        return None


def get_es_entity(label='es'):
    global __es_entitys
    if label not in __es_entitys or __es_entitys[label] is None:
        __es_entitys[label] = create_es_entity(label)
    return __es_entitys[label]


def get_es_client(label='es'):
    global __es_clients
    if label not in __es_clients:
        __es_clients[label] = create_connection(label)
    return __es_clients[label]


class BaseSearcher(object):

    def __init__(self, label, doc_model=None):
        """
        :param doc_model:
        class Entity(Document):
            id = Keyword()
            name = Text(index=False)
            type = Text(analyzer='ik_max_word')

            class Meta:
                doc_type = "main"

            class Index:
                name = 'entity'
                using = 'es'
        """
        self.conn = get_es_client(label)
        self.doc_model = doc_model

    def search_by_id(self, id):
        if self.doc_model is None:
            raise Exception('no doc model found')
        try:
            return self.doc_model.get(id)
        except NotFoundError as ex:
            return None
        except Exception as e:
            raise e

    def search_by_multi_bool(self, must=None, must_not=None, should=None, filter=None, source_include=None,
                             sort_field=None, offset=0, limit=1):
        """
        :param must: [Term(name='xw')]
        :param must_not: [Term(name='xw')]
        :param should: [Term(name='xw')]
        :param filter: [Term(name='xw')]
        :param source_include: ['field_1', 'field_1'] 查出的结果包含哪些字段
        :param sort_field: str 按照哪个字段升序排序('year')、降序('-year')
        :param offset: int 查询的第几页数据
        :param limit: int 每页数量
        """

        search = self.doc_model.search()
        kwargs = dict()
        if should:
            kwargs['should'] = should
            kwargs['minimum_should_match'] = 1
        if must: kwargs['must'] = must
        if must_not: kwargs['must_not'] = must_not
        if filter: kwargs['filter'] = filter
        if kwargs:
            if source_include:
                search = search.query(Q('bool', **kwargs)).source(source_include)
            else:
                search = search.query(Q('bool', **kwargs))
        else:
            if source_include:
                search = search.query().source(source_include)
            else:
                search = search.query()
        if sort_field:
            search = search.sort(sort_field)
        _ = search[offset: offset + limit].execute()
        total = _.hits.total
        query_list = _.to_dict()['hits']['hits']
        return total, query_list


class BaseIndexer(object):

    def __init__(self, label, doc_model, keep_index_num=2):
        self.conn = get_es_client(label)
        self.doc_model = doc_model
        self.bulk_list = []
        self.old_index_list = []
        self.bulk_num = None
        self.alias_index = None
        self.new_index = None
        self.keep_index_num = keep_index_num
        self.index_name = self.doc_model._index._name  # hard trick
        if keep_index_num >= 10:
            self.keep_index_num = 10
        elif keep_index_num <= 1:
            self.keep_index_num = 1

    def init_rebuild_index(self, bulk_num=100):
        self.bulk_num = bulk_num
        self.old_index_list = self._get_all_exist_index()
        self.alias_index = self._get_alias_index()
        self.new_index = self._get_new_index_name()
        self.doc_model.init(index=self.new_index)
        self.delete_expired_indexes()
        log.info(
            "init_rebuild_index new_index={0} alias_index={1} old_index_list={2}".format(self.new_index,
                                                                                         self.alias_index,
                                                                                         self.old_index_list))

    def add_bulk_data(self, id, data):
        self._filter_data(id, data)
        self.bulk_list.append(data)
        self.flush_bulk(False)

    def _filter_data(self, id, data):
        data['_index'] = self.new_index
        if '_type' not in data:
            data['_type'] = 'doc'
        data['_id'] = id
        return data

    def save_data(self, id, data):
        self._filter_data(id, data)
        bulk(self.conn, [data], index=self.index_name, doc_type=self.doc_model)

    def flush_bulk(self, must=False):
        if len(self.bulk_list) == 0:
            return
        if len(self.bulk_list) >= self.bulk_num or must:
            bulk(self.conn, self.bulk_list, index=self.new_index, doc_type=self.doc_model)
            self.bulk_list = []

    def done_rebuild_index(self):
        self.flush_bulk(True)
        self._update_alias()
        log.info('done_rebuild_index for ' + self.index_name)

    def _get_new_index_name(self):
        index_name = self.index_name
        old_index = ''
        if len(self.old_index_list) > 0:
            old_index = self.old_index_list[0]
        if old_index == '' or index_name == old_index:
            return index_name + '_v1'
        version = self._get_index_version(old_index)
        if version is None:
            log.raise_exception('bad old_index name ' + old_index)
            return
        times = 5
        while times >= 0:
            times -= 1
            version += 1
            if version > 10:
                version = 1
            name = index_name + '_v' + str(version)
            if name in self.old_index_list:
                continue
            return name
        log.raise_exception("cannot found valid new index for {}".format(self.old_index_list))

    def _get_index_version(self, index_name):
        pos = index_name.rfind('_v')
        if pos < 0 or pos >= len(index_name) - 2:
            return None
        version = int(index_name[pos + 2:len(index_name)])
        return version

    def _update_alias(self):
        if self.new_index != self.index_name:
            self.conn.indices.put_alias(index=self.new_index, name=self.index_name)
            log.info('put_alias ' + self.new_index + ' for ' + self.index_name)
        if self.alias_index == '':
            log.info('no old_index need to update')
            return
        self.conn.indices.delete_alias(index=self.alias_index, name=self.index_name, ignore=[400, 404])
        log.info('delete_alias ' + self.alias_index + ' for ' + self.index_name)

    def _get_all_exist_index(self):
        result = list()
        indices = self.conn.indices.get('*')
        for index in indices:
            if index.find(self.index_name) == 0:
                result.append(index)
        result.sort(key=self._sort_index)
        return result

    def _sort_index(self, index_name):
        version = self._get_index_version(index_name)
        if not version:
            return 0
        return -version

    def delete_expired_indexes(self):
        if len(self.old_index_list) <= self.keep_index_num or len(self.alias_index) == 0:
            log.info("no need delete_expired_indexes")
            return
        pos = self.old_index_list.index(self.alias_index)
        pos1 = pos - 1
        if pos1 < 0:
            pos1 = len(self.old_index_list) - 1
        if pos == pos1:
            log.error("found bad pos for delete_expired_indexes")
            return
        self._delete_index(self.old_index_list[pos1])

    def _delete_index(self, index_name):
        self.conn.indices.delete(index_name)
        log.info("_delete_index " + index_name)

    def _get_alias_index(self):
        if len(self.old_index_list) == 0:
            return ''
        if self.index_name in self.old_index_list:
            return self.index_name
        try:
            aliases = self.conn.indices.get_alias(",".join(self.old_index_list), self.index_name)
            if len(aliases) == 0:
                return ''
            if len(aliases) >= 2:
                log.raise_exception("_get_alias_index found dup alias")
            keys = list(aliases.keys())
            return keys[0]
        except NotFoundError as ex:
            pass
        return ''


class DocumentBase(Document):
    id = Keyword()
    create_time = Integer()
    mark_time = Integer()

    @classmethod
    def del_by_id(cls, id, is_refresh=False):
        # 根据id删除对象,如需立即刷新可见性is_refresh=True,默认False
        es_entity = get_es_entity(cls.Index.label)
        es_entity.delete(index=cls.Index.name, doc_type='doc', id=id, ignore=[400, 404])
        if is_refresh:
            es_entity.indices.refresh(index=cls.Index.name)

    @classmethod
    def del_by_body(cls, body):
        es = get_es_entity(cls.Index.label)
        allDoc = es.delete_by_query(index=cls.Index.name, doc_type='doc', body=body)
        return allDoc

    @classmethod
    def get_obj_by_id(cls, id):
        # 根据id获取对象
        con = get_es_client(cls.Index.label)
        _entity = cls.get(id=id, using=cls.Index.label, ignore=[400, 404])
        return _entity

    @classmethod
    def get_objs_by_time(cls, start, end, limit=1000, format='%Y-%m-%d %H:%M:%S'):
        # 根据时间段获取数据
        con = get_es_client(cls.Index.label)
        _s, _e = make_timestamp(start, format), make_timestamp(end, format)
        search = cls.search(using=cls.Index.label)
        search = search.filter('range', create_time={'gte': _s, 'lt': _e})
        _ = search[: limit].execute()
        _res = _.to_dict()['hits']['hits']
        return _res

    @classmethod
    def get_news_by_body(cls, body):
        con = get_es_entity(cls.Index.label)
        allDoc = con.search(index=cls.Index.name, doc_type='doc', body=body)
        res_data = allDoc['hits']['hits']
        return res_data

    class Index:
        name = ''
        label = ''


def _get_ner_key_type_str(kw, key, query_list, case_sensitive, entity_in_kw):
    name_list = []
    for x in query_list:
        if not case_sensitive:
            if entity_in_kw:
                if x.get(key) and x.get(key).lower() in kw.lower(): name_list.append(x[key])
            else:
                if x.get(key) and kw.lower() in x.get(key).lower(): name_list.append(x[key])
        else:
            if entity_in_kw:
                if x.get(key) and x.get(key) in kw: name_list.append(x[key])
            else:
                if x.get(key) and kw in x.get(key): name_list.append(x[key])
    return name_list


def _get_ner_key_type_list(kw, key, query_list, case_sensitive, entity_in_kw):
    name_list = []
    for item in query_list:
        for x in json.loads(item[key]):
            if not case_sensitive:
                if entity_in_kw:
                    if x.lower() in kw.lower(): name_list.append(x)
                else:
                    if kw.lower() in x.lower(): name_list.append(x)
            else:
                if entity_in_kw:
                    if x in kw: name_list.append(x)
                else:
                    if kw in x: name_list.append(x)
    return name_list


def getNerByEs(
        kw, es_model, limit=40, key='name',
        key_type="str", reverse=True, case_sensitive=False,
        entity_in_kw=True
):
    """
    :param kw: 输入的文本内容
    :param es_model: 自定义的es model类
    :param limit: 从es中查出前多少条数据进行分析
    :param key: es document中的字段名称
    :param key_type: es document中的字段存的是单个字符串还是字符串列表（json.dumps后的列表）
    :param reverse: 识别出的实体是否按实体长度从大到小排序
    :param case_sensitive: 大小写是否敏感，默认不区分大小写
    :return:
    """
    es_search = BaseSearcher(es_model.Index.using, doc_model=es_model)
    total, query_list = es_search.search_by_multi_bool(should=[Match(**{key: kw})], limit=limit)
    query_list = [r['_source'] for r in query_list]

    if key_type == 'list':
        name_list = _get_ner_key_type_list(kw, key, query_list, case_sensitive, entity_in_kw)
    else:
        name_list = _get_ner_key_type_str(kw, key, query_list, case_sensitive, entity_in_kw)

    if not name_list: return {'origin_ner': name_list, 'merge_ner': name_list}

    merge_ner = drop_element_contain_in_str_list(name_list, reverse=reverse, case_sensitive=case_sensitive)
    return {'origin_ner': name_list, 'merge_ner': merge_ner}
