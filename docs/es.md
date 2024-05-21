### ES 封装

ES封装了两个类：BaseSearcher 和 BaseIndexer。

BaseSearcher 使用很简单，目前里面就包装了一个根据id查询的方法。其代码如下：

    class BaseSearcher(object):

    def __init__(self, label, doc_model=None):
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
            return e
            

继承 BaseSearcher 时需要指定label（settings文件里配置）和 doc_model类。label格式如：

    [es_default]
    host = http://52.82.58.138:9200/
    
如果使用的镜像是opendistro，则需要补充密码信息：

    [es_default]
    host = http://52.82.101.119:9200/
    user = admin
    pass = isenseforever2019

BaseIndexer 封装重做索引的过程，使用时最好看下BaseIndexer的源码，理解其工作原理。

BaseIndexer 的构造函数和 BaseSearcher 一样，需要传入label 和 doc_model类。

添加索引数据可以调用其 save_data(self, id, data) 方法，其中id是每条记录的唯一标识， 
data是doc_model 的dict结构参数。

重做索引的过程主要包括：

1）调用 init_rebuild_index 初始化新索引。其会根据doc_model中的index name 生成新的版本号索引名，
同时删除掉旧的多余的索引数据，版本号会自增，但最大是20，然后再从1开始。
2）调用 add_bulk_data 批量添加数据。
3）调用 done_rebuild_index 完成索引创建，其中包括flush数据，更新别名等操作。


重做索引代码示例参见test/test_es.py。


    