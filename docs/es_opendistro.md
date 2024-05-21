### ES 镜像使用相关

sense_deploy 新增的目录 [opendistro](http://122.200.68.26:8060/qxdc/sense_deploy/tree/master/opendistro) 是基于 https://opendistro.github.io/for-elasticsearch/ 的新镜像。
ES官方镜像的xpack security需要付费购买，而opendistro的security是免费的，并且兼容ES，所以可以统一使用该镜像。


使用方法说明：

1、通常需要外挂目录/data/es/data、/data/es/logs、/data/es/config。

2、将 opendistro/config 目录复制到/data/es/。

3、根据需要修改config/jvm.options中的-Xms和-Xmx（内存大小，可以设置成一样的）

4、security使用的是http的basic认证，账号admin 密码 isenseforever2019 。可以修改config/elasticsearch.yml
注释掉 opendistro_security.disabled: true 后关闭认证（一搬用于初始导数据时）

5、分词使用的是ik，config/analysis-ik 是分词ik的配置信息，统一维护其扩展词库，不需要单独修改。

6、xwtools需要升级到 0.2.23及以上版本。settings.ini配置相应修改成类似：


    [es_default]
    host = http://52.82.101.119:9200/
    user = admin
    pass = isenseforever2019