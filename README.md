# xwtools

xwtools目前包含的功能主要有：

1）配置解析和管理

2）日志打印和收集

3）mysql、es、redis、rabbit基类

4）唯一主键生成

5）多线程和多进程操作封装

6）实用函数

## 安装方式

    pip install xwtools
    
当前版本是0.1.12

## 使用指南

使用
    
    import xwtools as gt
   
导入模块。xwtools实现上是把库里的文件都导入到__init__.py，所以不需要指定xwtools下的文件。
另外，最好使用gt别名，避免和其他库使用上冲突。

### 配置解析和管理
约定：项目根目录放置配置文件settings.ini，按模块label分块配置各服务模块，格式类似：

    [rabbit]
    host = 52.82.48.248
    port = 5672
    user = admin
    password = sense_mq@2018
    

通用配置（如log_path）放到[settings]下，[settings]建议放到配置文件最后。

程序内通过gt.config('label','item')调用,要确保item的key是存在的，否则解析配置会抛出异常。

如果不确定item是否存在可以使用gt.config('label','item','')，不存在的item会赋默认的空值。

对于非docker部署模式，根目录可以放settings.local.ini用于本地开发使用，该文件不要提到git里。


### [日志打印和收集](./docs/log.md)

### [Mysql封装](./docs/sqlalchemy.md)

### [ES封装](./docs/es.md) / [ES镜像](./docs/es_opendistro.md)

### [RabbitMQ封装](./docs/rabbit.md)

### Reids 封装

只是简单的提供了一个 get_redis_client

### 其他实用方法

#### 装饰器 try_catch_exception
脚本的最顶层调用最好都加上这个装饰器，把抛出的异常都捕捉到

#### 装饰器 catch_raise_exception

底层代码调用，打出log并把日志抛到外部

另外，django项目可以依赖sense-django,其提供了 @catch_view_exception ,在每个view方法上面装饰下，
不需要自己catch异常返回错误的json的信息，简化代码。

#### 多线程和多进程封装

示例代码：

    def handle_process_work(job):
        gt.log_info("job={0}".format(job))
        time.sleep(0.1)
    
    def test_multi_process():
        jobs = [i for i in range(100)]
        gt.execute_multi_core("dumb", handle_process_work, jobs, 4, True)

execute_multi_core的最后一个参数表示使用多线程还是多进程，如果work num是1就主进程顺序执行了。

#### 其他

可参考utils.py源码




